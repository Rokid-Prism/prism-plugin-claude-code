import { createHash, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import {
  getSessionMessages,
  listSessions,
  query,
  type ModelInfo,
  type Options,
  type PermissionResult,
  type PermissionUpdate,
  type Query,
  type SDKMessage,
  type SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";

/**
 * Logical prompt blocks handed over by the adapter. The runtime converts them
 * into Anthropic Messages API content: text stays text, images are inlined as
 * base64, and any other local file is referenced by path in a text line that
 * Claude Code can open with its own local tools.
 */
export type PromptBlock =
  | { type: "text"; text: string }
  | { type: "image"; path: string; mime: string; name: string }
  | { type: "file"; path: string; mime: string; name: string };

export type SdkUpdate =
  | { kind: "session_ready"; model?: string; permissionMode?: string; effort?: string | null }
  | { kind: "assistant_text"; messageId: string; text: string; provisional?: boolean }
  | { kind: "assistant_text_commit"; provisionalId: string; messageId: string; text: string }
  | { kind: "assistant_delta"; delta: string }
  | { kind: "thought_delta"; delta: string }
  | { kind: "tool_update"; callId: string; title: string; status: "running" | "completed" | "failed"; detail: string }
  | { kind: "external_user"; messageId: string; text: string }
  | { kind: "controls_update"; model?: string; permissionMode?: string; effort?: string | null };

export type SdkPermissionRequest = {
  sessionId: string;
  requestId: string;
  toolUseId: string;
  toolName: string;
  title: string;
  displayName: string;
  detail: string;
  suggestions: PermissionUpdate[];
};

export type SdkPermissionDecision =
  | { outcome: "allow"; remember: boolean }
  | { outcome: "deny"; message: string };

export type SdkSessionSummary = { sessionId: string; title: string; cwd: string; updatedAt: string };

export type SdkTranscriptToolUse = { callId: string; title: string; failed?: boolean };

export type SdkTranscriptEntry = {
  uuid: string;
  role: "user" | "assistant";
  text: string;
  toolUses: SdkTranscriptToolUse[];
  timestamp?: string;
};

export type SdkControlOption = { id: string; label: string };

export type SdkControlsSnapshot = {
  protocol: "claude-agent-sdk";
  models: SdkControlOption[];
  efforts: SdkControlOption[];
  permissionModes: SdkControlOption[];
  currentModel?: string;
  currentEffort?: string | null;
  currentPermissionMode?: string;
};

export type SdkRuntimeHandlers = {
  onUpdate(sessionId: string, update: SdkUpdate): void;
  onPermission(request: SdkPermissionRequest): Promise<SdkPermissionDecision>;
  onStderr(line: string): void;
};

/** Adapter-facing runtime contract. Tests replace this with a fake. */
export type SdkClient = {
  probe(): Promise<void>;
  newSession(cwd: string): Promise<{ sessionId: string }>;
  listSessions(): Promise<SdkSessionSummary[]>;
  readTranscript(sessionId: string): Promise<SdkTranscriptEntry[]>;
  prompt(sessionId: string, cwd: string, blocks: Array<Record<string, unknown>>): Promise<{ stopReason: string }>;
  cancel(sessionId: string): Promise<void>;
  setMode(sessionId: string, modeId: string): Promise<void>;
  setConfig(sessionId: string, configId: string, value: string | boolean): Promise<void>;
  controls(sessionId: string): SdkControlsSnapshot;
  closeSession(sessionId: string): Promise<void>;
  close(): Promise<void>;
};

const PERMISSION_MODES: SdkControlOption[] = [
  { id: "default", label: "Default" },
  { id: "acceptEdits", label: "Accept edits" },
  { id: "plan", label: "Plan" },
  { id: "dontAsk", label: "Don't ask" },
];
const FALLBACK_MODELS: SdkControlOption[] = [
  { id: "default", label: "Default" },
  { id: "opus", label: "Opus" },
  { id: "sonnet", label: "Sonnet" },
  { id: "haiku", label: "Haiku" },
];
const FALLBACK_EFFORTS: SdkControlOption[] = [
  { id: "low", label: "Low" },
  { id: "medium", label: "Medium" },
  { id: "high", label: "High" },
];
// A turn normally ends in seconds; keep the CLI process around briefly so a
// follow-up message reuses the warm session, then reclaim it to bound memory.
const IDLE_STREAM_MS = 5 * 60 * 1000;
const MAX_DETAIL_CHARS = 400;

function safeError(error: unknown): Error {
  const raw = error instanceof Error ? error.message : String(error);
  // SDK errors often contain a local command line. The hub error surface must
  // be useful without leaking a user path, token, or whole prompt.
  const message = raw
    .replace(/(?:sk-ant-|sk-|api[_-]?key=)[A-Za-z0-9_\-]{8,}/gi, "[redacted]")
    .replace(/\/Users\/[^\s:'\"]+/g, "[local path]")
    .replace(/\/home\/[^\s:'\"]+/g, "[local path]")
    .slice(0, 600);
  return new Error(message || "Claude Agent SDK failed");
}

function brief(value: unknown): string {
  try {
    const text = typeof value === "string" ? value : JSON.stringify(value);
    return (text ?? "").replace(/\s+/g, " ").slice(0, MAX_DETAIL_CHARS);
  } catch {
    return "";
  }
}

type ContentBlock = Record<string, unknown>;

function contentBlocks(message: unknown): ContentBlock[] {
  const param = message as { content?: unknown } | null;
  if (!param || typeof param !== "object") return [];
  if (typeof param.content === "string") return [{ type: "text", text: param.content }];
  return Array.isArray(param.content) ? param.content.filter((block) => block && typeof block === "object") : [];
}

function blockText(block: ContentBlock): string {
  if (typeof block.text === "string") return block.text;
  return "";
}

function resultText(block: ContentBlock): string {
  const content = block.content;
  if (typeof content === "string") return content;
  if (Array.isArray(content)) return content.map((item) => blockText(item as ContentBlock)).join("\n");
  return "";
}

async function promptContent(blocks: Array<Record<string, unknown>>): Promise<ContentBlock[]> {
  const content: ContentBlock[] = [];
  const fileLines: string[] = [];
  for (const raw of blocks) {
    const block = raw as Partial<PromptBlock>;
    if (block.type === "text" && typeof block.text === "string") {
      content.push({ type: "text", text: block.text });
    } else if (block.type === "image" && block.path) {
      try {
        const data = (await readFile(block.path)).toString("base64");
        content.push({ type: "image", source: { type: "base64", media_type: block.mime || "image/png", data } });
      } catch {
        fileLines.push(`Attached image "${block.name}" could not be read.`);
      }
    } else if (block.type === "file" && block.path) {
      fileLines.push(`Attached file: ${block.name} (${block.path})`);
    }
  }
  if (fileLines.length > 0) content.push({ type: "text", text: fileLines.join("\n") });
  return content;
}

class PushQueue<T> {
  private values: T[] = [];
  private waiting: ((r: IteratorResult<T>) => void) | null = null;
  private ended = false;
  push(value: T): void {
    if (this.ended) return;
    if (this.waiting) { const resolve = this.waiting; this.waiting = null; resolve({ value, done: false }); return; }
    this.values.push(value);
  }
  end(): void {
    if (this.ended) return;
    this.ended = true;
    if (this.waiting) { const resolve = this.waiting; this.waiting = null; resolve({ value: undefined as never, done: true }); }
  }
  get isEnded(): boolean { return this.ended; }
  private next(): Promise<IteratorResult<T>> {
    const value = this.values.shift();
    if (value !== undefined) return Promise.resolve({ value, done: false });
    if (this.ended) return Promise.resolve({ value: undefined as never, done: true });
    return new Promise((resolve) => { this.waiting = resolve; });
  }
  [Symbol.asyncIterator](): AsyncIterator<T> { return { next: () => this.next() }; }
}

type PendingTurn = {
  promptUuid: string;
  resolve: (result: { stopReason: string }) => void;
  reject: (error: Error) => void;
};

type SessionSlot = {
  cwd: string;
  /** Pre-generated session id that has no transcript on disk yet. */
  fresh: boolean;
  pendingModel?: string;
  pendingEffort?: string;
  pendingMode?: string;
};

type SessionStream = {
  sessionId: string;
  cwd: string;
  slot: SessionSlot;
  input: PushQueue<SDKUserMessage>;
  query: Query | null;
  starting: Promise<Query> | null;
  pending: PendingTurn | null;
  promptUuids: Set<string>;
  openTextBlocks: string[];
  permissionWaiters: Set<{ cancel?: () => void }>;
  closed: boolean;
  idleTimer: NodeJS.Timeout | null;
  lastActivity: number;
};

/**
 * Claude Agent SDK runtime: one streaming-input query() process per active
 * session, session enumeration and transcript reads through the SDK's local
 * store APIs (no subprocess), and permission requests brokered through
 * canUseTool.
 */
export class ClaudeSdkRuntime implements SdkClient {
  private readonly slots = new Map<string, SessionSlot>();
  private readonly streams = new Map<string, SessionStream>();
  private modelCache: ModelInfo[] | null = null;
  private modelCacheLoading: Promise<void> | null = null;
  private readonly controlState = new Map<string, { model?: string; effort?: string | null; permissionMode?: string }>();
  private closed = false;

  constructor(private readonly handlers: SdkRuntimeHandlers) {}

  async probe(): Promise<void> {
    if (this.closed) throw new Error("Claude Agent SDK runtime is closed");
    const explicit = process.env.CLAUDE_CODE_EXECUTABLE?.trim();
    if (explicit) {
      // An explicit override must exist; the bundled CLI is never searched for
      // in the user's shell PATH.
      const { accessSync, constants } = await import("node:fs");
      accessSync(explicit, constants.X_OK);
    }
    // Reads the local session store only; it spawns nothing and sends no prompt.
    await listSessions();
  }

  private baseOptions(): Pick<Options, "stderr"> & { pathToClaudeCodeExecutable?: string } {
    const options: Pick<Options, "stderr"> & { pathToClaudeCodeExecutable?: string } = {
      stderr: (data: string) => {
        for (const line of String(data).split("\n")) {
          const trimmed = line.trim();
          if (trimmed) this.handlers.onStderr(trimmed.slice(0, 400));
        }
      },
    };
    const explicit = process.env.CLAUDE_CODE_EXECUTABLE?.trim();
    if (explicit) options.pathToClaudeCodeExecutable = explicit;
    return options;
  }

  async newSession(cwd: string): Promise<{ sessionId: string }> {
    const sessionId = randomUUID();
    this.slots.set(sessionId, { cwd, fresh: true });
    return { sessionId };
  }

  async listSessions(): Promise<SdkSessionSummary[]> {
    const sessions = await listSessions({ limit: 500 });
    const summaries: SdkSessionSummary[] = [];
    for (const info of sessions) {
      const existing = this.slots.get(info.sessionId);
      if (existing) existing.fresh = false;
      summaries.push({
        sessionId: info.sessionId,
        title: info.customTitle || info.summary || info.firstPrompt || "Claude Code session",
        cwd: info.cwd ?? existing?.cwd ?? "",
        updatedAt: new Date(info.lastModified || Date.now()).toISOString(),
      });
    }
    return summaries;
  }

  async readTranscript(sessionId: string): Promise<SdkTranscriptEntry[]> {
    const messages = await getSessionMessages(sessionId, { limit: 2000 });
    if (this.slots.has(sessionId)) this.slots.get(sessionId)!.fresh = false;
    const toolStatus = new Map<string, boolean>();
    const entries: SdkTranscriptEntry[] = [];
    for (const row of messages) {
      if (row.type === "system") continue;
      const blocks = contentBlocks(row.message);
      if (row.type === "user") {
        const results = blocks.filter((block) => block.type === "tool_result");
        for (const block of results) {
          if (typeof block.tool_use_id === "string") toolStatus.set(block.tool_use_id, block.is_error === true);
        }
        const text = blocks.filter((block) => block.type === "text").map(blockText).join("\n").trim();
        if (!text) continue;
        entries.push({ uuid: row.uuid, role: "user", text, toolUses: [], timestamp: undefined });
        continue;
      }
      const toolUses: SdkTranscriptToolUse[] = [];
      let text = "";
      for (const block of blocks) {
        if (block.type === "text") text += blockText(block);
        else if (block.type === "tool_use" && typeof block.id === "string" && typeof block.name === "string") {
          toolUses.push({ callId: block.id, title: block.name });
        }
      }
      if (!text && toolUses.length === 0) continue;
      entries.push({ uuid: row.uuid, role: "assistant", text, toolUses, timestamp: undefined });
    }
    for (const entry of entries) {
      for (const tool of entry.toolUses) {
        const status = toolStatus.get(tool.callId);
        if (status !== undefined) tool.failed = status;
      }
    }
    return entries;
  }

  async prompt(sessionId: string, cwd: string, blocks: Array<Record<string, unknown>>): Promise<{ stopReason: string }> {
    const stream = await this.ensureStream(sessionId, cwd);
    if (stream.pending) throw new Error("Claude Code is still processing the previous message");
    const promptUuid = randomUUID();
    const message: SDKUserMessage = {
      type: "user",
      message: { role: "user", content: await promptContent(blocks) as never },
      parent_tool_use_id: null,
      uuid: promptUuid,
    };
    return await new Promise<{ stopReason: string }>((resolve, reject) => {
      stream.pending = {
        promptUuid,
        resolve: (result) => { stream.pending = null; resolve(result); },
        reject: (error) => { stream.pending = null; reject(error); },
      };
      stream.promptUuids.add(promptUuid);
      this.touch(stream);
      stream.input.push(message);
    });
  }

  async cancel(sessionId: string): Promise<void> {
    const stream = this.streams.get(sessionId);
    if (!stream?.query) return;
    try { await stream.query.interrupt(); } catch { /* the turn result still arrives */ }
  }

  async setMode(sessionId: string, modeId: string): Promise<void> {
    const stream = this.streams.get(sessionId);
    this.controlState.set(sessionId, { ...this.controlState.get(sessionId), permissionMode: modeId });
    if (stream?.query) {
      await stream.query.setPermissionMode(modeId as never);
    } else {
      const slot = this.ensureSlot(sessionId);
      slot.pendingMode = modeId;
    }
  }

  async setConfig(sessionId: string, configId: string, value: string | boolean): Promise<void> {
    if (typeof value !== "string") throw new Error(`Claude Code control ${configId} requires a string value`);
    const stream = this.streams.get(sessionId);
    if (configId === "model") {
      this.controlState.set(sessionId, { ...this.controlState.get(sessionId), model: value });
      if (stream?.query) await stream.query.setModel(value);
      else this.ensureSlot(sessionId).pendingModel = value;
      return;
    }
    if (configId === "effort") {
      this.controlState.set(sessionId, { ...this.controlState.get(sessionId), effort: value });
      if (stream?.query) await stream.query.applyFlagSettings({ effortLevel: value as never });
      else this.ensureSlot(sessionId).pendingEffort = value;
      return;
    }
    throw new Error(`Claude Agent SDK does not support control option: ${configId}`);
  }

  controls(sessionId: string): SdkControlsSnapshot {
    const state = this.controlState.get(sessionId) ?? {};
    const slot = this.slots.get(sessionId);
    const models = (this.modelCache ?? []).map((model) => ({ id: model.value, label: model.displayName || model.value }));
    const efforts = this.effortOptions();
    return {
      protocol: "claude-agent-sdk",
      models: models.length > 0 ? models : FALLBACK_MODELS,
      efforts,
      permissionModes: PERMISSION_MODES,
      currentModel: state.model ?? slot?.pendingModel,
      currentEffort: state.effort ?? slot?.pendingEffort ?? null,
      currentPermissionMode: state.permissionMode ?? slot?.pendingMode,
    };
  }

  private effortOptions(): SdkControlOption[] {
    if (!this.modelCache) return FALLBACK_EFFORTS;
    for (const model of this.modelCache) {
      if (model.supportedEffortLevels?.length) {
        return model.supportedEffortLevels.map((level) => ({ id: level, label: level[0].toUpperCase() + level.slice(1) }));
      }
    }
    return FALLBACK_EFFORTS;
  }

  async closeSession(sessionId: string): Promise<void> {
    const stream = this.streams.get(sessionId);
    if (stream) await this.reapStream(stream, "session closed");
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    for (const stream of this.streams.values()) await this.reapStream(stream, "runtime closed");
    this.streams.clear();
    this.slots.clear();
  }

  private ensureSlot(sessionId: string): SessionSlot {
    let slot = this.slots.get(sessionId);
    if (!slot) {
      slot = { cwd: "", fresh: false };
      this.slots.set(sessionId, slot);
    }
    return slot;
  }

  private touch(stream: SessionStream): void {
    stream.lastActivity = Date.now();
    if (stream.idleTimer) clearTimeout(stream.idleTimer);
    stream.idleTimer = setTimeout(() => {
      stream.idleTimer = null;
      if (!stream.pending) void this.reapStream(stream, "idle");
    }, IDLE_STREAM_MS);
    stream.idleTimer.unref?.();
  }

  private async reapStream(stream: SessionStream, reason: string): Promise<void> {
    if (stream.closed) return;
    stream.closed = true;
    if (stream.idleTimer) { clearTimeout(stream.idleTimer); stream.idleTimer = null; }
    if (stream.pending) stream.pending.reject(new Error(`Claude Code stream ended before the turn finished (${reason})`));
    for (const waiter of stream.permissionWaiters) waiter.cancel?.();
    stream.input.end();
    if (this.streams.get(stream.sessionId) === stream) this.streams.delete(stream.sessionId);
    const slot = this.slots.get(stream.sessionId);
    if (slot) slot.fresh = false;
  }

  private async ensureStream(sessionId: string, cwd: string): Promise<SessionStream> {
    if (this.closed) throw new Error("Claude Agent SDK runtime is closed");
    const existing = this.streams.get(sessionId);
    if (existing && !existing.closed) {
      existing.slot.cwd = cwd || existing.slot.cwd;
      return existing;
    }
    let stream = this.pendingStream(sessionId, cwd);
    if (stream.starting) return await stream.starting.then(() => stream!);
    const starting = this.startStream(stream);
    stream.starting = starting;
    try {
      await starting;
      return stream;
    } catch (error) {
      if (this.streams.get(sessionId) === stream) this.streams.delete(sessionId);
      stream.closed = true;
      throw safeError(error);
    }
  }

  private pendingStream(sessionId: string, cwd: string): SessionStream {
    let stream = this.streams.get(sessionId);
    if (!stream || stream.closed) {
      const slot = this.ensureSlot(sessionId);
      slot.cwd = cwd || slot.cwd;
      stream = {
        sessionId, cwd: slot.cwd, slot,
        input: new PushQueue<SDKUserMessage>(),
        query: null, starting: null, pending: null,
        promptUuids: new Set<string>(), openTextBlocks: [],
        permissionWaiters: new Set(),
        closed: false, idleTimer: null, lastActivity: Date.now(),
      };
      this.streams.set(sessionId, stream);
    }
    return stream;
  }

  private async startStream(stream: SessionStream): Promise<Query> {
    const slot = stream.slot;
    const options: Options = {
      ...this.baseOptions(),
      cwd: slot.cwd || undefined,
      // A pre-generated id names a brand-new transcript; anything else must
      // resume the persisted session.
      ...(slot.fresh ? { sessionId: stream.sessionId } : { resume: stream.sessionId }),
      includePartialMessages: true,
      canUseTool: (toolName, input, canUseOptions) => this.handlePermission(stream, toolName, input, canUseOptions),
    };
    if (slot.pendingModel) options.model = slot.pendingModel;
    if (slot.pendingEffort) options.effort = slot.pendingEffort as never;
    if (slot.pendingMode) options.permissionMode = slot.pendingMode as never;
    const input = stream.input;
    const q = query({
      prompt: (async function* () { for await (const item of input) yield item; })(),
      options,
    });
    stream.query = q;
    slot.pendingModel = undefined;
    slot.pendingEffort = undefined;
    slot.pendingMode = undefined;
    slot.fresh = false;
    this.touch(stream);
    void this.pump(stream, q);
    try {
      await q.initializationResult();
      this.loadModelCache(q);
      return q;
    } catch (error) {
      await this.reapStream(stream, "initialization failed");
      throw error;
    }
  }

  private loadModelCache(q: Query): void {
    if (this.modelCache || this.modelCacheLoading) return;
    this.modelCacheLoading = q
      .supportedModels()
      .then((models) => { this.modelCache = models; })
      .catch(() => { /* keep the static fallback list */ })
      .finally(() => { this.modelCacheLoading = null; });
  }

  private async pump(stream: SessionStream, q: Query): Promise<void> {
    try {
      for await (const message of q) {
        this.touch(stream);
        this.dispatch(stream, message);
      }
    } catch {
      // A crashed stream settles its pending turn below; the transcript on
      // disk stays authoritative for the next attach.
    }
    if (!stream.closed && this.streams.get(stream.sessionId) === stream) {
      await this.reapStream(stream, "stream ended");
    }
  }

  private dispatch(stream: SessionStream, message: SDKMessage): void {
    // Subagent frames describe nested Task-tool activity, not this
    // conversation's turns; they must not leak into the main history.
    if ("parent_tool_use_id" in message && message.parent_tool_use_id) return;
    const emit = (update: SdkUpdate) => this.handlers.onUpdate(stream.sessionId, update);
    if (message.type === "system" && message.subtype === "init") {
      const state = { model: message.model, permissionMode: message.permissionMode, effort: message.effort ?? null };
      this.controlState.set(stream.sessionId, { ...this.controlState.get(stream.sessionId), ...state });
      emit({ kind: "session_ready", ...state });
      return;
    }
    if (message.type === "system" && message.subtype === "status") {
      if (message.permissionMode) {
        this.controlState.set(stream.sessionId, { ...this.controlState.get(stream.sessionId), permissionMode: message.permissionMode });
        emit({ kind: "controls_update", permissionMode: message.permissionMode });
      }
      return;
    }
    if (message.type === "stream_event") {
      const event = message.event as unknown as Record<string, unknown>;
      const eventType = event.type;
      if (eventType === "content_block_start") {
        const block = event.content_block as ContentBlock | undefined;
        if (block?.type === "text") {
          const provisionalId = `sdk-partial:${message.uuid}`;
          stream.openTextBlocks.push(provisionalId);
        }
        return;
      }
      if (eventType === "content_block_delta") {
        const delta = event.delta as ContentBlock | undefined;
        if (delta?.type === "text_delta" && typeof delta.text === "string") {
          const provisionalId = stream.openTextBlocks.at(-1);
          if (provisionalId) emit({ kind: "assistant_text", messageId: provisionalId, text: delta.text, provisional: true });
          emit({ kind: "assistant_delta", delta: delta.text });
        } else if (delta?.type === "thinking_delta" && typeof delta.thinking === "string") {
          emit({ kind: "thought_delta", delta: delta.thinking });
        }
        return;
      }
      return;
    }
    if (message.type === "assistant") {
      for (const block of contentBlocks(message.message)) {
        if (block.type === "text") {
          const text = blockText(block);
          const provisionalId = stream.openTextBlocks.shift();
          if (provisionalId) emit({ kind: "assistant_text_commit", provisionalId, messageId: message.uuid, text });
          else emit({ kind: "assistant_text", messageId: message.uuid, text });
        } else if (block.type === "tool_use" && typeof block.id === "string" && typeof block.name === "string") {
          emit({ kind: "tool_update", callId: block.id, title: block.name, status: "running", detail: brief(block.input) });
        } else if (block.type === "thinking") {
          const thinking = typeof block.thinking === "string" ? block.thinking : "";
          if (thinking) emit({ kind: "thought_delta", delta: thinking.slice(0, MAX_DETAIL_CHARS) });
        }
      }
      return;
    }
    if (message.type === "user") {
      if ((message as { isReplay?: boolean }).isReplay) return;
      const uuid = message.uuid;
      if (!uuid) return;
      if (stream.promptUuids.has(uuid)) return; // our own submitted prompt
      if (message.isSynthetic) return;
      const blocks = contentBlocks(message.message);
      for (const block of blocks) {
        if (block.type === "tool_result" && typeof block.tool_use_id === "string") {
          emit({ kind: "tool_update", callId: block.tool_use_id, title: "Tool result", status: block.is_error === true ? "failed" : "completed", detail: brief(resultText(block)) });
        }
      }
      const text = blocks.filter((block) => block.type === "text").map(blockText).join("\n");
      if (text.trim()) emit({ kind: "external_user", messageId: uuid, text });
      return;
    }
    if (message.type === "result") {
      const pending = stream.pending;
      if (!pending) return;
      const result = message as { subtype: string; is_error?: boolean; stop_reason?: string | null; result?: string };
      if (result.subtype === "success" && result.is_error !== true) {
        pending.resolve({ stopReason: result.stop_reason || "end_turn" });
      } else {
        const reason = result.subtype === "success" ? (result.result || "turn ended with an error") : `turn ${result.subtype}`;
        pending.reject(new Error(reason.slice(0, 300)));
      }
    }
  }

  private async handlePermission(
    stream: SessionStream,
    toolName: string,
    input: Record<string, unknown>,
    options: { requestId: string; toolUseID: string; title?: string; displayName?: string; suggestions?: PermissionUpdate[]; signal: AbortSignal },
  ): Promise<PermissionResult | null> {
    const request: SdkPermissionRequest = {
      sessionId: stream.sessionId,
      requestId: options.requestId,
      toolUseId: options.toolUseID,
      toolName,
      title: options.title || "",
      displayName: options.displayName || toolName,
      detail: brief(input),
      suggestions: options.suggestions ?? [],
    };
    const waiter: { cancel?: () => void } = {};
    stream.permissionWaiters.add(waiter);
    const cancelled = new Promise<never>((_, reject) => {
      waiter.cancel = () => reject(new Error("permission request cancelled"));
      options.signal.addEventListener("abort", () => waiter.cancel?.(), { once: true });
    });
    try {
      const decision = await Promise.race([this.handlers.onPermission(request), cancelled]);
      if (decision.outcome === "deny") return { behavior: "deny", message: decision.message || "Denied in Prism" };
      return { behavior: "allow", ...(decision.remember && request.suggestions.length > 0 ? { updatedPermissions: request.suggestions } : {}) };
    } catch {
      return { behavior: "deny", message: "Permission request was cancelled" };
    } finally {
      stream.permissionWaiters.delete(waiter);
    }
  }
}

export function fingerprint(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function newOpaqueID(prefix: string): string {
  return `${prefix}:${randomUUID()}`;
}
