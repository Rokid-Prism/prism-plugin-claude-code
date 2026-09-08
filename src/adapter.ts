import { createHash, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import { isAbsolute, resolve } from "node:path";
import type {
  ApprovalResolutionRequest, AttachSessionRequest, Capability, ControlSessionRequest,
  ControlSessionResult, DraftControlRequest, DraftControlResult, DraftOpenRequest,
  DraftOpenResult, InboundMessage, NativeSession, NativeSessionHint, PluginAdapter,
  PluginEvent, RunStatus, SendReceipt, StartDraftWithMessageRequest,
  StartSessionWithMessageRequest, StartSessionWithMessageResult, VisibilityResult,
} from "@rokid-prism/pluginbridge-plugin-sdk";
import { ClaudeAcpRuntime, type AcpNotification, type AcpPermissionRequest, fingerprint, newOpaqueID } from "./acp-runtime.js";

const PLUGIN_ID = "claudecode";
const SURFACE = "claudecode-acp";
const MAX_HISTORY_TURNS = 100;

type HistoryProgressStep = { ID: string; Kind: string; CallID?: string; Title: string; Detail: string; Status: string; CreatedAt: string };
type HistoryMessage = { ID: string; Role: string; Type: string; Content: string; Status: string; CreatedAt: string; UpdatedAt: string; Revision?: number; Progress?: { Status: string; StartedAt: string; CompletedAt?: string; Steps: HistoryProgressStep[] }; Metadata?: Record<string, unknown> };
type HistoryStreamRequest = { stream_id: string; limit: number; live?: boolean };
type HistoryStreamEvent = { stream_id: string; type: "turn" | "page_end" | "end" | "error"; source?: "initial" | "live"; operation?: "append" | "replace"; turn?: { turn_id: string; order_key: string; revision: number; messages: HistoryMessage[] }; error?: string };
type HistoryTurn = { ID: string; OrderKey: string; Revision: number; Messages: HistoryMessage[]; MessageIDs: Set<string> };

type Run = {
  id: string;
  startedAt: string;
  progressMessageID: string;
  done: Promise<PluginEvent>;
  resolve: (event: PluginEvent) => void;
  status: "running" | "completed" | "failed" | "interrupted";
  steps: HistoryProgressStep[];
  preview: string;
  final?: PluginEvent;
};

type SessionState = {
  id: string;
  cwd: string;
  title: string;
  updatedAt: string;
  history: HistoryMessage[];
  historyLoaded: boolean;
  historyLoading?: Promise<SessionState>;
  run?: Run;
  modes?: Record<string, unknown>;
  configOptions?: Array<Record<string, unknown>>;
  listeners: Set<AsyncQueue<PluginEvent>>;
};

type Draft = { id: string; sessionId: string; cwd: string; fingerprint: string };
type PendingApproval = { id: string; sessionId: string; options: Array<Record<string, unknown>>; resolve: (response: Record<string, unknown>) => void };
export type AcpClient = Pick<ClaudeAcpRuntime, "ensureStarted" | "newSession" | "loadSession" | "listSessions" | "prompt" | "cancel" | "setMode" | "setConfig" | "closeSession" | "close">;

class AsyncQueue<T> implements AsyncIterable<T> {
  private values: T[] = [];
  private waiting: ((r: IteratorResult<T>) => void) | null = null;
  private ended = false;
  push(value: T): void {
    if (this.ended) return;
    if (this.waiting) { const resolve = this.waiting; this.waiting = null; resolve({ value, done: false }); return; }
    this.values.push(value);
  }
  end(): void {
    this.ended = true;
    if (this.waiting) { const resolve = this.waiting; this.waiting = null; resolve({ value: undefined as never, done: true }); }
  }
  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async () => {
        const value = this.values.shift();
        if (value !== undefined) return { value, done: false };
        if (this.ended) return { value: undefined as never, done: true };
        return await new Promise<IteratorResult<T>>((resolve) => { this.waiting = resolve; });
      },
    };
  }
}

function now(): string { return new Date().toISOString(); }
function nonEmpty(value: unknown): string { return typeof value === "string" ? value.trim() : ""; }
function safeSummary(value: unknown, fallback: string): string { return nonEmpty(value).replace(/\s+/g, " ").slice(0, 240) || fallback; }
function nativeSession(id: string, cwd = ""): NativeSession {
  return { PluginID: PLUGIN_ID, NativeSessionID: id, NativeThreadID: id, Surface: SURFACE, Endpoint: "local ACP", Cwd: cwd, Visible: false };
}
function sessionID(session: NativeSession): string { return nonEmpty(session.NativeSessionID) || nonEmpty(session.NativeThreadID); }
function event(type: string, status: string, summary: string, payload: Record<string, unknown> = {}): PluginEvent {
  return { ID: `${PLUGIN_ID}:${randomUUID()}`, Type: type, Status: status, Summary: summary, Payload: payload, CreatedAt: now() };
}
function textFromContent(content: unknown): string {
  if (!content || typeof content !== "object") return "";
  const block = content as Record<string, unknown>;
  return typeof block.text === "string" ? block.text : typeof block.content === "string" ? block.content : "";
}
function record(value: unknown): Record<string, unknown> { return value && typeof value === "object" ? value as Record<string, unknown> : {}; }
function historyMessageSnapshot(message: HistoryMessage): HistoryMessage {
  return {
    ...message,
    Progress: message.Progress && { ...message.Progress, Steps: message.Progress.Steps.map((step) => ({ ...step })) },
    Metadata: message.Metadata && { ...message.Metadata },
  };
}
function historyMessageRevision(message: HistoryMessage): number { return Math.max(1, message.Revision ?? 1); }
function groupHistoryTurns(messages: readonly HistoryMessage[]): HistoryTurn[] {
  const turns: HistoryTurn[] = [];
  let current: HistoryTurn | undefined;
  for (const message of messages) {
    const role = message.Role.trim().toLowerCase();
    if (role === "user") {
      current = {
        ID: message.ID,
        OrderKey: message.CreatedAt,
        Revision: historyMessageRevision(message),
        Messages: [historyMessageSnapshot(message)],
        MessageIDs: new Set([message.ID]),
      };
      turns.push(current);
      continue;
    }
    // A HistoryTurn is anchored by a real user message. Ignore any orphaned
    // ACP output rather than presenting it as a synthetic conversation turn.
    if (role !== "assistant" || !current) continue;
    current.Messages.push(historyMessageSnapshot(message));
    current.MessageIDs.add(message.ID);
    // Message revisions are increased for every ACP publication. Their sum is
    // therefore a monotonic revision for the complete user-anchored turn.
    current.Revision += historyMessageRevision(message);
  }
  return turns;
}

/** A protocol-native Claude Code ACP adapter. It deliberately never uses CDP. */
export class ClaudeCodeAdapter implements PluginAdapter {
  private readonly sessions = new Map<string, SessionState>();
  private readonly drafts = new Map<string, Draft>();
  private readonly approvals = new Map<string, PendingApproval>();
  private readonly diagnostics: string[] = [];
  private readonly runtime: AcpClient;

  constructor(runtime?: AcpClient) {
    this.runtime = runtime ?? new ClaudeAcpRuntime({
      onUpdate: (notification) => this.onUpdate(notification),
      onPermission: (request) => this.onPermission(request),
      onStderr: (line) => { this.diagnostics.push(line); if (this.diagnostics.length > 8) this.diagnostics.shift(); },
    });
  }

  id(): string { return PLUGIN_ID; }

  async probe(): Promise<Capability> {
    try {
      await this.runtime.ensureStarted();
      return this.capability(true, "");
    } catch (error) {
      return this.capability(false, safeSummary(error instanceof Error ? error.message : error, "Claude Code ACP is unavailable"));
    }
  }

  private capability(available: boolean, unavailableReason: string): Capability {
    return {
      PluginID: PLUGIN_ID, Available: available,
      NativeVisibleInput: false, NativeVisibleOutput: false,
      CanAttachSession: true, CanStartSessionWithMessage: true, CanOpenDraft: true,
      CanListSessions: true, CanReadHistory: true, CanInterrupt: true, CanApproval: true,
      CanForwardSync: true, CanReverseSync: false, CanPluginWideWatch: false,
      CanWaitRun: true, CanReadStatus: true, CanControlSession: true,
      IntegrationMode: "protocol-native", VisibilitySurface: SURFACE, UnavailableReason: unavailableReason,
    };
  }

  async discover() {
    const capability = await this.probe();
    return {
      PluginID: PLUGIN_ID, Surface: SURFACE, Endpoint: "local ACP subprocess", ProcessID: process.pid,
      SessionHints: { protocol: "ACP", agent: "@agentclientprotocol/claude-agent-acp" },
      Verified: capability.Available, Detail: capability.Available ? "Claude Code ACP initialized" : capability.UnavailableReason,
    };
  }

  async openDraft(req: DraftOpenRequest): Promise<DraftOpenResult> {
    const cwd = this.validCwd(req.Cwd);
    let response: Record<string, unknown>;
    try {
      response = await this.runtime.newSession(cwd);
    } catch (error) {
      const reason = safeSummary(error instanceof Error ? error.message : error, "Claude Code ACP could not create a session");
      const diagnostic = this.diagnostics.at(-1);
      throw new Error(diagnostic ? `${reason} (${safeSummary(diagnostic, "ACP runtime error")})` : reason);
    }
    const id = nonEmpty(response.sessionId);
    if (!id) throw new Error("Claude Code ACP did not return a session id for the draft");
    this.rememberSession(id, cwd, response, true);
    const draft: Draft = { id: req.DraftID, sessionId: id, cwd, fingerprint: fingerprint(`${req.DraftID}:${id}:${cwd}`) };
    this.drafts.set(req.DraftID, draft);
    return { DraftID: req.DraftID, Cwd: cwd, Controls: this.controls(id), DraftFingerprint: draft.fingerprint };
  }

  async controlDraft(req: DraftControlRequest): Promise<DraftControlResult> {
    const draft = this.drafts.get(req.DraftID);
    if (!draft) throw new Error("Claude Code draft is no longer active");
    await this.applyControl(draft.sessionId, req.Action, req.Target);
    return { DraftID: draft.id, Cwd: draft.cwd, Controls: this.controls(draft.sessionId) };
  }

  async startDraftWithMessage(req: StartDraftWithMessageRequest): Promise<StartSessionWithMessageResult> {
    const draft = this.drafts.get(req.DraftID);
    if (!draft) throw new Error("Claude Code draft is no longer active");
    this.drafts.delete(req.DraftID);
    const session = nativeSession(draft.sessionId, draft.cwd);
    const receipt = await this.send(session, req.Message);
    return { Session: session, Receipt: receipt, Visibility: await this.verifyVisibility(session, req.Message.PrismMessageID) };
  }

  async startSessionWithMessage(req: StartSessionWithMessageRequest): Promise<StartSessionWithMessageResult> {
    const draft = await this.openDraft({ DraftID: `implicit:${req.Message.PrismMessageID}`, PluginID: PLUGIN_ID, Cwd: req.Cwd, SourceDevice: req.SourceDevice, Metadata: req.Metadata });
    return await this.startDraftWithMessage({ DraftID: draft.DraftID, PluginID: PLUGIN_ID, Cwd: draft.Cwd, Message: req.Message, SourceDevice: req.SourceDevice, Metadata: req.Metadata });
  }

  async listSessions(): Promise<NativeSessionHint[]> {
    const all: NativeSessionHint[] = [];
    let cursor: string | undefined;
    do {
      const page = await this.runtime.listSessions(cursor);
      for (const item of (Array.isArray(page.sessions) ? page.sessions : [])) {
        const raw = record(item); const id = nonEmpty(raw.sessionId); if (!id) continue;
        const cwd = nonEmpty(raw.cwd); this.rememberSession(id, cwd, raw);
        all.push({ PluginID: PLUGIN_ID, NativeSessionID: id, NativeThreadID: id, Surface: SURFACE, Endpoint: "local ACP", Cwd: cwd,
          Title: nonEmpty(raw.title) || "Claude Code session", PrismConversationID: "", Active: this.sessions.get(id)?.run?.status === "running", Visible: false,
          LastActivityAt: nonEmpty(raw.updatedAt) || now(), Metadata: { protocol: "acp" } });
      }
      cursor = nonEmpty(page.nextCursor) || undefined;
    } while (cursor && all.length < 500);
    return all;
  }

  async attachSession(req: AttachSessionRequest): Promise<NativeSession> {
    const id = nonEmpty(req.NativeSessionID) || nonEmpty(req.NativeThreadID);
    if (!id) throw new Error("Claude Code session id is required");
    const cwd = this.validCwd(req.Cwd);
    await this.ensureHistoryLoaded(id, cwd);
    return nativeSession(id, cwd);
  }

  async readHistory(session: NativeSession, limit: number): Promise<HistoryMessage[]> {
    const state = await this.ensureSession(session);
    return state.history.slice(-Math.max(1, Math.min(limit || 50, MAX_HISTORY_TURNS * 2)));
  }

  async *readHistoryStream(session: NativeSession, request: HistoryStreamRequest, signal?: AbortSignal): AsyncIterable<HistoryStreamEvent> {
    const state = await this.ensureSession(session);
    const streamID = request.stream_id || newOpaqueID("history");
    const queue = request.live ? new AsyncQueue<PluginEvent>() : undefined;
    const onAbort = () => queue?.end();
    const emittedTurnRevisions = new Map<string, number>();
    // Subscribe before reading the initial page so an ACP chunk that arrives
    // during replay remains queued instead of being invisible until reload.
    if (queue) {
      state.listeners.add(queue);
      signal?.addEventListener("abort", onAbort, { once: true });
    }
    try {
      const limit = Math.max(1, Math.min(request.limit || 20, 20));
      // Capture the complete user-anchored turns before yielding any frame.
      // Initial delivery intentionally starts with the newest turn; consumers
      // use order_key to place the following older turns above it.
      const initialTurns = groupHistoryTurns(state.history).slice(-limit).reverse();
      for (const turn of initialTurns) emittedTurnRevisions.set(turn.ID, turn.Revision);
      for (const turn of initialTurns) {
        if (signal?.aborted) return;
        yield {
          stream_id: streamID,
          type: "turn",
          source: "initial",
          operation: "append",
          turn: { turn_id: turn.ID, order_key: turn.OrderKey, revision: turn.Revision, messages: turn.Messages },
        };
      }
      yield { stream_id: streamID, type: request.live ? "page_end" : "end", source: "initial", operation: "append" };
      if (!queue) return;
      for await (const item of queue) {
        const payload = item.Payload;
        const message = payload.history_message as HistoryMessage | undefined;
        if (!message) continue;
        const turn = groupHistoryTurns(state.history).find((candidate) => candidate.MessageIDs.has(message.ID));
        if (!turn) continue;
        const previousRevision = emittedTurnRevisions.get(turn.ID);
        if (previousRevision !== undefined && previousRevision >= turn.Revision) continue;
        emittedTurnRevisions.set(turn.ID, turn.Revision);
        yield {
          stream_id: streamID,
          type: "turn",
          source: "live",
          operation: previousRevision === undefined ? "append" : "replace",
          turn: { turn_id: turn.ID, order_key: turn.OrderKey, revision: turn.Revision, messages: turn.Messages },
        };
      }
    } finally {
      if (queue) state.listeners.delete(queue);
      signal?.removeEventListener("abort", onAbort);
    }
  }

  async send(session: NativeSession, msg: InboundMessage): Promise<SendReceipt> {
    const state = await this.ensureSession(session);
    if (state.run?.status === "running") throw new Error("Claude Code is still processing the previous message");
    const existing = state.history.find((m) => m.ID === msg.PrismMessageID);
    if (existing) return this.receipt(state.id, `acp-run:${msg.PrismMessageID}`, true, "duplicate message already accepted");
    const createdAt = msg.Timestamp || now();
    this.appendHistory(state, { ID: msg.PrismMessageID, Role: "user", Type: "text", Content: msg.Text, Status: "completed", CreatedAt: createdAt, UpdatedAt: createdAt,
      Metadata: { marker: msg.PrismMessageID, attachments: (msg.Attachments ?? []).map((a) => ({ name: a.Name, mime_type: a.MIMEType })) } });
    let resolveRun!: (event: PluginEvent) => void;
    const run: Run = { id: `acp-run:${msg.PrismMessageID}`, startedAt: now(), progressMessageID: `acp-progress:${msg.PrismMessageID}`, status: "running", steps: [], preview: "", done: new Promise((resolve) => { resolveRun = resolve; }), resolve: resolveRun };
    state.run = run;
    this.appendHistory(state, {
      ID: run.progressMessageID,
      Role: "assistant",
      Type: "progress",
      Content: "",
      Status: "running",
      CreatedAt: run.startedAt,
      UpdatedAt: run.startedAt,
      Progress: {
        Status: "running",
        StartedAt: run.startedAt,
        Steps: [{ ID: `acp:thinking:${run.id}`, Kind: "status", Title: "Claude Code thinking", Detail: "", Status: "running", CreatedAt: run.startedAt }],
      },
    });
    this.publish(state, event("run.started", "running", "Claude Code is processing", { native_session: nativeSession(state.id, state.cwd), run_id: run.id }));
    const blocks = this.promptBlocks(msg);
    void this.runtime.prompt(state.id, blocks).then(
      (response) => this.completeRun(state, run, "completed", `Claude Code completed: ${nonEmpty(response.stopReason) || "finished"}`),
      (error) => this.completeRun(state, run, run.status === "interrupted" ? "interrupted" : "failed", safeSummary(error instanceof Error ? error.message : error, "Claude Code ACP failed")),
    );
    // PluginBridge uses NativeMessageID as the opaque identifier it later
    // passes to waitForRun. For ACP that identifier is the run ID, not the
    // Prism user-message ID. Returning the latter caused Hub to immediately
    // fail an otherwise successful Claude response with "run not found".
    return this.receipt(state.id, run.id, true, "accepted by Claude Code ACP");
  }

  async *subscribe(session: NativeSession, signal?: AbortSignal): AsyncIterable<PluginEvent> {
    const state = await this.ensureSession(session); const queue = new AsyncQueue<PluginEvent>(); state.listeners.add(queue);
    const onAbort = () => queue.end(); signal?.addEventListener("abort", onAbort, { once: true });
    try { for await (const item of queue) yield item; } finally { state.listeners.delete(queue); signal?.removeEventListener("abort", onAbort); }
  }

  async interrupt(session: NativeSession, taskID: string): Promise<void> {
    const state = await this.ensureSession(session); const run = state.run;
    if (!run || run.status !== "running" || (taskID && taskID !== run.id)) throw new Error("Claude Code has no matching running task");
    run.status = "interrupted"; await this.runtime.cancel(state.id);
  }

  async waitForRun(session: NativeSession, runID: string): Promise<PluginEvent> {
    const state = await this.ensureSession(session); const run = state.run;
    if (!run || run.id !== runID) throw new Error("Claude Code run not found");
    return await run.done;
  }

  async readStatus(session: NativeSession, runID: string): Promise<RunStatus> {
    const state = await this.ensureSession(session); const run = state.run;
    if (!run || run.id !== runID) return { status: "unknown", interruptible: false, approval_blocked: false };
    return { status: run.status, phase: { protocol: "acp" }, preview: run.preview, steps: run.steps as unknown as Record<string, unknown>[], interruptible: run.status === "running", approval_blocked: Array.from(this.approvals.values()).some((a) => a.sessionId === state.id), started_at: run.startedAt, completed_at: run.final?.CreatedAt, duration_ms: run.final ? Date.parse(run.final.CreatedAt) - Date.parse(run.startedAt) : undefined };
  }

  async controlSession(req: ControlSessionRequest): Promise<ControlSessionResult> {
    const state = await this.ensureSession(req.session); await this.applyControl(state.id, req.action, req.target);
    const details = this.controls(state.id); this.publish(state, event("desktop.state.changed", "completed", "Claude Code session controls updated", { detail_snapshot: details }));
    return { ok: true, action: req.action, thread_id: state.id, details };
  }

  async resolveApproval(req: ApprovalResolutionRequest): Promise<void> {
    const approval = this.approvals.get(req.ApprovalRequestID);
    if (!approval || approval.sessionId !== sessionID(req.Session)) throw new Error("Claude Code approval is no longer pending");
    if (!approval.options.some((option) => nonEmpty(option.optionId) === req.ActionID)) throw new Error("Claude Code approval action is stale");
    this.approvals.delete(approval.id); approval.resolve({ outcome: { outcome: "selected", optionId: req.ActionID } });
  }

  async verifyVisibility(session: NativeSession, marker: string): Promise<VisibilityResult> {
    const state = await this.ensureSession(session); const found = state.history.some((message) => message.ID === marker || message.Metadata?.marker === marker);
    return { Visible: found, Marker: marker, Evidence: "ACP session history", CheckedAt: now(), FailureReason: found ? "" : "marker was not found in the local ACP projection" };
  }

  async close(): Promise<void> { await this.runtime.close(); for (const state of this.sessions.values()) for (const listener of state.listeners) listener.end(); }

  private validCwd(cwd: string): string { const value = cwd.trim(); if (!value) return process.cwd(); return isAbsolute(value) ? value : resolve(value); }
  private receipt(id: string, messageID: string, visible: boolean, detail: string): SendReceipt { return { NativeMessageID: messageID, CanonicalNativeSessionID: id, CanonicalNativeThreadID: id, Accepted: true, Visible: visible, Detail: detail }; }
  private rememberSession(id: string, cwd: string, response: Record<string, unknown> = {}, historyLoaded = false): SessionState {
    const current = this.sessions.get(id);
    if (current) {
      current.cwd = cwd || current.cwd;
      if (historyLoaded) current.historyLoaded = true;
      this.updateControls(current, response);
      return current;
    }
    const state: SessionState = { id, cwd, title: "Claude Code session", updatedAt: now(), history: [], historyLoaded, listeners: new Set() };
    this.sessions.set(id, state); this.updateControls(state, response); return state;
  }
  private async ensureSession(session: NativeSession): Promise<SessionState> {
    const id = sessionID(session); if (!id) throw new Error("Claude Code session id is required");
    return await this.ensureHistoryLoaded(id, this.validCwd(session.Cwd));
  }
  private async ensureHistoryLoaded(id: string, cwd: string): Promise<SessionState> {
    const known = this.sessions.get(id);
    if (known?.historyLoaded) {
      known.cwd = cwd || known.cwd;
      return known;
    }
    if (known?.historyLoading) return await known.historyLoading;
    // listSessions intentionally creates lightweight session records without
    // replaying their transcript. Do not let that index cache suppress the
    // first body read after a Hub restart.
    const state = known ?? this.rememberSession(id, cwd);
    const loading = (async () => {
      state.history = [];
      const response = await this.runtime.loadSession(id, cwd);
      return this.rememberSession(id, cwd, response, true);
    })();
    state.historyLoading = loading;
    try {
      return await loading;
    } finally {
      if (state.historyLoading === loading) state.historyLoading = undefined;
    }
  }
  private updateControls(state: SessionState, response: Record<string, unknown>): void {
    if (response.modes && typeof response.modes === "object") state.modes = record(response.modes);
    if (Array.isArray(response.configOptions)) state.configOptions = response.configOptions.map(record);
  }
  private controls(id: string): Record<string, unknown> { const state = this.sessions.get(id); return { modes: state?.modes ?? {}, config_options: state?.configOptions ?? [], protocol: "acp" }; }
  private publishHistoryMessage(state: SessionState, message: HistoryMessage): void {
    message.Revision = Math.max(0, message.Revision ?? 0) + 1;
    state.updatedAt = message.UpdatedAt;
    this.publish(state, event("conversation.history.changed", message.Status, `${message.Role} message`, { history_message: historyMessageSnapshot(message) }));
  }
  private appendHistory(state: SessionState, message: HistoryMessage): void {
    state.history.push(message);
    this.trimHistory(state);
    this.publishHistoryMessage(state, message);
  }
  private runProgressMessage(state: SessionState, run: Run): HistoryMessage {
    const existing = state.history.find((message) => message.ID === run.progressMessageID);
    if (existing?.Progress) return existing;
    const message: HistoryMessage = {
      ID: run.progressMessageID,
      Role: "assistant",
      Type: "progress",
      Content: "",
      Status: "running",
      CreatedAt: run.startedAt,
      UpdatedAt: now(),
      Progress: {
        Status: "running",
        StartedAt: run.startedAt,
        Steps: [{ ID: `acp:thinking:${run.id}`, Kind: "status", Title: "Claude Code thinking", Detail: "", Status: "running", CreatedAt: run.startedAt }],
      },
    };
    state.history.push(message);
    this.trimHistory(state);
    return message;
  }
  private publishRunProgress(state: SessionState, run: Run): void {
    const message = this.runProgressMessage(state, run);
    message.Status = run.status;
    if (message.Progress) message.Progress.Status = run.status;
    message.UpdatedAt = now();
    this.publishHistoryMessage(state, message);
  }
  private trimHistory(state: SessionState): void {
    let retainedStart = 0;
    let users = 0;
    for (let index = state.history.length - 1; index >= 0; index -= 1) {
      if (state.history[index].Role.toLowerCase() !== "user") continue;
      users += 1;
      if (users === MAX_HISTORY_TURNS) { retainedStart = index; break; }
    }
    if (retainedStart > 0) state.history.splice(0, retainedStart);
  }
  private publish(state: SessionState, item: PluginEvent): void { for (const listener of state.listeners) listener.push(item); }
  private promptBlocks(msg: InboundMessage): Array<Record<string, unknown>> {
    const blocks: Array<Record<string, unknown>> = [{ type: "text", text: msg.Text }];
    for (const attachment of msg.Attachments ?? []) {
      if (!attachment.LocalPath || !existsSync(attachment.LocalPath)) continue;
      blocks.push({ type: "resource_link", uri: `file://${attachment.LocalPath}`, name: attachment.Name, mimeType: attachment.MIMEType });
    }
    return blocks;
  }
  private completeRun(state: SessionState, run: Run, status: Run["status"], summary: string): void {
    if (run.final) return; run.status = status;
    const completedAt = now();
    for (const message of state.history) {
      if (message.Role !== "assistant" || !message.Progress || message.Progress.Status !== "running") continue;
      message.Status = status;
      message.Progress.Status = status;
      message.Progress.CompletedAt = completedAt;
      for (const step of message.Progress.Steps) if (step.Status === "running") step.Status = status === "completed" ? "completed" : "failed";
      message.UpdatedAt = completedAt;
      this.publishHistoryMessage(state, message);
    }
    for (const step of run.steps) if (step.Status === "running") step.Status = status === "completed" ? "completed" : "failed";
    const item = event("run.completed", status, summary, { native_session: nativeSession(state.id, state.cwd), run_id: run.id, final: true });
    run.final = item; run.resolve(item); this.publish(state, item);
  }
  private onUpdate(notification: AcpNotification): void {
    const state = this.rememberSession(notification.sessionId, this.sessions.get(notification.sessionId)?.cwd ?? process.cwd());
    const update = record(notification.update); const kind = nonEmpty(update.sessionUpdate); const run = state.run;
    if (kind === "user_message_chunk") {
      const text = textFromContent(update.content); if (!text) return;
      const messageID = nonEmpty(update.messageId) || `acp-user:${randomUUID()}`;
      let user = state.history.find((message) => message.ID === messageID);
      if (!user) {
        const createdAt = now();
        user = { ID: messageID, Role: "user", Type: "text", Content: "", Status: "completed", CreatedAt: createdAt, UpdatedAt: createdAt };
        state.history.push(user);
        this.trimHistory(state);
      }
      user.Content += text;
      user.UpdatedAt = now();
      this.publishHistoryMessage(state, user);
      return;
    }
    if (kind === "agent_message_chunk" || kind === "agent_thought_chunk") {
      const text = textFromContent(update.content); if (!text) return;
      if (run) { run.preview = `${run.preview}${text}`.slice(-1200); run.steps.push({ ID: `acp:${update.messageId ?? randomUUID()}:${run.steps.length}`, Kind: kind === "agent_thought_chunk" ? "status" : "assistant_text", Title: kind === "agent_thought_chunk" ? "Claude Code thinking" : "Claude Code response", Detail: text.slice(0, 1000), Status: "running", CreatedAt: now() }); }
      // Claude's thoughts are intentionally never forwarded as message content.
      // The progress message created at run start gives the Panel a safe, visible
      // activity state until a response or tool activity is available.
      if (kind === "agent_thought_chunk" && run) this.publishRunProgress(state, run);
      if (kind === "agent_message_chunk") {
        const messageID = nonEmpty(update.messageId) || `acp-message:${randomUUID()}`;
        let assistant = state.history.find((message) => message.ID === messageID);
        if (!assistant) {
          const createdAt = now();
          assistant = { ID: messageID, Role: "assistant", Type: "text", Content: "", Status: "running", CreatedAt: createdAt, UpdatedAt: createdAt, Progress: { Status: "running", StartedAt: createdAt, Steps: [] } };
          state.history.push(assistant);
          this.trimHistory(state);
        }
        assistant.Content += text; assistant.UpdatedAt = now();
        assistant.Progress?.Steps.push({ ID: `acp:${messageID}:${assistant.Progress?.Steps.length ?? 0}`, Kind: "assistant_text", Title: "Claude Code response", Detail: text.slice(0, 1000), Status: "running", CreatedAt: now() });
        this.publishHistoryMessage(state, assistant);
      }
      this.publish(state, event("assistant.delta", "running", text.slice(0, 240), { native_session: nativeSession(state.id, state.cwd), text, update: kind })); return;
    }
    if (kind === "tool_call" || kind === "tool_call_update") {
      const tool = nonEmpty(update.title) || nonEmpty(update.kind) || "Claude Code tool";
      if (run) {
        const callID = nonEmpty(update.toolCallId);
        const rawStatus = nonEmpty(update.status).toLowerCase();
        const status = rawStatus.includes("fail") || rawStatus.includes("error") ? "failed" : rawStatus.includes("complete") || rawStatus.includes("success") ? "completed" : "running";
        run.steps.push({ ID: `acp:${callID || randomUUID()}`, CallID: callID || undefined, Kind: "tool", Title: tool, Detail: nonEmpty(update.status), Status: status, CreatedAt: now() });
        const progress = this.runProgressMessage(state, run);
        const step = progress.Progress?.Steps.find((item) => callID && item.CallID === callID);
        if (step) { step.Title = tool; step.Status = status; }
        else progress.Progress?.Steps.push({ ID: `acp:tool:${callID || randomUUID()}`, CallID: callID || undefined, Kind: "tool", Title: tool, Detail: "", Status: status, CreatedAt: now() });
        this.publishRunProgress(state, run);
      }
      this.publish(state, event("tool.updated", "running", tool, { native_session: nativeSession(state.id, state.cwd), tool_call_id: nonEmpty(update.toolCallId), status: nonEmpty(update.status) || "running" })); return;
    }
    if (kind === "current_mode_update") { state.modes = { ...(state.modes ?? {}), current_mode_id: update.modeId }; }
    if (kind === "config_option_update") { state.configOptions = Array.isArray(update.configOptions) ? update.configOptions.map(record) : state.configOptions; }
    if (kind === "session_info_update") { state.title = nonEmpty(update.title) || state.title; state.updatedAt = nonEmpty(update.updatedAt) || now(); }
    this.publish(state, event("acp.update", "running", kind || "Claude Code update", { native_session: nativeSession(state.id, state.cwd), update: kind }));
  }
  private onPermission(request: AcpPermissionRequest): Promise<Record<string, unknown>> {
    const id = `acp-approval:${createHash("sha256").update(`${request.sessionId}:${nonEmpty(request.toolCall.toolCallId)}:${Date.now()}`).digest("hex").slice(0, 24)}`;
    const state = this.rememberSession(request.sessionId, this.sessions.get(request.sessionId)?.cwd ?? process.cwd());
    return new Promise((resolve) => {
      this.approvals.set(id, { id, sessionId: request.sessionId, options: request.options, resolve });
      this.publish(state, event("approval.required", "waiting_approval", nonEmpty(request.toolCall.title) || "Claude Code requires approval", { native_session: nativeSession(state.id, state.cwd), approval_request_id: id, tool_call_id: nonEmpty(request.toolCall.toolCallId), title: nonEmpty(request.toolCall.title), actions: request.options.map((option) => ({ id: nonEmpty(option.optionId), label: nonEmpty(option.name) || nonEmpty(option.optionId), available: true })) }));
    });
  }
  private async applyControl(id: string, action: string, target: unknown): Promise<void> {
    const normalized = action.toLowerCase().replace(/_/g, "."); const value = record(target);
    if (normalized === "mode.switch" || normalized === "permission.switch") { const mode = nonEmpty(value.mode_id) || nonEmpty(value.id) || nonEmpty(target); if (!mode) throw new Error("Claude Code mode id is required"); const response = await this.runtime.setMode(id, mode); this.updateControls(this.sessions.get(id)!, response); return; }
    if (normalized === "model.switch" || normalized === "reasoning.switch" || normalized === "plan.set") { const config = nonEmpty(value.config_id) || (normalized === "model.switch" ? "model" : normalized === "reasoning.switch" ? "effort" : "plan"); const raw = value.value ?? value.enabled ?? value.id ?? target; const scalar = typeof raw === "boolean" || typeof raw === "string" ? raw : ""; if (scalar === "") throw new Error("Claude Code control value is required"); const response = await this.runtime.setConfig(id, config, scalar); this.updateControls(this.sessions.get(id)!, response); return; }
    throw new Error(`Claude Code ACP does not support control action: ${action}`);
  }
}
