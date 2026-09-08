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
import {
  ClaudeSdkRuntime, fingerprint, newOpaqueID,
  type SdkClient, type SdkControlOption, type SdkPermissionDecision,
  type SdkPermissionRequest, type SdkTranscriptEntry, type SdkUpdate,
} from "./sdk-runtime.js";

export type { SdkClient } from "./sdk-runtime.js";

const PLUGIN_ID = "claudecode";
const SURFACE = "claudecode-sdk";
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
  controls?: Record<string, unknown>;
  listeners: Set<AsyncQueue<PluginEvent>>;
};

type Draft = { id: string; sessionId: string; cwd: string; fingerprint: string };
type PendingApproval = { id: string; sessionId: string; options: Array<Record<string, unknown>>; resolve: (decision: SdkPermissionDecision) => void };

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
  return { PluginID: PLUGIN_ID, NativeSessionID: id, NativeThreadID: id, Surface: SURFACE, Endpoint: "local Claude Agent SDK", Cwd: cwd, Visible: false };
}
function sessionID(session: NativeSession): string { return nonEmpty(session.NativeSessionID) || nonEmpty(session.NativeThreadID); }
function event(type: string, status: string, summary: string, payload: Record<string, unknown> = {}): PluginEvent {
  return { ID: `${PLUGIN_ID}:${randomUUID()}`, Type: type, Status: status, Summary: summary, Payload: payload, CreatedAt: now() };
}
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
    // agent output rather than presenting it as a synthetic conversation turn.
    if (role !== "assistant" || !current) continue;
    current.Messages.push(historyMessageSnapshot(message));
    current.MessageIDs.add(message.ID);
    // Message revisions are increased for every publication. Their sum is
    // therefore a monotonic revision for the complete user-anchored turn.
    current.Revision += historyMessageRevision(message);
  }
  return turns;
}

/** A protocol-native Claude Code adapter built directly on the Claude Agent SDK. */
export class ClaudeCodeAdapter implements PluginAdapter {
  private readonly sessions = new Map<string, SessionState>();
  private readonly drafts = new Map<string, Draft>();
  private readonly approvals = new Map<string, PendingApproval>();
  private readonly diagnostics: string[] = [];
  private readonly runtime: SdkClient;

  constructor(runtime?: SdkClient) {
    this.runtime = runtime ?? new ClaudeSdkRuntime({
      onUpdate: (sessionId, update) => this.onSdkUpdate(sessionId, update),
      onPermission: (request) => this.onPermission(request),
      onStderr: (line) => { this.diagnostics.push(line); if (this.diagnostics.length > 8) this.diagnostics.shift(); },
    });
  }

  id(): string { return PLUGIN_ID; }

  async probe(): Promise<Capability> {
    try {
      await this.runtime.probe();
      return this.capability(true, "");
    } catch (error) {
      return this.capability(false, safeSummary(error instanceof Error ? error.message : error, "Claude Agent SDK is unavailable"));
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
      PluginID: PLUGIN_ID, Surface: SURFACE, Endpoint: "local Claude Agent SDK streams", ProcessID: process.pid,
      SessionHints: { protocol: "claude-agent-sdk", agent: "@anthropic-ai/claude-agent-sdk" },
      Verified: capability.Available, Detail: capability.Available ? "Claude Agent SDK runtime initialized" : capability.UnavailableReason,
    };
  }

  async openDraft(req: DraftOpenRequest): Promise<DraftOpenResult> {
    const cwd = this.validCwd(req.Cwd);
    let sessionId: string;
    try {
      ({ sessionId } = await this.runtime.newSession(cwd));
    } catch (error) {
      const reason = safeSummary(error instanceof Error ? error.message : error, "Claude Agent SDK could not create a session");
      const diagnostic = this.diagnostics.at(-1);
      throw new Error(diagnostic ? `${reason} (${safeSummary(diagnostic, "SDK runtime error")})` : reason);
    }
    if (!sessionId) throw new Error("Claude Agent SDK did not return a session id for the draft");
    this.rememberSession(sessionId, cwd, true);
    const draft: Draft = { id: req.DraftID, sessionId, cwd, fingerprint: fingerprint(`${req.DraftID}:${sessionId}:${cwd}`) };
    this.drafts.set(req.DraftID, draft);
    return { DraftID: req.DraftID, Cwd: cwd, Controls: this.controls(sessionId), DraftFingerprint: draft.fingerprint };
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
    const summaries = await this.runtime.listSessions();
    const all: NativeSessionHint[] = [];
    for (const summary of summaries.slice(0, 500)) {
      const id = nonEmpty(summary.sessionId);
      if (!id) continue;
      this.rememberSession(id, nonEmpty(summary.cwd));
      all.push({
        PluginID: PLUGIN_ID, NativeSessionID: id, NativeThreadID: id, Surface: SURFACE, Endpoint: "local Claude Agent SDK", Cwd: nonEmpty(summary.cwd),
        Title: nonEmpty(summary.title) || "Claude Code session", PrismConversationID: "", Active: this.sessions.get(id)?.run?.status === "running", Visible: false,
        LastActivityAt: nonEmpty(summary.updatedAt) || now(), Metadata: { protocol: "claude-agent-sdk" },
      });
    }
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
    // Subscribe before reading the initial page so an agent chunk that arrives
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
    if (existing) return this.receipt(state.id, `sdk-run:${msg.PrismMessageID}`, true, "duplicate message already accepted");
    const createdAt = msg.Timestamp || now();
    this.appendHistory(state, { ID: msg.PrismMessageID, Role: "user", Type: "text", Content: msg.Text, Status: "completed", CreatedAt: createdAt, UpdatedAt: createdAt,
      Metadata: { marker: msg.PrismMessageID, attachments: (msg.Attachments ?? []).map((a) => ({ name: a.Name, mime_type: a.MIMEType })) } });
    let resolveRun!: (event: PluginEvent) => void;
    const run: Run = { id: `sdk-run:${msg.PrismMessageID}`, startedAt: now(), progressMessageID: `sdk-progress:${msg.PrismMessageID}`, status: "running", steps: [], preview: "", done: new Promise((resolve) => { resolveRun = resolve; }), resolve: resolveRun };
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
        Steps: [{ ID: `sdk:thinking:${run.id}`, Kind: "status", Title: "Claude Code thinking", Detail: "", Status: "running", CreatedAt: run.startedAt }],
      },
    });
    this.publish(state, event("run.started", "running", "Claude Code is processing", { native_session: nativeSession(state.id, state.cwd), run_id: run.id }));
    const blocks = this.promptBlocks(msg);
    void this.runtime.prompt(state.id, state.cwd, blocks).then(
      (response) => this.completeRun(state, run, "completed", `Claude Code completed: ${nonEmpty(response.stopReason) || "finished"}`),
      (error) => this.completeRun(state, run, run.status === "interrupted" ? "interrupted" : "failed", safeSummary(error instanceof Error ? error.message : error, "Claude Agent SDK failed")),
    );
    // PluginBridge uses NativeMessageID as the opaque identifier it later
    // passes to waitForRun. For the SDK runtime that identifier is the run ID,
    // not the Prism user-message ID.
    return this.receipt(state.id, run.id, true, "accepted by Claude Code");
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
    return { status: run.status, phase: { protocol: "claude-agent-sdk" }, preview: run.preview, steps: run.steps as unknown as Record<string, unknown>[], interruptible: run.status === "running", approval_blocked: Array.from(this.approvals.values()).some((a) => a.sessionId === state.id), started_at: run.startedAt, completed_at: run.final?.CreatedAt, duration_ms: run.final ? Date.parse(run.final.CreatedAt) - Date.parse(run.startedAt) : undefined };
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
    this.approvals.delete(approval.id);
    approval.resolve(this.decisionFor(nonEmpty(req.ActionID), approval.options));
  }

  async verifyVisibility(session: NativeSession, marker: string): Promise<VisibilityResult> {
    const state = await this.ensureSession(session); const found = state.history.some((message) => message.ID === marker || message.Metadata?.marker === marker);
    return { Visible: found, Marker: marker, Evidence: "SDK session history", CheckedAt: now(), FailureReason: found ? "" : "marker was not found in the local SDK projection" };
  }

  async close(): Promise<void> { await this.runtime.close(); for (const state of this.sessions.values()) for (const listener of state.listeners) listener.end(); }

  private decisionFor(actionID: string, options: Array<Record<string, unknown>>): SdkPermissionDecision {
    if (actionID === "allow_always" || options.some((option) => nonEmpty(option.optionId) === "allow_always" && nonEmpty(option.optionId) === actionID)) {
      return { outcome: "allow", remember: true };
    }
    if (actionID.startsWith("allow")) return { outcome: "allow", remember: false };
    return { outcome: "deny", message: "Denied in Prism" };
  }

  private validCwd(cwd: string): string { const value = cwd.trim(); if (!value) return process.cwd(); return isAbsolute(value) ? value : resolve(value); }
  private receipt(id: string, messageID: string, visible: boolean, detail: string): SendReceipt { return { NativeMessageID: messageID, CanonicalNativeSessionID: id, CanonicalNativeThreadID: id, Accepted: true, Visible: visible, Detail: detail }; }
  private rememberSession(id: string, cwd: string, historyLoaded = false): SessionState {
    const current = this.sessions.get(id);
    if (current) {
      current.cwd = cwd || current.cwd;
      if (historyLoaded) current.historyLoaded = true;
      current.controls = this.controls(id);
      return current;
    }
    const state: SessionState = { id, cwd, title: "Claude Code session", updatedAt: now(), history: [], historyLoaded, controls: this.controls(id), listeners: new Set() };
    this.sessions.set(id, state); return state;
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
      let entries: SdkTranscriptEntry[] = [];
      try {
        entries = await this.runtime.readTranscript(id);
      } catch {
        // A session the SDK store cannot read is treated as a fresh
        // conversation surface rather than a hard attach failure.
      }
      state.history = entries.map((entry) => this.transcriptMessage(entry));
      this.trimHistory(state);
      return this.rememberSession(id, cwd, true);
    })();
    state.historyLoading = loading;
    try {
      return await loading;
    } finally {
      if (state.historyLoading === loading) state.historyLoading = undefined;
    }
  }
  private transcriptMessage(entry: SdkTranscriptEntry): HistoryMessage {
    const stamp = entry.timestamp || now();
    if (entry.role === "user") {
      return { ID: entry.uuid, Role: "user", Type: "text", Content: entry.text, Status: "completed", CreatedAt: stamp, UpdatedAt: stamp };
    }
    const steps: HistoryProgressStep[] = entry.toolUses.map((tool) => ({
      ID: `sdk:tool:${tool.callId}`, CallID: tool.callId, Kind: "tool", Title: tool.title, Detail: "", Status: tool.failed ? "failed" : "completed", CreatedAt: stamp,
    }));
    return {
      ID: entry.uuid, Role: "assistant", Type: "text", Content: entry.text, Status: "completed", CreatedAt: stamp, UpdatedAt: stamp,
      ...(steps.length > 0 ? { Progress: { Status: "completed", StartedAt: stamp, CompletedAt: stamp, Steps: steps } } : {}),
    };
  }
  private controls(id: string): Record<string, unknown> {
    const snapshot = this.runtime.controls(id);
    return {
      protocol: snapshot.protocol,
      modes: { current_mode_id: snapshot.currentPermissionMode ?? "" },
      current_model: snapshot.currentModel ?? "",
      model_options: snapshot.models,
      current_reasoning: snapshot.currentEffort ?? "",
      reasoning_options: snapshot.efforts,
      current_permission: snapshot.currentPermissionMode ?? "",
      permission_options: snapshot.permissionModes,
      config_options: [
        { configId: "model", title: "Model", type: "string", options: snapshot.models, current: snapshot.currentModel ?? "" },
        { configId: "effort", title: "Reasoning effort", type: "string", options: snapshot.efforts, current: snapshot.currentEffort ?? "" },
      ],
    };
  }
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
        Steps: [{ ID: `sdk:thinking:${run.id}`, Kind: "status", Title: "Claude Code thinking", Detail: "", Status: "running", CreatedAt: run.startedAt }],
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
      const isImage = attachment.MIMEType.startsWith("image/");
      blocks.push(isImage
        ? { type: "image", path: attachment.LocalPath, mime: attachment.MIMEType, name: attachment.Name }
        : { type: "file", path: attachment.LocalPath, mime: attachment.MIMEType, name: attachment.Name });
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
  private onSdkUpdate(sessionId: string, update: SdkUpdate): void {
    const state = this.rememberSession(sessionId, this.sessions.get(sessionId)?.cwd ?? process.cwd());
    const run = state.run;
    switch (update.kind) {
      case "session_ready":
      case "controls_update": {
        state.controls = this.controls(sessionId);
        this.publish(state, event("sdk.update", "running", "Claude Code controls updated", { native_session: nativeSession(state.id, state.cwd), controls: state.controls }));
        return;
      }
      case "external_user": {
        const text = update.text;
        if (!text) return;
        let user = state.history.find((message) => message.ID === update.messageId);
        if (!user) {
          const createdAt = now();
          user = { ID: update.messageId, Role: "user", Type: "text", Content: "", Status: "completed", CreatedAt: createdAt, UpdatedAt: createdAt };
          state.history.push(user);
          this.trimHistory(state);
        }
        user.Content += text;
        user.UpdatedAt = now();
        this.publishHistoryMessage(state, user);
        return;
      }
      case "assistant_text": {
        const text = update.text;
        if (!text) return;
        let assistant = state.history.find((message) => message.ID === update.messageId);
        if (!assistant) {
          const createdAt = now();
          assistant = { ID: update.messageId, Role: "assistant", Type: "text", Content: "", Status: "running", CreatedAt: createdAt, UpdatedAt: createdAt, Progress: { Status: "running", StartedAt: createdAt, Steps: [] } };
          state.history.push(assistant);
          this.trimHistory(state);
        }
        assistant.Content += text; assistant.UpdatedAt = now();
        assistant.Progress?.Steps.push({ ID: `sdk:${update.messageId}:${assistant.Progress?.Steps.length ?? 0}`, Kind: "assistant_text", Title: "Claude Code response", Detail: text.slice(0, 1000), Status: "running", CreatedAt: now() });
        this.publishHistoryMessage(state, assistant);
        return;
      }
      case "assistant_text_commit": {
        const existing = state.history.find((message) => message.ID === update.messageId);
        const provisional = state.history.find((message) => message.ID === update.provisionalId);
        if (provisional) {
          // The complete assistant message is authoritative: adopt its stable
          // transcript uuid and full text, replacing the streamed deltas.
          provisional.ID = update.messageId;
          provisional.Content = update.text;
          provisional.UpdatedAt = now();
          this.publishHistoryMessage(state, provisional);
          return;
        }
        if (existing) return;
        this.onSdkUpdate(sessionId, { kind: "assistant_text", messageId: update.messageId, text: update.text });
        return;
      }
      case "assistant_delta": {
        const text = update.delta;
        if (!text) return;
        if (run) { run.preview = `${run.preview}${text}`.slice(-1200); }
        this.publish(state, event("assistant.delta", "running", text.slice(0, 240), { native_session: nativeSession(state.id, state.cwd), text, update: "assistant_delta" }));
        return;
      }
      case "thought_delta": {
        const text = update.delta;
        if (!text || !run) return;
        run.preview = `${run.preview}${text}`.slice(-1200);
        run.steps.push({ ID: `sdk:thought:${run.steps.length}`, Kind: "status", Title: "Claude Code thinking", Detail: text.slice(0, 1000), Status: "running", CreatedAt: now() });
        // Claude's thoughts are intentionally never forwarded as message
        // content. The progress message created at run start gives the Panel a
        // safe, visible activity state until a response or tool activity
        // arrives.
        this.publishRunProgress(state, run);
        return;
      }
      case "tool_update": {
        const tool = update.title || "Claude Code tool";
        if (run) {
          run.steps.push({ ID: `sdk:${update.callId || randomUUID()}`, CallID: update.callId || undefined, Kind: update.status === "completed" || update.status === "failed" ? "tool_result" : "tool", Title: tool, Detail: update.detail, Status: update.status, CreatedAt: now() });
          const progress = this.runProgressMessage(state, run);
          const step = progress.Progress?.Steps.find((item) => update.callId && item.CallID === update.callId);
          if (step) { step.Title = tool; step.Status = update.status; step.Detail = update.detail; }
          else progress.Progress?.Steps.push({ ID: `sdk:tool:${update.callId || randomUUID()}`, CallID: update.callId || undefined, Kind: "tool", Title: tool, Detail: update.detail, Status: update.status, CreatedAt: now() });
          this.publishRunProgress(state, run);
        }
        this.publish(state, event("tool.updated", update.status === "running" ? "running" : update.status, tool, { native_session: nativeSession(state.id, state.cwd), tool_call_id: update.callId, status: update.status, detail: update.detail }));
        return;
      }
    }
  }
  private onPermission(request: SdkPermissionRequest): Promise<SdkPermissionDecision> {
    const id = `sdk-approval:${createHash("sha256").update(`${request.sessionId}:${request.toolUseId}:${request.requestId}`).digest("hex").slice(0, 24)}`;
    const state = this.rememberSession(request.sessionId, this.sessions.get(request.sessionId)?.cwd ?? process.cwd());
    const options: Array<Record<string, unknown>> = [
      { optionId: "allow_once", name: "Allow once", kind: "allow_once" },
      ...(request.suggestions.length > 0 ? [{ optionId: "allow_always", name: "Always allow", kind: "allow_always" }] : []),
      { optionId: "reject_once", name: "Deny", kind: "reject_once" },
    ];
    return new Promise((resolve) => {
      this.approvals.set(id, { id, sessionId: request.sessionId, options, resolve });
      this.publish(state, event("approval.required", "waiting_approval", request.title || request.displayName || `${request.toolName} requires approval`, {
        native_session: nativeSession(state.id, state.cwd), approval_request_id: id, tool_call_id: request.toolUseId,
        title: request.title || request.displayName, detail: request.detail,
        actions: options.map((option) => ({ id: nonEmpty(option.optionId), label: nonEmpty(option.name) || nonEmpty(option.optionId), available: true })),
      }));
    });
  }
  private async applyControl(id: string, action: string, target: unknown): Promise<void> {
    const normalized = action.toLowerCase().replace(/_/g, ".");
    const value = target && typeof target === "object" ? target as Record<string, unknown> : {};
    if (normalized === "mode.switch" || normalized === "permission.switch") {
      const mode = nonEmpty(value.mode_id) || nonEmpty(value.id) || (typeof target === "string" ? target : "");
      if (!mode) throw new Error("Claude Code mode id is required");
      await this.runtime.setMode(id, mode);
      this.refreshControls(id);
      return;
    }
    if (normalized === "model.switch" || normalized === "reasoning.switch") {
      const config = normalized === "model.switch" ? "model" : "effort";
      const raw = value.value ?? value.id ?? (typeof target === "string" ? target : "");
      if (typeof raw !== "string" || !raw) throw new Error("Claude Code control value is required");
      await this.runtime.setConfig(id, config, raw);
      this.refreshControls(id);
      return;
    }
    if (normalized === "plan.set") {
      const enabled = value.enabled ?? value.value;
      await this.runtime.setMode(id, enabled === false || enabled === "false" ? "default" : "plan");
      this.refreshControls(id);
      return;
    }
    throw new Error(`Claude Agent SDK does not support control action: ${action}`);
  }
  private refreshControls(id: string): void {
    const state = this.sessions.get(id);
    if (state) state.controls = this.controls(id);
  }
}
