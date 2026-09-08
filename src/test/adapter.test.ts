import test from "node:test";
import assert from "node:assert/strict";
import { ClaudeCodeAdapter, type SdkClient } from "../adapter.js";
import type { SdkControlsSnapshot, SdkPermissionDecision, SdkPermissionRequest, SdkUpdate } from "../sdk-runtime.js";

function fakeControls(): SdkControlsSnapshot {
  return {
    protocol: "claude-agent-sdk",
    models: [{ id: "default", label: "Default" }, { id: "sonnet", label: "Sonnet" }],
    efforts: [{ id: "low", label: "Low" }, { id: "high", label: "High" }],
    permissionModes: [{ id: "default", label: "Default" }, { id: "plan", label: "Plan" }],
    currentModel: "default",
    currentEffort: null,
    currentPermissionMode: "default",
  };
}

function fakeRuntime(): SdkClient & { calls: Array<{ kind: string; args: unknown[] }> } {
  const calls: Array<{ kind: string; args: unknown[] }> = [];
  return {
    calls,
    async probe() { calls.push({ kind: "probe", args: [] }); },
    async newSession(cwd) { calls.push({ kind: "new", args: [cwd] }); return { sessionId: "sdk-test-session" }; },
    async listSessions() { return []; },
    async readTranscript(sessionId) { calls.push({ kind: "load", args: [sessionId] }); return []; },
    async prompt(sessionId, cwd, blocks) { calls.push({ kind: "prompt", args: [sessionId, cwd, blocks] }); return { stopReason: "end_turn" }; },
    async cancel(sessionId) { calls.push({ kind: "cancel", args: [sessionId] }); },
    async setMode(sessionId, mode) { calls.push({ kind: "mode", args: [sessionId, mode] }); },
    async setConfig(sessionId, config, value) { calls.push({ kind: "config", args: [sessionId, config, value] }); },
    controls() { return fakeControls(); },
    async closeSession() {},
    async close() {},
  };
}

function sessionFixture(id = "sdk-test-session") {
  return {
    PluginID: "claudecode",
    NativeSessionID: id,
    NativeThreadID: id,
    Surface: "claudecode-sdk",
    Endpoint: "local Claude Agent SDK",
    Cwd: process.cwd(),
    Visible: false,
  };
}

function messageFixture(id: string, text: string) {
  return { PrismMessageID: id, Text: text, SourceDevice: "test", Timestamp: new Date().toISOString(), Metadata: {} };
}

function privateAdapter(adapter: ClaudeCodeAdapter): {
  onSdkUpdate(sessionId: string, update: SdkUpdate): void;
  onPermission(request: SdkPermissionRequest): Promise<SdkPermissionDecision>;
} {
  return adapter as unknown as {
    onSdkUpdate(sessionId: string, update: SdkUpdate): void;
    onPermission(request: SdkPermissionRequest): Promise<SdkPermissionDecision>;
  };
}

test("draft controls and send stay inside the SDK runtime", async () => {
  const runtime = fakeRuntime();
  const adapter = new ClaudeCodeAdapter(runtime);
  const draft = await adapter.openDraft({ DraftID: "draft-1", PluginID: "claudecode", Cwd: process.cwd(), SourceDevice: "test", Metadata: {} });
  assert.equal(draft.DraftID, "draft-1");
  assert.match(draft.DraftFingerprint, /^[a-f0-9]{64}$/);
  assert.equal((draft.Controls as { protocol: string }).protocol, "claude-agent-sdk");
  await adapter.controlDraft({ DraftID: "draft-1", PluginID: "claudecode", Action: "model.switch", Target: { value: "sonnet" } });
  const started = await adapter.startDraftWithMessage({ DraftID: "draft-1", PluginID: "claudecode", Cwd: process.cwd(), SourceDevice: "test", Metadata: {}, Message: messageFixture("message-1", "hello") });
  assert.equal(started.Receipt.Accepted, true);
  assert.equal(started.Receipt.NativeMessageID, "sdk-run:message-1");
  assert.deepEqual(runtime.calls.map((call) => call.kind), ["new", "config", "prompt"]);
  assert.equal((await adapter.waitForRun(started.Session, "sdk-run:message-1")).Status, "completed");
  await adapter.close();
});

test("live history stream replaces an in-progress Claude response for every chunk", async () => {
  const adapter = new ClaudeCodeAdapter(fakeRuntime());
  const session = await adapter.attachSession({
    PluginID: "claudecode",
    PrismConversationID: "conversation-1",
    NativeSessionID: "sdk-test-session",
    NativeThreadID: "sdk-test-session",
    Cwd: process.cwd(),
    SourceDevice: "test",
    Metadata: {},
  });
  const stream = adapter.readHistoryStream(session, { stream_id: "history-1", limit: 50, live: true })[Symbol.asyncIterator]();

  const pageEnd = await stream.next();
  assert.equal(pageEnd.value?.type, "page_end");

  const priv = privateAdapter(adapter);
  priv.onSdkUpdate("sdk-test-session", { kind: "external_user", messageId: "user-1", text: "现在是什么模型？" });

  const user = await stream.next();
  assert.equal(user.value?.turn?.turn_id, "user-1");
  assert.deepEqual(user.value?.turn?.messages.map((message: { Role: string; Content: string }) => [message.Role, message.Content]), [["user", "现在是什么模型？"]]);
  assert.equal(user.value?.operation, "append");

  priv.onSdkUpdate("sdk-test-session", { kind: "assistant_text", messageId: "assistant-1", text: "正在" });

  const first = await stream.next();
  assert.equal(first.value?.turn?.turn_id, "user-1");
  assert.deepEqual(first.value?.turn?.messages.map((message: { Role: string; Content: string }) => [message.Role, message.Content]), [["user", "现在是什么模型？"], ["assistant", "正在"]]);
  assert.equal(first.value?.operation, "replace");

  priv.onSdkUpdate("sdk-test-session", { kind: "assistant_text", messageId: "assistant-1", text: "回复" });

  const second = await stream.next();
  assert.equal(second.value?.turn?.turn_id, "user-1");
  assert.deepEqual(second.value?.turn?.messages.map((message: { Role: string; Content: string }) => [message.Role, message.Content]), [["user", "现在是什么模型？"], ["assistant", "正在回复"]]);
  assert.equal(second.value?.operation, "replace");
  assert.ok((second.value?.turn?.revision ?? 0) > (first.value?.turn?.revision ?? 0));

  const history = await adapter.readHistory(session, 50);
  assert.deepEqual(history.map((message) => [message.Role, message.Content]), [["user", "现在是什么模型？"], ["assistant", "正在回复"]]);

  await adapter.close();
});

test("streamed assistant deltas are committed to the stable transcript message id", async () => {
  const adapter = new ClaudeCodeAdapter(fakeRuntime());
  const session = await adapter.attachSession({
    PluginID: "claudecode",
    PrismConversationID: "conversation-commit",
    NativeSessionID: "sdk-test-session",
    NativeThreadID: "sdk-test-session",
    Cwd: process.cwd(),
    SourceDevice: "test",
    Metadata: {},
  });
  const stream = adapter.readHistoryStream(session, { stream_id: "history-commit", limit: 20, live: true })[Symbol.asyncIterator]();
  await stream.next(); // page_end

  const priv = privateAdapter(adapter);
  priv.onSdkUpdate("sdk-test-session", { kind: "external_user", messageId: "user-commit", text: "写一段总结" });
  await stream.next();
  priv.onSdkUpdate("sdk-test-session", { kind: "assistant_text", messageId: "sdk-partial:block-1", text: "这是流式", provisional: true });
  await stream.next();
  priv.onSdkUpdate("sdk-test-session", { kind: "assistant_text", messageId: "sdk-partial:block-1", text: "增量内容", provisional: true });
  await stream.next();

  priv.onSdkUpdate("sdk-test-session", { kind: "assistant_text_commit", provisionalId: "sdk-partial:block-1", messageId: "assistant-final-1", text: "这是流式增量内容（完整）" });
  const committed = await stream.next();
  const assistant = committed.value?.turn?.messages.at(-1) as { ID: string; Content: string };
  assert.equal(assistant.ID, "assistant-final-1");
  assert.equal(assistant.Content, "这是流式增量内容（完整）");

  const history = await adapter.readHistory(session, 50);
  assert.deepEqual(history.map((message) => [message.ID, message.Content]).at(-1), ["assistant-final-1", "这是流式增量内容（完整）"]);
  await adapter.close();
});

test("Claude publishes a safe thinking state and named tool progress before its first visible response", async () => {
  const runtime = fakeRuntime();
  let finishPrompt!: () => void;
  runtime.prompt = async () => await new Promise((resolve) => { finishPrompt = () => resolve({ stopReason: "end_turn" }); });
  const adapter = new ClaudeCodeAdapter(runtime);
  const session = await adapter.attachSession({
    PluginID: "claudecode", PrismConversationID: "conversation-progress", NativeSessionID: "sdk-test-session", NativeThreadID: "sdk-test-session",
    Cwd: process.cwd(), SourceDevice: "test", Metadata: {},
  });
  const stream = adapter.readHistoryStream(session, { stream_id: "history-progress", limit: 20, live: true })[Symbol.asyncIterator]();
  assert.equal((await stream.next()).value?.type, "page_end");

  await adapter.send(session, messageFixture("user-progress", "检查目录"));
  const started = await stream.next();
  const progress = started.value?.turn?.messages[1] as { Type: string; Progress: { Steps: Array<{ ID: string; Kind: string; Title: string; Detail: string; Status: string; CreatedAt: string }> } };
  assert.equal(started.value?.turn?.messages.length, 2);
  assert.equal(progress.Type, "progress");
  assert.equal(progress.Progress.Steps.length, 1);
  assert.match(progress.Progress.Steps[0]!.ID, /^sdk:thinking:/);
  assert.deepEqual(progress.Progress.Steps[0], { ID: progress.Progress.Steps[0]!.ID, Kind: "status", Title: "Claude Code thinking", Detail: "", Status: "running", CreatedAt: progress.Progress.Steps[0]!.CreatedAt });

  const priv = privateAdapter(adapter);
  priv.onSdkUpdate("sdk-test-session", { kind: "thought_delta", delta: "这是不应展示给用户的推理细节" });
  const thinking = await stream.next();
  const thinkingProgress = thinking.value?.turn?.messages[1] as { Progress: { Steps: Array<{ Detail: string }> } };
  assert.equal(thinkingProgress.Progress.Steps[0]?.Detail, "");

  priv.onSdkUpdate("sdk-test-session", { kind: "tool_update", callId: "tool-1", title: "Terminal", status: "running", detail: "ls -la" });
  const tool = await stream.next();
  const toolProgress = tool.value?.turn?.messages[1] as { Progress: { Steps: Array<{ ID: string; CallID?: string; Kind: string; Title: string; Detail: string; Status: string; CreatedAt: string }> } };
  assert.deepEqual(toolProgress.Progress.Steps.at(-1), { ID: "sdk:tool:tool-1", CallID: "tool-1", Kind: "tool", Title: "Terminal", Detail: "ls -la", Status: "running", CreatedAt: toolProgress.Progress.Steps.at(-1)!.CreatedAt });

  finishPrompt();
  await adapter.waitForRun(session, "sdk-run:user-progress");
  const completed = await stream.next();
  const completedProgress = completed.value?.turn?.messages[1] as { Status: string; Progress: { Status: string } };
  assert.equal(completedProgress.Status, "completed");
  assert.equal(completedProgress.Progress.Status, "completed");
  await adapter.close();
});

test("initial history returns complete latest turns instead of raw messages", async () => {
  const adapter = new ClaudeCodeAdapter(fakeRuntime());
  const session = await adapter.attachSession({
    PluginID: "claudecode",
    PrismConversationID: "conversation-1",
    NativeSessionID: "sdk-test-session",
    NativeThreadID: "sdk-test-session",
    Cwd: process.cwd(),
    SourceDevice: "test",
    Metadata: {},
  });
  const priv = privateAdapter(adapter);
  for (const [messageId, kind, text] of [
    ["user-1", "external_user", "第一个问题"],
    ["assistant-1", "assistant_text", "第一个回答"],
    ["user-2", "external_user", "第二个问题"],
    ["assistant-2", "assistant_text", "第二个回答"],
  ] as const) {
    priv.onSdkUpdate("sdk-test-session", { kind, messageId, text } as SdkUpdate);
  }

  const stream = adapter.readHistoryStream(session, { stream_id: "history-2", limit: 1 })[Symbol.asyncIterator]();
  const latest = await stream.next();
  assert.equal(latest.value?.type, "turn");
  assert.equal(latest.value?.turn?.turn_id, "user-2");
  assert.deepEqual(latest.value?.turn?.messages.map((message: { Role: string; Content: string }) => [message.Role, message.Content]), [["user", "第二个问题"], ["assistant", "第二个回答"]]);
  assert.ok((latest.value?.turn?.revision ?? 0) >= 2);
  assert.equal((await stream.next()).value?.type, "end");

  await adapter.close();
});

test("discovering a session does not suppress its first SDK history read", async () => {
  const runtime = fakeRuntime();
  runtime.listSessions = async () => [{ sessionId: "sdk-discovered-session", title: "Discovered", cwd: process.cwd(), updatedAt: new Date().toISOString() }];
  const adapter = new ClaudeCodeAdapter(runtime);

  const sessions = await adapter.listSessions();
  assert.equal(sessions.length, 1);
  await adapter.readHistory(sessionFixture("sdk-discovered-session"), 20);
  await adapter.readHistory(sessionFixture("sdk-discovered-session"), 20);

  assert.equal(runtime.calls.filter((call) => call.kind === "load").length, 1);
  await adapter.close();
});

test("attach replays the SDK transcript as user-anchored turns with tool steps", async () => {
  const runtime = fakeRuntime();
  runtime.readTranscript = async () => [
    { uuid: "tu-1", role: "user", text: "检查目录", toolUses: [] },
    {
      uuid: "ta-1", role: "assistant", text: "目录检查完成",
      toolUses: [{ callId: "call-1", title: "Bash", failed: false }, { callId: "call-2", title: "Edit", failed: true }],
    },
  ];
  const adapter = new ClaudeCodeAdapter(runtime);
  const session = await adapter.attachSession({
    PluginID: "claudecode",
    PrismConversationID: "conversation-replay",
    NativeSessionID: "sdk-replay-session",
    NativeThreadID: "sdk-replay-session",
    Cwd: process.cwd(),
    SourceDevice: "test",
    Metadata: {},
  });
  const history = await adapter.readHistory(session, 20);
  assert.deepEqual(history.map((message) => [message.Role, message.ID, message.Content]), [
    ["user", "tu-1", "检查目录"],
    ["assistant", "ta-1", "目录检查完成"],
  ]);
  const steps = history[1]!.Progress!.Steps;
  assert.deepEqual(steps.map((step) => [step.CallID, step.Title, step.Status]), [
    ["call-1", "Bash", "completed"],
    ["call-2", "Edit", "failed"],
  ]);
  await adapter.close();
});

test("approval actions map to allow once, allow always, and deny", async () => {
  const adapter = new ClaudeCodeAdapter(fakeRuntime());
  const session = await adapter.attachSession({
    PluginID: "claudecode",
    PrismConversationID: "conversation-approval",
    NativeSessionID: "sdk-test-session",
    NativeThreadID: "sdk-test-session",
    Cwd: process.cwd(),
    SourceDevice: "test",
    Metadata: {},
  });
  const events = adapter.subscribe(session)[Symbol.asyncIterator]();
  const approvalEventPromise = events.next();
  // Give the subscription a tick to register its queue before publishing.
  await new Promise((resolve) => setTimeout(resolve, 20));

  const priv = privateAdapter(adapter);
  const pendingDecision = priv.onPermission({
    sessionId: "sdk-test-session",
    requestId: "req-1",
    toolUseId: "tool-9",
    toolName: "Bash",
    title: "Claude wants to run a command",
    displayName: "Run command",
    detail: "rm -rf /tmp/prism-test",
    suggestions: [{ type: "addRules", rules: [{ toolName: "Bash", ruleContent: "rm -rf /tmp/prism-test" }], behavior: "allow", destination: "session" }],
  });

  const approvalEvent = await approvalEventPromise;
  const payload = approvalEvent.value.Payload as { approval_request_id: string; actions: Array<{ id: string; label: string }> };
  assert.equal(approvalEvent.value.Type, "approval.required");
  assert.deepEqual(payload.actions.map((action) => action.id), ["allow_once", "allow_always", "reject_once"]);

  await adapter.resolveApproval({
    PrismConversationID: "conversation-approval",
    PluginID: "claudecode",
    Session: sessionFixture("sdk-test-session"),
    ApprovalRequestID: payload.approval_request_id,
    ActionID: "allow_always",
    SourceDevice: "test",
    Metadata: {},
  });
  assert.deepEqual(await pendingDecision, { outcome: "allow", remember: true });

  await assert.rejects(
    () => adapter.resolveApproval({
      PrismConversationID: "conversation-approval",
      PluginID: "claudecode",
      Session: sessionFixture("sdk-test-session"),
      ApprovalRequestID: payload.approval_request_id,
      ActionID: "allow_once",
      SourceDevice: "test",
      Metadata: {},
    }),
    /no longer pending/,
  );
  await adapter.close();
});
