import test from "node:test";
import assert from "node:assert/strict";
import { ClaudeCodeAdapter, type AcpClient } from "../adapter.js";

function fakeRuntime(): AcpClient & { calls: Array<{ kind: string; args: unknown[] }> } {
  const calls: Array<{ kind: string; args: unknown[] }> = [];
  return {
    calls,
    async ensureStarted() { calls.push({ kind: "initialize", args: [] }); return {}; },
    async newSession(cwd) { calls.push({ kind: "new", args: [cwd] }); return { sessionId: "acp-test-session", configOptions: [] }; },
    async loadSession(sessionId, cwd) { calls.push({ kind: "load", args: [sessionId, cwd] }); return { sessionId }; },
    async listSessions() { return { sessions: [] }; },
    async prompt(sessionId, blocks) { calls.push({ kind: "prompt", args: [sessionId, blocks] }); return { stopReason: "end_turn" }; },
    async cancel(sessionId) { calls.push({ kind: "cancel", args: [sessionId] }); },
    async setMode(sessionId, mode) { calls.push({ kind: "mode", args: [sessionId, mode] }); return {}; },
    async setConfig(sessionId, config, value) { calls.push({ kind: "config", args: [sessionId, config, value] }); return {}; },
    async closeSession() {},
    async close() {},
  };
}

test("draft controls and send stay inside ACP runtime", async () => {
  const runtime = fakeRuntime();
  const adapter = new ClaudeCodeAdapter(runtime);
  const draft = await adapter.openDraft({ DraftID: "draft-1", PluginID: "claudecode", Cwd: process.cwd(), SourceDevice: "test", Metadata: {} });
  assert.equal(draft.DraftID, "draft-1");
  assert.match(draft.DraftFingerprint, /^[a-f0-9]{64}$/);
  await adapter.controlDraft({ DraftID: "draft-1", PluginID: "claudecode", Action: "model.switch", Target: { value: "sonnet" } });
  const started = await adapter.startDraftWithMessage({ DraftID: "draft-1", PluginID: "claudecode", Cwd: process.cwd(), SourceDevice: "test", Metadata: {}, Message: { PrismMessageID: "message-1", Text: "hello", SourceDevice: "test", Timestamp: new Date().toISOString(), Metadata: {} } });
  assert.equal(started.Receipt.Accepted, true);
  assert.equal(started.Receipt.NativeMessageID, "acp-run:message-1");
  assert.deepEqual(runtime.calls.map((call) => call.kind), ["new", "config", "prompt"]);
  assert.equal((await adapter.waitForRun(started.Session, "acp-run:message-1")).Status, "completed");
  await adapter.close();
});

test("live history stream replaces an in-progress Claude response for every chunk", async () => {
  const adapter = new ClaudeCodeAdapter(fakeRuntime());
  const session = await adapter.attachSession({
    PluginID: "claudecode",
    PrismConversationID: "conversation-1",
    NativeSessionID: "acp-test-session",
    NativeThreadID: "acp-test-session",
    Cwd: process.cwd(),
    SourceDevice: "test",
    Metadata: {},
  });
  const stream = adapter.readHistoryStream(session, { stream_id: "history-1", limit: 50, live: true })[Symbol.asyncIterator]();

  const pageEnd = await stream.next();
  assert.equal(pageEnd.value?.type, "page_end");

  const privateAdapter = adapter as unknown as { onUpdate(notification: { sessionId: string; update: Record<string, unknown> }): void };
  privateAdapter.onUpdate({
    sessionId: "acp-test-session",
    update: { sessionUpdate: "user_message_chunk", messageId: "user-1", content: { text: "现在是什么模型？" } },
  });

  const user = await stream.next();
  assert.equal(user.value?.turn?.turn_id, "user-1");
  assert.deepEqual(user.value?.turn?.messages.map((message: { Role: string; Content: string }) => [message.Role, message.Content]), [["user", "现在是什么模型？"]]);
  assert.equal(user.value?.operation, "append");

  privateAdapter.onUpdate({
    sessionId: "acp-test-session",
    update: { sessionUpdate: "agent_message_chunk", messageId: "assistant-1", content: { text: "正在" } },
  });

  const first = await stream.next();
  assert.equal(first.value?.turn?.turn_id, "user-1");
  assert.deepEqual(first.value?.turn?.messages.map((message: { Role: string; Content: string }) => [message.Role, message.Content]), [["user", "现在是什么模型？"], ["assistant", "正在"]]);
  assert.equal(first.value?.operation, "replace");

  privateAdapter.onUpdate({
    sessionId: "acp-test-session",
    update: { sessionUpdate: "agent_message_chunk", messageId: "assistant-1", content: { text: "回复" } },
  });

  const second = await stream.next();
  assert.equal(second.value?.turn?.turn_id, "user-1");
  assert.deepEqual(second.value?.turn?.messages.map((message: { Role: string; Content: string }) => [message.Role, message.Content]), [["user", "现在是什么模型？"], ["assistant", "正在回复"]]);
  assert.equal(second.value?.operation, "replace");
  assert.ok((second.value?.turn?.revision ?? 0) > (first.value?.turn?.revision ?? 0));

  const history = await adapter.readHistory(session, 50);
  assert.deepEqual(history.map((message) => [message.Role, message.Content]), [["user", "现在是什么模型？"], ["assistant", "正在回复"]]);

  await adapter.close();
});

test("Claude publishes a safe thinking state and named tool progress before its first visible response", async () => {
  const runtime = fakeRuntime();
  let finishPrompt!: () => void;
  runtime.prompt = async () => await new Promise((resolve) => { finishPrompt = () => resolve({ stopReason: "end_turn" }); });
  const adapter = new ClaudeCodeAdapter(runtime);
  const session = await adapter.attachSession({
    PluginID: "claudecode", PrismConversationID: "conversation-progress", NativeSessionID: "acp-test-session", NativeThreadID: "acp-test-session",
    Cwd: process.cwd(), SourceDevice: "test", Metadata: {},
  });
  const stream = adapter.readHistoryStream(session, { stream_id: "history-progress", limit: 20, live: true })[Symbol.asyncIterator]();
  assert.equal((await stream.next()).value?.type, "page_end");

  await adapter.send(session, { PrismMessageID: "user-progress", Text: "检查目录", SourceDevice: "test", Timestamp: new Date().toISOString(), Metadata: {} });
  const started = await stream.next();
  const progress = started.value?.turn?.messages[1] as { Type: string; Progress: { Steps: Array<{ ID: string; Kind: string; Title: string; Detail: string; Status: string; CreatedAt: string }> } };
  assert.equal(started.value?.turn?.messages.length, 2);
  assert.equal(progress.Type, "progress");
  assert.equal(progress.Progress.Steps.length, 1);
  assert.match(progress.Progress.Steps[0]!.ID, /^acp:thinking:/);
  assert.deepEqual(progress.Progress.Steps[0], { ID: progress.Progress.Steps[0]!.ID, Kind: "status", Title: "Claude Code thinking", Detail: "", Status: "running", CreatedAt: progress.Progress.Steps[0]!.CreatedAt });

  const privateAdapter = adapter as unknown as { onUpdate(notification: { sessionId: string; update: Record<string, unknown> }): void };
  privateAdapter.onUpdate({ sessionId: "acp-test-session", update: { sessionUpdate: "agent_thought_chunk", messageId: "thought-1", content: { text: "这是不应展示给用户的推理细节" } } });
  const thinking = await stream.next();
  const thinkingProgress = thinking.value?.turn?.messages[1] as { Progress: { Steps: Array<{ Detail: string }> } };
  assert.equal(thinkingProgress.Progress.Steps[0]?.Detail, "");

  privateAdapter.onUpdate({ sessionId: "acp-test-session", update: { sessionUpdate: "tool_call", toolCallId: "tool-1", title: "Terminal" } });
  const tool = await stream.next();
  const toolProgress = tool.value?.turn?.messages[1] as { Progress: { Steps: Array<{ ID: string; CallID?: string; Kind: string; Title: string; Detail: string; Status: string; CreatedAt: string }> } };
  assert.deepEqual(toolProgress.Progress.Steps.at(-1), { ID: "acp:tool:tool-1", CallID: "tool-1", Kind: "tool", Title: "Terminal", Detail: "", Status: "running", CreatedAt: toolProgress.Progress.Steps.at(-1)!.CreatedAt });

  finishPrompt();
  await adapter.waitForRun(session, "acp-run:user-progress");
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
    NativeSessionID: "acp-test-session",
    NativeThreadID: "acp-test-session",
    Cwd: process.cwd(),
    SourceDevice: "test",
    Metadata: {},
  });
  const privateAdapter = adapter as unknown as { onUpdate(notification: { sessionId: string; update: Record<string, unknown> }): void };
  for (const [messageId, sessionUpdate, text] of [
    ["user-1", "user_message_chunk", "第一个问题"],
    ["assistant-1", "agent_message_chunk", "第一个回答"],
    ["user-2", "user_message_chunk", "第二个问题"],
    ["assistant-2", "agent_message_chunk", "第二个回答"],
  ] as const) {
    privateAdapter.onUpdate({ sessionId: "acp-test-session", update: { sessionUpdate, messageId, content: { text } } });
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

test("discovering a session does not suppress its first ACP history replay", async () => {
  const runtime = fakeRuntime();
  runtime.listSessions = async () => ({ sessions: [{ sessionId: "acp-discovered-session", cwd: process.cwd() }] });
  const adapter = new ClaudeCodeAdapter(runtime);

  const sessions = await adapter.listSessions();
  assert.equal(sessions.length, 1);
  const session = {
    PluginID: "claudecode",
    NativeSessionID: "acp-discovered-session",
    NativeThreadID: "acp-discovered-session",
    Surface: "claudecode-acp",
    Endpoint: "local ACP",
    Cwd: process.cwd(),
    Visible: false,
  };
  await adapter.readHistory(session, 20);
  await adapter.readHistory(session, 20);

  assert.equal(runtime.calls.filter((call) => call.kind === "load").length, 1);
  await adapter.close();
});
