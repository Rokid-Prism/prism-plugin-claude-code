# Claude Code plugin (Claude Agent SDK)

This is Prism Hub's protocol-native Claude Code adapter. Since 0.2.0 it talks
to Claude Code directly through the official `@anthropic-ai/claude-agent-sdk`
(pinned `0.3.263`) instead of the previous ACP double-hop. The earlier design
launched `@agentclientprotocol/claude-agent-acp` as a child process and spoke
ACP over a private stdio channel; that whole protocol layer is gone. Its own
stdin/stdout remains reserved for PluginBridge, so it never uses Codex CDP or
browser automation.

Current behavior:

- one streaming-input `query()` process per active Claude session; idle
  streams are reaped after 5 minutes and transparently resumed on the next
  message
- session enumeration, transcript reads, and session info use the SDK's local
  store APIs (`listSessions`, `getSessionMessages`) — no subprocess involved
- drafts are pure plugin state: the session UUID is pre-generated at
  `openDraft`, model / effort / permission selections are recorded as pending
  options, and the CLI process only starts at `startDraftWithMessage`
- model, reasoning effort, and permission mode are applied both as spawn
  options and as live control requests (`setModel`, `applyFlagSettings`,
  `setPermissionMode`); the model list comes from the live session's
  `supportedModels` (with a static fallback)
- permission requests arrive through `canUseTool` and surface as standard
  `approval.required` events with `allow_once` / `allow_always` (when the SDK
  supplies permission suggestions) / `reject_once` actions
- partial-message streaming (`includePartialMessages`) drives live preview,
  thinking progress, and per-chunk assistant deltas; each completed assistant
  message is then committed under its stable transcript UUID
- attachments: images are inlined as base64 content blocks, other local files
  are referenced by path in a text line Claude Code can open with its own
  tools

The plugin bundles Claude Code through the SDK's platform packages, so it no
longer searches the user's shell PATH for a `claude` binary and no longer
depends on nvm/zsh discovery. Set `CLAUDE_CODE_EXECUTABLE` only to force a
specific Claude Code executable; it is validated at probe time. Login state is
still the machine's own `~/.claude`.

Production packages run with Prism's shared Node 22 runtime and depend on the
published `@rokid-prism/pluginbridge-plugin-sdk`; the SDK is not copied into a
plugin archive.

For standalone development:

```bash
npm ci --registry=https://registry.npmmirror.com
npm test
node dist/index.js
```

Run `adapter.probe` first. It verifies the SDK's session store is readable
(and that an explicit executable override, if any, exists) without creating a
Claude session or sending a model prompt.

Packaging note: the SDK ships per-platform CLI binaries via optional
dependencies (`@anthropic-ai/claude-agent-sdk-<platform>`); install on the
target platform so the right variant is present.
