# Claude Code ACP plugin

This is Prism Hub's protocol-native Claude Code adapter. It launches the
pinned `@agentclientprotocol/claude-agent-acp` executable as a child process
and speaks ACP over its private stdio channel. Its own stdin/stdout remains
reserved for PluginBridge, so it never uses Codex CDP or browser automation.

The plugin uses the local `claude` command by default. Set
`CLAUDE_CODE_EXECUTABLE` only when Claude Code is installed outside `PATH`.

Production packages run with Prism's shared Node 22 runtime and depend on the
published `@rokid/pluginbridge-plugin-sdk`; the SDK is not copied into a
plugin archive.

For standalone development:

```bash
npm ci --registry=https://registry.npmmirror.com
npm test
CLAUDE_CODE_EXECUTABLE="$(command -v claude)" node dist/index.js
```

Run `adapter.probe` first. It starts and initializes ACP but does not create a
Claude session or send a model prompt.
