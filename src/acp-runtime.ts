import { createHash, randomUUID } from "node:crypto";
import { execFile, spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { access, readdir, realpath } from "node:fs/promises";
import { constants } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Readable, Writable } from "node:stream";
import { promisify } from "node:util";
import { ClientSideConnection, ndJsonStream } from "@agentclientprotocol/sdk";

export type AcpNotification = { sessionId: string; update: Record<string, unknown> };
export type AcpPermissionRequest = {
  sessionId: string;
  toolCall: Record<string, unknown>;
  options: Array<Record<string, unknown>>;
};

export type AcpRuntimeHandlers = {
  onUpdate(notification: AcpNotification): void;
  onPermission(request: AcpPermissionRequest): Promise<Record<string, unknown>>;
  onStderr(line: string): void;
};

type ClientConnection = InstanceType<typeof ClientSideConnection>;

const pluginRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const execFileAsync = promisify(execFile);

function safeError(error: unknown): Error {
  const raw = error instanceof Error ? error.message : String(error);
  // ACP errors often contain a local command line. The hub error surface must
  // be useful without leaking a user path, token, or whole prompt.
  const message = raw
    .replace(/(?:sk-ant-|sk-|api[_-]?key=)[A-Za-z0-9_\-]{8,}/gi, "[redacted]")
    .replace(/\/Users\/[^\s:'\"]+/g, "[local path]")
    .replace(/\/home\/[^\s:'\"]+/g, "[local path]")
    .slice(0, 600);
  return new Error(message || "Claude Code ACP failed");
}

function localAgentBin(): string {
  const name = process.platform === "win32" ? "claude-agent-acp.cmd" : "claude-agent-acp";
  return join(pluginRoot, "node_modules", ".bin", name);
}

async function resolvedAgentEntrypoint(): Promise<string> {
  const agent = localAgentBin();
  try {
    // npm's .bin entry is normally a symlink. `node symlink` keeps the
    // symlink's directory for relative ESM imports, while executing the
    // resolved package entry preserves the agent's own `./acp-agent.js`
    // import base. This matters for plugins installed under ~/.prism too.
    const resolved = await realpath(agent);
    if (resolved !== agent) return resolved;

    // Tauri materialises npm's .bin symlink as a launcher file when it copies
    // resources into an .app. That launcher imports `./acp-agent.js` relative
    // to .bin and cannot start. Execute the package's real ESM entrypoint.
    const packagedEntrypoint = join(
      pluginRoot,
      "node_modules",
      "@agentclientprotocol",
      "claude-agent-acp",
      "dist",
      "index.js",
    );
    await access(packagedEntrypoint);
    return packagedEntrypoint;
  } catch {
    return agent;
  }
}

async function resolveClaudeExecutable(): Promise<string> {
  const explicit = process.env.CLAUDE_CODE_EXECUTABLE?.trim();
  if (explicit) return explicit;
  // Finder-launched apps on macOS usually do not inherit nvm's PATH. Resolve
  // through the user's login shell so a normal local Claude Code install still
  // works, while retaining the plain command fallback on other platforms.
  if (process.platform === "darwin") {
    try {
      const { stdout } = await execFileAsync("/bin/zsh", ["-lc", "command -v claude"], { timeout: 3000, maxBuffer: 4096 });
      const discovered = stdout.trim().split("\n").pop()?.trim();
      if (discovered?.startsWith("/")) return discovered;
    } catch { /* keep the portable PATH fallback */ }
  }
  // Finder/Tauri launches inherit a deliberately small PATH. A user-installed
  // Claude Code binary is commonly under nvm and is therefore invisible to
  // `command -v claude` in that environment. Resolve known user-local paths
  // before falling back to PATH, without executing a shell profile.
  const home = process.env.HOME?.trim() || homedir();
  // Prefer a real Claude Code installation. `~/.superset/bin/claude` is a
  // shell shim used by a separate tool and merely reports that `claude` is
  // missing when Finder starts Hub with a minimal PATH.
  const candidates = [join(home, ".local", "bin", "claude")];
  try {
    const versions = await readdir(join(home, ".nvm", "versions", "node"), { withFileTypes: true });
    for (const version of versions.filter((entry) => entry.isDirectory()).map((entry) => entry.name).sort().reverse()) {
      candidates.push(join(home, ".nvm", "versions", "node", version, "bin", "claude"));
    }
  } catch { /* nvm is optional */ }
  for (const candidate of candidates) {
    try {
      await access(candidate, constants.X_OK);
      return candidate;
    } catch { /* try the next known location */ }
  }
  return "claude";
}

export class ClaudeAcpRuntime {
  private child: ChildProcessWithoutNullStreams | null = null;
  private connection: ClientConnection | null = null;
  private initResponse: Record<string, unknown> | null = null;
  private starting: Promise<void> | null = null;
  private closed = false;

  constructor(private readonly handlers: AcpRuntimeHandlers) {}

  async ensureStarted(): Promise<Record<string, unknown>> {
    if (this.closed) throw new Error("Claude Code ACP runtime is closed");
    if (this.initResponse) return this.initResponse;
    if (!this.starting) this.starting = this.start();
    await this.starting;
    return this.initResponse ?? {};
  }

  private async start(): Promise<void> {
    const agent = await resolvedAgentEntrypoint();
    try {
      await access(agent, constants.X_OK);
    } catch {
      throw new Error("Claude Code ACP runtime is not installed; reinstall the bundled Claude Code plugin");
    }
    const claudeExecutable = await resolveClaudeExecutable();
    const child = spawn(process.execPath, [agent], {
      cwd: process.cwd(),
      env: { ...process.env, CLAUDE_CODE_EXECUTABLE: claudeExecutable },
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.child = child;
    child.on("error", (error) => this.handlers.onStderr(safeError(error).message));
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      for (const line of chunk.split("\n")) if (line.trim()) this.handlers.onStderr(line.trim().slice(0, 400));
    });
    const stream = ndJsonStream(
      Writable.toWeb(child.stdin) as WritableStream<Uint8Array>,
      Readable.toWeb(child.stdout) as ReadableStream<Uint8Array>,
    );
    this.connection = new ClientSideConnection(() => ({
      sessionUpdate: async (notification) => this.handlers.onUpdate(notification as unknown as AcpNotification),
      requestPermission: async (request) => this.handlers.onPermission(request as unknown as AcpPermissionRequest) as never,
    }), stream);
    try {
      this.initResponse = await this.connection.initialize({
        protocolVersion: 1,
        clientCapabilities: {},
        clientInfo: { name: "Prism Hub", version: "0.1.0" },
      }) as unknown as Record<string, unknown>;
    } catch (error) {
      await this.close();
      throw safeError(error);
    }
  }

  private async conn(): Promise<ClientConnection> {
    await this.ensureStarted();
    if (!this.connection) throw new Error("Claude Code ACP connection was not created");
    return this.connection;
  }

  async newSession(cwd: string): Promise<Record<string, unknown>> {
    return await (await this.conn()).newSession({ cwd, mcpServers: [] }) as unknown as Record<string, unknown>;
  }

  async loadSession(sessionId: string, cwd: string): Promise<Record<string, unknown>> {
    return await (await this.conn()).loadSession({ sessionId, cwd, mcpServers: [] }) as unknown as Record<string, unknown>;
  }

  async listSessions(cursor?: string): Promise<Record<string, unknown>> {
    return await (await this.conn()).listSessions(cursor ? { cursor } : {}) as unknown as Record<string, unknown>;
  }

  async prompt(sessionId: string, blocks: Array<Record<string, unknown>>): Promise<Record<string, unknown>> {
    return await (await this.conn()).prompt({ sessionId, prompt: blocks as never }) as unknown as Record<string, unknown>;
  }

  async cancel(sessionId: string): Promise<void> {
    await (await this.conn()).cancel({ sessionId });
  }

  async setMode(sessionId: string, modeId: string): Promise<Record<string, unknown>> {
    return await (await this.conn()).setSessionMode({ sessionId, modeId }) as unknown as Record<string, unknown>;
  }

  async setConfig(sessionId: string, configId: string, value: string | boolean): Promise<Record<string, unknown>> {
    const params = typeof value === "boolean"
      ? { sessionId, configId, type: "boolean" as const, value }
      : { sessionId, configId, value };
    return await (await this.conn()).setSessionConfigOption(params) as unknown as Record<string, unknown>;
  }

  async closeSession(sessionId: string): Promise<void> {
    try { await (await this.conn()).closeSession({ sessionId }); } catch { /* best effort */ }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    const child = this.child;
    this.child = null;
    this.connection = null;
    if (!child) return;
    child.stdin.end();
    await new Promise<void>((resolve) => {
      const timeout = setTimeout(() => { child.kill("SIGTERM"); resolve(); }, 1500);
      child.once("exit", () => { clearTimeout(timeout); resolve(); });
    });
  }
}

export function fingerprint(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function newOpaqueID(prefix: string): string {
  return `${prefix}:${randomUUID()}`;
}
