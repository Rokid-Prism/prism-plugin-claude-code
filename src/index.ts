import { serve, checkProtocolVersion } from "@rokid/pluginbridge-plugin-sdk";
import { ClaudeCodeAdapter } from "./adapter.js";

checkProtocolVersion();
const adapter = new ClaudeCodeAdapter();

serve(adapter)
  .then(() => adapter.close())
  .catch((error) => {
    // stdout is exclusively reserved for PluginBridge JSON lines.
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
