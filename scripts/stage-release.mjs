import { cp, mkdir, readFile, readdir, readlink, rm, symlink } from "node:fs/promises";
import { realpathSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = realpathSync(resolve(fileURLToPath(new URL("..", import.meta.url))));
const platform = process.env.PRISM_PLATFORM;
const manifest = await readFile(resolve(root, "pluginbridge-plugin.yaml"), "utf8");
const id = manifest.match(/^id:\s*(.+)$/m)?.[1]?.trim();
const version = manifest.match(/^version:\s*(.+)$/m)?.[1]?.trim();
if (!platform || !id || !version) throw new Error("release platform, id and version are required");
const destination = resolve(root, "release", `${id}-${version}-${platform}`);
await rm(destination, { recursive: true, force: true });
await mkdir(destination, { recursive: true });
const skip = new Set([".git", ".github", "release", "scripts", "test", "src", ".DS_Store", ".npmrc", ".gitignore"]);
for (const entry of await readdir(root)) {
  if (!skip.has(entry)) await cp(resolve(root, entry), resolve(destination, entry), { recursive: true });
}

// npm creates node_modules/.bin shims as absolute symlinks that resolve
// against the build machine's source tree. Archived verbatim they dangle on
// every user machine (and the Hub installer rejects them), so rewrite each
// link to a portable relative target inside the staged package.
async function normalizeSymlinks(dir) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = resolve(dir, entry.name);
    if (entry.isDirectory()) {
      await normalizeSymlinks(path);
      continue;
    }
    if (!entry.isSymbolicLink()) continue;
    const target = await readlink(path);
    let stagedTarget;
    if (target.startsWith("/")) {
      // Map the source-tree path onto its staged counterpart.
      let physical = target;
      try {
        physical = realpathSync(target);
      } catch {
        // A dangling absolute link cannot be mapped into the package.
        throw new Error(`staged symlink target is missing: ${path} -> ${target}`);
      }
      const inSource = relative(root, physical);
      if (inSource.startsWith("..")) throw new Error(`staged symlink escapes the package: ${path} -> ${target}`);
      stagedTarget = resolve(destination, inSource);
    } else {
      stagedTarget = resolve(dirname(path), target);
    }
    // The link must stay inside the staged package, though "../pkg/file"
    // targets inside the package are fine.
    if (relative(destination, stagedTarget).startsWith("..")) {
      throw new Error(`staged symlink escapes the package: ${path} -> ${target}`);
    }
    const rel = relative(dirname(path), stagedTarget);
    if (!rel) throw new Error(`staged symlink points at itself: ${path}`);
    if (rel !== target) {
      await rm(path, { force: true });
      await symlink(rel, path);
    }
  }
}
await normalizeSymlinks(destination);
console.log(destination);
