// Syncs the built SPA bundle into the Python package so `uv build` embeds it
// and `solera serve` can expose it under /static/.
import { cpSync, existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ui = join(dirname(fileURLToPath(import.meta.url)), "..");
const source = join(ui, "dist", "client");
const target = join(ui, "..", "..", "python", "solera_server", "web");

if (!existsSync(join(source, "index.html"))) {
  console.error("SPA build output missing index.html at", source);
  process.exit(1);
}
rmSync(target, { recursive: true, force: true });
mkdirSync(target, { recursive: true });
for (const entry of readdirSync(source)) {
  cpSync(join(source, entry), join(target, entry), { recursive: true });
}
console.log(`Synced console bundle → ${target}`);
