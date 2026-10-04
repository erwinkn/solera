// Copies the built bundle into the Python package, so `uv build` embeds it
// and `solera serve` needs no Node: index.html at the root, hashed JS/CSS
// and fonts under assets/ (served at /static/assets/…).
import { cpSync, existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const app = join(dirname(fileURLToPath(import.meta.url)), "..");
const source = join(app, "dist");
const target = join(app, "..", "..", "python", "solera_server", "web");

if (!existsSync(join(source, "index.html"))) {
  console.error("build output is missing index.html at", source);
  process.exit(1);
}
rmSync(target, { recursive: true, force: true });
mkdirSync(target, { recursive: true });
for (const entry of readdirSync(source)) {
  cpSync(join(source, entry), join(target, entry), { recursive: true });
}
console.log(`synced console bundle → ${target}`);
