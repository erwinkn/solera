import { appendFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

const api = process.env.SOLERA_SERVER_URL ?? "http://127.0.0.1:8000";

// UI notes (src/dev/notes.tsx): the dev server appends each event the toolbar
// posts to .ui-notes.jsonl, one JSON object per line. Replaying the lines by
// note id gives the notes as they stand; nothing else stores them.
const notes = (): Plugin => ({
  name: "ui-notes",
  apply: "serve",
  configureServer(server) {
    server.middlewares.use("/__ui-notes", (request, response) => {
      let body = "";
      request.on("data", (chunk) => (body += chunk));
      request.on("end", () => {
        const line = JSON.stringify({ at: new Date().toISOString(), ...JSON.parse(body) });
        appendFileSync(fileURLToPath(new URL(".ui-notes.jsonl", import.meta.url)), line + "\n");
        response.statusCode = 204;
        response.end();
      });
    });
  },
});

export default defineConfig(({ command }) => ({
  // The Python app serves the bundle under /static/; client routes live at /.
  base: command === "build" ? "/static/" : "/",
  resolve: { tsconfigPaths: true },
  server: {
    proxy: { "/api": api, "/healthz": api },
  },
  build: { outDir: "dist", emptyOutDir: true, chunkSizeWarningLimit: 400 },
  plugins: [tailwindcss(), react(), notes()],
}));
