import { appendFile } from "node:fs/promises"
import { fileURLToPath } from "node:url"
import react from "@vitejs/plugin-react"
import { defineConfig, type Plugin } from "vite"

// `solera serve` listens on 8000 unless told otherwise.
const server = process.env.SOLERA_URL ?? "http://127.0.0.1:8000"
const notes = fileURLToPath(new URL("./.ui-notes.jsonl", import.meta.url))

/** Development only: each note left on the UI (see src/dev-notes.tsx) is
 * appended to .ui-notes.jsonl, one JSON line with the time it arrived. */
function uiNotes(): Plugin {
  return {
    name: "solera-ui-notes",
    apply: "serve",
    configureServer(vite) {
      vite.middlewares.use("/__ui-notes", (request, response) => {
        if (request.method !== "POST") {
          response.statusCode = 405
          response.end()
          return
        }
        let body = ""
        request.on("data", (chunk) => (body += chunk))
        request.on("end", async () => {
          try {
            const line = { at: new Date().toISOString(), ...JSON.parse(body) }
            await appendFile(notes, `${JSON.stringify(line)}\n`)
            response.statusCode = 204
          } catch {
            response.statusCode = 400
          }
          response.end()
        })
      })
    },
  }
}

export default defineConfig(({ command }) => ({
  // The Python server serves the bundle's files under /static/ and answers
  // every other path with index.html (python/solera_server/api.py).
  base: command === "build" ? "/static/" : "/",
  plugins: [react(), uiNotes()],
  build: {
    outDir: "../../python/solera_server/web",
    emptyOutDir: true,
    // The server's content security policy allows fonts from 'self' only,
    // so nothing may be inlined as a data: URL.
    assetsInlineLimit: 0,
    chunkSizeWarningLimit: 900,
  },
  server: {
    port: 5193,
    strictPort: true,
    proxy: { "/api": server, "/healthz": server },
  },
}))
