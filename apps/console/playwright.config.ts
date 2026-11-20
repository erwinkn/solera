import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import { defineConfig } from "@playwright/test"

/* The tests drive the built bundle as the Python server serves it, so the
 * /static/ base, the deep links and the content security policy are all in
 * play. With SOLERA_CONSOLE_URL set they use that server; without it they
 * start one on the demo project, with its state in a fresh temporary folder,
 * and a second whose project (tests/signin.py) lets everyone only read. */

const external = process.env.SOLERA_CONSOLE_URL
const port = 8799
const readerPort = 8798 // console.spec.ts finds it here
const repository = fileURLToPath(new URL("../..", import.meta.url))
const serve = (port: number, options: string) => ({
  command: `uv run --project "${repository}" solera serve --no-reload --port ${port} ${options}`,
  cwd: mkdtempSync(join(tmpdir(), "solera-console-")),
  url: `http://127.0.0.1:${port}/healthz`,
  timeout: 180_000,
  reuseExistingServer: false,
})

export default defineConfig({
  testDir: "tests",
  timeout: 45_000,
  fullyParallel: false,
  workers: 1,
  reporter: "list",
  use: { baseURL: external ?? `http://127.0.0.1:${port}`, viewport: { width: 1360, height: 860 } },
  webServer: external
    ? undefined
    : [
        serve(port, "--insecure"),
        { ...serve(readerPort, "signin:project"), env: { PYTHONPATH: fileURLToPath(new URL("tests", import.meta.url)) } },
      ],
})
