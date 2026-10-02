import { defineConfig, devices } from "@playwright/test";

// The console against a real server running the demo project, on desktop and
// mobile. The server is built and started once; tests share its state.
const port = 8013;

export default defineConfig({
  testDir: "./tests",
  testMatch: "*.spec.ts",
  timeout: 90_000,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  expect: { timeout: 15_000 },
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } } },
    { name: "mobile", use: { ...devices["iPhone 13"], defaultBrowserType: "chromium" } },
  ],
  webServer: {
    command: `pnpm run build && cd ../.. && uv run solera serve --port ${port}`,
    url: `http://127.0.0.1:${port}/healthz`,
    timeout: 180_000,
    reuseExistingServer: !process.env.CI,
    env: {
      SOLERA_API_TOKEN: "test-browser-token",
      SOLERA_STATE_URL: `file:///tmp/solera-browser-test-${Date.now()}`,
      SOLERA_NAMESPACE: "browser",
      // A dirty work tree changes the git build identity under a running server.
      SOLERA_BUILD: "browser-test",
    },
  },
});
