import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  testMatch: "*.spec.ts",
  timeout: 90000,
  workers: 1,
  retries: 0,
  expect: { timeout: 15000 },
  use: {
    baseURL: "http://127.0.0.1:8000",
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"] } },
    {
      name: "mobile",
      use: { ...devices["iPhone 13"], defaultBrowserType: "chromium" },
    },
  ],
  webServer: {
    command: "pnpm run build && cd ../.. && uv run dorc serve",
    url: "http://127.0.0.1:8000/healthz",
    timeout: 120000,
    reuseExistingServer: !process.env.CI,
    env: {
      DORC_API_TOKEN: "test-browser-token",
      DORC_STATE_URL: "file:///tmp/dorc-browser-test",
      DORC_NAMESPACE: "browser",
    },
  },
});
