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
    command: "pnpm run build && cd ../.. && uv run cursus serve",
    url: "http://127.0.0.1:8000/healthz",
    timeout: 120000,
    reuseExistingServer: !process.env.CI,
    env: {
      CURSUS_API_TOKEN: "test-browser-token",
      CURSUS_STATE_URL: "file:///tmp/cursus-browser-test",
      CURSUS_NAMESPACE: "browser",
    },
  },
});
