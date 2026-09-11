const {defineConfig, devices} = require('@playwright/test');
module.exports = defineConfig({
  testDir: '.', testMatch: '*.spec.js', timeout: 90000, workers: 1, retries: 0,
  use: {baseURL: 'http://127.0.0.1:8000', screenshot: 'only-on-failure', trace: 'retain-on-failure'},
  projects: [{name:'desktop', use:{...devices['Desktop Chrome']}}, {name:'mobile', use:{...devices['iPhone 13'], defaultBrowserType:'chromium'}}],
  webServer: {command: 'cd .. && uv run dorc serve', url:'http://127.0.0.1:8000/healthz', timeout:90000, env:{DORC_API_TOKEN:'test-browser-token', DORC_STATE_URL:'file:///tmp/dorc-browser-test', DORC_NAMESPACE:'browser'}}
});
