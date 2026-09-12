import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

async function login(page: Page) {
  await page.goto("/");
  await page
    .getByLabel("API token", { exact: true })
    .fill("test-browser-token");
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Asset catalog", exact: true }),
  ).toBeVisible();
  await expect(page.getByLabel("Filter assets")).toBeVisible();
}

test("catalog, filtering, storage, and responsive layout", async ({
  page,
}, testInfo) => {
  await login(page);
  await page.getByLabel("Filter assets").fill("sample_quality");
  await expect(page.locator("tbody tr")).toHaveCount(1);
  await page.getByLabel("Filter assets").fill("");
  await expect(page.locator("tbody tr")).toHaveCount(7);
  await page.screenshot({
    path: testInfo.outputPath("catalog.png"),
    fullPage: true,
  });
  await page.getByRole("link", { name: "Storage", exact: true }).click();
  await expect(
    page.getByText("Local filesystem", { exact: true }),
  ).toBeVisible();
  await expect(page.getByText("SlateDB 0.16", { exact: true })).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBeTruthy();
});

test("materialize a real multi-output DAG and inspect data", async ({
  page,
}, testInfo) => {
  await login(page);
  await page.getByRole("button", { name: "Materialize", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Materialize assets" });
  await dialog
    .locator('[role="checkbox"][aria-label="sample_quality"]')
    .click();
  await dialog
    .getByRole("button", { name: "Start materialization", exact: true })
    .click();
  const sheet = page.getByRole("dialog");
  await expect(sheet.locator('[data-status="succeeded"]').first()).toBeVisible({
    timeout: 70000,
  });
  await page.screenshot({
    path: testInfo.outputPath("run.png"),
    fullPage: true,
  });
  await sheet.getByRole("button", { name: "Close", exact: true }).click();
  await page
    .getByRole("button", { name: "sample_quality", exact: true })
    .click();
  const asset = page.getByRole("dialog", { name: "sample_quality" });
  await asset.getByRole("tab", { name: "Data", exact: true }).click();
  await expect(
    asset.getByRole("cell", { name: "Basalt A", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("asset.png"),
    fullPage: true,
  });
});

test("bounded backfill form submits real daily work", async ({ page }) => {
  await login(page);
  await page.getByRole("button", { name: "Materialize", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Materialize assets" });
  await dialog.locator('[role="checkbox"][aria-label="daily_report"]').click();
  await dialog.getByLabel("From", { exact: true }).fill("2026-01-01");
  await dialog.getByLabel("Through", { exact: true }).fill("2026-01-02");
  await dialog
    .getByRole("button", { name: "Start materialization", exact: true })
    .click();
  const sheet = page.getByRole("dialog");
  await expect(sheet.locator('[data-status="succeeded"]').first()).toBeVisible({
    timeout: 70000,
  });
  await expect(sheet.locator("details")).toHaveCount(4);
});

test("automation controls and validation", async ({ page }) => {
  await login(page);
  await page.getByRole("link", { name: "Automations", exact: true }).click();
  const automation = page.locator('[data-automation="refresh_laboratory"]');
  const toggle = automation.getByRole("switch");
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-checked", "true");
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-checked", "false");
  await automation
    .getByRole("button", { name: "Run now", exact: true })
    .click();
  const sheet = page.getByRole("dialog");
  await expect(sheet.locator("[data-status]").first()).toBeVisible();
  await sheet.getByRole("button", { name: "Close", exact: true }).click();
  await page.getByRole("button", { name: "Materialize", exact: true }).click();
  await page
    .getByRole("dialog", { name: "Materialize assets" })
    .getByRole("button", { name: "Start materialization", exact: true })
    .click();
  await expect(page.getByRole("alert")).toHaveText("Select at least one asset");
});
