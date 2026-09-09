import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";

test("catalog, filtering, lineage and keyboard-accessible drawer", async ({
  page,
}, info) => {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Asset catalog" }),
  ).toBeVisible();
  await page
    .getByRole("textbox", { name: "Filter assets" })
    .fill("nothing-matches");
  await expect(
    page.getByRole("heading", { name: "No matching assets" }),
  ).toBeVisible();
  await page.getByRole("textbox", { name: "Filter assets" }).fill("");
  await page.screenshot({
    path: info.outputPath("catalog.png"),
    fullPage: true,
  });
  await page.getByRole("button", { name: "Graph", exact: true }).click();
  await page
    .getByRole("button", { name: "Inspect measurements", exact: true })
    .click();
  const drawer = page.getByRole("dialog", { name: "measurements" });
  await expect(
    drawer.getByRole("heading", { name: "Definition", exact: true }),
  ).toBeVisible();
  await expect(
    drawer.getByText("Keyed inventory", { exact: true }),
  ).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(drawer).not.toBeVisible();
  await page.screenshot({
    path: info.outputPath("lineage.png"),
    fullPage: true,
  });
});

test("materializes real assets and inspects committed data", async ({
  page,
}, info) => {
  await page.goto("/");
  await page
    .getByRole("button", { name: "Materialize sample_quality", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Launch materialization", exact: true })
    .click();
  await expect(page).toHaveURL(/#\/runs\/[a-f0-9-]+$/);
  await expect(page.locator(".run-heading .badge")).toHaveText("Succeeded");
  await expect(
    page.getByRole("heading", { name: "Event log", exact: true }),
  ).toBeVisible();
  await page.screenshot({ path: info.outputPath("run.png"), fullPage: true });
  await page.goto("/#/assets/sample_quality");
  await page.getByRole("tab", { name: "Data", exact: true }).click();
  await expect(
    page.getByRole("cell", { name: "S-1001", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: info.outputPath("asset-data.png"),
    fullPage: true,
  });
});

test("creates and completes a bounded daily backfill", async ({ page }) => {
  await page.goto("/#/backfills");
  await page
    .getByRole("button", { name: "Create backfill", exact: true })
    .click();
  await page
    .getByLabel("Target asset", { exact: true })
    .selectOption("daily_report");
  await page.getByLabel("Start date", { exact: true }).fill("2026-08-20");
  await page.getByLabel("End date", { exact: true }).fill("2026-08-22");
  await page
    .getByRole("button", { name: "Launch backfill", exact: true })
    .click();
  await expect(page.locator(".run-heading .badge")).toHaveText("Succeeded");
  await expect(page.locator(".task-item")).toHaveCount(6);
});

test("automation controls persist and manual execution works", async ({
  page,
}) => {
  await page.goto("/#/automations");
  const toggle = page.getByRole("switch", {
    name: "Enable refresh_laboratory",
    exact: true,
  });
  await expect(toggle).toHaveAttribute("aria-checked", "false");
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-checked", "true");
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-checked", "false");
  await page
    .locator(".automation")
    .filter({ hasText: "refresh_laboratory" })
    .getByRole("button", { name: "Run now" })
    .click();
  await expect(page.locator(".run-heading .badge")).toHaveText("Succeeded");
});

test("catalog has no serious accessibility violations", async ({ page }) => {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Asset catalog" }),
  ).toBeVisible();
  const result = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
    .analyze();
  expect(
    result.violations.map((v) => ({
      id: v.id,
      nodes: v.nodes.map((n) => ({
        target: n.target,
        summary: n.failureSummary,
      })),
    })),
  ).toEqual([]);
});
