import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

// §12 console flows against the demo project (`cursus serve`): assets, runs,
// automations, sources, executors — desktop and mobile layouts.

async function login(page: Page) {
  await page.goto("/");
  await page
    .getByLabel("API token", { exact: true })
    .fill("test-browser-token");
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Assets", exact: true }),
  ).toBeVisible();
  await expect(page.getByLabel("Filter assets")).toBeVisible();
}

async function materialize(
  page: Page,
  asset: string,
  { upstream = false }: { upstream?: boolean } = {},
) {
  await page
    .locator("header")
    .getByRole("button", { name: /Materialize/ })
    .click();
  const dialog = page.getByRole("dialog", { name: "Materialize" });
  await dialog.locator(`[role="checkbox"][aria-label="${asset}"]`).click();
  if (upstream)
    await dialog
      .getByRole("switch", { name: "Materialize upstream first" })
      .click();
  await dialog
    .getByRole("button", { name: "Start materialization", exact: true })
    .click();
  const sheet = page.getByRole("dialog").last();
  // Wait for the run detail to load before interacting with its controls.
  await expect(sheet.getByText(/\d+ tasks?/).first()).toBeVisible();
  return sheet;
}

test("every page loads", async ({ page }) => {
  await login(page);
  for (const [link, heading] of [
    ["Runs", "Runs"],
    ["Automations", "Automations"],
    ["Sources", "Sources"],
    ["Executors", "Executors"],
    ["Storage", "Storage"],
    ["Assets", "Assets"],
  ] as const) {
    await page
      .getByRole("navigation", { name: "Main navigation" })
      .getByRole("link", { name: link })
      .click();
    await expect(
      page.getByRole("heading", { name: heading, exact: true }),
    ).toBeVisible();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBeTruthy();
  }
});

test("theme toggle switches light and dark", async ({ page }) => {
  await login(page);
  // Two theme toggles exist (sidebar on desktop, header on mobile); the visible
  // one depends on the viewport.
  const dark = page
    .getByRole("radio", { name: "Dark" })
    .filter({ visible: true });
  const light = page
    .getByRole("radio", { name: "Light" })
    .filter({ visible: true });
  await dark.click();
  await expect(page.locator("html")).toHaveClass(/dark/);
  await light.click();
  await expect(page.locator("html")).not.toHaveClass(/dark/);
});

test("lineage graph shows sources and edge kinds", async ({ page }) => {
  await login(page);
  await page.getByRole("radio", { name: "Graph" }).click();
  const graph = page.getByLabel("Asset lineage graph");
  await expect(graph).toBeVisible();
  // Sources render as nodes alongside assets.
  await expect(
    graph.getByRole("button", { name: "Inspect uploads" }),
  ).toBeVisible();
  await expect(
    graph.getByRole("button", { name: "Inspect site_feed" }),
  ).toBeVisible();
  // The edge-kind legend names every consumption kind.
  for (const kind of ["whole", "Incremental", "AllPartitions", "dep"])
    await expect(page.getByText(kind, { exact: true })).toBeVisible();
  // Clicking a source node opens its card on the Sources page.
  await graph.getByRole("button", { name: "Inspect uploads" }).click();
  await expect(page).toHaveURL(/\/sources#source-uploads$/);
  await expect(page.locator('[data-source="uploads"]')).toBeInViewport();
});

test("materialize latest from the dialog and watch the run", async ({
  page,
}) => {
  await login(page);
  const sheet = await materialize(page, "sites");
  await expect(sheet.locator('[data-status="succeeded"]').first()).toBeVisible({
    timeout: 60000,
  });
});

test("pick individual cells from the partition grid", async ({ page }) => {
  await login(page);
  // Open a partitioned asset and materialize one scope straight off the grid.
  await page.getByRole("button", { name: "site_feed", exact: true }).click();
  const asset = page.getByRole("dialog", { name: "site_feed" });
  await expect(asset.locator("[data-scope]").first()).toBeVisible({
    timeout: 30000,
  });
  await asset.locator("[data-scope]").first().click();
  const dialog = page.getByRole("dialog", { name: "Materialize" });
  await expect(dialog.getByRole("radio", { name: "pick" })).toHaveAttribute(
    "aria-checked",
    "true",
  );
  await expect(dialog.getByText(/Pick cells — \d+ selected/)).toBeVisible();
});

test("partition grid, attempt logs, upstream run", async ({ page }) => {
  await login(page);
  // site_feed is partitioned on the `sites` set; --upstream plans `sites`
  // first so the set exists before the per-site scopes run.
  const sheet = await materialize(page, "site_feed", { upstream: true });
  await expect(sheet.locator('[data-status="succeeded"]').first()).toBeVisible({
    timeout: 60000,
  });
  // Attempt logs live-tail: site_feed logs "polled" via ctx.log.
  const feedTask = sheet.locator("section", { hasText: "site_feed" }).first();
  await feedTask.locator("summary").first().click();
  await expect(sheet.getByLabel("Attempt logs").first()).toContainText(
    "polled",
    { timeout: 15000 },
  );
  await sheet.getByRole("button", { name: "Close", exact: true }).click();
  // The partition grid colors committed scopes complete.
  await page.getByRole("button", { name: "site_feed", exact: true }).click();
  const asset = page.getByRole("dialog", { name: "site_feed" });
  await expect(asset.locator('[data-status="complete"]').first()).toBeVisible({
    timeout: 30000,
  });
});

test("automation toggle and run-now", async ({ page }) => {
  await login(page);
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("link", { name: "Automations" })
    .click();
  const automation = page.locator('[data-automation="refresh-index"]');
  await expect(automation).toBeVisible();
  const toggle = automation.getByRole("switch");
  const before = await toggle.getAttribute("aria-checked");
  await toggle.click();
  await expect(toggle).toHaveAttribute(
    "aria-checked",
    before === "true" ? "false" : "true",
  );
  await toggle.click();
  await expect(toggle).toHaveAttribute("aria-checked", before ?? "true");
  await automation
    .getByRole("button", { name: "Run now", exact: true })
    .click();
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("link", { name: "Runs" })
    .click();
  await expect(
    page.locator("tbody tr", { hasText: "refresh-index" }).first(),
  ).toBeVisible({ timeout: 30000 });
});

test("source commit wakes downstream work", async ({ page }) => {
  await login(page);
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("link", { name: "Sources" })
    .click();
  const uploads = page.locator('[data-source="uploads"]');
  await expect(uploads).toBeVisible();
  const key = `up-${Date.now()}`;
  await uploads.getByLabel("uploads upsert").fill(`["${key}"]`);
  await uploads.getByRole("button", { name: "Commit", exact: true }).click();
  // The Every(30) automation with partitions="missing" plans the new key on
  // a pool placement; with no worker it stays queued — but the run exists.
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("link", { name: "Runs" })
    .click();
  await expect(
    page.locator("tbody tr", { hasText: "manual_ingest" }).first(),
  ).toBeVisible({ timeout: 45000 });
});

test("run cancellation", async ({ page }) => {
  await login(page);
  // Commit an upload so manual_ingest has a scope to plan.
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("link", { name: "Sources" })
    .click();
  const uploads = page.locator('[data-source="uploads"]');
  await uploads
    .getByLabel("uploads upsert")
    .fill(`["up-cancel-${Date.now()}"]`);
  await uploads.getByRole("button", { name: "Commit", exact: true }).click();
  // manual_ingest is pool-placed: without a worker it stays queued, which is
  // exactly the cancellable state.
  const sheet = await materialize(page, "manual_ingest");
  await sheet.getByRole("button", { name: "Cancel", exact: true }).click();
  await sheet
    .getByRole("button", { name: "Confirm cancel", exact: true })
    .click();
  await expect(sheet.locator('[data-status="canceled"]').first()).toBeVisible({
    timeout: 15000,
  });
});
