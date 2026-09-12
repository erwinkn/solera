import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

async function login(page: Page) {
  await page.goto("/");
  await page
    .getByLabel("API token", { exact: true })
    .fill("test-browser-token");
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByLabel("Filter assets")).toBeVisible();
}

test("mixed JSON previews remain readable", async ({ page }) => {
  await login(page);
  await page.route("**/api/assets/sample_quality?*", (route) =>
    route.fulfill({
      json: {
        head: { commit_id: "component-test" },
        checkpoint: null,
        commit: null,
        preview: [{ value: 1 }, null, 42],
      },
    }),
  );
  await page
    .getByRole("button", { name: "sample_quality", exact: true })
    .click();
  const drawer = page.getByRole("dialog", { name: "sample_quality" });
  await drawer.getByRole("tab", { name: "Data", exact: true }).click();
  await expect(drawer.locator("pre").first()).toContainText("null");
  await expect(drawer.locator("pre").first()).toContainText("42");
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("late asset responses cannot replace the current drawer", async ({
  page,
}) => {
  await login(page);
  let release!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  let requested!: () => void;
  const started = new Promise<void>((resolve) => {
    requested = resolve;
  });
  await page.route("**/api/assets/source_files?*", async (route) => {
    requested();
    await blocked;
    await route
      .fulfill({
        json: { head: null, checkpoint: null, commit: null, preview: null },
      })
      .catch(() => {
        // The detail view aborts in-flight fetches on unmount, so the
        // request may already be gone by the time the block releases.
      });
  });
  await page.getByRole("button", { name: "source_files", exact: true }).click();
  await started;
  await page
    .getByRole("button", { name: "sample_quality", exact: true })
    .click();
  const drawer = page.getByRole("dialog", { name: "sample_quality" });
  await expect(drawer).toBeVisible();
  const arrived = page.waitForResponse((response) =>
    response.url().includes("/api/assets/source_files?"),
  );
  release();
  // The response may never arrive if the client aborted the request when the
  // drawer switched assets; either way the current drawer must be unaffected.
  await Promise.race([arrived.catch(() => null), page.waitForTimeout(1000)]);
  await expect(
    page.getByRole("dialog", { name: "sample_quality" }),
  ).toBeVisible();
});
