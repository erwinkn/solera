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

test("a slow asset detail response does not break the sheet", async ({
  page,
}) => {
  await login(page);
  let release!: () => void;
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  let intercepted = false;
  await page.route("**/api/projects/*/assets/site_feed", async (route) => {
    if (intercepted) return route.fallback();
    intercepted = true;
    await blocked;
    await route.fallback();
  });
  await page.getByRole("button", { name: "site_feed", exact: true }).click();
  // Catalog data renders immediately even while the detail fetch hangs.
  const sheet = page.getByRole("dialog", { name: "site_feed" });
  await expect(sheet).toBeVisible();
  await expect(sheet.getByRole("row", { name: /site_events/ })).toBeVisible();
  release();
  await expect(sheet.locator("[data-automation]").first()).toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("expired token returns to the login screen", async ({ page }) => {
  await login(page);
  await page.evaluate(() => sessionStorage.setItem("solera-token", "wrong"));
  await page.reload();
  await expect(page.getByLabel("API token", { exact: true })).toBeVisible();
});
