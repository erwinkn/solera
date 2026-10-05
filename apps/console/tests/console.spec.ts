import { expect, test, type Page } from "@playwright/test";

// The console's main flows against the demo project (python/solera_server/demo.py),
// on desktop and mobile: connect, navigate, theme, materialize and inspect a
// run, cancel, keys and explain, automations, sources, health.

const TOKEN = "test-browser-token";

async function connect(page: Page, path = "/") {
  await page.goto(path);
  await page.getByLabel("API token", { exact: true }).fill(TOKEN);
  await page.getByRole("button", { name: "Connect", exact: true }).click();
}

const mobile = (page: Page) => (page.viewportSize()?.width ?? 1440) < 1024;

/** Follow a main-navigation link; on a phone it lives behind the menu button. */
async function nav(page: Page, name: string) {
  if (mobile(page)) await page.getByRole("button", { name: "Open navigation" }).click();
  await page
    .getByRole("navigation", { name: "Main" })
    .getByRole("link", { name, exact: false })
    .first()
    .click();
}

test("asks for a token, then shows the overview", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Connect to Solera" })).toBeVisible();
  await page.getByLabel("API token", { exact: true }).fill("wrong");
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByText("That token was refused")).toBeVisible();
  await page.getByLabel("API token", { exact: true }).fill(TOKEN);
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  await expect(page.getByText("Running now").first()).toBeVisible();
  await expect(page.getByRole("heading", { name: "Up next" })).toBeVisible();
});

test("every page loads from the navigation", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(e.message));
  await connect(page);
  for (const [link, heading] of [
    ["Assets", "Assets"],
    ["Runs", "Runs"],
    ["Automations", "Automations"],
    ["Sensors", "Sensors"],
    ["Sources", "Sources"],
    ["Executors", "Executors"],
    ["Health", "Health"],
    ["Overview", "Overview"],
  ] as const) {
    await nav(page, link);
    await expect(page.getByRole("heading", { name: heading, level: 1 })).toBeVisible();
  }
  expect(errors).toEqual([]);
});

test("the theme switches by tokens alone and persists", async ({ page }) => {
  await connect(page);
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  const look = () =>
    page.evaluate(() => {
      const card = document.querySelector("main section") as HTMLElement;
      const style = getComputedStyle(card);
      return [getComputedStyle(document.body).backgroundColor, style.borderRadius, style.boxShadow].join(
        " | ",
      );
    });
  const open = async () => {
    if (mobile(page)) await page.getByRole("button", { name: "Open navigation" }).click();
  };
  const seen = new Set([await look()]);
  for (const theme of ["cellar", "instrument", "observatory"]) {
    await open();
    await page.getByRole("combobox", { name: "Theme" }).selectOption(theme);
    await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
    if (mobile(page)) await page.keyboard.press("Escape");
    seen.add(await look());
  }
  expect(seen.size).toBe(4); // four looks, one component tree
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "observatory");
  await open();
  await page.getByRole("combobox", { name: "Theme" }).selectOption("normal");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "normal");
});

/** The API as the console calls it, with the test's token. */
const api = (path: string) => `/api/projects/demo${path}`;
const auth = { Authorization: `Bearer ${TOKEN}` };

/**
 * Pause an asset's automations and wait until none of its runs is live, so a
 * run submitted now has its partitions to itself instead of finding them
 * already running and planning nothing. Returns what to re-enable.
 */
async function quiesce(page: Page, asset: string): Promise<() => Promise<void>> {
  const { automations } = (await (await page.request.get(api("/automations"), { headers: auth })).json()) as {
    automations: { name: string; targets: string[]; enabled: boolean }[];
  };
  const paused = automations.filter((a) => a.enabled && a.targets.includes(asset)).map((a) => a.name);
  for (const name of paused) await page.request.post(api(`/automations/${name}/disable`), { headers: auth });
  await expect
    .poll(
      async () =>
        (
          (await (
            await page.request.get(api(`/runs?asset=${asset}&status=running&status=queued&limit=1`), {
              headers: auth,
            })
          ).json()) as { total: number }
        ).total,
      { timeout: 45_000 },
    )
    .toBe(0);
  return async () => {
    for (const name of paused) await page.request.post(api(`/automations/${name}/enable`), { headers: auth });
  };
}

/** site_feed's partitions are sites' keys: on a fresh namespace, make sure there are some. */
async function sitesExist(page: Page) {
  const count = async () => {
    const response = await page.request.get(api("/partitions/site_feed"), { headers: auth });
    return response.ok() ? ((await response.json()) as { partitions: unknown[] }).partitions.length : 0;
  };
  if ((await count()) > 0) return;
  await page.request.post(api("/runs"), { headers: auth, data: { targets: ["sites"], by: "test" } });
  await expect.poll(count, { timeout: 45_000 }).toBeGreaterThan(0);
}

test("materialize an asset and follow its run to the logs", async ({ page }) => {
  await sitesExist(page);
  // site_feed runs every 10 seconds on its own: hold that off, or our run finds its partitions running.
  const resume = await quiesce(page, "site_feed");
  try {
    await materialize(page);
  } finally {
    await resume();
  }
});

async function materialize(page: Page) {
  await connect(page, "/assets/site_feed");
  await expect(page.getByRole("heading", { name: "site_feed", level: 1 })).toBeVisible();
  await page.getByRole("button", { name: "Run", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Run" });
  await expect(dialog.getByRole("checkbox", { name: "site_feed" })).toBeChecked();
  await dialog.getByRole("radio", { name: "All" }).click();
  // Full: materializes every partition again, so the run has tasks even if site_feed is current.
  await dialog.getByRole("radio", { name: "Full" }).click();
  await dialog.getByRole("button", { name: "Start the run" }).click();
  await expect(page).toHaveURL(/\/runs\/[0-9A-Z]{26}/);
  await expect(page.getByRole("heading", { name: "site_feed", level: 1 })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Timeline" })).toBeVisible();
  await expect(page.getByText("succeeded").first()).toBeVisible({ timeout: 45_000 });
  await expect(page.getByRole("log")).toContainText("polled");
  await page.getByRole("tab", { name: "Spec" }).click();
  await expect(page.getByRole("tab", { name: "Spec" })).toHaveAttribute("aria-selected", "true");
  await expect(page).toHaveURL(/tab=spec/);
  await page.getByRole("tab", { name: "Events" }).click();
  await expect(page.getByText("committed").first()).toBeVisible();
}

test("commit to a source, then cancel the run waiting for a pool worker", async ({ page }, info) => {
  const upload = `e2e-${info.project.name}`; // the projects share one server
  await connect(page, "/sources/uploads");
  await expect(page.getByRole("heading", { name: "uploads", level: 1 })).toBeVisible();
  await page.getByPlaceholder(/u-1/).fill(upload);
  await page.getByRole("button", { name: "Commit", exact: true }).click();
  await expect(page.getByRole("cell", { name: upload })).toBeVisible();

  // manual_ingest runs on Pool("ingest"): with no worker, its attempt waits.
  await page.goto("/assets/manual_ingest");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Run" });
  await dialog.getByRole("radio", { name: "Pick…" }).click();
  await dialog.getByLabel("Partition keys").fill(upload);
  await dialog.getByRole("button", { name: "Start the run" }).click();
  await expect(page).toHaveURL(/\/runs\//);
  await page.getByRole("button", { name: "Cancel run" }).click();
  await page.getByRole("alertdialog").getByRole("button", { name: "Cancel run" }).click();
  await expect(page.getByText("canceled").first()).toBeVisible({ timeout: 45_000 });
  // A finished run never changes: retrying it is a new run that names it.
  const canceled = page.url();
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page).not.toHaveURL(canceled);
  await expect(page.getByText("Retries", { exact: true })).toBeVisible();
});

test("runs filter through the address bar", async ({ page }) => {
  await connect(page, "/runs");
  await expect(page.getByRole("heading", { name: "Runs", level: 1 })).toBeVisible();
  await page
    .getByRole("group", { name: "Status" })
    .getByRole("button", { name: /succeeded/ })
    .click();
  await expect(page).toHaveURL(/status=succeeded/);
  await expect(page.locator("tbody tr").first()).toContainText("succeeded");
  await page.getByRole("radio", { name: "1h" }).click();
  await expect(page).toHaveURL(/range=1h/);
  await page.reload();
  await expect(page.getByRole("button", { name: /succeeded/ })).toHaveAttribute("aria-pressed", "true");
});

test("assets graph, keys and explain", async ({ page }) => {
  await connect(page, "/assets");
  await page
    .getByRole("link", { name: /file_checks/ })
    .first()
    .click();
  await expect(page.getByRole("heading", { name: "file_checks", level: 1 })).toBeVisible();
  await page.getByRole("navigation", { name: "Asset sections" }).getByRole("link", { name: /Keys/ }).click();
  await expect(page.getByRole("heading", { name: "Key outcomes" })).toBeVisible();
  // alpha-file-2 is excluded by the input's "drafts" pattern: explain says so.
  await page.getByLabel("Key to explain").fill("alpha-file-2");
  await page.getByRole("button", { name: "Explain" }).click();
  await expect(page.getByText(/Excluded by the input's pattern/)).toBeVisible({ timeout: 45_000 });
  await page
    .getByRole("navigation", { name: "Asset sections" })
    .getByRole("link", { name: "Inputs" })
    .click();
  await expect(page.getByText("each=True").first()).toBeVisible();
  await page.goto("/assets?view=list");
  await expect(page.getByRole("link", { name: /file_index/ })).toBeVisible();
});

test("automations toggle and health", async ({ page }) => {
  await connect(page, "/automations");
  const toggle = page.getByRole("switch", { name: /refresh-index/ });
  await expect(toggle).toBeChecked();
  await toggle.click();
  await expect(page.getByRole("switch", { name: /Enable refresh-index/ })).not.toBeChecked();
  await page.reload();
  await page.getByRole("switch", { name: /Enable refresh-index/ }).click();
  await expect(page.getByRole("switch", { name: /Disable refresh-index/ })).toBeChecked();

  await page.goto("/health");
  await expect(page.getByRole("heading", { name: "Repairs owed" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Cleanups" })).toBeVisible();
  await expect(page.getByText("browser", { exact: true })).toBeVisible();
});

test("jump anywhere from the keyboard", async ({ page }) => {
  test.skip(mobile(page), "a keyboard shortcut");
  await connect(page);
  await expect(page.getByRole("heading", { name: "Overview", level: 1 })).toBeVisible();
  await page.keyboard.press("Control+k");
  const input = page.getByRole("combobox", { name: "Jump to" });
  await input.fill("file_ch");
  await expect(page.getByRole("option", { name: /file_checks/ }).first()).toHaveAttribute(
    "aria-selected",
    "true",
  );
  await input.press("Enter");
  await expect(page.getByRole("heading", { name: "file_checks", level: 1 })).toBeVisible();
  // A run id goes straight to the run.
  await page.keyboard.press("Control+k");
  await page.getByRole("combobox", { name: "Jump to" }).fill("01M3Y6KRZKEDY3BR0EN6J58M9T");
  await expect(page.getByRole("option", { name: /run/ })).toBeVisible();
});
