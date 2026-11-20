import { expect, test, type APIRequestContext, type Page } from "@playwright/test"

/* A small suite over the real thing: the bundle the server serves, against a
 * server running the demo project. It checks that the overview says how the
 * project is doing, that every place opens, that a run can be started and
 * read on its timeline, that partitions can be picked for it, and that the
 * theme switches, sticks and follows the system. */

const problems: string[] = []

test.beforeEach(async ({ page }) => {
  problems.length = 0
  page.on("pageerror", (error) => problems.push(error.message))
  // the server's content security policy reports a breach as a console error
  page.on("console", (message) => {
    if (message.type() === "error" && /Content Security Policy|Refused to/.test(message.text())) problems.push(message.text())
  })
})

test.afterEach(() => {
  expect(problems, "no script error and no content security policy breach").toEqual([])
})

const chart = (page: Page) => page.getByRole("treegrid")

/** Pick a theme from the menu beside the health status. */
async function setTheme(page: Page, theme: string) {
  await page.getByRole("button", { name: "Change the theme" }).click()
  await page.getByRole("menuitemradio", { name: new RegExp(`^${theme}`) }).click()
  await page.keyboard.press("Escape")
}

/** site_digest has a partition for each day and each site, and the sites come from a run of `sites`. Its schedule
 * fires on the minute, so a server that has just started may have none yet: start that run instead of waiting for it. */
async function sitesLoaded(request: APIRequestContext) {
  await request.post("/api/projects/demo/runs", { data: { targets: ["sites"], partitions: "latest", mode: "incremental", upstream: false, by: "test" } })
  await expect.poll(async () => (await (await request.get("/api/projects/demo/partitions/site_digest")).json()).partitions.length, { timeout: 30_000 }).toBeGreaterThan(100)
}

test("the overview says how every asset is doing and what it did lately", async ({ page }) => {
  await page.goto("/?window=86400")
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Overview")
  await expect(page.getByText(/\d+ of \d+ up to date/)).toBeVisible()
  // every asset has a row before it has run; sources, which do not run, have none
  for (const asset of ["sites", "site_feed", "file_checks", "weekly_digest"]) await expect(page.getByRole("row", { name: new RegExp(`^${asset}\\b`) })).toBeVisible()
  await expect(page.getByRole("row", { name: /^uploads\b/ })).toHaveCount(0)

  // the job that runs on each deploy has run: its strip has a cell that says so and leads to the run
  const strip = page.getByRole("img", { name: /^deploy_notice: \d+ runs?, 0 failed/ })
  await expect(strip).toBeVisible({ timeout: 30_000 })
  const cell = strip.locator("a").last()
  await cell.hover()
  await expect(page.getByText(/\d+ runs? · \d+ tasks? succeeded/)).toBeVisible()
  await cell.click()
  await expect(page).toHaveURL(/\/runs\/[0-9A-Z]{26}$/)
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("deploy_notice")
})

test("every place opens, by its link and by its address", async ({ page }) => {
  await page.goto("/")
  const places = page.getByRole("navigation", { name: "Places" })
  for (const [link, heading] of [
    ["Runs", "Runs"],
    ["Assets", "Assets"],
    ["Automations", "Automations"],
    ["Find a key", "Find a key"],
    ["Engine", "Engine"],
    ["Settings", "Settings"],
  ] as const) {
    await places.getByRole("link", { name: link }).click()
    await expect(page.getByRole("heading", { level: 1 })).toHaveText(heading)
  }
  // an address the server does not know is the console's to route, dots included
  await page.goto("/sensors/upload_drop")
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("upload_drop")
  await expect(page.getByRole("navigation", { name: "Where you are" }).getByRole("link", { name: "Automations" })).toBeVisible()
  await page.goto("/assets/file_checks?tab=definition")
  await expect(page.getByRole("tab", { name: "Definition" })).toHaveAttribute("aria-selected", "true")
  await expect(page.getByText("one call per changed key")).toBeVisible()
  await page.goto("/no/such/place")
  await expect(page.getByText("No page at this address")).toBeVisible()
})

test("a run can be started, and read on its timeline", async ({ page }) => {
  await page.goto("/")
  await page.getByRole("button", { name: "Start a run" }).first().click()
  const dialog = page.getByRole("dialog", { name: "Start a run" })
  await dialog.getByRole("combobox", { name: "Assets to run" }).click()
  await page.getByRole("option", { name: "deploy_notice" }).click()
  await page.keyboard.press("Escape")
  // a job has no partitions and reads nothing incrementally: neither choice is offered
  await expect(dialog.getByLabel("Which partitions")).toHaveCount(0)
  await expect(dialog.getByLabel("Mode")).toHaveCount(0)
  await dialog.getByRole("button", { name: "Start", exact: true }).click()

  await expect(page).toHaveURL(/\/runs\/[0-9A-Z]{26}$/)
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("deploy_notice")
  await expect(page.getByText("1 of 1 task done")).toBeVisible({ timeout: 30_000 })

  // picking the step shows what it did
  const step = chart(page).getByRole("row", { name: /^deploy_notice/ })
  await step.click()
  await expect(page).toHaveURL(/at=task/)
  await expect(page.getByText("Where the time went")).toBeVisible()
  await expect(page.getByText("project deployed")).toBeVisible()

  // and the run is in the list
  await page.getByRole("navigation", { name: "Places" }).getByRole("link", { name: "Runs" }).click()
  await expect(page.getByRole("row", { name: /deploy_notice.*Manual, from the console/ }).first()).toBeVisible()
})

test("starting a run offers the choices that apply to the assets picked", async ({ page }) => {
  await page.goto("/assets/file_checks")
  await page.getByRole("button", { name: "Run", exact: true }).click() // the asset's own button, with the asset filled in
  const dialog = page.getByRole("dialog", { name: "Start a run" })
  await expect(dialog.getByRole("combobox", { name: "Assets to run" })).toContainText("file_checks")
  // partitioned by site, and it reads its files incrementally
  const which = dialog.getByLabel("Which partitions")
  for (const choice of ["Stale", "All", "Selected"]) await expect(which.getByText(choice, { exact: true })).toBeVisible()
  await which.getByText("All", { exact: true }).click()
  await expect(dialog.getByText(/^\d+ partitions?$/)).toBeVisible()
  const mode = dialog.getByLabel("Mode")
  await expect(mode.getByText("Incremental", { exact: true })).toBeVisible()
  await expect(mode.getByText("Full run", { exact: true })).toBeVisible()
  await expect(dialog.getByText("Run dependencies first")).toBeVisible()
  await dialog.getByRole("button", { name: "Cancel" }).click()
  await expect(dialog).toHaveCount(0)
})

test("partitions are picked from a list that filters, and the run gets their names", async ({ page, request }) => {
  await sitesLoaded(request)
  await page.goto("/assets/site_digest")
  await page.getByRole("button", { name: "Run", exact: true }).click()
  const dialog = page.getByRole("dialog", { name: "Start a run" })
  const start = dialog.getByRole("button", { name: "Start", exact: true })
  await dialog.getByLabel("Which partitions").getByText("Selected", { exact: true }).click()

  // one partition a day for each site since September: hundreds, of which only the rows in view are drawn
  const list = dialog.getByRole("listbox", { name: "Partitions" })
  const options = list.getByRole("option")
  await expect(dialog.getByText(/^0 of \d{3,} selected$/)).toBeVisible()
  expect(await options.count()).toBeLessThan(30)
  await expect(options.first()).toHaveAttribute("aria-setsize", /^\d{3,}$/)
  await expect(start).toBeDisabled()

  // by status: each status that occurs is offered with its count, and narrows the list to its own
  const statuses = dialog.getByRole("group", { name: "Filter by status" }).getByRole("button")
  await expect(statuses.first()).toHaveText(/^(Failed|Stale|Never built|Running|Up to date)\d+$/)
  await statuses.first().click()
  await expect(statuses.first()).toHaveAttribute("aria-pressed", "true")
  await statuses.first().click()

  // by name, then everything shown in one click
  const filter = dialog.getByLabel("Filter partitions")
  await filter.fill("2026-09-01")
  await expect(options.first()).toHaveAttribute("aria-setsize", /^\d$/) // that day's partitions: one for each site
  const shown = await options.count()
  await dialog.getByRole("button", { name: "Select shown" }).click()
  await expect(dialog.getByText(new RegExp(`^${shown} of \\d+ selected$`))).toBeVisible()
  await dialog.getByRole("button", { name: "Clear" }).click()
  await expect(dialog.getByText(/^0 of \d+ selected$/)).toBeVisible()

  // from the keyboard: type, step into the list, tick with Space; Enter in the filter starts nothing
  await filter.fill("2026-09-02")
  await filter.press("Enter")
  await filter.press("ArrowDown")
  await expect(options.first()).toBeFocused()
  await page.keyboard.press("Space")
  await page.keyboard.press("ArrowDown")
  await page.keyboard.press("Space")
  await expect(options.nth(1)).toHaveAttribute("aria-selected", "true")
  // what a filter hides stays selected
  await filter.fill("")
  await expect(dialog.getByText(/^2 of \d+ selected$/)).toBeVisible()

  const [sent] = await Promise.all([page.waitForRequest((r) => r.method() === "POST" && r.url().endsWith("/runs")), start.click()])
  expect(sent.postDataJSON()).toMatchObject({ targets: ["site_digest"], partitions: ["day=2026-09-02,site=alpha", "day=2026-09-02,site=bravo"] })
})

test("a partition's own button opens the dialog with it selected", async ({ page, request }) => {
  await sitesLoaded(request)
  await page.goto("/assets/site_digest")
  await page.getByRole("listitem", { name: /^2026-09-03 · bravo:/ }).click()
  await page.getByRole("button", { name: "Run this partition" }).click()
  const dialog = page.getByRole("dialog", { name: "Start a run" })
  await expect(dialog.getByText(/^1 of \d+ selected$/)).toBeVisible()
  await dialog.getByLabel("Filter partitions").fill("2026-09-03 · bravo")
  await expect(dialog.getByRole("option")).toHaveCount(1)
  await expect(dialog.getByRole("option")).toHaveAttribute("aria-selected", "true")
  await expect(dialog.getByRole("button", { name: "Start", exact: true })).toBeEnabled()
})

test("the timeline works from the keyboard", async ({ page, request }) => {
  const started = await request.post("/api/projects/demo/runs", { data: { targets: ["sites", "deploy_notice"], partitions: "latest", mode: "incremental", upstream: false, by: "test" } })
  const run = await started.json()
  await page.goto(`/runs/${run.id}`)
  const rows = chart(page).getByRole("row")
  const first = rows.nth(1) // after the header row
  await expect(first).toBeVisible()
  await first.focus()
  await page.keyboard.press("Enter")
  await expect(first).toHaveAttribute("aria-selected", "true")
  await expect(page.getByRole("link", { name: "Open the asset" })).toBeVisible()
  await page.keyboard.press("ArrowDown")
  await expect(rows.nth(2)).toBeFocused()
})

test("the theme switches at once and is remembered", async ({ page }) => {
  await page.goto("/")
  const html = page.locator("html")
  // nothing chosen yet: the theme follows the system, which is light in this browser
  await expect(html).toHaveAttribute("data-theme", "light")
  const white = await html.evaluate((element) => getComputedStyle(element).backgroundColor)

  await setTheme(page, "Dark")
  await expect(html).toHaveAttribute("data-theme", "dark")
  expect(await html.evaluate((element) => getComputedStyle(element).backgroundColor)).not.toBe(white)

  await page.reload()
  await expect(html).toHaveAttribute("data-theme", "dark")
  await expect(page.getByRole("row", { name: /^sites\b/ })).toBeVisible()

  await setTheme(page, "Dimmed")
  await expect(html).toHaveAttribute("data-theme", "dimmed")
  // one typeface for the interface in every theme, served by the console itself
  expect(await page.locator("body").evaluate((element) => getComputedStyle(element).fontFamily)).toContain("Inter Variable")
  expect(await page.evaluate(() => document.fonts.check('13px "Inter Variable"'))).toBe(true)
})

test("System follows the system, live, until a theme is picked", async ({ page }) => {
  const html = page.locator("html")
  await page.emulateMedia({ colorScheme: "dark" })
  await page.goto("/")
  // a browser that has chosen nothing is on System
  await expect(html).toHaveAttribute("data-theme", "dark")
  await page.getByRole("button", { name: "Change the theme" }).click()
  await expect(page.getByRole("menuitemradio", { name: /^System/ })).toHaveAttribute("aria-checked", "true")
  await page.keyboard.press("Escape")
  await page.emulateMedia({ colorScheme: "light" })
  await expect(html).toHaveAttribute("data-theme", "light")

  // a theme that was picked stays, whatever the system does
  await setTheme(page, "Dimmed")
  await page.emulateMedia({ colorScheme: "dark" })
  await expect(html).toHaveAttribute("data-theme", "dimmed")

  // back on System, in Settings this time, and across a reload during which the system changed
  await page.goto("/settings")
  await page.getByRole("radio", { name: /^System/ }).click()
  await expect(html).toHaveAttribute("data-theme", "dark")
  await page.emulateMedia({ colorScheme: "light" })
  await page.reload()
  await expect(html).toHaveAttribute("data-theme", "light")
  await expect(page.getByRole("radio", { name: /^System/ })).toHaveAttribute("aria-checked", "true")
})

test("a browser that stored a theme before System existed keeps it", async ({ page }) => {
  // what the console wrote until now: the theme's id, and the painted tokens as bare CSS
  await page.addInitScript(() => {
    localStorage.setItem("solera.console.look", JSON.stringify({ theme: "dark", custom: [] }))
    localStorage.setItem("solera.console.paint", "--color-bg: #0b0b0c; color-scheme: dark;")
  })
  await page.goto("/")
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark") // the system is light in this browser
  await page.getByRole("button", { name: "Change the theme" }).click()
  await expect(page.getByRole("menuitemradio", { name: /^Dark/ })).toHaveAttribute("aria-checked", "true")
})

test("a custom theme is checked, applied and kept", async ({ page }) => {
  await page.goto("/settings")
  await page.getByRole("button", { name: "New theme" }).click()
  const editor = page.getByRole("textbox", { name: "Theme as JSON" })
  await editor.fill('{"name": "Probe", "extends": "light", "tokens": {"no-such-token": "1"}}')
  await expect(page.getByText("No such token")).toBeVisible()
  await expect(page.getByRole("button", { name: "Save theme" })).toBeDisabled()

  await editor.fill('{"name": "Probe", "extends": "light", "tokens": {"color-bg": "rgb(1, 2, 3)"}}')
  await page.getByRole("button", { name: "Save theme" }).click()
  await expect(page.locator("html")).toHaveCSS("background-color", "rgb(1, 2, 3)")
  await page.reload()
  await expect(page.locator("html")).toHaveCSS("background-color", "rgb(1, 2, 3)")
  await expect(page.getByRole("radio", { name: /^Probe/ })).toHaveAttribute("aria-checked", "true")
})

test("one who may only read sees nothing that would change anything", async ({ page, request }) => {
  test.skip(Boolean(process.env.SOLERA_CONSOLE_URL), "needs the reading server playwright.config.ts starts")
  const reader = "http://127.0.0.1:8798"
  // the job that runs on each deploy has run: a finished run to look at
  const runs = async () => (await (await request.get(`${reader}/api/projects/demo/runs?status=succeeded&asset=deploy_notice`)).json()).runs as { id: string }[]
  await expect.poll(async () => (await runs()).length, { timeout: 30_000 }).toBeGreaterThan(0)
  const refused = await request.post(`${reader}/api/projects/demo/runs`, { data: { targets: ["sites"] } })
  expect(refused.status()).toBe(403)
  expect((await refused.json()).detail).toBe("This needs the write role; reader@example.com has read.")

  await page.goto(`${reader}/settings`)
  await expect(page.getByText("reader@example.com")).toBeVisible()
  const writes = /^(Start a run|Run|Run again|Run this partition|Retry what failed|Retry failing keys|Cancel|Pause|Resume|Delete|Delete old runs|Commit the change|Clear)$/
  for (const [path, heading] of [
    ["/", "Overview"],
    ["/assets/sites", "sites"],
    [`/runs/${(await runs())[0]!.id}`, null],
    ["/automations", "Automations"],
    ["/sources/uploads", "uploads"],
    ["/engine", "Engine"],
  ] as const) {
    await page.goto(reader + path)
    if (heading) await expect(page.getByRole("heading", { level: 1 })).toHaveText(heading)
    else await expect(page.getByText("Started by")).toBeVisible()
    await expect(page.getByRole("button", { name: writes })).toHaveCount(0)
    await expect(page.getByRole("switch")).toHaveCount(0)
    await expect(page.getByRole("button", { name: /^Run \S+ now$/ })).toHaveCount(0)
  }
  // nor in the command palette
  await page.keyboard.press("ControlOrMeta+k")
  await expect(page.getByRole("option", { name: "Start a run" })).toHaveCount(0)
})
