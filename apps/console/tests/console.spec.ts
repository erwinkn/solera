import { expect, test, type Page } from "@playwright/test"

/* A small suite over the real thing: the bundle the server serves, against a
 * server running the demo project. It checks that the overview says how the
 * project is doing, that every place opens, that a run can be started and
 * read on its timeline, and that the theme switches and sticks. */

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
