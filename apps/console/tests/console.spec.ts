import { expect, test, type Page } from "@playwright/test"

/* A small suite over the real thing: the bundle the server serves, against a
 * server running the demo project. It checks that the console loads, that
 * every place opens, that a run can be started and read on its timeline,
 * and that the layout and the theme switch and stick. */

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

/** Pick a layout and a theme from the menu every layout carries. Changing the
 * layout redraws the frame, menu included, so each choice opens it afresh. */
async function setLook(page: Page, layout: string, theme: string) {
  for (const choice of [theme, layout]) {
    await page.getByRole("button", { name: "Change the layout or the theme" }).click()
    await page.getByRole("menuitemradio", { name: new RegExp(`^${choice}`) }).click()
    await page.keyboard.press("Escape")
  }
}

test("the overview shows the project and its timeline", async ({ page }) => {
  await page.goto("/")
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("demo")
  // every asset of the plan has a row before it has run
  for (const asset of ["sites", "site_feed", "file_checks", "weekly_digest"]) await expect(chart(page).getByRole("row", { name: new RegExp(`^${asset}\\b`) })).toBeVisible()
  await expect(chart(page).getByRole("columnheader", { name: "Timeline" })).toBeVisible()
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
  await page.goto("/assets/file_checks?tab=definition")
  await expect(page.getByRole("tab", { name: "How it is declared" })).toHaveAttribute("aria-selected", "true")
  await expect(page.getByText("one call per changed key")).toBeVisible()
  await page.goto("/no/such/place")
  await expect(page.getByText("This address leads nowhere")).toBeVisible()
})

test("a run can be started, and read on its timeline", async ({ page }) => {
  await page.goto("/")
  await page.getByRole("button", { name: "Start a run" }).first().click()
  const dialog = page.getByRole("dialog", { name: "Start a run" })
  await dialog.getByRole("combobox", { name: "Assets to run" }).click()
  await page.getByRole("option", { name: "deploy_notice" }).click()
  await page.keyboard.press("Escape")
  await dialog.getByRole("button", { name: "Start the run" }).click()

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
  await expect(page.getByRole("row", { name: /deploy_notice.*Started by console/ }).first()).toBeVisible()
})

test("the timeline works from the keyboard", async ({ page }) => {
  await page.goto("/")
  const row = chart(page).getByRole("row", { name: /^sites\b/ })
  await expect(row).toBeVisible()
  await row.focus()
  await page.keyboard.press("Enter")
  await expect(row).toHaveAttribute("aria-selected", "true")
  await expect(page.getByRole("link", { name: "Open the asset" })).toBeVisible()
  await page.keyboard.press("ArrowDown")
  await expect(chart(page).getByRole("row", { name: /^site_feed\b/ })).toBeFocused()
})

test("the layout and the theme switch at once and are remembered", async ({ page }) => {
  await page.goto("/")
  const html = page.locator("html")
  await expect(html).toHaveAttribute("data-variation", "workbench")
  const paper = await html.evaluate((element) => getComputedStyle(element).backgroundColor)

  await setLook(page, "Control room", "Oloroso")
  await expect(html).toHaveAttribute("data-variation", "deck")
  await expect(html).toHaveAttribute("data-theme", "oloroso")
  await expect(page.getByRole("complementary", { name: "Assets" })).toBeVisible() // the list pane only this layout has
  expect(await html.evaluate((element) => getComputedStyle(element).backgroundColor)).not.toBe(paper)

  await page.reload()
  await expect(html).toHaveAttribute("data-variation", "deck")
  await expect(html).toHaveAttribute("data-theme", "oloroso")
  await expect(chart(page).getByRole("row", { name: /^sites\b/ })).toBeVisible()

  await setLook(page, "Briefing", "Tinta")
  await expect(html).toHaveAttribute("data-variation", "briefing")
  await expect(page.getByRole("heading", { name: "What ran, and what runs next" })).toBeVisible()
  expect(await page.locator("body").evaluate((element) => getComputedStyle(element).fontFamily)).toContain("JetBrains Mono")
})

test("a custom theme is checked, applied and kept", async ({ page }) => {
  await page.goto("/settings")
  await page.getByRole("button", { name: "New theme" }).click()
  const editor = page.getByRole("textbox", { name: "Theme as JSON" })
  await editor.fill('{"name": "Probe", "extends": "fino", "tokens": {"no-such-token": "1"}}')
  await expect(page.getByText("No such token")).toBeVisible()
  await expect(page.getByRole("button", { name: "Save theme" })).toBeDisabled()

  await editor.fill('{"name": "Probe", "extends": "fino", "tokens": {"color-bg": "rgb(1, 2, 3)"}}')
  await page.getByRole("button", { name: "Save theme" }).click()
  await expect(page.locator("html")).toHaveCSS("background-color", "rgb(1, 2, 3)")
  await page.reload()
  await expect(page.locator("html")).toHaveCSS("background-color", "rgb(1, 2, 3)")
  await expect(page.getByRole("radio", { name: /^Probe/ })).toHaveAttribute("aria-checked", "true")
})
