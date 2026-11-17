// Holds the built-in themes to what THEMES.md promises: text that can be read,
// status colours that can colour text, a chart whose bars stand off its empty
// cells, and surfaces that step the same way in every theme. Run by
// `pnpm check` and before every build. It prints each theme's contrast ratios
// (WCAG 2) and fails on the first promise a theme breaks.
// Node runs the TypeScript file as it is: it holds only values.
import { BUILT_IN } from "../src/theme/themes.ts"

const channel = (c) => (c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4)
function luminance(hex) {
  if (!/^#[0-9a-f]{6}$/i.test(hex)) throw new Error(`${hex}: a built-in theme writes these colours as #rrggbb`)
  const [r, g, b] = [1, 3, 5].map((i) => channel(parseInt(hex.slice(i, i + 2), 16) / 255))
  return 0.2126 * r + 0.7152 * g + 0.0722 * b
}
function contrast(a, b) {
  const [light, dark] = [luminance(a), luminance(b)].sort((x, y) => y - x)
  return (light + 0.05) / (dark + 0.05)
}

// Anything that can be read as text reaches this against the page.
const TEXT = 4.5
// A mark that is not text (a bar, a cell) reaches this against what it sits on.
const MARK = 3
// Two levels of text are told apart when their contrast differs by this factor.
const STEP = 1.25

const STATUS = ["color-ok", "color-running", "color-failed", "color-stale", "color-waiting", "color-idle"]
const READ = ["color-text", "color-text-muted", "color-text-faint", ...STATUS]

const broken = []
for (const theme of BUILT_IN) {
  const t = theme.tokens
  const on = (token, ground = "color-bg") => contrast(t[token], t[ground])
  const fail = (what) => broken.push(`${theme.name}: ${what}`)

  for (const token of READ) if (on(token) < TEXT) fail(`${token} is ${on(token).toFixed(2)}:1 on the page, under ${TEXT}:1`)
  if (on("color-text-faint", "color-surface") < TEXT) fail(`color-text-faint is ${on("color-text-faint", "color-surface").toFixed(2)}:1 on color-surface, under ${TEXT}:1`)
  if (on("color-text") < on("color-text-muted") * STEP || on("color-text-muted") < on("color-text-faint") * STEP) fail("the three levels of text are too close to tell apart")
  if (on("chart-bar") < MARK) fail(`chart-bar is ${on("chart-bar").toFixed(2)}:1 on the page, under ${MARK}:1`)
  if (on("chart-bar", "chart-grid") < MARK) fail(`chart-bar is ${on("chart-bar", "chart-grid").toFixed(2)}:1 on chart-grid, under ${MARK}:1`)

  // The surface rule. `ink` is how far a surface has moved from the page toward the text colour.
  const towardText = Math.sign(luminance(t["color-text"]) - luminance(t["color-bg"]))
  const ink = (token) => towardText * (luminance(t[token]) - luminance(t["color-bg"]))
  if (!(ink("color-surface") > 0 && ink("color-sunken") > ink("color-surface"))) fail("color-surface must be a step from the page toward the text colour, and color-sunken a step further")
  if (["color-bg", "color-surface", "color-sunken"].some((token) => luminance(t[token]) > luminance(t["color-raised"]))) fail("color-raised must be the lightest surface")

  console.log(`${theme.name.padEnd(8)}` + [...READ, "chart-bar"].map((token) => `${token.replace("color-", "")} ${on(token).toFixed(1)}`).join("  ") + `  bar on grid ${on("chart-bar", "chart-grid").toFixed(1)}`)
}

if (broken.length) {
  console.error(`\n${broken.length} broken promise${broken.length === 1 ? "" : "s"} (THEMES.md, "What the built-in themes keep"):\n` + broken.join("\n"))
  process.exit(1)
}
console.log("Every built-in theme keeps its contrast and the surface rule.")
