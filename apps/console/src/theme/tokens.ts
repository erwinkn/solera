/** The tokens a theme owns. A theme is data: a value for each of these names.
 * Components read them as CSS variables (`--color-bg`) and hard-code none.
 * THEMES.md lists the same tokens for people who write themes: keep the two
 * in step. */

export const TOKENS = {
  scheme: "light or dark: tells the browser how to draw its own controls and scrollbars",

  "color-bg": "the page behind everything",
  "color-surface": "panels, tables and the timeline",
  "color-raised": "menus, dialogs and anything that floats",
  "color-sunken": "wells: code, logs, inputs, the track behind a bar",
  "color-border": "hairlines between rows and around panels",
  "color-border-strong": "the edge of a control, and hairlines that must be seen",
  "color-text": "body text",
  "color-text-muted": "secondary text and labels",
  "color-text-faint": "hints, placeholders, what has not happened yet",
  "color-accent": "primary buttons, the selected item, links",
  "color-accent-contrast": "text on the accent",
  "color-focus": "the keyboard focus ring",
  "color-overlay": "the dim behind a dialog",
  "color-ok": "succeeded, up to date",
  "color-running": "running now",
  "color-failed": "failed, rejected",
  "color-stale": "stale, retrying, needs attention soon",
  "color-waiting": "queued or held back",
  "color-idle": "never built, skipped, canceled",

  "font-sans": "the interface",
  "font-mono": "names, ids, numbers in columns, logs",
  "font-display": "page titles and big numbers",
  "font-size": "the base size; every other size is a ratio of it",
  "font-weight": "body weight",
  "font-weight-strong": "emphasis and headings in the interface font",
  "font-weight-display": "weight of the display font",
  "font-tracking-display": "letter spacing of the display font",
  "line-height": "body line height, unitless",

  "radius-s": "bars, chips, checkboxes",
  "radius-m": "buttons, inputs, menu items",
  "radius-l": "panels, dialogs",

  space: "the spacing unit; every gap and padding is a multiple of it",
  density: "a multiplier on row heights and control sizes: below 1 is tighter",
  "border-width": "hairline thickness",

  "shadow-raised": "panels that sit above the page",
  "shadow-overlay": "menus and dialogs",

  "motion-fast": "hover, press, small state changes",
  "motion-slow": "panels opening, bars growing",
  "motion-ease": "the easing curve of both",

  "chart-bar": "a finished bar on the timeline",
  "chart-bar-radius": "corner radius of a bar",
  "chart-bar-height": "bar height as a fraction of its row, 0 to 1",
  "chart-grid": "the timeline's vertical grid lines",
} as const

export type TokenName = keyof typeof TOKENS
export type Tokens = Record<TokenName, string>
export const TOKEN_NAMES = Object.keys(TOKENS) as TokenName[]

export type Theme = {
  id: string
  name: string
  /** One line, shown beside the name in the picker. */
  note: string
  tokens: Tokens
}

/** What a person writes: a name, the built-in theme it starts from, and only
 * the tokens it changes. */
export type CustomTheme = {
  id: string
  name: string
  extends: string
  tokens: Partial<Tokens>
}

export type Problem = { path: string; message: string }

/** Check a pasted theme before it is trusted (it comes from a text box or a
 * file). Returns the theme, or what is wrong with it in plain words. */
export function parseCustomTheme(
  text: string,
  builtIns: readonly string[],
): { theme: Omit<CustomTheme, "id"> } | { problems: Problem[] } {
  let data: unknown
  try {
    data = JSON.parse(text)
  } catch (error) {
    return { problems: [{ path: "", message: `Not valid JSON: ${(error as Error).message}` }] }
  }
  if (typeof data !== "object" || data === null || Array.isArray(data))
    return { problems: [{ path: "", message: "A theme is a JSON object with name, extends and tokens." }] }
  const { name, extends: base, tokens, ...rest } = data as Record<string, unknown>
  const problems: Problem[] = []
  for (const key of Object.keys(rest))
    if (key !== "id") problems.push({ path: key, message: "Unknown field. A theme has name, extends and tokens." })
  if (typeof name !== "string" || !name.trim()) problems.push({ path: "name", message: "Give the theme a name." })
  if (typeof base !== "string" || !builtIns.includes(base))
    problems.push({ path: "extends", message: `Must be one of: ${builtIns.join(", ")}.` })
  const out: Partial<Tokens> = {}
  if (typeof tokens !== "object" || tokens === null || Array.isArray(tokens))
    problems.push({ path: "tokens", message: "Must be an object of token names and values." })
  else
    for (const [token, value] of Object.entries(tokens)) {
      if (!(token in TOKENS)) problems.push({ path: `tokens.${token}`, message: "No such token. See THEMES.md for the list." })
      else if (typeof value !== "string" || !value.trim())
        problems.push({ path: `tokens.${token}`, message: "A token's value is a non-empty string." })
      else if (token === "scheme" && value !== "light" && value !== "dark")
        problems.push({ path: "tokens.scheme", message: 'Must be "light" or "dark".' })
      else out[token as TokenName] = value
    }
  if (problems.length) return { problems }
  return { theme: { name: (name as string).trim(), extends: base as string, tokens: out } }
}
