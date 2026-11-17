import type { Theme, Tokens } from "./tokens"

/* The three themes the console ships. Each is only values: adding a fourth is
 * adding an object here, or pasting one in Settings (see THEMES.md). They are
 * the themes of a tool: grey surfaces with almost no hue, one accent, and
 * colour kept for status. */

const SANS = '"Inter Variable", ui-sans-serif, system-ui, sans-serif'
const MONO = '"JetBrains Mono Variable", ui-monospace, "SF Mono", Menlo, monospace'

/** What the three share: type, spacing, corners and motion. */
const SHAPE = {
  "font-sans": SANS,
  "font-mono": MONO,
  "font-size": "13px",
  "font-weight": "400",
  "font-weight-strong": "600",
  "line-height": "1.5",
  "radius-s": "3px",
  "radius-m": "5px",
  "radius-l": "8px",
  space: "4px",
  density: "1",
  "border-width": "1px",
  "motion-fast": "100ms",
  "motion-slow": "180ms",
  "motion-ease": "cubic-bezier(0.2, 0, 0, 1)",
  "chart-bar-radius": "1px",
  "chart-bar-height": "0.44",
} satisfies Partial<Tokens>

/** Near-black and neutral. The accent is the text colour itself. */
const dark: Theme = {
  id: "dark",
  name: "Dark",
  note: "Neutral near-black",
  tokens: {
    ...SHAPE,
    scheme: "dark",
    "color-bg": "#0b0b0c",
    "color-surface": "#101011",
    "color-raised": "#18181a",
    "color-sunken": "#141416",
    "color-border": "#202023",
    "color-border-strong": "#2f2f34",
    "color-text": "#ececee",
    "color-text-muted": "#9c9ca5",
    "color-text-faint": "#66666f",
    "color-accent": "#ececee",
    "color-accent-contrast": "#0b0b0c",
    "color-focus": "#5b9cf8",
    "color-overlay": "rgb(0 0 0 / 0.6)",
    "color-ok": "#3fb97c",
    "color-running": "#5b9cf8",
    "color-failed": "#ef5a5a",
    "color-stale": "#dda032",
    "color-waiting": "#8f8f99",
    "color-idle": "#5c5c65",
    "shadow-overlay": "0 12px 32px rgb(0 0 0 / 0.5), 0 0 0 1px #2f2f34",
    "chart-bar": "#6e6e78",
    "chart-grid": "#1a1a1d",
  },
}

/** White and neutral: the same console in daylight. */
const light: Theme = {
  id: "light",
  name: "Light",
  note: "Neutral white",
  tokens: {
    ...SHAPE,
    scheme: "light",
    "color-bg": "#ffffff",
    "color-surface": "#fafafa",
    "color-raised": "#ffffff",
    "color-sunken": "#f4f4f5",
    "color-border": "#ebebee",
    "color-border-strong": "#d6d6db",
    "color-text": "#18181b",
    "color-text-muted": "#62626b",
    "color-text-faint": "#9b9ba4",
    "color-accent": "#18181b",
    "color-accent-contrast": "#ffffff",
    "color-focus": "#2f6fed",
    "color-overlay": "rgb(24 24 27 / 0.32)",
    "color-ok": "#1a9a59",
    "color-running": "#2f6fed",
    "color-failed": "#d93636",
    "color-stale": "#b9770a",
    "color-waiting": "#7c7c86",
    "color-idle": "#a6a6af",
    "shadow-overlay": "0 10px 28px rgb(24 24 27 / 0.12), 0 0 0 1px rgb(24 24 27 / 0.06)",
    "chart-bar": "#a3a3ad",
    "chart-grid": "#f0f0f2",
  },
}

/** Dark, but lower in contrast and tinted blue-grey, with a blue accent: for
 * long sessions and for people who find near-black harsh. */
const dimmed: Theme = {
  id: "dimmed",
  name: "Dimmed",
  note: "Blue-grey, softer contrast",
  tokens: {
    ...SHAPE,
    scheme: "dark",
    "color-bg": "#22262e",
    "color-surface": "#1d2128",
    "color-raised": "#2b3039",
    "color-sunken": "#1b1f25",
    "color-border": "#313742",
    "color-border-strong": "#424956",
    "color-text": "#cfd6e0",
    "color-text-muted": "#919bab",
    "color-text-faint": "#667080",
    "color-accent": "#74b0f4",
    "color-accent-contrast": "#111820",
    "color-focus": "#74b0f4",
    "color-overlay": "rgb(14 17 22 / 0.6)",
    "color-ok": "#62bd7d",
    "color-running": "#74b0f4",
    "color-failed": "#ee7168",
    "color-stale": "#d9a548",
    "color-waiting": "#919bab",
    "color-idle": "#667080",
    "radius-m": "6px",
    "shadow-overlay": "0 12px 32px rgb(10 13 18 / 0.45), 0 0 0 1px #424956",
    "chart-bar": "#768091",
    "chart-grid": "#2a2f38",
  },
}

export const BUILT_IN: readonly Theme[] = [dark, light, dimmed]

/** What the theme editor opens with: a small change to `base`, to edit from. */
export const starterTheme = (base: string) => JSON.stringify({ name: "My theme", extends: base, tokens: { "color-accent": "#7c8cf8", "radius-m": "8px" } }, null, 2)

/** With nothing chosen yet, the console follows the system: dark or light. */
export const defaultTheme = (): Theme => (typeof matchMedia === "function" && matchMedia("(prefers-color-scheme: light)").matches ? light : dark)
