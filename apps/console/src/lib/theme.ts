import { createStore, useStore } from "./store";

/**
 * The theme is a single attribute on <html>; every color, font, radius,
 * spacing unit and easing reads from it (styles/themes.css). public/theme.js
 * applies the stored choice before first paint.
 *
 * The default is Voltage (D160): Signal by day and Arc at night, following
 * the system's light or dark preference until someone picks a theme. "auto"
 * is that default, chosen again.
 */
export const THEMES = [
  "signal",
  "arc",
  "signal-ink",
  "signal-tint",
  "voltage",
  "normal",
  "obsidian",
  "workbench",
  "reactor",
  "cellar",
  "instrument",
  "observatory",
  "fun",
  "brutal",
] as const;
export type Theme = (typeof THEMES)[number];
export type Preference = Theme | "auto";

export const THEME_NAMES: Record<Preference, string> = {
  auto: "Automatic",
  signal: "Voltage · Signal",
  arc: "Voltage · Arc",
  "signal-ink": "Signal · Ink",
  "signal-tint": "Signal · Tint",
  voltage: "Voltage",
  normal: "Normal",
  obsidian: "Obsidian",
  workbench: "Workbench",
  reactor: "Reactor",
  cellar: "Cellar",
  instrument: "Instrument",
  observatory: "Observatory",
  fun: "Fun",
  brutal: "Brutal",
};

const KEY = "solera.theme";
const dark = matchMedia("(prefers-color-scheme: dark)");
const automatic = (): Theme => (dark.matches ? "arc" : "signal");

function stored(): Preference {
  try {
    const value = localStorage.getItem(KEY);
    return THEMES.find((t) => t === value) ?? "auto";
  } catch {
    return "auto";
  }
}

const preference = createStore<Preference>(stored());

function apply(next: Preference) {
  document.documentElement.dataset.theme = next === "auto" ? automatic() : next;
}

// While automatic, a switch of the system's appearance switches the console with it.
dark.addEventListener("change", () => {
  if (preference.get() === "auto") apply("auto");
});

export function setTheme(next: Preference) {
  apply(next);
  try {
    if (next === "auto") localStorage.removeItem(KEY);
    else localStorage.setItem(KEY, next);
  } catch {
    /* not persisted */
  }
  preference.set(next);
}

/** The chosen preference: a theme, or "auto". */
export const useTheme = () => useStore(preference);
