import { createStore, useStore } from "./store";

/**
 * The theme is a single attribute on <html>; every color, font, radius,
 * spacing unit and easing reads from it (styles/themes.css). public/theme.js
 * applies the stored choice before first paint.
 */
export const THEMES = [
  "normal",
  "voltage",
  "blueprint",
  "ledger",
  "signal",
  "night",
  "arc",
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

export const THEME_NAMES: Record<Theme, string> = {
  normal: "Normal",
  voltage: "Voltage",
  blueprint: "Voltage · Blueprint",
  ledger: "Voltage · Ledger",
  signal: "Voltage · Signal",
  night: "Voltage · Night",
  arc: "Voltage · Arc",
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

const stamped = document.documentElement.dataset.theme;
const initial: Theme = THEMES.find((t) => t === stamped) ?? "normal";
const theme = createStore<Theme>(initial);

export function setTheme(next: Theme) {
  document.documentElement.dataset.theme = next;
  try {
    localStorage.setItem(KEY, next);
  } catch {
    /* not persisted */
  }
  theme.set(next);
}

export const useTheme = () => useStore(theme);
