import { createStore, useStore } from "./store";

/**
 * The theme is a single attribute on <html>; every color, font, radius,
 * spacing unit and easing reads from it (styles/themes.css). public/theme.js
 * applies the stored choice before first paint.
 */
export const THEMES = ["normal", "fun"] as const;
export type Theme = (typeof THEMES)[number];

const KEY = "solera.theme";

const initial: Theme = document.documentElement.dataset.theme === "fun" ? "fun" : "normal";
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
