import { useSyncExternalStore } from "react"
import { BUILT_IN, DEFAULT_THEME } from "./themes"
import { TOKEN_NAMES, type CustomTheme, type Theme } from "./tokens"

/* What the browser remembers about how the console looks: the layout, the
 * theme, and the themes this person made. One localStorage entry. */

export const VARIATIONS = [
  { id: "workbench", name: "Workbench", note: "Sidebar and panels. A page for each thing" },
  { id: "deck", name: "Control room", note: "Dense. The timeline first, details in a side pane" },
  { id: "briefing", name: "Briefing", note: "One column that reads like a status report" },
] as const
export type VariationId = (typeof VARIATIONS)[number]["id"]

export type Look = { variation: VariationId; theme: string; custom: CustomTheme[] }

const KEY = "solera.console.look"
const PAINT = "solera.console.paint"
const INITIAL: Look = { variation: "workbench", theme: DEFAULT_THEME.id, custom: [] }

function read(): Look {
  try {
    const saved = JSON.parse(localStorage.getItem(KEY) ?? "null")
    if (saved && typeof saved === "object")
      return {
        variation: VARIATIONS.some((v) => v.id === saved.variation) ? saved.variation : INITIAL.variation,
        theme: typeof saved.theme === "string" ? saved.theme : INITIAL.theme,
        custom: Array.isArray(saved.custom) ? saved.custom : [],
      }
  } catch {
    // unreadable or blocked storage: the defaults
  }
  return INITIAL
}

let look = read()
const listeners = new Set<() => void>()

export function setLook(change: Partial<Look>) {
  look = { ...look, ...change }
  try {
    localStorage.setItem(KEY, JSON.stringify(look))
  } catch {
    // storage full or blocked: the choice lasts for this page only
  }
  paint()
  for (const listener of listeners) listener()
}

export function useLook(): Look {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    () => look,
  )
}

/** A custom theme over the built-in one it extends. */
export function resolve(custom: CustomTheme): Theme {
  const base = BUILT_IN.find((t) => t.id === custom.extends) ?? DEFAULT_THEME
  return { id: custom.id, name: custom.name, note: `Yours, from ${base.name}`, tokens: { ...base.tokens, ...custom.tokens } }
}

export function themes(of: Look = look): Theme[] {
  return [...BUILT_IN, ...of.custom.map(resolve)]
}

export function currentTheme(of: Look = look): Theme {
  return themes(of).find((t) => t.id === of.theme) ?? DEFAULT_THEME
}

/** Put a theme's tokens on the page. Nothing reloads: every component reads
 * them as CSS variables. `preview` paints a theme without choosing it. */
export function paint(preview?: Theme) {
  const theme = preview ?? currentTheme()
  const root = document.documentElement
  for (const name of TOKEN_NAMES) root.style.setProperty(`--${name}`, theme.tokens[name])
  root.style.setProperty("color-scheme", theme.tokens.scheme)
  root.dataset.variation = look.variation
  root.dataset.theme = theme.id
  if (!preview)
    try {
      localStorage.setItem(PAINT, root.style.cssText) // public/boot.js restores it before first paint
    } catch {
      // see setLook
    }
}
