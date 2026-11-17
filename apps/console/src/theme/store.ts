import { useSyncExternalStore } from "react"
import { BUILT_IN, onSystemChange, SYSTEM_THEMES, systemTheme } from "./themes"
import { TOKEN_NAMES, type CustomTheme, type Theme } from "./tokens"

/* What the browser remembers about how the console looks: the
 * theme, and the themes this person made. One localStorage entry. */

/** The choice that is not a theme: follow the operating system. */
export const SYSTEM = "system"

/** `theme` is a theme's id, or SYSTEM. */
export type Look = { theme: string; custom: CustomTheme[] }

const KEY = "solera.console.look"
const PAINT = "solera.console.paint"
const INITIAL: Look = { theme: SYSTEM, custom: [] }

function read(): Look {
  try {
    const saved = JSON.parse(localStorage.getItem(KEY) ?? "null")
    if (saved && typeof saved === "object")
      return {
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
  const base = BUILT_IN.find((t) => t.id === custom.extends) ?? systemTheme()
  return { id: custom.id, name: custom.name, note: `Yours, from ${base.name}`, tokens: { ...base.tokens, ...custom.tokens } }
}

export function themes(of: Look = look): Theme[] {
  return [...BUILT_IN, ...of.custom.map(resolve)]
}

/** The theme on the page: the one chosen, or the system's under SYSTEM (and when the one chosen is gone). */
export function currentTheme(of: Look = look): Theme {
  return themes(of).find((t) => t.id === of.theme) ?? systemTheme()
}

/** One entry of a theme picker. `shows` is what its swatch draws: the theme, or the two System switches between. */
export type Choice = { id: string; name: string; note: string; shows: readonly Theme[] }

/** What a theme picker offers: System, then every theme. */
export function choices(of: Look = look): Choice[] {
  return [{ id: SYSTEM, name: "System", note: "Light or Dark", shows: SYSTEM_THEMES }, ...themes(of).map((theme) => ({ id: theme.id, name: theme.name, note: theme.note, shows: [theme] }))]
}

/** The choice in effect, as a picker marks it. */
export const chosen = (of: Look = look) => (of.theme === SYSTEM ? SYSTEM : currentTheme(of).id)

onSystemChange(() => {
  if (look.theme !== SYSTEM) return
  look = { ...look } // the same choice, now another theme: whoever reads it draws again
  paint()
  for (const listener of listeners) listener()
})

/** Put a theme's tokens on the page. Nothing reloads: every component reads
 * them as CSS variables. `preview` paints a theme without choosing it. */
export function paint(preview?: Theme) {
  const theme = preview ?? currentTheme()
  const root = document.documentElement
  for (const name of TOKEN_NAMES) root.style.setProperty(`--${name}`, theme.tokens[name])
  root.style.setProperty("color-scheme", theme.tokens.scheme)
  root.dataset.theme = theme.id
  if (!preview)
    try {
      // public/boot.js restores it before first paint. `system` is the scheme it was painted for, under SYSTEM.
      localStorage.setItem(PAINT, JSON.stringify({ css: root.style.cssText, system: look.theme === SYSTEM ? theme.tokens.scheme : undefined }))
    } catch {
      // see setLook
    }
}
