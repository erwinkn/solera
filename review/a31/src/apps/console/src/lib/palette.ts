import { createStore, useStore } from "./store";

/**
 * Whether the jump palette is open. ⌘K / Ctrl+K anywhere, or "/" outside a
 * text field, opens it: one listener for the page's lifetime, registered
 * once at startup rather than by a component.
 */
export const palette = createStore(false);
export const usePalette = () => useStore(palette);

export function listenForPalette() {
  window.addEventListener("keydown", (event) => {
    const target = event.target as HTMLElement | null;
    const typing = !!target && (target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName));
    if ((event.key === "k" && (event.metaKey || event.ctrlKey)) || (event.key === "/" && !typing)) {
      event.preventDefault();
      palette.set(!palette.get());
    }
  });
}
