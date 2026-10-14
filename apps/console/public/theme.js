// Applies the persisted theme before first paint (see src/lib/theme.ts). With no
// choice stored, or "auto", or storage blocked, it follows the system: Signal by
// day, Arc at night.
var theme = null;
try {
  theme = localStorage.getItem("solera.theme");
} catch (_) {
  /* storage blocked: automatic */
}
var known = ["signal", "arc", "signal-ink", "signal-tint", "voltage", "normal", "obsidian", "workbench", "reactor", "cellar", "instrument", "observatory", "fun", "brutal"];
document.documentElement.dataset.theme =
  known.indexOf(theme) >= 0 ? theme : matchMedia("(prefers-color-scheme: dark)").matches ? "arc" : "signal";
