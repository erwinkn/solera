// Applies the persisted theme before first paint (see src/theme.ts).
try {
  var theme = localStorage.getItem("solera.theme");
  var known = ["normal", "cellar", "instrument", "observatory", "fun", "brutal"];
  document.documentElement.dataset.theme = known.indexOf(theme) >= 0 ? theme : "normal";
} catch (_) {
  document.documentElement.dataset.theme = "normal";
}
