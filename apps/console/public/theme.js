// Applies the persisted theme before first paint (see src/theme.ts).
try {
  var theme = localStorage.getItem("solera.theme");
  document.documentElement.dataset.theme = theme === "fun" || theme === "brutal" ? theme : "normal";
} catch (_) {
  document.documentElement.dataset.theme = "normal";
}
