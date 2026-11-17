// Runs before the first paint, as a file because the server's content security
// policy forbids inline scripts. It restores the tokens of the theme last
// applied (src/theme/apply.ts keeps them), so a reload does not flash.
try {
  var saved = localStorage.getItem("solera.console.paint")
  if (saved) document.documentElement.style.cssText = saved
} catch (error) {}
