// Runs before the first paint, as a file because the server's content security
// policy forbids inline scripts. It restores the tokens of the theme last
// applied (src/theme/store.ts keeps them), so a reload does not flash. Under
// System they are left out when the system has changed since: the page then
// waits for the console to paint the other theme.
try {
  var saved = JSON.parse(localStorage.getItem("solera.console.paint"))
  var scheme = matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark"
  if (saved && (!saved.system || saved.system === scheme)) document.documentElement.style.cssText = saved.css
} catch (error) {}
