// Checks every text/background pair the console uses, in both themes,
// against WCAG AA: 4.5:1 for text, 3:1 for focus rings and large marks.
// Reads the token values straight from src/styles/themes.css.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const css = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "..", "src", "styles", "themes.css"),
  "utf8",
);

function theme(selector) {
  const start = css.indexOf(selector + " {") >= 0 ? css.indexOf(selector + " {") : css.indexOf(selector);
  const block = css.slice(css.indexOf("{", start) + 1, css.indexOf("\n}", start));
  return Object.fromEntries(
    [...block.matchAll(/--([\w-]+):\s*(#[0-9a-f]{6}|var\(--[\w-]+\));/gi)].map((m) => [m[1], m[2]]),
  );
}

const luminance = (hex) => {
  const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255);
  const lin = (c) => (c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4);
  return 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b);
};
const ratio = (a, b) => {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
};

const TEXT = 4.5;
const MARK = 3;
const pairs = [
  ...["fg", "fg-muted", "fg-subtle"].flatMap((fg) =>
    ["bg", "surface", "surface-2", "sunken"].map((bg) => [fg, bg, TEXT]),
  ),
  ...["ok", "run", "wait", "warn", "fail", "idle"].flatMap((s) => [
    [`${s}-fg`, `${s}-soft`, TEXT],
    [`${s}-fg`, "surface", TEXT],
  ]),
  ["accent-fg", "accent", TEXT],
  ["fg", "accent-soft", TEXT],
  ["fg", "select", TEXT],
  ["fg-inverse", "fg", TEXT],
  ["link", "surface", TEXT],
  ["link", "bg", TEXT],
  ["focus", "surface", MARK],
  ["focus", "bg", MARK],
  ["line-strong", "surface", 1.2],
];

// What the navigation draws: its text, its badges, the active item.
const navPairs = [
  ...["fg", "fg-muted", "fg-subtle"].map((fg) => [fg, "surface", TEXT]),
  ...["ok", "run", "warn", "fail", "idle"].map((s) => [`${s}-fg`, `${s}-soft`, TEXT]),
  ["nav-active-fg", "accent-soft", TEXT],
  ["focus", "surface", MARK],
];

let failed = 0;
for (const [name, ...selectors] of [
  ["normal", '[data-theme="normal"]'],
  ["fun", '[data-theme="fun"]'],
  ["brutal", '[data-theme="brutal"]'],
  ["cellar", '[data-theme="cellar"]'],
  ["instrument", '[data-theme="instrument"]'],
  ["observatory", '[data-theme="observatory"]'],
  ["voltage", '[data-theme="voltage"]'],
  ["blueprint", '[data-theme="blueprint"]'],
  ["ledger", '[data-theme="ledger"]'],
  ["signal", '[data-theme="signal"]'],
  ["night", '[data-theme="night"]'],
  ["arc", '[data-theme="arc"]'],
  ["obsidian", '[data-theme="obsidian"]'],
  ["workbench", '[data-theme="workbench"]'],
  ["reactor", '[data-theme="reactor"]'],
  // The navigation's own scope: a black slab in Brutal, a blue one in Signal.
  ["brutal navigation", '[data-theme="brutal"]', '[data-theme="brutal"] [data-chrome]'],
  ["signal navigation", '[data-theme="signal"]', '[data-theme="signal"] [data-chrome]'],
]) {
  const t = Object.assign({}, theme(":root,"), ...selectors.map(theme));
  const resolve = (v) => (v?.startsWith("var(--") ? t[v.slice(6, -1)] : v);
  for (const k of Object.keys(t)) t[k] = resolve(t[k]);
  const rows = (
    name.endsWith("navigation") ? navPairs : [...pairs, ["nav-active-fg", "accent-soft", TEXT]]
  ).map(([fg, bg, min]) => {
    const value = ratio(t[fg], t[bg]);
    if (value < min) failed++;
    return { pair: `${fg} on ${bg}`, ratio: value.toFixed(2), min, ok: value >= min ? "pass" : "FAIL" };
  });
  console.log(`\n${name}`);
  console.table(rows);
}
if (failed) {
  console.error(`${failed} pair(s) below AA`);
  process.exit(1);
}
console.log("every pair meets WCAG AA");
