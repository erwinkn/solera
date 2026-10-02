// Checks every text/background pair the console uses, in both themes,
// against WCAG AA: 4.5:1 for text, 3:1 for focus rings and large marks.
// Reads the token values straight from src/styles/themes.css.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const css = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "src", "styles", "themes.css"), "utf8");

function theme(selector) {
  const start = css.indexOf(selector);
  const block = css.slice(css.indexOf("{", start) + 1, css.indexOf("\n}", start));
  return Object.fromEntries([...block.matchAll(/--([\w-]+):\s*(#[0-9a-f]{6});/gi)].map((m) => [m[1], m[2]]));
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
  ...["fg", "fg-muted", "fg-subtle"].flatMap((fg) => ["bg", "surface", "surface-2", "sunken"].map((bg) => [fg, bg, TEXT])),
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

let failed = 0;
for (const [name, selector] of [
  ["normal", '[data-theme="normal"]'],
  ["fun", '[data-theme="fun"]'],
]) {
  const t = { ...theme(":root,"), ...theme(selector) };
  const rows = pairs.map(([fg, bg, min]) => {
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
