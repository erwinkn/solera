// A theme owns every colour, typeface, corner and duration. This fails when
// a stylesheet or a component sets one by hand instead of reading a token.
// Run by `pnpm check` and before every build.
import { readdirSync, readFileSync, statSync } from "node:fs"
import { join, relative } from "node:path"
import { fileURLToPath } from "node:url"

const root = fileURLToPath(new URL("../src", import.meta.url))
// Where values are allowed: the themes themselves and the font files.
const ALLOWED = ["theme/themes.ts", "styles/fonts.css"]

const COLOUR = /#[0-9a-fA-F]{3,8}\b|\b(?:rgb|rgba|hsl|hsla|oklch|oklab)\(/
const DECLARATION = /(font-family|border-radius|transition|animation)\s*:\s*([^;}]+)/g
// What a declaration's value may be made of, besides tokens.
const FREE = {
  "font-family": /^(?:var\([^)]*\)|inherit)$/,
  // square, round, or computed from a token
  "border-radius": /^(?:(?:var\([^)]*\)|calc\((?:[^()]|\([^()]*\))*\)|0|50%|inherit)\s*)+$/,
}
const SECONDS = /(?<![\w-])\d*\.?\d+m?s\b/

function files(dir) {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name)
    return statSync(path).isDirectory() ? files(path) : /\.(css|tsx?)$/.test(name) ? [path] : []
  })
}

const found = []
for (const path of files(root)) {
  const name = relative(root, path)
  if (ALLOWED.includes(name)) continue
  readFileSync(path, "utf8")
    .split("\n")
    .forEach((line, i) => {
      if (/^\s*(\/\/|\/?\*)/.test(line)) return
      const report = (what) => found.push(`${name}:${i + 1}: ${what}: ${line.trim().slice(0, 110)}`)
      if (COLOUR.test(line)) report("a colour")
      if (!name.endsWith(".css")) return
      for (const [, property, value] of line.matchAll(DECLARATION)) {
        const text = value.trim()
        if (property === "transition" || property === "animation") {
          if (SECONDS.test(text.replace(/var\([^)]*\)/g, ""))) report("a duration")
        } else if (!FREE[property].test(text)) report(property === "font-family" ? "a typeface" : "a corner radius")
      }
    })
}

if (found.length) {
  console.error(`${found.length} value${found.length === 1 ? "" : "s"} a theme should own:\n` + found.join("\n"))
  process.exit(1)
}
console.log("Every colour, typeface, corner and duration comes from a token.")
