# Themes

A theme is data: a value for each of 44 named tokens. Components read the tokens as CSS variables and set no colour, typeface, corner radius or duration of their own. `pnpm check` fails if one does (`scripts/check-tokens.mjs`).

The console ships three themes, in `src/theme/themes.ts`:

- **Fino**: light, warm paper, serif titles.
- **Oloroso**: dark, walnut and cream.
- **Tinta**: high contrast, one monospace typeface, square corners, no motion.

Layout is a separate choice. Any theme works with any of the three layouts (Workbench, Control room, Briefing), and both switch in the running app from the palette button in the header or from Settings. The browser remembers them.

## Make your own

Open **Settings → Your themes → New theme** and write a JSON object with three fields:

```json
{
  "name": "Rioja",
  "extends": "oloroso",
  "tokens": {
    "color-bg": "#1a0f12",
    "color-surface": "#22141a",
    "color-accent": "#d04a5a",
    "radius-m": "14px"
  }
}
```

- `name` is what the theme is called in the menus.
- `extends` is the built-in theme it starts from: `fino`, `oloroso` or `tinta`.
- `tokens` holds only the tokens you change. Each value is a string of CSS, the same you would write in a stylesheet.

The page repaints as you type, so you see the theme before you save it. The editor refuses an unknown token, an empty value, or a `scheme` other than `light` or `dark`, and says which line is wrong. **List every token** fills the editor with all 44 of the base theme's values, to edit from. **Open a file** loads a `.json` file, and the copy button beside a saved theme puts its JSON on the clipboard, which is how you pass a theme to someone else.

Saved themes live in this browser's storage, under the key `solera.console.look`. Nothing is sent to the server. To ship a theme to everyone, add it to `BUILT_IN` in `src/theme/themes.ts` and rebuild.

Two limits to know about. The server's content security policy allows fonts from the console's own files only, so `font-*` tokens can name the four bundled families (`Instrument Sans Variable`, `Inter Variable`, `Newsreader Variable`, `JetBrains Mono Variable`) or a font installed on the reader's machine. And a theme cannot reach a layout: where things sit is the layout's business.

## The tokens

Every size in the console is a multiple of three of them: `font-size` (every text size is a ratio of it), `space` (every gap and padding) and `density` (row heights and control sizes). Setting `motion-fast` and `motion-slow` to `0ms` stops all movement, spinners included.

| Token | What it sets | In Fino |
| --- | --- | --- |
| `scheme` | Light or dark: tells the browser how to draw its own controls and scrollbars | `light` |
| `color-bg` | The page behind everything | `#f5f2ec` |
| `color-surface` | Panels, tables and the timeline | `#fcfbf8` |
| `color-raised` | Menus, dialogs and anything that floats | `#ffffff` |
| `color-sunken` | Wells: code, logs, inputs, the track behind a bar | `#eeeae2` |
| `color-border` | Hairlines between rows and around panels | `#e4dfd4` |
| `color-border-strong` | The edge of a control, and hairlines that must be seen | `#cbc4b5` |
| `color-text` | Body text | `#211e19` |
| `color-text-muted` | Secondary text and labels | `#6a6457` |
| `color-text-faint` | Hints, placeholders, what has not happened yet | `#9c9586` |
| `color-accent` | Primary buttons, the selected item, links | `#211e19` |
| `color-accent-contrast` | Text on the accent | `#fcfbf8` |
| `color-focus` | The keyboard focus ring | `#3b5fc0` |
| `color-overlay` | The dim behind a dialog | `rgb(33 30 25 / 0.28)` |
| `color-ok` | Succeeded, up to date | `#33794f` |
| `color-running` | Running now | `#3b5fc0` |
| `color-failed` | Failed, rejected | `#c2392e` |
| `color-stale` | Stale, retrying, needs attention soon | `#ad6f0c` |
| `color-waiting` | Queued or held back | `#7566ad` |
| `color-idle` | Never built, skipped, canceled | `#a39c8d` |
| `font-sans` | The interface | `"Instrument Sans Variable", system-ui, sans-serif` |
| `font-mono` | Names, ids, numbers in columns, logs | `"JetBrains Mono Variable", monospace` |
| `font-display` | Page titles and big numbers | `'"Newsreader Variable", Georgia, serif'` |
| `font-size` | The base size; every other size is a ratio of it | `14px` |
| `font-weight` | Body weight | `420` |
| `font-weight-strong` | Emphasis and headings in the interface font | `600` |
| `font-weight-display` | Weight of the display font | `480` |
| `font-tracking-display` | Letter spacing of the display font | `-0.015em` |
| `line-height` | Body line height, unitless | `1.45` |
| `radius-s` | Bars, chips, checkboxes | `3px` |
| `radius-m` | Buttons, inputs, menu items | `6px` |
| `radius-l` | Panels, dialogs | `10px` |
| `space` | The spacing unit; every gap and padding is a multiple of it | `4px` |
| `density` | A multiplier on row heights and control sizes: below 1 is tighter | `1` |
| `border-width` | Hairline thickness | `1px` |
| `shadow-raised` | Panels that sit above the page | `0 1px 2px rgb(33 30 25 / 0.05)` |
| `shadow-overlay` | Menus and dialogs | `0 12px 32px rgb(33 30 25 / 0.14), 0 2px 6px rgb(33 30 25 / 0.08)` |
| `motion-fast` | Hover, press, small state changes | `120ms` |
| `motion-slow` | Panels opening, bars growing | `260ms` |
| `motion-ease` | The easing curve of both | `cubic-bezier(0.2, 0.7, 0.2, 1)` |
| `chart-bar` | A finished bar on the timeline | `#8f897c` |
| `chart-bar-radius` | Corner radius of a bar | `2px` |
| `chart-bar-height` | Bar height as a fraction of its row, 0 to 1 | `0.5` |
| `chart-grid` | The timeline's vertical grid lines | `#ebe6dc` |

The six status colours (`color-ok`, `color-running`, `color-failed`, `color-stale`, `color-waiting`, `color-idle`) are the only ones that carry meaning. Keep them apart from each other and from the text colour, and check them against `color-surface`, which is what they sit on.
