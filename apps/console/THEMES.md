# Themes

A theme is data: a value for each of 40 named tokens. Components read the tokens as CSS variables and set no colour, typeface, corner radius or duration of their own. `pnpm check` fails if one does (`scripts/check-tokens.mjs`).

The console ships three themes, in `src/theme/themes.ts`. They are the themes of a tool: grey surfaces with almost no hue, one sans typeface at 13px with a monospace for ids, keys, values and logs, hairlines instead of boxes, and colour kept for status.

- **Dark** (`dark`): neutral near-black. The accent is the text colour itself.
- **Light** (`light`): the same on white.
- **Dimmed** (`dimmed`): dark but lower in contrast and tinted blue-grey, with a blue accent.

Until someone picks a theme, the console follows the system: Dark, or Light when the system is light.

Layout is a separate choice. Any theme works with any of the three layouts (Workbench, Control room, Briefing), and both switch in the running app from the palette button beside the health status, or from Settings. The browser remembers them.

## Make your own

Open **Settings → Your themes → New theme** and write a JSON object with three fields:

```json
{
  "name": "Violet",
  "extends": "dark",
  "tokens": {
    "color-accent": "#8b7cf6",
    "color-accent-contrast": "#0b0b0c",
    "color-focus": "#8b7cf6",
    "radius-m": "8px"
  }
}
```

- `name` is what the theme is called in the menus.
- `extends` is the built-in theme it starts from: `dark`, `light` or `dimmed`.
- `tokens` holds only the tokens you change. Each value is a string of CSS, the same you would write in a stylesheet.

The page repaints as you type, so you see the theme before you save it. The editor refuses an unknown token, an empty value, or a `scheme` other than `light` or `dark`, and says which line is wrong. **List every token** fills the editor with all 40 of the base theme's values, to edit from. **Open a file** loads a `.json` file, and the copy button beside a saved theme puts its JSON on the clipboard, which is how you pass a theme to someone else.

Saved themes live in this browser's storage, under the key `solera.console.look`. Nothing is sent to the server. To ship a theme to everyone, add it to `BUILT_IN` in `src/theme/themes.ts` and rebuild.

Two limits to know about. The server's content security policy allows fonts from the console's own files only, so `font-*` tokens can name the two bundled families (`Inter Variable`, `JetBrains Mono Variable`) or a font installed on the reader's machine. And a theme cannot reach a layout: where things sit is the layout's business.

## The tokens

Every size in the console is a multiple of three of them: `font-size` (every text size is a ratio of it), `space` (every gap and padding) and `density` (row heights and control sizes). Setting `motion-fast` and `motion-slow` to `0ms` stops all movement, spinners included.

| Token | What it sets | In Dark |
| --- | --- | --- |
| `scheme` | Light or dark: tells the browser how to draw its own controls and scrollbars | `dark` |
| `color-bg` | The page | `#0b0b0c` |
| `color-surface` | The sidebar, bars and anything set a step apart from the page | `#101011` |
| `color-raised` | Menus, dialogs and anything that floats | `#18181a` |
| `color-sunken` | Wells: code, logs, inputs, the track behind a bar | `#141416` |
| `color-border` | Hairlines between rows and sections | `#202023` |
| `color-border-strong` | The edge of a control, and hairlines that must be seen | `#2f2f34` |
| `color-text` | Body text | `#ececee` |
| `color-text-muted` | Secondary text and column headers | `#9c9ca5` |
| `color-text-faint` | Hints, placeholders, what has not happened yet | `#66666f` |
| `color-accent` | Primary buttons, the selected tab, checked boxes | `#ececee` |
| `color-accent-contrast` | Text on the accent | `#0b0b0c` |
| `color-focus` | The keyboard focus ring | `#5b9cf8` |
| `color-overlay` | The dim behind a dialog | `rgb(0 0 0 / 0.6)` |
| `color-ok` | Succeeded, up to date | `#3fb97c` |
| `color-running` | Running now | `#5b9cf8` |
| `color-failed` | Failed, rejected | `#ef5a5a` |
| `color-stale` | Stale, retrying, needs attention soon | `#dda032` |
| `color-waiting` | Queued or held back | `#8f8f99` |
| `color-idle` | Never built, skipped, canceled | `#5c5c65` |
| `font-sans` | The interface | `"Inter Variable", ui-sans-serif, system-ui, sans-serif` |
| `font-mono` | Ids, keys, values and logs | `"JetBrains Mono Variable", ui-monospace, "SF Mono", Menlo, monospace` |
| `font-size` | The base size; every other size is a ratio of it | `13px` |
| `font-weight` | Body weight | `400` |
| `font-weight-strong` | Titles and emphasis | `600` |
| `line-height` | Body line height, unitless | `1.5` |
| `radius-s` | Bars, chips, checkboxes | `3px` |
| `radius-m` | Buttons, inputs, menu items | `5px` |
| `radius-l` | Dialogs | `8px` |
| `space` | The spacing unit; every gap and padding is a multiple of it | `4px` |
| `density` | A multiplier on row heights and control sizes: below 1 is tighter | `1` |
| `border-width` | Hairline thickness | `1px` |
| `shadow-overlay` | Menus and dialogs | `0 12px 32px rgb(0 0 0 / 0.5), 0 0 0 1px #2f2f34` |
| `motion-fast` | Hover, press, small state changes | `100ms` |
| `motion-slow` | Dialogs and panels opening | `180ms` |
| `motion-ease` | The easing curve of both | `cubic-bezier(0.2, 0, 0, 1)` |
| `chart-bar` | Finished work on a chart: a bar of the timeline, a cell of an activity strip | `#6e6e78` |
| `chart-bar-radius` | Corner radius of a bar | `1px` |
| `chart-bar-height` | Bar height as a fraction of its row, 0 to 1 | `0.44` |
| `chart-grid` | A chart's grid lines, and the cells of a strip where nothing ran | `#1a1a1d` |

Four hues carry meaning: `color-ok`, `color-running`, `color-failed` and `color-stale`. `color-waiting` and `color-idle` are greys in the built-in themes, told apart by the shape of their mark. Keep the four apart from each other and from the text colour, and check them against `color-bg`, which is what they sit on. Finished work on a chart is drawn in `chart-bar`, a grey, so that a failure is the only colour on a quiet page.
