# The Solera console — design note

The console is a read-mostly operator's view over the HTTP API
(`python/solera_server/api.py`). It holds no state of its own beyond the
API token (session storage) and the chosen theme (local storage).

## What an operator needs at a glance

Five questions drive every screen, in this order:

1. **What's running?** Runs, their tasks and attempts in flight, and how far
   each task's walk has got: batches committed, the key it reached.
2. **What failed, and why?** Failed runs with their first error line, failed
   keys with their message and retry record, one click from the traceback
   or log line.
3. **What's stale, and why?** Partitions and keys due a rebuild, each with
   its reasons (input changed, upstream stale, definition changed), and what
   each partition owes each input (added, updated, removed keys).
4. **What needs an operator?** Repairs a dead writer left, stuck ones first,
   and stuck cleanups, with the action that clears what an operator must.
5. **What's next?** Automations by their next fire, keys due for retry.

Dense where the data is dense (runs, keys, logs, events: tables with
monospace ids, tabular numerals, one line per thing); calm everywhere else
(cards with one idea each, generous spacing, prose explanations of empty
states).

## Information architecture

| Route | Answers |
|---|---|
| `/` Overview | the five questions: vitals, activity histogram, running now, recent failures, assets needing attention, up next |
| `/assets` | the asset graph (layered DAG; edge style = edge kind) or a list (`?view=list`) |
| `/assets/$asset` | declaration, heads per output × partition, staleness, cursor, automations |
| `…/partitions` | the partition grid (1-D strip, 2-D matrix) colored by status, stale reasons, stale keys |
| `…/keys` | stale keys with reasons, failed keys and their retries, per-key outcomes, **explain a key**, live key browser, forced retries |
| `…/inputs` | every input: kind, patterns, and per partition what it owes (added, updated, removed, or pending), a full run due, the commit it is observed through |
| `…/history` | materializations (versions, rows, metadata) and lineage |
| `…/runs` | runs that touched the asset |
| `/runs` | filterable run history (facets, histogram, text search), all in the URL; the engine's cleanup tasks behind a toggle |
| `/runs/$run` | tasks × attempts waterfall, each task's walk batch by batch (key range, added/updated/removed/unchanged, its attempts and retries), attempt phases and cancel phases, logs with tailing, spec/result, the run's event timeline, cancel/retry/pause |
| `/automations` | triggers, last and next fire, enable/disable, run now |
| `/sensors`, `/sensors/$sensor` | sensors, hosts, tick history |
| `/sources`, `/sources/$source` | head, keys, commit history, commit form |
| `/executors` | executors with in-flight vs limit, pool workers, sensor hosts |
| `/health` | engine diagnostics, repairs owed (stuck ones flagged), cleanups stuck or awaiting their task (clear), store kinds |

Navigation state lives in the URL: filters, the selected partition, task,
attempt and tab are search params validated per route, so every view is a
link. The browser's back button is the undo.

## Theming: one component tree, three token sets

Components reference **semantic tokens only** — no hex values, no
Tailwind palette (`--color-*: initial` removes it). A theme is a block of
CSS custom properties under `[data-theme=…]`; switching sets one attribute
on `<html>`. No component reads the theme name. Three themes: **Normal**
(a calm, professional tool), **Fun** (warm paper, a serif with character,
sticker shadows) and **Brutal** (neubrutalism: black outlines, hard offset
shadows, square corners, flat saturated fills, a visible grid).

A region can carry its own scope: the navigation is marked `data-chrome`,
and a theme may redefine tokens inside it. Brutal makes it a black slab
with white rules (after Gumroad); Normal and Fun leave it as the page.

| Group | Tokens | Normal | Fun |
|---|---|---|---|
| Surfaces | `bg`, `surface`, `surface-2`, `sunken`, `overlay` | cool off-white canvas, white panels | warm paper, cream panels, grain texture |
| Ink | `fg`, `fg-muted`, `fg-subtle`, `fg-inverse` | zinc ramp | oak-brown ink ramp |
| Lines | `line`, `line-strong`, `border-w` | hairlines, 1px | inked outlines, 1.5px |
| Accent | `accent`, `accent-soft`, `accent-fg`, `focus` | ink-dark buttons, blue focus | vermilion buttons, marigold focus |
| Status | `ok`, `run`, `wait`, `warn`, `fail`, `idle` × (solid, soft, fg) | muted, color means state only | saturated, same semantics |
| Phases | `ph-1` … `ph-7` | a cool sequential ramp | a sherry-cellar categorical set |
| Type | `font-sans`, `font-display`, `font-mono`, display weight/tracking/variation | Geist / Geist / Geist Mono, 13px base | Bricolage Grotesque / Fraunces (soft, wonky) / Geist Mono, 14px base |
| Shape | `radius-sm…lg`, `shadow-1…3`, `shadow-press` | 6px, soft shadows | 14px, hard offset "sticker" shadows; buttons press in |
| Space | `--spacing` (Tailwind's base unit) | 0.25rem | 0.27rem: a touch roomier |
| Motion | `ease`, `ease-bounce`, `dur-1…3` | quick, no overshoot | springy overshoot |
| Accents | `texture`, `canvas-pattern`, `title-mark` (+ size, repeat, pad), `art-*` | none, grey dots, no underline, quiet art | paper grain, colored dots, squiggle underline, full-color art |

Brutal sets every row too: an off-white page with a faint 32px grid,
black ink, Gumroad pink for primary actions and the active item, yellow
highlighter under titles, saturated status fills with dark text, Space
Grotesk under Archivo Black titles (uppercase, `head-case`), radius 0
everywhere (`r-pill` and `r-mark` included, so pills and chart marks go
square too), 2px borders, shadows of 2/4/7px at zero blur, and buttons
that lift on hover (`lift-*`, `shadow-button-hover`) and press flat.

Names are never case-transformed: headings that are an asset, a run's
targets or a sensor (`ident`) keep their case in every theme.

A theme may also change a little structure, through tokens with a
default in `:root`: a rule under page headers (`masthead-rule`), the role
of card titles (`card-title-*`: font, size, weight, case, tracking, a width
axis, italic), a card header as a title bar (`card-head-bg`, `card-head-rule`),
the figure and label fonts (`figure-*`, `label-*`), a display width and style
(`head-stretch`, `head-style`, for variable faces with a width axis and for
italic display faces), registration marks at card corners (`tick`), a bar
beside the active navigation item (`nav-bar`), the navigation's own
background (`chrome-bg`), a glow around things running now (`live-glow`),
and the color scheme (`scheme`, so native controls follow a dark theme).

Four draft directions for the console's voice sit beside the three themes
above and the earlier drafts (Cellar, Instrument, Observatory):

| Draft | Idea | After |
|---|---|---|
| **Voltage** (light) | one current: paper and ink, and a single electric blue that means energized (running, the primary action, the active item, focus); grey means fine, red means broken; tall condensed capitals for titles and figures, a mono for labels, square corners, no shadows | Nous Research |
| **Obsidian** (dark) | editorial authority in the dark: a warm near-black, rose quartz for every action, hairlines instead of shadows, an italic serif for titles over a plain grotesk, mineral status colours | Hex, Hebbia |
| **Workbench** (light) | everything is an object: a warm grey desk, white panels with an outline and a title bar, flat opaque chips, a friendly rounded grotesk, one orange action, buttons with a bottom lip | PostHog |
| **Reactor** (dark) | broadcast graphics: a black void, cool white ink, heavy expanded capitals, tracked mono labels, zero radius, white for the action and one luminous lime for live | Hermes 4 |

Voltage was picked, then explored as five variants (Blueprint, Ledger,
Signal, Night and Arc; the branch history has them and their screenshots).
Two survive, one for day and one for night, each keeping the rule (one hue
means energized, grey means fine, red means broken):

| Variant | Idea | Changed from Voltage |
|---|---|---|
| **Signal** (light) | the power rail: the blue becomes the navigation | white paper; the navigation is a solid blue slab with white type (its own token scope), the page beside it is monochrome; figures very tall and narrow; a flat blue halo for live. Two siblings, **Signal · Ink** and **Signal · Tint**, keep the page and try a darker slab and a pale one with dark type, until one is picked |
| **Arc** (dark) | the arc in the dark | deep navy paper, blue-white ink, white for the action and one phosphor cyan for live with a real glow; card titles drop to a tracked mono, the Reactor way |

Both are softened in PostHog's spirit without leaving the square voice: 4px
on controls and chips (`r-sm`, `r-pill`), 6px on panels (`r-md`, `r-lg`),
outlines a step heavier in place of shadows, a 1px outline and a 2px bottom
lip on buttons (`shadow-button` as a translucent ink under the button,
`press-y` 2px so it presses flat), a calmer focus ring, and easing a touch
longer. Signal's blue moved from pure ultramarine to a cobalt ink, a step
less saturated. No token was added for any of this: every change is a value
of a token that already existed.

The illustrations (empty states: a stack of solera barrels, the
fractional-blending system the product is named after) are one SVG
component drawn with `currentColor` and status tokens; the theme decides how
loud they are. `prefers-reduced-motion` zeroes the motion tokens in both
themes. `pnpm contrast` checks every text/background pair of every theme, and
of Brutal's navigation scope, against WCAG AA.

**The default is Voltage (D160):** Signal by day and Arc at night. With no
theme chosen, the console follows the system's light or dark preference, and
switches with it; "Automatic" at the top of the theme menu returns to that
after a pick. Every other theme stays selectable.

The theme is persisted in `localStorage` and applied before first paint by
`public/theme.js`, a blocking external script (no inline script, so the
server's CSP can drop `'unsafe-inline'` for scripts).

## Libraries

- **React 19 + TypeScript**, Vite, no SSR: the server serves a static bundle.
- **TanStack Router**, code-based routes with typed, validated search params.
- **TanStack Query** for every read and write.
- **Base UI** for behaviour that is hard to get right — dialog, popover,
  menu, tooltip, switch, toast — styled with our tokens. No shadcn: we'd
  restyle all of it anyway.
- **Tailwind v4** as the styling vocabulary over the tokens.
- **lucide-react** icons. Charts, the DAG layout and the timelines are our
  own SVG: small, and they speak this domain (phases, lag, partitions).

## Data-fetching conventions

- `src/api/client.ts` is the only `fetch`. It attaches the bearer token and
  throws `ApiError(status, detail)`. A 401 anywhere flips the session to
  "needs a token" and the shell renders the connect form.
- `src/api/queries.ts` holds one `queryOptions` factory per resource, keys
  hierarchical from the resource down (`["runs", "list", filter]`,
  `["runs", id]`, `["runs", id, "events"]`, `["attempts", run, id, "logs", tail]`),
  so invalidation can be as wide or narrow as a mutation's effect.
- **Live data polls**, and only while it can change: `refetchInterval` is a
  function of the data (an active run every second, a finished one never;
  lists every few seconds; the manifest never). Logs of a running attempt
  tail every second (`?tail=`); a finished attempt's log is fetched once.
  Polling pauses in background tabs.
- The manifest is keyed by the project revision from `/api/diagnostics`:
  a deploy changes the key, so the new manifest loads with no effect or
  manual invalidation.
- `src/api/mutations.ts`: each mutation invalidates the resources it
  changes (a cancel invalidates that run and the run lists), and reports
  errors as toasts.
- No `useEffect` for data: derived values are computed in render
  (`useMemo` only when measured to matter). The clock for relative times
  is one shared ticker read with `useSyncExternalStore`. Session, theme and
  clock are tiny external stores; nothing is mirrored into component state.

## API additions

Views that would otherwise reconstruct engine state client-side get small
read-only endpoints instead (with tests in `tests/server`):

| Route | For |
|---|---|
| `GET /assets:status` | per-asset rollup for the graph and the overview: partition counts by status, last outcome, failed keys, repairs owed and stuck |
| `GET /assets/{a}/failed-keys` | failed keys with class, tries, due, message |
| `GET /assets/{a}/stale-keys` | a partition's stale keys and why |
| `GET /assets/{a}/key-outcomes` | the `key_outcomes` history, searchable by key |
| `GET /assets/{a}/explain?key=` | why a key is (not) in the output: patterns, failure, last outcome, generations |
| `GET /assets/{a}/inputs` | every input, and per partition its observed-set summary: owed keys or pending, a full run due, observed through |
| `GET /repairs`, `GET /cleanups` | repairs owed (stuck after their run limit), stuck cleanups and removed outputs awaiting their cleanup task |
| `next_at` on automations | the next scheduled fire, from the engine's own clock rule |

Fields the observed-set rebuild adds as it lands — a task's `progress`, an
attempt's `batch`, an input partition's `observed`, per-key stale reasons,
the `pending` partition status, a source's `loader`, `version`, `dims` and
`observe`, and served versions on source keys — are optional in
`src/api/types.ts`; a view shows them when the engine sends them and hides
them otherwise, so the console runs against an engine on either side of the
rebuild. `src/api/read.ts` reads the few fields whose shape is still settling.
