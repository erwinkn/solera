import { useShortcut } from "@ui-sdk/react/shortcuts"
import { Activity, Boxes, Cog, KeyRound, LayoutDashboard, Play, Search, Server, Zap } from "lucide-react"
import { useSyncExternalStore, type ComponentType, type ReactNode } from "react"
import { useLocation } from "wouter"
import { useManifest, useRuns } from "../api/queries"
import { shortId } from "../model/format"
import { AssetActions, AssetState, firstLine } from "../blocks/assets"
import { runWhat } from "../blocks/runs"
import { openStartRun } from "../blocks/start-run"
import { setLook, themes, useLook, VARIATIONS } from "../theme/store"
import { Button } from "../ui/controls"
import { Palette, type Command_ } from "../ui/overlays"
import { Link } from "wouter"

/* What the three layouts share: the list of places, the command palette, and
 * the contract each layout fills in. */

export const PLACES = [
  { href: "/", label: "Overview", icon: LayoutDashboard },
  { href: "/runs", label: "Runs", icon: Activity },
  { href: "/assets", label: "Assets", icon: Boxes },
  { href: "/automations", label: "Automations", icon: Zap },
  { href: "/keys", label: "Find a key", icon: KeyRound },
  { href: "/engine", label: "Engine", icon: Server },
  { href: "/settings", label: "Settings", icon: Cog },
] as const

/** True for the place a path belongs to: /runs/abc is under Runs, /sources/x under Assets. */
export function isCurrent(place: string, path: string): boolean {
  if (place === "/") return path === "/"
  if (place === "/assets") return path.startsWith("/assets") || path.startsWith("/sources")
  if (place === "/automations") return path.startsWith("/automations") || path.startsWith("/sensors")
  return path.startsWith(place)
}

/** A layout: its frame, and how it composes each page. */
export type Layout = {
  Shell: ComponentType<{ children: ReactNode }>
  Home: ComponentType
  Runs: ComponentType
  Run: ComponentType<{ id: string }>
  Assets: ComponentType
  Asset: ComponentType<{ name: string }>
  Source: ComponentType<{ name: string }>
  Automations: ComponentType
  Sensor: ComponentType<{ name: string }>
  Engine: ComponentType
  Keys: ComponentType
  Settings: ComponentType
  NotFound: ComponentType
}

let paletteOpen = false
const listeners = new Set<() => void>()
export function openPalette(open = true) {
  paletteOpen = open
  listeners.forEach((listener) => listener())
}

export function CommandPalette() {
  const open = useSyncExternalStore(
    (listener) => {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    () => paletteOpen,
  )
  useShortcut("mod+k", () => openPalette(!paletteOpen), { description: "Search and commands" })
  return open ? <PaletteBody /> : null
}

function PaletteBody() {
  const [, navigate] = useLocation()
  const manifest = useManifest()
  const runs = useRuns({}, 12, false)
  const look = useLook()
  const commands: Command_[] = [
    { value: "do:run", group: "Do", label: "Start a run", icon: <Play size={15} />, run: () => openStartRun() },
    ...PLACES.map((place): Command_ => ({ value: `go:${place.href}`, group: "Go to", label: place.label, icon: <place.icon size={15} />, run: () => navigate(place.href) })),
    ...Object.entries(manifest.data?.assets ?? {}).map(([name, spec]): Command_ => ({ value: `asset:${name}`, group: "Assets", label: name, note: firstLine(spec.doc).slice(0, 60), keywords: ["asset"], run: () => navigate(`/assets/${encodeURIComponent(name)}`) })),
    ...Object.keys(manifest.data?.sources ?? {}).map((name): Command_ => ({ value: `source:${name}`, group: "Sources", label: name, keywords: ["source", "commit"], run: () => navigate(`/sources/${encodeURIComponent(name)}`) })),
    ...Object.keys(manifest.data?.sensors ?? {}).map((name): Command_ => ({ value: `sensor:${name}`, group: "Sensors", label: name, keywords: ["sensor"], run: () => navigate(`/sensors/${encodeURIComponent(name)}`) })),
    ...(runs.data?.runs ?? []).map((run): Command_ => ({ value: `run:${run.id}`, group: "Recent runs", label: runWhat(run), note: `${run.status} · ${shortId(run.id)}`, keywords: ["run", run.id], run: () => navigate(`/runs/${run.id}`) })),
    ...VARIATIONS.filter((v) => v.id !== look.variation).map((v): Command_ => ({ value: `layout:${v.id}`, group: "Look", label: `Layout: ${v.name}`, note: v.note, keywords: ["layout", "variation"], run: () => setLook({ variation: v.id }) })),
    ...themes(look).filter((t) => t.id !== look.theme).map((t): Command_ => ({ value: `theme:${t.id}`, group: "Look", label: `Theme: ${t.name}`, note: t.note, keywords: ["theme"], run: () => setLook({ theme: t.id }) })),
  ]
  return <Palette open onOpenChange={openPalette} commands={commands} />
}

export function SearchButton({ compact }: { compact?: boolean }) {
  return (
    <button className="search-button" onClick={() => openPalette()} aria-label="Search and commands">
      <Search size={14} aria-hidden="true" />
      {!compact && <span className="grow">Search or jump to</span>}
      <kbd className="kbd">⌘K</kbd>
    </button>
  )
}

export function StartRunButton({ size = "m" }: { size?: "s" | "m" }) {
  return (
    <Button variant="primary" size={size} icon={<Play size={14} />} onClick={() => openStartRun()}>
      Start a run
    </Button>
  )
}

/** A quick look at one asset, where a layout shows what is picked on the project chart. */
export function AssetPeek({ id }: { id: string }) {
  const manifest = useManifest()
  const name = id.startsWith("part:") ? id.slice(5).split("|")[0]! : id
  const partition = id.startsWith("part:") ? id.slice(5).split("|").slice(1).join("|") : null
  if (manifest.data?.sources[name])
    return (
      <div className="stack">
        <h3 className="inspector-title mono">{name}</h3>
        <p className="muted">A source: data that lives outside, which the engine knows at versions.</p>
        <Link href={`/sources/${encodeURIComponent(name)}`} className="link">Open the source</Link>
      </div>
    )
  const spec = manifest.data?.assets[name]
  if (!spec) return null
  return (
    <div className="stack">
      <div>
        <h3 className="inspector-title mono">
          {name}
          {partition && <span className="muted"> {partition}</span>}
        </h3>
        <p className="muted">{firstLine(spec.doc)}</p>
      </div>
      <AssetState name={name} />
      <div className="row wrap">
        <AssetActions name={name} size="s" />
        <Link href={`/assets/${encodeURIComponent(name)}`} className="link">Open the asset</Link>
      </div>
    </div>
  )
}
