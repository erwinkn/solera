import { useShortcut } from "@ui-sdk/react/shortcuts"
import { Activity, Boxes, Cog, KeyRound, LayoutDashboard, Play, Search, Server, Zap } from "lucide-react"
import { useSyncExternalStore, type ComponentType, type ReactNode } from "react"
import { useLocation } from "wouter"
import { useManifest, useRuns } from "../api/queries"
import { shortId } from "../model/format"
import { firstLine } from "../blocks/assets"
import { runWhat } from "../blocks/runs"
import { openStartRun } from "../blocks/start-run"
import { setLook, themes, useLook, VARIATIONS } from "../theme/store"
import { Button } from "../ui/controls"
import { Palette, type Command_ } from "../ui/overlays"

/* What the three layouts share: the list of places, the command palette, and
 * the contract each layout fills in. */

export const PLACES = [
  { href: "/", label: "Overview", icon: LayoutDashboard, group: "project" },
  { href: "/runs", label: "Runs", icon: Activity, group: "project" },
  { href: "/assets", label: "Assets", icon: Boxes, group: "project" },
  { href: "/automations", label: "Automations", icon: Zap, group: "project" },
  { href: "/keys", label: "Find a key", icon: KeyRound, group: "project" },
  { href: "/engine", label: "Engine", icon: Server, group: "system" },
  { href: "/settings", label: "Settings", icon: Cog, group: "system" },
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
      {!compact && <span className="grow">Search</span>}
      <kbd className="kbd">⌘K</kbd>
    </button>
  )
}

export function StartRunButton({ size = "m" }: { size?: "s" | "m" }) {
  return (
    <Button variant="primary" size={size} icon={<Play size={13} />} onClick={() => openStartRun()}>
      Start a run
    </Button>
  )
}

/** The mark of the console: staggered bars, as on a timeline. */
export function Logo({ size = 18 }: { size?: number }) {
  return (
    <svg className="logo" width={size} height={size} viewBox="0 0 18 18" aria-hidden="true">
      <rect x="2" y="3.5" width="8" height="2.6" rx="1.3" />
      <rect x="6" y="7.7" width="10" height="2.6" rx="1.3" />
      <rect x="3" y="11.9" width="7" height="2.6" rx="1.3" />
    </svg>
  )
}
