import { useMemo, useState } from "react"
import { useAttemptResults, useAutomations, useLiveTasks, useManifest, useStatuses } from "../api/queries"
import type { RunDetail } from "../api/types"
import { useActivity } from "../model/activity"
import { layoutRail, projectGraph, topological } from "../model/graph"
import { isLive, TERMINAL_TASK } from "../model/status"
import { assetRows, projectRows } from "../timeline/project"
import { runChart, type RunChart } from "../timeline/run"
import { Timeline, useNow } from "../timeline/Timeline"
import { Checkmark, Segmented } from "../ui/controls"
import { Failure, Loading } from "./atoms"

/* The timeline graph in its three places: the whole project's recent activity,
 * one asset's, and one run. */

export const WINDOWS = [
  { value: "300", label: "5 min" },
  { value: "900", label: "15 min" },
  { value: "3600", label: "1 hour" },
  { value: "21600", label: "6 hours" },
]

/** The axis of a live window: the last `seconds`, plus a little of what comes
 * next. Its start steps forward in whole ticks so the grid does not crawl. */
function windowRange(seconds: number, now: number): [number, number] {
  const ahead = seconds * 0.12
  const step = seconds / 60
  const end = Math.ceil((now + ahead) / step) * step
  return [end - seconds - ahead, end]
}

type ProjectProps = { window: number; selected: string | null; onSelect: (id: string | null) => void; maxHeight?: string }

export function ProjectTimeline({ window, selected, onSelect, maxHeight }: ProjectProps) {
  const manifest = useManifest()
  const statuses = useStatuses()
  const automations = useAutomations()
  const activity = useActivity(window)
  const live = useLiveTasks()
  const now = useNow(true, 1000)
  const [expanded, setExpanded] = useState<string[]>([])
  const plan = useMemo(() => {
    if (!manifest.data) return null
    const graph = projectGraph(manifest.data)
    const order = topological(graph.nodes.map((n) => n.id), graph.edges)
    const read = new Set(graph.edges.filter((e) => e.kind === "reads").map((e) => e.from))
    const faint = new Set(graph.edges.map((e) => e.from).filter((id) => !read.has(id)))
    return { graph, order, rail: { ...layoutRail(order, graph.edges), faint } }
  }, [manifest.data])
  const range = windowRange(window, now)
  const rows = useMemo(
    () =>
      plan && manifest.data
        ? projectRows({ manifest: manifest.data, ...plan, statuses: statuses.data?.assets ?? {}, tasks: activity.data?.tasks ?? [], live, automations: automations.data?.automations ?? [], now, range })
        : [],
    // `range` and `live` follow `now` and the polls: listing them would rebuild on every render
    [plan, manifest.data, statuses.data, activity.data, automations.data, now],
  )
  if (manifest.error) return <Failure error={manifest.error} />
  if (!plan) return <Loading what="Reading the project" />
  return (
    <Timeline
      label="What every asset did lately, and what runs next"
      rows={rows}
      range={range}
      now={now}
      rail={plan.rail}
      selected={selected}
      onSelect={onSelect}
      expanded={expanded}
      onExpandedChange={setExpanded}
      headers={{ name: "Asset", progress: "Built", total: "Tasks" }}
      maxHeight={maxHeight}
    />
  )
}

export function AssetTimeline({ asset, window, maxHeight }: { asset: string; window: number; maxHeight?: string }) {
  const activity = useActivity(window, asset)
  const live = useLiveTasks()
  const now = useNow(true, 1000)
  const [selected, setSelected] = useState<string | null>(null)
  const range = windowRange(window, now)
  const rows = useMemo(() => assetRows(asset, activity.data?.tasks ?? [], now, live), [asset, activity.data, now])
  if (activity.error) return <Failure error={activity.error} />
  return (
    <Timeline
      label={`What ${asset} did lately`}
      rows={rows}
      range={range}
      now={now}
      selected={selected}
      onSelect={setSelected}
      expanded={[]}
      onExpandedChange={() => {}}
      headers={{ name: "Partition", progress: "", total: "Tasks" }}
      maxHeight={maxHeight}
      empty={activity.isLoading ? "Loading…" : "It did not run in this window."}
    />
  )
}

export function WindowPicker({ value, onChange }: { value: number; onChange: (seconds: number) => void }) {
  return <Segmented label="How far back the timeline goes" value={String(value)} onChange={(next) => onChange(Number(next))} options={WINDOWS} />
}

export type RunView = { chart: RunChart; live: boolean; expanded: string[]; setExpanded: (ids: string[]) => void; onlyTrouble: boolean; setOnlyTrouble: (on: boolean) => void; now: number }

/** A run's chart and the state around it, so a page can put the controls where it likes. */
export function useRunView(detail: RunDetail): RunView {
  const manifest = useManifest()
  // a canceled run has ended while the tasks it was running still wind down
  const live = isLive(detail.request) || detail.tasks.some((task) => !TERMINAL_TASK.has(task.status))
  const now = useNow(live)
  const [expanded, setExpanded] = useState<string[] | null>(null)
  const [onlyTrouble, setOnlyTrouble] = useState(false)
  const [wanted, setWanted] = useState<string[]>([])
  // the keys of the per-key attempts whose rows are open, one request each
  const results = useAttemptResults(detail.request.id, wanted)
  const chart = runChart(detail, manifest.data, now, results, { onlyTrouble, expanded })
  if (chart.wanted.join() !== wanted.join()) setWanted(chart.wanted)
  return { chart, live, expanded: expanded ?? chart.open, setExpanded, onlyTrouble, setOnlyTrouble, now }
}

export function RunTimeline({ view, selected, onSelect, maxHeight }: { view: RunView; selected: string | null; onSelect: (id: string | null) => void; maxHeight?: string }) {
  const { chart } = view
  return (
    <Timeline
      label="The run's tasks, and when each ran"
      rows={chart.rows}
      range={chart.range}
      origin={chart.origin}
      now={view.live ? view.now : undefined}
      rail={chart.rail}
      selected={selected}
      onSelect={onSelect}
      expanded={view.expanded}
      onExpandedChange={view.setExpanded}
      headers={{ name: "Step", progress: "Progress", total: "Time" }}
      maxHeight={maxHeight}
    />
  )
}

export function TroubleFilter({ view }: { view: RunView }) {
  if (view.chart.counts.total < 13) return null
  return (
    <Checkmark checked={view.onlyTrouble} onChange={view.setOnlyTrouble}>
      Only what failed, runs or waits
    </Checkmark>
  )
}
