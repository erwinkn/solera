import type { LiveTask } from "../api/queries"
import type { AssetStatus, Automation, Manifest, TaskRow } from "../api/types"
import { taskSpan } from "../model/activity"
import { clock, duration, partitionLabel, plural } from "../model/format"
import type { Graph } from "../model/graph"
import { assetStatus, taskStatus, TERMINAL_TASK, type Tone } from "../model/status"
import type { Bar, Mark, Row } from "./types"

/* The project as a chart: every asset and source in the order data flows, what
 * each did over the last while, and what its automations will start next. */

const MARK: Record<Tone, Mark> = { ok: "done", running: "running", failed: "failed", stale: "stale", waiting: "waiting", idle: "todo" }

const natural = new Intl.Collator(undefined, { numeric: true })

type Live = Map<string, LiveTask>

/** A task in progress that is not running yet: queued, or waiting for a worker to take it. */
const waits = (task: TaskRow, live: Live) => {
  const status = live.get(task.id)?.status ?? task.status
  return !TERMINAL_TASK.has(task.status) && (status === "queued" || status === "claimable")
}

function taskBar(task: TaskRow, now: number, live_: Live): Bar | null {
  if (task.status === "skipped") return null // nothing was owed: no worker ran
  const known = TERMINAL_TASK.has(task.status) ? undefined : live_.get(task.id)
  const span = taskSpan(task, now)
  const { end, live } = span
  const start = known?.since ?? span.start
  const href = `/runs/${task.run}?at=${encodeURIComponent(`task:${task.id}`)}`
  const what = partitionLabel(task.partition)
  if (waits(task, live_)) {
    const why = known?.status === "claimable" ? "waiting for a worker" : "waiting for its turn"
    return { key: task.id, start: known?.since ?? task.created_at, end: null, tone: "waiting", kind: "wait", href, title: `${what} ${why} since ${clock(known?.since ?? task.created_at)}`.trim() }
  }
  const tone: Tone = live ? "running" : task.status === "succeeded" ? "ok" : task.status === "failed" || task.status === "blocked" ? "failed" : "idle"
  const title = `${what} ${taskStatus(task.status).label.toLowerCase()} at ${clock(start)}${live ? "" : `, ${duration(end - start)}`}`.trim()
  return { key: task.id, start, end: live ? null : end, tone, kind: "work", href, title }
}

function partitionRows(asset: string, tasks: TaskRow[], now: number, live: Live): Row[] {
  const by = new Map<string, TaskRow[]>()
  for (const task of tasks) by.set(task.partition, [...(by.get(task.partition) ?? []), task])
  return [...by.entries()]
    .sort(([a], [b]) => natural.compare(a, b))
    .map(([partition, own]): Row => {
      const latest = own.reduce((a, b) => (b.created_at >= a.created_at ? b : a))
      const tone: Tone = !TERMINAL_TASK.has(latest.status) ? (waits(latest, live) ? "waiting" : "running") : latest.status === "succeeded" || latest.status === "skipped" ? "ok" : latest.status === "canceled" ? "idle" : "failed"
      const bars = own.map((task) => taskBar(task, now, live)).filter((bar): bar is Bar => bar !== null)
      return {
        id: `part:${asset}|${partition}`,
        mark: tone === "idle" ? "skipped" : MARK[tone],
        tone: tone === "failed" ? "failed" : undefined,
        name: partitionLabel(partition) || asset,
        note: latest.error ?? undefined,
        bars,
        total: String(bars.length),
      }
    })
}

/** When an automation will next start a run, as far as the axis reaches. A
 * fixed interval repeats, so its next three firings are drawn; a schedule
 * gives its next time only. */
function planned(automations: Automation[], asset: string, now: number, until: number): Row["planned"] {
  const out: NonNullable<Row["planned"]> = []
  for (const automation of automations) {
    if (!automation.enabled || automation.next_at === null || !automation.targets.includes(asset)) continue
    const step = automation.trigger.kind === "every" ? automation.trigger.seconds : Infinity
    for (let at = Math.max(automation.next_at, now), n = 0; at <= until && n < 3; at += step, n++) out.push({ at, title: `${automation.name} starts a run at ${clock(at)}` })
  }
  return out
}

type Inputs = {
  manifest: Manifest
  graph: Graph
  order: string[]
  statuses: Record<string, AssetStatus>
  tasks: TaskRow[]
  live: Live
  automations: Automation[]
  now: number
  range: [number, number]
}

export function projectRows({ manifest, graph, order, statuses, tasks, live, automations, now, range }: Inputs): Row[] {
  const byAsset = new Map<string, TaskRow[]>()
  for (const task of tasks) byAsset.set(task.asset, [...(byAsset.get(task.asset) ?? []), task])
  const kind = new Map(graph.nodes.map((node) => [node.id, node.kind]))
  return order.map((id): Row => {
    if (kind.get(id) === "source") return { id, mark: "source", name: id, note: "source", bars: [] }
    const spec = manifest.assets[id]
    const status = statuses[id]
    const state = assetStatus(status)
    const own = byAsset.get(id) ?? []
    const bars = own.map((task) => taskBar(task, now, live)).filter((bar): bar is Bar => bar !== null)
    const partitioned = status?.partitioned ?? false
    const pool = spec && spec.placement.kind !== "Local" ? `runs on ${spec.placement.executor}` : undefined
    // "running" in the roll-up means a task is pending: say waiting when none of them has a worker yet
    const pending = own.filter((task) => !TERMINAL_TASK.has(task.status))
    const stuck = state.tone === "running" && pending.length > 0 && pending.every((task) => waits(task, live))
    const failingKeys = Object.entries(status?.stored_counts ?? {}).filter(([, n]) => n > 0)
    return {
      id,
      mark: stuck ? "waiting" : MARK[state.tone],
      tone: state.tone === "failed" ? "failed" : undefined,
      name: id,
      note:
        stuck && pool
          ? `waits for a worker on ${spec!.placement.executor}`
          : state.tone === "failed" || state.tone === "stale"
          ? state.short
          : failingKeys.length
            ? failingKeys.map(([outcome, n]) => `${plural(n, "key")} ${outcome.replace("_", " ")}`).join(", ")
            : kind.get(id) === "job"
              ? "job"
              : pool,
      progress: partitioned && status ? `${status.partitions.materialized} / ${status.partitions.total}` : undefined,
      bars,
      planned: planned(automations, id, now, range[1]),
      total: bars.length ? String(bars.length) : undefined,
      children: partitioned ? partitionRows(id, own, now, live) : undefined,
    }
  })
}

/** One asset's recent history, a row per partition. */
export function assetRows(asset: string, tasks: TaskRow[], now: number, live: Live): Row[] {
  return partitionRows(
    asset,
    tasks.filter((task) => task.asset === asset),
    now,
    live,
  )
}
