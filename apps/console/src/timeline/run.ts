import type { Attempt, AttemptResult, Manifest, RunDetail, Task } from "../api/types"
import { duration, partitionLabel, plural } from "../model/format"
import { layoutRail, projectGraph, topological } from "../model/graph"
import { heldReason, isLive, keyStatus, taskStatus, TERMINAL_TASK, type Tone } from "../model/status"
import { roomyEnd } from "./scale"
import type { Bar, Mark, RailSpec, RailState, Row } from "./types"

/* A run as a chart. The plan is its tasks, grouped by asset and joined by what
 * reads what; the outcome is each task's attempts on the time axis. Batches
 * and attempts get rows of their own only when there is more than one, and a
 * per-key attempt opens into one row per key. */

export const rowId = {
  asset: (name: string) => `asset:${name}`,
  task: (id: string) => `task:${id}`,
  attempt: (task: string, attempt: string) => `attempt:${task}|${attempt}`,
  key: (task: string, attempt: string, key: string) => `key:${task}|${attempt}|${key}`,
}

export type Picked =
  | { kind: "asset"; asset: string }
  | { kind: "task"; task: string }
  | { kind: "attempt"; task: string; attempt: string }
  | { kind: "key"; task: string; attempt: string; key: string }

/** What a row id of the run chart points at. Task ids hold `/` and `:`, so the parts are split on `|` only. */
export function parseRowId(id: string | null): Picked | null {
  if (!id) return null
  const at = id.indexOf(":")
  const kind = id.slice(0, at)
  const [a, b, ...rest] = id.slice(at + 1).split("|")
  if (kind === "asset" && a) return { kind, asset: a }
  if (kind === "task" && a) return { kind, task: a }
  if (kind === "attempt" && a && b) return { kind, task: a, attempt: b }
  if (kind === "key" && a && b) return { kind, task: a, attempt: b, key: rest.join("|") }
  return null
}

const MAX_KEYS = 40
const OPEN_GROUPS_UP_TO = 12

/** The index of the slowest of several, when it is slow enough to be worth
 * pointing at: well above the typical one, and not a matter of milliseconds. */
function slowestOf(seconds: number[]): number {
  if (seconds.length < 3) return -1
  const sorted = [...seconds].sort((a, b) => a - b)
  const typical = sorted[Math.floor(sorted.length / 2)]!
  const max = sorted[sorted.length - 1]!
  return max >= 0.05 && max >= typical * 1.25 ? seconds.indexOf(max) : -1
}

const taskMark = (task: Task): Mark => {
  switch (task.status) {
    case "succeeded":
      return "done"
    case "failed":
    case "blocked":
      return "failed"
    case "running":
    case "launching":
      return "running"
    case "claimable":
      return "waiting"
    case "queued":
      return task.held ? "waiting" : "todo"
    default:
      return "skipped"
  }
}

const attemptTone = (outcome: string): Tone =>
  outcome === "succeeded" ? "ok" : outcome === "failed" ? "failed" : outcome === "running" || outcome === "launching" ? "running" : outcome === "claimable" ? "waiting" : "idle"

/** Seconds an attempt spent before the asset's code had anything to do: getting a worker and importing the project. */
const startup = (a: Attempt) => (a.preparing ?? 0) + (a.provisioning ?? 0) + (a.importing ?? 0)

function attemptBars(attempt: Attempt, href?: string): Bar[] {
  if (attempt.started_at === null) return []
  const tone = attemptTone(attempt.outcome)
  const end = attempt.finished_at ?? null
  const title = `${taskStatus(attempt.outcome).label}${end === null ? "" : ` in ${duration(end - attempt.started_at)}`}`
  if (tone === "waiting") return [{ key: attempt.id, start: attempt.started_at, end, tone, kind: "wait", title: "Waiting for a worker to take it" }]
  const warm = Math.min(startup(attempt), (end ?? Infinity) - attempt.started_at)
  if (end !== null && warm > 0.02 && tone !== "failed")
    return [
      { key: `${attempt.id}:start`, start: attempt.started_at, end: attempt.started_at + warm, tone: "idle", kind: "work", title: `Starting up: ${duration(warm)}`, href },
      { key: attempt.id, start: attempt.started_at + warm, end, tone, kind: "work", title, href },
    ]
  return [{ key: attempt.id, start: attempt.started_at, end, tone, kind: "work", title, href }]
}

function batchText(attempt: Attempt): string | undefined {
  const b = attempt.batch
  if (!b) return undefined
  const parts = [b.added && `${b.added} added`, b.updated && `${b.updated} updated`, b.removed && `${b.removed} removed`, b.unchanged && `${b.unchanged} unchanged`].filter(Boolean)
  return parts.join(", ") || undefined
}

function keysText(keys: Record<string, number> | undefined): string | undefined {
  if (!keys) return undefined
  const total = Object.values(keys).reduce((a, b) => a + b, 0)
  const ok = (keys.ok ?? 0) + (keys.removed ?? 0)
  return ok === total ? plural(total, "key") : `${ok} / ${total} keys`
}

function keyRows(task: Task, attempt: Attempt, result: AttemptResult | undefined): Row[] {
  if (!result) return [{ id: `${rowId.attempt(task.id, attempt.id)}:loading`, mark: "none", name: "Loading the keys of this batch", bars: [] }]
  const calls = result.key_outcomes ?? []
  const bad = (outcome: string) => keyStatus(outcome).tone === "failed" || keyStatus(outcome).tone === "stale"
  const sorted = [...calls].sort((a, b) => Number(bad(b.outcome)) - Number(bad(a.outcome)) || (b.duration ?? 0) - (a.duration ?? 0))
  const slowest = calls[slowestOf(calls.map((call) => call.duration ?? 0))] ?? null
  const began = (attempt.started_at ?? 0) + startup(attempt) + (attempt.loading ?? 0)
  const rows = sorted.slice(0, MAX_KEYS).map((call): Row => {
    const status = keyStatus(call.outcome)
    return {
      id: rowId.key(task.id, attempt.id, call.key),
      mark: status.tone === "ok" ? "done" : status.tone === "failed" ? "failed" : status.tone === "stale" ? "stale" : "skipped",
      tone: status.tone === "failed" ? "failed" : undefined,
      name: call.key,
      note: call.error ?? undefined,
      tag: call === slowest ? "slowest" : undefined,
      progress: status.label,
      // The engine records how long each call took, not when it began: every key is drawn from the moment the batch started calling.
      bars: call.duration === null ? [] : [{ key: call.key, start: began, end: began + call.duration, tone: status.tone === "ok" ? "ok" : status.tone, kind: "work", loud: call === slowest }],
      total: duration(call.duration),
    }
  })
  if (sorted.length > MAX_KEYS)
    rows.push({ id: `${rowId.attempt(task.id, attempt.id)}:more`, mark: "none", name: `${plural(sorted.length - MAX_KEYS, "more key")}, each faster than the ones above`, bars: [] })
  return rows
}

function attemptRows(task: Task, attempts: Attempt[], results: Map<string, AttemptResult>, href: (id: string) => string): Row[] {
  const perKey = attempts.some((a) => a.keys)
  const batched = attempts.some((a) => (a.batch?.count ?? 1) > 1)
  if (attempts.length < 2 && !perKey && !batched) return []
  const tries = new Map<number, number>()
  return attempts.map((attempt, n): Row => {
    let name = `Attempt ${n + 1}`
    if (attempt.batch && (batched || perKey)) {
      const nth = (tries.get(attempt.batch.index) ?? 0) + 1
      tries.set(attempt.batch.index, nth)
      name = `Batch ${attempt.batch.index + 1}${batched ? ` of ${Math.max(attempt.batch.count, attempt.batch.index + 1)}` : ""}${nth > 1 ? `, attempt ${nth}` : ""}`
    }
    const tone = attemptTone(attempt.outcome)
    const id = rowId.attempt(task.id, attempt.id)
    return {
      id,
      mark: tone === "ok" ? "done" : tone === "failed" ? "failed" : tone === "running" ? "running" : tone === "waiting" ? "waiting" : "skipped",
      tone: tone === "failed" ? "failed" : undefined,
      name,
      note: attempt.error ?? batchText(attempt),
      progress: keysText(attempt.keys),
      bars: attemptBars(attempt, href(id)),
      total: attempt.started_at !== null && attempt.finished_at ? duration(attempt.finished_at - attempt.started_at) : undefined,
      children: attempt.keys && attempt.finished_at ? keyRows(task, attempt, results.get(attempt.id)) : undefined,
    }
  })
}

function taskRow(task: Task, attempts: Attempt[], name: string, note: string | undefined, run: RunDetail["request"], now: number, results: Map<string, AttemptResult>, href: (id: string) => string): Row {
  const id = rowId.task(task.id)
  const bars = attempts.flatMap((a) => attemptBars(a, href(id)))
  const first = attempts.find((a) => a.started_at !== null)?.started_at ?? null
  const live = !TERMINAL_TASK.has(task.status)
  if (first !== null && task.wait > 0.05) bars.unshift({ key: `${task.id}:wait`, start: first - task.wait, end: first, tone: "waiting", kind: "wait", title: `Waited ${duration(task.wait)}` })
  if (first === null && live && isLive(run)) bars.push({ key: `${task.id}:queued`, start: run.created_at, end: null, tone: "waiting", kind: "wait", title: heldReason(task.held) ?? "Waiting for its turn" })
  const last = attempts[attempts.length - 1]
  const ended = last?.finished_at ?? (live ? now : null)
  const batch = last?.batch
  const keys = attempts.reduce<Record<string, number>>((sum, a) => {
    for (const [k, v] of Object.entries(a.keys ?? {})) sum[k] = (sum[k] ?? 0) + v
    return sum
  }, {})
  const progress = Object.keys(keys).length
    ? keysText(keys)
    : batch && batch.count > 1
      ? `${batch.index + (last?.outcome === "succeeded" ? 1 : 0)} / ${Math.max(batch.count, batch.index + 1)} batches`
      : attempts.length > 1
        ? `attempt ${attempts.length}`
        : undefined
  const waiting = task.status === "claimable" ? "Waiting for a worker" : task.status === "queued" ? heldReason(task.held) : null
  return {
    id,
    mark: taskMark(task),
    tone: task.status === "failed" || task.status === "blocked" ? "failed" : undefined,
    name,
    note: task.error ?? waiting ?? note,
    progress,
    bars,
    total: first !== null && ended !== null ? duration(ended - first) : undefined,
    children: attemptRows(task, attempts, results, href),
  }
}

function groupMark(tasks: Task[]): Mark {
  const marks = tasks.map(taskMark)
  if (marks.includes("running")) return "running"
  if (marks.includes("failed")) return "failed"
  if (marks.includes("waiting")) return "waiting"
  if (marks.every((m) => m === "todo")) return "todo"
  if (marks.includes("todo")) return "running"
  return marks.every((m) => m === "skipped") ? "skipped" : "done"
}

export type RunChart = {
  rows: Row[]
  rail?: RailSpec
  range: [number, number]
  origin: number
  /** Rows open before anyone touches the chart. */
  open: string[]
  /** Attempt ids whose keys the open rows need. */
  wanted: string[]
  counts: { total: number; done: number; failed: number; canceled: number; running: number; waiting: number }
}

export function runChart(
  detail: RunDetail,
  manifest: Manifest | undefined,
  now: number,
  results: Map<string, AttemptResult>,
  options: { onlyTrouble: boolean; expanded: string[] | null },
): RunChart {
  const run = detail.request
  const href = (id: string) => `/runs/${run.id}?at=${encodeURIComponent(id)}`
  const byAsset = new Map<string, Task[]>()
  for (const task of detail.tasks) byAsset.set(task.asset, [...(byAsset.get(task.asset) ?? []), task])
  const names = [...byAsset.keys()]
  const edges = manifest ? projectGraph(manifest).edges.filter((e) => byAsset.has(e.from) && byAsset.has(e.to)) : []
  const order = topological(names, edges)
  const state = new Map<string, RailState>()
  const open: string[] = []

  const rows = order.map((asset): Row => {
    const tasks = byAsset.get(asset)!
    const mark = groupMark(tasks)
    state.set(asset, mark === "done" || mark === "skipped" ? "done" : mark === "failed" ? "failed" : mark === "running" ? "running" : "todo")
    const store = manifest?.assets[asset]?.outputs[0]?.store
    if (tasks.length === 1) {
      const task = tasks[0]!
      const attempts = detail.attempts[task.id] ?? []
      const row = taskRow(task, attempts, asset, partitionLabel(task.partition) || undefined, run, now, results, href)
      if (row.children?.length && row.children.length <= OPEN_GROUPS_UP_TO) open.push(row.id)
      return { ...row, node: asset }
    }
    const settled = tasks.filter((t) => TERMINAL_TASK.has(t.status)).length
    const failed = tasks.filter((t) => t.status === "failed" || t.status === "blocked").length
    const children = tasks
      .filter((t) => !options.onlyTrouble || !["succeeded", "skipped"].includes(t.status))
      .map((task) => taskRow(task, detail.attempts[task.id] ?? [], partitionLabel(task.partition) || asset, undefined, run, now, results, href))
    const work = children.flatMap((c) => c.bars.filter((b) => b.kind === "work"))
    const id = rowId.asset(asset)
    if (tasks.length <= OPEN_GROUPS_UP_TO) open.push(id)
    const isOpen = (options.expanded ?? open).includes(id)
    // the slowest partition stands out among its siblings
    const lengths = children.map((c) => c.bars.filter((b) => b.kind === "work").reduce((sum, b) => sum + ((b.end ?? now) - b.start), 0))
    const slowest = options.onlyTrouble ? -1 : slowestOf(lengths)
    if (slowest >= 0) {
      const row = children[slowest]!
      children[slowest] = { ...row, tag: "slowest", bars: row.bars.map((b) => (b.kind === "work" && b.tone === "ok" ? { ...b, loud: true } : b)) }
    }
    const from = work.length ? Math.min(...work.map((b) => b.start)) : null
    const to = work.length ? (work.some((b) => b.end === null) ? null : Math.max(...work.map((b) => b.end!))) : null
    return {
      id,
      node: asset,
      mark,
      tone: failed ? "failed" : undefined,
      name: asset,
      note: failed ? plural(failed, "partition") + " failed" : store ? `${plural(tasks.length, "partition")}` : undefined,
      progress: `${settled} / ${tasks.length}`,
      // closed: every partition's bars on one line; open: the extent, with the partitions below
      bars: isOpen && from !== null ? [{ key: id, start: from, end: to, tone: failed ? "failed" : to === null ? "running" : "ok", kind: "span" }] : work.map((b) => ({ ...b, loud: false })),
      total: from !== null ? duration((to ?? now) - from) : undefined,
      children,
    }
  })

  const live = isLive(run) || detail.tasks.some((t) => !TERMINAL_TASK.has(t.status))
  const ends = Object.values(detail.attempts).flatMap((attempts) => attempts.map((a) => a.finished_at ?? 0))
  const end = live ? now : Math.max(run.updated_at, ...ends)
  const expanded = options.expanded ?? open
  const wanted = Object.entries(detail.attempts).flatMap(([task, attempts]) =>
    attempts.filter((a) => a.keys && a.finished_at && expanded.includes(rowId.attempt(task, a.id))).map((a) => a.id),
  )
  const count = (test: (t: Task) => boolean) => detail.tasks.filter(test).length
  return {
    rows,
    rail: order.length > 1 ? { ...layoutRail(order, edges), state } : undefined,
    range: [run.created_at, roomyEnd(run.created_at, end, live ? 0.18 : 0.06)],
    origin: run.created_at,
    open,
    wanted,
    counts: {
      total: detail.tasks.length,
      done: count((t) => t.status === "succeeded" || t.status === "skipped"),
      failed: count((t) => t.status === "failed" || t.status === "blocked"),
      canceled: count((t) => t.status === "canceled"),
      running: count((t) => t.status === "running" || t.status === "launching"),
      waiting: count((t) => t.status === "queued" || t.status === "claimable"),
    },
  }
}
