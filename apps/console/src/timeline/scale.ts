import { clock, clockShort } from "../model/format"
import type { Bar } from "./types"

const STEPS = [
  0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400,
]

export type Tick = { at: number; label: string }

/** Round times across `[start, end]`: counted from `origin` when the axis shows
 * time elapsed since it, else on the clock. */
export function ticks(start: number, end: number, max: number, origin: number | null): Tick[] {
  const span = Math.max(end - start, 0.001)
  const step = STEPS.find((s) => span / s <= max) ?? 86400 * Math.ceil(span / 86400 / max)
  // the clock counts from local midnight, so that hours and minutes fall on round times
  const zero = origin ?? -new Date(start * 1000).getTimezoneOffset() * 60
  const out: Tick[] = []
  for (let n = Math.ceil((start - zero) / step - 1e-9); zero + n * step <= end + 1e-9; n++) {
    const at = zero + n * step
    out.push({ at, label: origin === null ? (step < 60 ? clock(at) : clockShort(at)) : elapsed(n * step, step) })
  }
  return out
}

function elapsed(seconds: number, step: number): string {
  if (seconds === 0) return "0s"
  if (step < 1) return seconds < 1 ? `${Math.round(seconds * 1000)}ms` : `${+seconds.toFixed(3)}s`
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return seconds % 60 === 0 ? `${seconds / 60}m` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`
  return seconds % 3600 === 0 ? `${seconds / 3600}h` : `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`
}

/** The end of an axis that starts at `start` and must show `end`, with some
 * room after it so a growing bar is never flush against the edge. */
export function roomyEnd(start: number, end: number, room = 0.12): number {
  return start + Math.max(end - start, 0.5) * (1 + room)
}

/** Bars too close to tell apart become one: a row with thousands of tasks
 * draws at most a few hundred marks. A bar joins the one before it when they
 * are of the same kind and tone and the gap between them is under `seconds`
 * (two pixels). Only like merges with like, so a failed or running task is
 * never hidden inside a run of finished ones. */
export function mergeBars(bars: Bar[], seconds: number, now: number): (Bar & { count: number })[] {
  const sorted = [...bars].sort((a, b) => a.start - b.start)
  const out: (Bar & { count: number })[] = []
  const open = new Map<string, Bar & { count: number }>() // the last bar of each kind and tone
  for (const bar of sorted) {
    const like = `${bar.kind}/${bar.tone}`
    const last = open.get(like)
    if (last && !bar.loud && !last.loud && bar.start - (last.end ?? now) < seconds) {
      last.end = last.end === null || bar.end === null ? null : Math.max(last.end, bar.end)
      last.count += 1
      last.href = undefined
      continue
    }
    const copy = { ...bar, count: 1 }
    open.set(like, copy)
    out.push(copy)
  }
  return out
}
