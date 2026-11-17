import type { HistogramCell } from "../api/types"
import type { Tone } from "./status"

/* Activity over the last while, cut into equal cells of time. The cells are
 * fixed to the clock (each starts on a multiple of its length), so they do
 * not crawl as time passes: the newest fills up, then a new one starts. */

export const WINDOWS = [
  { value: "900", label: "15 min" },
  { value: "3600", label: "1 hour" },
  { value: "21600", label: "6 hours" },
  { value: "86400", label: "24 hours" },
]

/** Every strip has this many cells: 15 seconds each over 15 minutes, a minute each over an hour. */
export const CELLS = 60

/** Where the cells of a window ending now begin, and how long each lasts. */
export function grid(windowSeconds: number, now: number): { since: number; bucket: number } {
  const bucket = windowSeconds / CELLS
  return { since: (Math.ceil(now / bucket) - CELLS) * bucket, bucket }
}

const FAILED = ["failed", "blocked"]
const QUIET = ["skipped", "canceled"]

/** A cell's colour is its worst outcome: one failure among many successes is what the eye should find. */
export function cellTone(counts: Record<string, number>): Tone {
  const statuses = Object.keys(counts).filter((status) => counts[status])
  if (statuses.some((status) => FAILED.includes(status))) return "failed"
  if (statuses.some((status) => status !== "succeeded" && !QUIET.includes(status))) return "running"
  return statuses.includes("succeeded") ? "ok" : "idle"
}

export type Totals = { runs: number; tasks: number; failed: number }

export function totals(cells: HistogramCell[]): Totals {
  const out = { runs: 0, tasks: 0, failed: 0 }
  for (const cell of cells) {
    out.runs += cell.runs
    for (const [status, n] of Object.entries(cell.counts)) {
      out.tasks += n
      if (FAILED.includes(status)) out.failed += n
    }
  }
  return out
}
