/* How the console writes times, durations and counts. */

export function duration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || Number.isNaN(seconds)) return "–"
  const s = Math.max(0, seconds)
  if (s < 0.0005) return "<1ms"
  if (s < 1) return `${Math.round(s * 1000)}ms`
  if (s < 10) return `${s.toFixed(1)}s`
  if (s < 60) return `${Math.round(s)}s`
  if (s < 3600) return `${Math.floor(s / 60)}m ${String(Math.round(s % 60)).padStart(2, "0")}s`
  if (s < 86400) return `${Math.floor(s / 3600)}h ${String(Math.round((s % 3600) / 60)).padStart(2, "0")}m`
  return `${Math.floor(s / 86400)}d ${Math.round((s % 86400) / 3600)}h`
}

/** A span in coarse words: "12 seconds", "3 minutes". */
function span(seconds: number): string {
  const s = Math.abs(seconds)
  const [n, unit] =
    s < 60 ? [Math.round(s), "second"] : s < 3600 ? [Math.round(s / 60), "minute"] : s < 86400 ? [Math.round(s / 3600), "hour"] : [Math.round(s / 86400), "day"]
  return `${n} ${unit}${n === 1 ? "" : "s"}`
}

export function ago(at: number | null | undefined, now: number): string {
  if (at === null || at === undefined) return "never"
  const s = now - at
  return s < 2 ? "just now" : `${span(s)} ago`
}

export function until(at: number | null | undefined, now: number): string {
  if (at === null || at === undefined) return "–"
  const s = at - now
  return s < 1 ? "now" : `in ${span(s)}`
}

const TIME = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false })
const SHORT = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", hour12: false })
const DAY = new Intl.DateTimeFormat(undefined, { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", hour12: false })
const LONG = new Intl.DateTimeFormat(undefined, { weekday: "long", day: "numeric", month: "long", hour: "2-digit", minute: "2-digit", hour12: false })

export const clock = (at: number) => TIME.format(at * 1000)
export const clockShort = (at: number) => SHORT.format(at * 1000)
export const longDate = (at: number) => LONG.format(at * 1000)

/** The time alone today, the date with it on any other day. */
export function when(at: number | null | undefined, now: number): string {
  if (at === null || at === undefined) return "–"
  const sameDay = new Date(at * 1000).toDateString() === new Date(now * 1000).toDateString()
  return sameDay ? clock(at) : DAY.format(at * 1000)
}

const NUMBER = new Intl.NumberFormat()
export const count = (n: number) => NUMBER.format(n)

export function plural(n: number, one: string, many = `${one}s`): string {
  return `${count(n)} ${n === 1 ? one : many}`
}

export function bytes(n: number | null | undefined): string {
  if (n === null || n === undefined) return "–"
  if (n < 1024) return `${n} B`
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(0)} kB`
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(0)} MB`
  return `${(n / 1024 ** 3).toFixed(1)} GB`
}

/** A run or attempt id is a ULID: its last characters tell two apart. */
export const shortId = (id: string) => id.slice(-6).toLowerCase()

/** "day=2026-09-01,site=alpha" → "2026-09-01 · alpha"; the unpartitioned key is never shown. */
export function partitionLabel(partition: string | null | undefined): string {
  if (!partition) return ""
  if (!partition.includes("=")) return partition
  return partition
    .split(",")
    .map((part) => part.slice(part.indexOf("=") + 1))
    .join(" · ")
}

export function list(items: string[], max = 3): string {
  if (items.length <= max) return items.join(", ")
  return `${items.slice(0, max).join(", ")} and ${items.length - max} more`
}
