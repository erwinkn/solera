import type { Attempt, AssetStatus, RunRow, Task, Trigger } from "../api/types"

/* The engine has several status vocabularies (runs, tasks, attempts,
 * partitions, keys). The console shows each in plain words and one of six
 * tones, which is all a theme has to colour. */

export type Tone = "ok" | "running" | "failed" | "stale" | "waiting" | "idle"

export type Status = { tone: Tone; label: string }

const s = (tone: Tone, label: string): Status => ({ tone, label })

const RUN: Record<string, Status> = {
  queued: s("waiting", "Queued"),
  running: s("running", "Running"),
  paused: s("waiting", "Paused"),
  succeeded: s("ok", "Succeeded"),
  skipped: s("idle", "Nothing to do"),
  failed: s("failed", "Failed"),
  canceled: s("idle", "Canceled"),
}

const TASK: Record<string, Status> = {
  queued: s("waiting", "Waiting"),
  claimable: s("waiting", "Waiting for a worker"),
  launching: s("running", "Starting"),
  running: s("running", "Running"),
  succeeded: s("ok", "Succeeded"),
  skipped: s("idle", "Nothing to do"),
  failed: s("failed", "Failed"),
  blocked: s("failed", "Blocked"),
  canceled: s("idle", "Canceled"),
}

const PARTITION: Record<string, Status> = {
  materialized: s("ok", "Up to date"),
  stale: s("stale", "Stale"),
  running: s("running", "Running"),
  failed: s("failed", "Failed"),
  missing: s("idle", "Never built"),
  removed: s("idle", "Removed"),
}

const KEY: Record<string, Status> = {
  ok: s("ok", "OK"),
  rejected: s("failed", "Rejected"),
  failed: s("failed", "Failed"),
  retrying: s("stale", "Retrying"),
  timed_out: s("failed", "Timed out"),
  canceled: s("idle", "Canceled"),
  unmatched: s("idle", "Left out by a rule"),
  removed: s("idle", "Removed"),
}

const unknown = (word: string): Status => s("idle", word ? word[0]!.toUpperCase() + word.slice(1).replaceAll("_", " ") : "Unknown")

export const runStatus = (status: string): Status => RUN[status] ?? unknown(status)
export const taskStatus = (status: string): Status => TASK[status] ?? unknown(status)
export const partitionStatus = (status: string): Status => PARTITION[status] ?? unknown(status)
export const keyStatus = (outcome: string): Status => KEY[outcome] ?? unknown(outcome)

export const TERMINAL_RUN = new Set(["succeeded", "failed", "canceled", "skipped"])
export const TERMINAL_TASK = new Set(["succeeded", "skipped", "failed", "blocked", "canceled"])
/** The outcomes of a key that still needs someone or something to act. */
export const FAILING_KEYS = ["rejected", "failed", "retrying", "timed_out", "canceled"] as const

export const isLive = (run: { status: string }) => !TERMINAL_RUN.has(run.status)

/** Why a task waits (RUN-4), as a sentence. */
export function heldReason(held: Task["held"]): string | null {
  if (!held) return null
  const [reason, detail] = held
  switch (reason) {
    case "claim":
      return `Another run is working on this ${detail ? `(${detail})` : "partition"}`
    case "concurrency":
      return `The asset's limit of partitions at once is reached${detail ? ` (${detail})` : ""}`
    case "engine":
      return "Every engine slot is busy"
    case "executor":
      return `The executor${detail ? ` ${detail}` : ""} is at its limit`
    case "merges":
      return "An output it writes is behind on upkeep"
    case "invalid":
      return "Its placement cannot be built"
    case "upstream incomplete":
      return `An asset it reads is in the middle of a full run${detail ? ` (${detail})` : ""}`
    default:
      return `Held: ${reason}${detail ? ` (${detail})` : ""}`
  }
}

/** One status for an asset, from the roll-up of its partitions. Failed wins
 * over running so a broken asset stays visible while it retries. `detail` is
 * a sentence, `short` a few words for tight places. */
export function assetStatus(status: AssetStatus | undefined): Status & { detail: string; short: string } {
  if (!status) return { ...s("idle", "Unknown"), detail: "", short: "" }
  const p = status.partitions
  const failingKeys = Object.values(status.stored_counts ?? {}).reduce((a, b) => a + b, 0)
  const of = status.partitioned ? ` of ${p.total}` : ""
  const many = status.partitioned
  if (p.failed) return { ...s("failed", "Failed"), detail: many ? `${p.failed}${of} partitions failed` : "Its last run failed", short: many ? `${p.failed} failed` : "failed" }
  if (p.running) return { ...s("running", "Running"), detail: many ? `${p.running}${of} partitions running` : "Running now", short: many ? `${p.running} running` : "running" }
  if (p.stale) return { ...s("stale", "Stale"), detail: many ? `${p.stale}${of} partitions stale` : "A run would load something", short: many ? `${p.stale} stale` : "stale" }
  if (failingKeys) return { ...s("stale", "Keys failing"), detail: `${failingKeys} ${failingKeys === 1 ? "key needs" : "keys need"} attention`, short: `${failingKeys} ${failingKeys === 1 ? "key" : "keys"} failing` }
  if (p.total > 0 && p.missing >= p.total) return { ...s("idle", "Never built"), detail: "It has not run yet", short: "never built" }
  if (p.missing) return { ...s("ok", "Up to date"), detail: `${p.materialized}${of} built, ${p.missing} never built`, short: "" }
  return { ...s("ok", "Up to date"), detail: many ? `${p.materialized} partitions` : "", short: "" }
}

export function attemptStatus(attempt: Attempt): Status {
  return TASK[attempt.outcome] ?? unknown(attempt.outcome)
}

/** What started a run, in a few words. */
export function origin(run: Pick<RunRow, "origin" | "automation" | "by" | "retry_of" | "source">): string {
  if (run.source) return `Commit to ${run.source}${run.by ? ` by ${run.by}` : ""}`
  if (run.retry_of) return "Retry of an earlier run"
  if (run.automation) return `Automation ${run.automation}`
  if (run.by && run.by !== "api") return `Started by ${run.by}`
  return "Started by hand"
}

export function triggerText(trigger: Trigger): string {
  switch (trigger.kind) {
    case "every":
      return trigger.seconds % 60 === 0 && trigger.seconds >= 60 ? `Every ${trigger.seconds / 60} min` : `Every ${trigger.seconds}s`
    case "cron":
      return `On a schedule (${trigger.expression}${trigger.timezone && trigger.timezone !== "UTC" ? ` ${trigger.timezone}` : ""})`
    case "onchange":
      return "When an input changes"
    case "ondeploy":
      return "On each deploy"
  }
}
