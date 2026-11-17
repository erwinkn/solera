/* The shapes python/solera_server/api.py returns, as far as the console reads
 * them. Times are epoch seconds. */

export type Trigger =
  | { kind: "cron"; expression: string; timezone?: string }
  | { kind: "every"; seconds: number }
  | { kind: "onchange"; outputs?: string[] }
  | { kind: "ondeploy" }

export type InputSpec = {
  kind: string
  output: string
  batch_size?: number
  each?: { concurrency: number } | null
  patterns?: { include: unknown[]; exclude: unknown[] }
}

export type OutputSpec = { name: string; store: string; key: string | null; incremental: boolean }

export type Dimension =
  | { kind: "dynamic"; output: string }
  | { kind: "time"; start: string; every: string; timezone?: string }
  | { kind: string; [more: string]: unknown }

export type AssetSpec = {
  outputs: OutputSpec[]
  inputs: Record<string, InputSpec>
  deps: string[]
  partitions: { dims: Record<string, Dimension> } | null
  placement: { executor: string; kind: string }
  retries: { n: number; delay: number; backoff: string } | null
  timeout: number | null
  concurrency: number | null
  version: string | null
  tags: Record<string, string>
  doc: string | null
  automations: string[]
}

export type Manifest = {
  name: string
  deploy: string
  assets: Record<string, AssetSpec>
  outputs: Record<string, OutputSpec & { asset: string | null; source: boolean }>
  sources: Record<string, { name: string; store: string; key: string | null }>
  executors: Record<string, { kind: string; config: Record<string, unknown> }>
  automations: Record<string, AutomationSpec>
  sensors: Record<string, SensorSpec>
  build: { id: string; source: string; commit: string | null } | null
  errors: unknown[]
}

export type AutomationSpec = {
  name: string
  targets: string[]
  trigger: Trigger
  enabled: boolean
  partitions: string | string[] | null
  mode: string
  watched: string[]
}

export type Automation = AutomationSpec & {
  since: number
  last_fired: number | null
  last_run: string | null
  pending: unknown[]
  next_at: number | null
}

export type SensorSpec = { name: string; every: number; commits: string[]; timeout: number; doc: string | null }
export type Sensor = SensorSpec & {
  cursor: unknown
  ticking: unknown
  due_in: number | null
  accepted: { tick: string; runs: string[]; commits: Record<string, string> } | null
}
export type SensorWorker = { id: string; executor: string; deploy: string; seen_at: number }
export type Tick = {
  sensor: string
  tick: string
  started_at: number
  ended_at: number | null
  host: string | null
  outcome: string
  error: string | null
  runs: string[]
}

export type PartitionStatus = "materialized" | "stale" | "running" | "failed" | "missing" | "removed"

export type AssetStatus = {
  partitions: Record<PartitionStatus | "total", number>
  partitioned: boolean
  stale: boolean
  last: { partition: string; outcome: string; at: number; attempt: string | null } | null
  /** A per-key asset's failing keys by outcome; null for any other asset. */
  stored_counts: Record<string, number> | null
  repairs: number
  updated_at: number | null
}

export type Reason = { kind: string; input?: string; [more: string]: unknown }
export type PartitionRow = {
  partition: string
  status: PartitionStatus
  last_outcome: string | null
  last_attempt: string | null
  reasons?: Reason[]
}

export type Head = {
  partition?: string
  version: string | null
  key_count: number | null
  commit_number: number | null
  materialized: boolean
  at: number
  commit: string | null
  run?: string
  cleanups?: { pending: number; stuck: unknown[] }
}

export type AssetDetail = {
  asset: AssetSpec
  heads: Record<string, [string, Head][]>
  cursor: unknown
  current_keys: string[][]
  repairs: Record<string, string[]>
  partitions: Record<string, { last_outcome: string; last_attempt: string | null; at: number }>
  automations: Automation[]
}

export type RunRow = {
  id: string
  created_at: number
  finished_at: number | null
  status: string
  origin: string
  automation: string | null
  by: string | null
  retry_of: string | null
  source: string | null
  targets: string[]
  assets: string[]
  committed: string[]
  mode: string
  partitions: Record<string, string[]> | string | null
  tags: Record<string, string>
  task_count: number
  failed_count: number
  error: string | null
}

export type RunRequest = {
  id: string
  targets: string[]
  partitions: Record<string, string[]> | string | null
  mode: string
  upstream?: boolean
  config?: Record<string, unknown>
  keys?: unknown
  automation?: string | null
  by?: string | null
  retry_of?: string | null
  tags?: Record<string, string>
  status: string
  paused: boolean
  created_at: number
  updated_at: number
  source?: string
  upserted?: string[]
  deleted?: string[]
  commit_number?: number
  version?: string
}

export type Task = {
  id: string
  run: string
  asset: string
  partition: string
  status: string
  deps: string[]
  max_attempts: number
  wait: number
  /** Why it waits: `[reason, detail]`, e.g. `["executor", "ingest"]`. */
  held?: [string, string | null] | null
  progress: { batch: number; key: string | number | null } | null
  attempt_count: number
  error?: string | null
  outputs?: string[] | null
}

export const PHASES = ["preparing", "provisioning", "importing", "loading", "computing", "writing", "settling"] as const
export type Phase = (typeof PHASES)[number]

export type Batch = {
  index: number
  count: number
  final: boolean
  added: number
  updated: number
  removed: number
  unchanged: number
}

export type Attempt = {
  id: string
  task: string
  generation: number
  outcome: string
  started_at: number | null
  finished_at?: number | null
  executor?: string
  error?: string
  outputs?: string[]
  commit?: string
  /** A per-key attempt's keys by outcome. */
  keys?: Record<string, number>
  batch?: Batch
  peak_memory?: number
  cpu_seconds?: number
} & Partial<Record<Phase, number>>

export type RunDetail = { request: RunRequest; tasks: Task[]; attempts: Record<string, Attempt[]> }

export type RunEvent = {
  run: string
  n: number
  at: number
  type: string
  task: string | null
  attempt: string | null
  by: string | null
  name: string | null
  reason: string | null
  until: number | null
  rows: number | null
}

export type KeyCall = { key: string; outcome: string; duration: number | null; error: string | null; generation?: number }
export type AttemptResult = { status: string; key_outcomes?: KeyCall[]; keys?: Record<string, number> }

export type LogLine = { at: number; level: string; message: string; fields?: Record<string, unknown> }

export type KeyOutcome = {
  key: string
  partition: string
  outcome: string
  owed: boolean
  version: number | string | null
  tries?: number
  since?: number
  last?: number
  next_at?: number | null
  message?: string
}
export type Outcomes = {
  asset: string
  partitions: { partition: string; stored_counts: Record<string, number> }[]
  now: number
  keys: KeyOutcome[]
  next: string | null
}
export type OutcomeEvent = {
  run: string
  attempt: string
  partition: string
  key: string
  outcome: string
  error: string | null
  duration: number | null
  at: number
}

export type Commit = {
  output: string
  partition: string
  run: string
  attempt: string | null
  at: number
  commit_number: number | null
  added: number | null
  removed: number | null
  key_count: number | null
  rows: number | null
  final: boolean | null
  batch: { index: number; count: number } | null
}

export type StaleKeys = { tracked: boolean; keys: { key: string; reasons: Reason[] }[]; next: string | null; reasons: Reason[] }

export type Executor = { name: string; kind: string; max_concurrent: number | null; in_flight: number }
export type Worker = { id: string; pools: string[]; capacity: Record<string, number>; seen_at: number }

export type Diagnostics = {
  state: string
  namespace: string
  project: string
  deploy: string
  deploy_number: number
  inflight: number
  active_runs: number
  postgres: boolean
  last_error: string | null
  incomplete_full_runs: { output: string; partition: string; asset: string }[]
  anomalies: Record<string, unknown>[]
  skipped_history_rows: unknown[]
  stuck_cleanups: { output: string; partition: string; id: string }[]
}

export type Cleanup = { output: string; partition?: string; pending?: number; stuck?: unknown[] | string; id?: string; store?: string; due?: number }

export type Facets = Record<string, { value: string; count: number }[]>
/** `since` is null, and `bars` empty, when no run matches. */
export type Histogram = { bucket: number; since: number; until: number; bars: { t: number; counts: Record<string, number> }[] }

/** One cell of a task histogram: bucket `i` starts at `since + i * bucket`. */
export type HistogramCell = { i: number; counts: Record<string, number>; runs: number; /** One of its runs: one that did not succeed if any. */ run: string }
export type TaskHistogram = { since: number; until: number; bucket: number; by: "asset" | "partition"; series: Record<string, HistogramCell[]> }
