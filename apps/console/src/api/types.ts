/**
 * Response shapes of the server's HTTP API (python/solera_server/api.py).
 * Times are epoch seconds. Only the fields the console reads are typed.
 */

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

// -- manifest -------------------------------------------------------------------

export interface OutputDecl {
  name: string;
  store: string;
  key: string | null;
  incremental: boolean;
  migrations: string[] | { name: string }[];
  config: Record<string, Json>;
  dynamic_partitions: boolean;
  keyed?: boolean;
}

/** An input's key patterns as the manifest records them: globs or regexes, excludes named. */
export interface Patterns {
  include?: PatternSpec[] | null;
  exclude?: [string | null, PatternSpec][] | null;
}
export type PatternSpec = { glob?: string; regex?: string };

export interface InputDecl {
  kind: "in" | "incremental" | "all_partitions";
  output: string;
  meta: Json;
  batch_size?: number;
  each?: { concurrency: number } | null;
  patterns?: Patterns | null;
}

export type DimDecl =
  | { kind: "dynamic"; output: string }
  | { kind: "static"; keys: string[] }
  | {
      kind: "time";
      start: string;
      every: string;
      end: string | null;
      end_offset: string | null;
      timezone: string;
      format: string;
    };

export interface Placement {
  executor: string;
  kind: string;
  environment: Record<string, Json>;
  placement: Record<string, Json>;
}

export interface AssetDecl {
  outputs: OutputDecl[];
  inputs: Record<string, InputDecl>;
  deps: string[];
  partitions: { dims: Record<string, DimDecl> } | null;
  placement: Placement;
  retries: { n: number; delay: number; backoff: string };
  timeout: number;
  version: string;
  retention: Json;
  aliases: string[];
  tags: Record<string, string>;
  doc: string | null;
  automations: string[];
}

export interface ManifestOutput extends OutputDecl {
  asset: string | null;
  source: boolean;
}

export interface Ref {
  output: string;
  store: string;
  handle: Record<string, Json>;
  partition: string;
  /** Its version: the generation of the write that made it. */
  generation: number;
  meta: Record<string, Json>;
}

export interface SourceDecl {
  name: string;
  store: string | null;
  key: string | null;
  handle: Record<string, Json>;
  head: Ref;
}

export type Trigger =
  | { kind: "every"; seconds: number }
  | { kind: "cron"; expression: string; timezone: string }
  | { kind: "onchange"; outputs: string[] }
  | { kind: "ondeploy" };

export interface AutomationDecl {
  name: string;
  targets: string[];
  trigger: Trigger;
  enabled: boolean;
  partitions: string | string[] | null;
  mode: string;
  upstream: boolean;
  config: Json;
  keys: Json;
  tags: Record<string, string>;
  skip_missing_inputs: boolean;
  watched: string[];
}

export interface Automation extends AutomationDecl {
  last_fired: number | null;
  last_run: string | null;
  last_deploy: string | null;
  pending: [string | null, string][];
  next_at?: number | null;
}

export interface SensorDecl {
  name: string;
  every?: number;
  commits?: string[];
  executor?: string;
  [key: string]: Json | undefined;
}

export interface StoreDecl {
  version: string;
  ref: string;
  writes: "immutable" | "fenced";
}

export interface Manifest {
  name: string;
  assets: Record<string, AssetDecl>;
  outputs: Record<string, ManifestOutput>;
  sources: Record<string, SourceDecl>;
  stores: Record<string, StoreDecl>;
  executors: Record<string, { kind: string; environment: Record<string, Json> }>;
  automations: Record<string, AutomationDecl>;
  sensors: Record<string, SensorDecl>;
  build: {
    id: string;
    source: string;
    commit: string | null;
    dirty: boolean;
  } | null;
  deploy: string;
}

// -- engine --------------------------------------------------------------------

export interface Diagnostics {
  backend: string;
  state: string;
  objects: string;
  namespace: string;
  project: string;
  deploy: string;
  inflight: number;
  active_runs: number;
  postgres: boolean;
  last_error: string | null;
  stuck_cleanups: { output: string; partition: string; id: string }[];
}

export interface Head {
  ref: Ref;
  materialized: boolean;
  asset?: string | null;
  version?: string;
  commit_number?: number;
  base?: number;
  run?: string;
  attempt?: string;
  at: number;
  count?: number;
  partitions?: string[];
  commit: string | null;
  schema?: string;
}

export interface CatalogAsset extends AssetDecl {
  name: string;
  heads: Record<string, Record<string, Head>>;
}

export interface AssetStatus {
  partitions: Record<PartitionStatus, number> & { total: number };
  partitioned: boolean;
  stale: boolean; // one of its partitions is
  last: {
    partition: string;
    outcome: string;
    at: number;
    attempt: string | null;
  } | null;
  failures: Partial<Record<FailureClass, number>> | null;
  repairs: number;
  updated_at: number | null;
}

export type PartitionStatus = "materialized" | "stale" | "missing" | "failed" | "running" | "removed";

/** Why a partition is stale (docs/positions-from-reads.md): an input it read
 * changed, an upstream it reads is itself stale, or its asset changed. */
export type StaleReason = "input changed" | "upstream stale" | "definition changed";

/** A page of an asset partition's stale keys: all of them or none for a
 * keyed output that is not `each`; `tracked` false for an unkeyed one. */
export interface StaleKeys {
  tracked: boolean;
  keys: string[];
  next: string | null;
  reasons: StaleReason[];
}

export interface PartitionOutcome {
  last_outcome: string;
  last_attempt: string | null;
  at: number;
}

export interface AssetDetail {
  asset: AssetDecl;
  heads: Record<string, [string, Head][]>;
  cursor: Json;
  positions: Record<string, Position | null>;
  current_keys: string[][];
  repairs: Record<string, string[]>;
  partitions: Record<string, PartitionOutcome>;
  automations: Automation[];
}

export interface PartitionRow {
  partition: string;
  status: PartitionStatus;
  last_outcome: string | null;
  last_attempt: string | null;
  reasons?: StaleReason[]; // when `stale`
}

export interface OutputHead {
  partition: string;
  ref: Ref;
  /** The asset's code version; a source's own version, as its last commit gave it. */
  version: string | null;
  key_count: number | null;
  commit_number: number | null;
  materialized: boolean;
  cursor: boolean;
  at: number;
  commit: string | null;
  cleanups: { output: string; partition: string; pending: number; stuck: Json[] };
}

export interface KeyPage {
  output: string;
  partition: string;
  total: number;
  /** Each key, and the generation that last wrote it: its version. */
  keys: Record<string, number>;
  next: string | null;
}

export type FailureClass = "rejected" | "failed" | "retrying" | "canceled" | "timed_out";

export interface FailureKey {
  partition: string;
  key: string;
  outcome: FailureClass;
  tries: number;
  since: number;
  last: number;
  next_at: number | null;
  until: number | null;
  /** The generation of the upstream key it failed at. */
  generation: number;
  message: string;
  eligible: boolean;
}

export interface FailurePartition {
  partition: string;
  counts: Partial<Record<FailureClass, number>>;
  due: number | null;
  deploy_min: number | null;
  passes?: number | null;
  retry: Json;
  forced: Record<string, number>;
  last?: string | null;
  has_retries?: boolean;
}

export interface FailedKeys {
  asset: string;
  partitions: FailurePartition[];
  deploy: number;
  now: number;
  keys: FailureKey[];
  next: string | null;
}

export type KeyOutcomeKind = "ok" | "removed" | "unmatched" | FailureClass;

export interface KeyOutcome {
  run: string;
  attempt: string;
  asset: string;
  partition: string;
  key: string;
  /** The generation of the upstream key it processed. */
  generation: number | null;
  outcome: KeyOutcomeKind;
  error: string | null;
  duration: number | null;
  at: number;
}

export interface Explain {
  asset: string;
  partition: string;
  key: string;
  input: string;
  upstream: string;
  upstream_asset: string | null;
  upstream_partition: string;
  upstream_generation: number | null;
  input_state: InputState;
  outputs: Record<string, { present: boolean; generation: number | null }>;
  patterns: {
    spec: Patterns | null;
    included: boolean;
    excluded_by: string | null;
    pending: Patterns | null;
  };
  failure: FailureKey | null;
  last: KeyOutcome | null;
  last_ok: KeyOutcome | null;
  verdict: "ok" | "failing" | "excluded" | "not_matched" | "pending" | "removed" | "absent";
}

export type InputState = "never" | "caught_up" | "behind" | "delta" | "full" | "pattern_change" | "reconcile";

/** An input's position (python/solera_server/positions.py): `next`,
 * the first upstream commit not yet delivered; `pass`, one under way —
 * its mode, boundary (`from`..`to`) and place (`at`: the last key
 * delivered, or the next commit). */
export interface Position {
  kind: "keys" | "commits";
  output: string;
  up: string;
  fingerprint: string;
  reset_by?: string | null;
  next: number;
  pass?: {
    mode: "full" | "delta" | "diff";
    from?: number;
    to?: number;
    at: string | number | null;
    batch: number;
    batches: number;
    pin?: number | null;
    reconcile?: boolean;
  };
  patterns?: Json;
  pattern_change?: { old: Json; new: Json; at: number; pin: number };
  reconcile?: { after: string | null };
}

export interface InputPartition {
  partition: string;
  upstream_partition: string;
  position: Position | null;
  head_commit: number | null;
  lag: number | null;
  state: InputState;
}

export interface Input {
  param: string;
  kind: "incremental" | "each" | "in" | "all_partitions" | "dep";
  output: string;
  upstream_asset: string | null;
  source: boolean;
  batch_size: number | null;
  concurrency: number | null;
  patterns: Patterns | null;
  partitions: InputPartition[];
}

export interface Commit {
  output: string;
  asset: string | null;
  partition: string;
  store: string;
  run: string;
  attempt: string | null;
  at: number;
  commit_number: number | null;
  added: number | null;
  removed: number | null;
  added_keys: string[] | null;
  removed_keys: string[] | null;
  rows: number | null;
  materialized: boolean | null;
  metadata: Json;
  generation: number;
}

export interface LineageNode {
  output: string;
  partition: string;
  generation: number;
  asset: string | null;
  run: string | null;
  attempt: string | null;
  at: number | null;
  rows: number | null;
  current: boolean;
}

export interface Lineage {
  root: { output: string; partition: string; generation: number };
  direction: "upstream" | "downstream";
  nodes: LineageNode[];
  edges: {
    from: { output: string; partition: string; generation: number };
    to: { output: string; partition: string; generation: number };
    param: string;
    run: string;
  }[];
}

// -- runs ------------------------------------------------------------------------

export type RunStatus =
  "queued" | "running" | "succeeded" | "failed" | "canceled" | "skipped" | "paused" | string;

export interface RunRow {
  id: string;
  created_at: number;
  finished_at: number | null;
  status: RunStatus;
  origin: "manual" | "automation" | "sensor" | "commit" | string;
  automation: string | null;
  by: string | null;
  source: string | null;
  retry_of?: string | null;
  targets: string[];
  assets: string[];
  committed: string[] | null;
  mode: string;
  partitions: Partitions | null;
  upstream: boolean;
  tags: Record<string, string>;
  task_count: number | null;
  failed_count: number | null;
  error: string | null;
}

/** A run's selection: a named one, a list of partitions, or each asset's own (an
 * OnChange firing, a retry). Its JSON text, from the history. */
export type Partitions = string | string[] | Record<string, string[]>;

export interface RunPage {
  runs: RunRow[];
  next: string | null;
  total: number;
}

export interface Facet {
  value: string;
  count: number;
}

export type Facets = Record<"status" | "origin" | "automation" | "by" | "source" | "asset" | "tag", Facet[]>;

export interface Histogram {
  bucket: number;
  /** null when no run matches. */
  since: number | null;
  until: number;
  bars: { t: number; counts: Record<string, number> }[];
}

export interface RunRequest {
  id: string;
  targets: string[];
  partitions: Partitions;
  mode: string;
  upstream: boolean;
  config: Json;
  keys: Json;
  automation?: string | null;
  by?: string | null;
  source?: string;
  retry_of?: string | null;
  tags: Record<string, string>;
  status: RunStatus;
  paused: boolean;
  created_at: number;
  updated_at: number;
  finished_at?: number | null;
  error?: string | null;
  tasks: string[];
}

export interface Task {
  id: string;
  run: string;
  asset: string;
  partition: string;
  status: string;
  deps: string[];
  max_attempts: number;
  retry: { n: number; delay: number; backoff: string } | null;
  wait: number | null;
  generation: number;
  attempt_count: number;
  error: string | null;
  outputs: string[] | null;
  held?: [string, string | null] | null;
  started_at?: number | null;
  finished_at?: number | null;
}

export const PHASES = [
  "preparing",
  "provisioning",
  "importing",
  "loading",
  "computing",
  "writing",
  "settling",
] as const;
export type Phase = (typeof PHASES)[number];

export interface Attempt extends Partial<Record<Phase, number>> {
  id: string;
  task: string;
  generation: number;
  outcome: string;
  started_at: number | null;
  finished_at?: number | null;
  error?: AttemptError | string | null;
  outputs?: string[];
  commit?: string;
  keys?: Partial<Record<KeyOutcomeKind, number>>;
  executor?: string | null;
  cpu?: number | null;
  memory?: number | null;
  gpu?: number | null;
  peak_memory?: number | null;
  cpu_seconds?: number | null;
}

export interface AttemptError {
  type?: string;
  message?: string;
  traceback?: string;
  retryable?: boolean;
  class?: string;
  retry_after?: number | null;
}

export interface RunDetail {
  request: RunRequest;
  tasks: Task[];
  attempts: Record<string, Attempt[]>;
}

export interface RunEvent {
  run: string;
  n: number;
  at: number;
  type: string;
  task: string | null;
  attempt: string | null;
  by: string | null;
  name: string | null;
  reason: string | null;
  until: number | null;
  rows: number | null;
}

export interface LogLine {
  at: number;
  level: "debug" | "info" | "warning" | "error" | "critical" | string;
  message: string;
  fields?: Record<string, Json>;
}

export interface AttemptResult {
  status: string;
  write?: "none" | "writing" | "complete";
  outputs?: Record<string, { ref?: Ref; unchanged?: boolean; keys?: Json }>;
  delivered?: Record<string, Json>;
  key_outcomes?: Omit<KeyOutcome, "run" | "attempt" | "asset" | "partition" | "at">[];
  keys?: Partial<Record<KeyOutcomeKind, number>>;
  error?: AttemptError;
  cancel?: { phase: string; reason: string; since: number } | null;
  usage?: Record<string, number>;
  [key: string]: unknown;
}

export type AttemptSpec = Record<string, Json>;

// -- automations, sensors, executors -------------------------------------------

export interface Tick {
  sensor: string;
  tick: string;
  started_at: number;
  ended_at: number | null;
  host: string | null;
  outcome: "skipped" | "advanced" | "committed" | "requested" | "refused" | "failed" | string;
  error: string | null;
  runs: string[] | null;
  [key: string]: Json | undefined;
}

export interface SensorView {
  name: string;
  every: number;
  commits: string[];
  placement: Placement;
  timeout: number;
  doc: string | null;
  cursor: Json;
  accepted: {
    tick: string;
    runs: string[];
    commits: Record<string, string>;
  } | null;
  ticking: { tick: string; host: string; started_at: number } | null;
  due_in: number;
}

export interface SensorWorker {
  id: string;
  executor: string;
  deploy: string;
  seen_at: number;
}

export interface Executor {
  name: string;
  kind: string;
  environment: Record<string, Json>;
  max_concurrent: number | null;
  in_flight: number;
}

export interface Worker {
  id: string;
  [key: string]: Json | undefined;
}

/** GET /repairs: output partitions a writer that died left owing a repair, for the next attempt. */
export interface Repair {
  output: string;
  partition: string;
  intents: { run: string; attempt: string; files?: string[] }[];
}

/** GET /cleanups: output partitions whose cleanups have stuck entries, for an operator to clear. */
export interface Cleanup {
  output: string;
  partition: string;
  pending: number;
  stuck: { id: string; [key: string]: Json }[];
}

export interface Stats {
  assets: {
    asset: string;
    tasks: number;
    skipped: number;
    failed: number;
    p50: number | null;
    p95: number | null;
    wait_p50: number | null;
    wait_p95: number | null;
    hours: number | null;
  }[];
  executors?: Record<string, Json>[];
  [key: string]: Json | undefined;
}
