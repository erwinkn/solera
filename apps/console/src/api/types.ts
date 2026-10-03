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
  partition_set: boolean;
  keyed?: boolean;
}

/** An edge's key patterns as the manifest records them: globs or regexes, excludes named. */
export interface Patterns {
  include?: PatternSpec[] | null;
  exclude?: [string | null, PatternSpec][] | null;
}
export type PatternSpec = { glob?: string; regex?: string };

export interface EdgeDecl {
  kind: "in" | "incremental" | "all_partitions";
  output: string;
  meta: Json;
  page_size?: number;
  each?: { concurrency: number } | null;
  patterns?: Patterns | null;
}

export type DimDecl =
  | { kind: "set"; output: string }
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
  inputs: Record<string, EdgeDecl>;
  deps: string[];
  partitions: { dims: Record<string, DimDecl> } | null;
  placement: Placement;
  retries: { n: number; delay: number; backoff: string };
  timeout: number;
  version: string;
  on_version_change: string;
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
  last_at: number | null;
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
  stuck_discards: { output: string; scope: string; id: string }[];
}

export interface Head {
  ref: Ref;
  complete: boolean;
  asset?: string | null;
  version?: string;
  commit_number?: number;
  base?: number;
  run?: string;
  attempt?: string;
  at: number;
  count?: number;
  elements?: string[];
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
  last: {
    scope: string;
    outcome: string;
    at: number;
    attempt: string | null;
  } | null;
  failures: Partial<Record<FailureClass, number>> | null;
  unsettled: number;
  updated_at: number | null;
}

export type PartitionStatus = "complete" | "missing" | "failed" | "running" | "retired";

export interface ScopeOutcome {
  last_outcome: string;
  last_attempt: string | null;
  at: number;
}

export interface AssetDetail {
  asset: AssetDecl;
  heads: Record<string, [string, Head][]>;
  cursor: Json;
  watermarks: Record<string, Watermark | null>;
  current_keys: string[][];
  unsettled: Record<string, string[]>;
  scopes: Record<string, ScopeOutcome>;
  automations: Automation[];
}

export interface PartitionRow {
  scope: string;
  status: PartitionStatus;
  last_outcome: string | null;
  last_attempt: string | null;
}

export interface OutputHead {
  scope: string;
  ref: Ref;
  /** The asset's code version; a source's own version, as its last commit gave it. */
  version: string | null;
  key_count: number | null;
  commit_number: number | null;
  complete: boolean;
  cursor: boolean;
  at: number;
  commit: string | null;
  discards: { output: string; scope: string; pending: number; stuck: Json[] };
}

export interface KeyPage {
  output: string;
  scope: string;
  total: number;
  exact: boolean;
  /** Each key, and the generation that last wrote it: its version. */
  keys: Record<string, number>;
  next: string | null;
}

export type FailureClass = "rejected" | "failed" | "retrying" | "canceled" | "timed_out";

export interface FailureKey {
  scope: string;
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

export interface FailureScope {
  scope: string;
  counts: Partial<Record<FailureClass, number>>;
  due: number | null;
  deploy_min: number | null;
  passes?: number | null;
  retry: Json;
  forced: Record<string, number>;
  last?: string | null;
  has_retries?: boolean;
}

export interface Failures {
  asset: string;
  scopes: FailureScope[];
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
  scope: string;
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
  scope: string;
  key: string;
  edge: string;
  upstream: string;
  upstream_asset: string | null;
  up_scope: string;
  upstream_generation: number | null;
  edge_state: EdgeState;
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

export type EdgeState = "never" | "caught_up" | "behind" | "paging" | "full" | "rescope" | "reconcile";

/** An edge's delivery progress (python/solera_server/delivery.py): `next`,
 * the first upstream commit not yet delivered; `delivery`, one under way —
 * its mode, boundary (`from`..`to`) and position (`at`: the last key
 * delivered, or the next commit). */
export interface Watermark {
  kind: "keys" | "commits";
  output: string;
  up: string;
  fingerprint: string;
  reset_by?: string | null;
  next: number;
  delivery?: {
    mode: "full" | "delta" | "diff";
    from?: number;
    to?: number;
    at: string | number | null;
    page: number;
    pages: number;
    pin?: number | null;
    reconcile?: boolean;
  };
  patterns?: Json;
  rescope?: { old: Json; new: Json; cutover: number; pin: number };
  reconcile?: { after: string | null };
}

export interface EdgeScope {
  scope: string;
  up_scope: string;
  watermark: Watermark | null;
  head_commit: number | null;
  lag: number | null;
  state: EdgeState;
}

export interface Edge {
  param: string;
  kind: "incremental" | "each" | "in" | "all_partitions" | "dep";
  output: string;
  upstream_asset: string | null;
  source: boolean;
  page_size: number | null;
  concurrency: number | null;
  patterns: Patterns | null;
  scopes: EdgeScope[];
}

export interface Materialization {
  output: string;
  asset: string | null;
  scope: string;
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
  complete: boolean | null;
  metadata: Json;
  generation: number;
}

export interface LineageNode {
  output: string;
  scope: string;
  generation: number;
  asset: string | null;
  run: string | null;
  attempt: string | null;
  at: number | null;
  rows: number | null;
  current: boolean;
}

export interface Lineage {
  root: { output: string; scope: string; generation: number };
  direction: "upstream" | "downstream";
  nodes: LineageNode[];
  edges: {
    from: { output: string; scope: string; generation: number };
    to: { output: string; scope: string; generation: number };
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
  trigger: "manual" | "automation" | "sensor" | "commit" | string;
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

/** A run's selection: a named one, a list of scopes, or each asset's own (an
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

export type Facets = Record<"status" | "trigger" | "automation" | "by" | "source" | "asset" | "tag", Facet[]>;

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
  scope: string;
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
  status: string;
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
  key_outcomes?: Omit<KeyOutcome, "run" | "attempt" | "asset" | "scope" | "at">[];
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

export interface SensorHost {
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

/** GET /holds: what writers that died left for an operator (or the next attempt) to settle. */
export interface Holds {
  unsettled: {
    output: string;
    scope: string;
    intents: { run: string; attempt: string; files?: string[] }[];
  }[];
  discards: {
    output: string;
    scope: string;
    pending: number;
    stuck: { id: string; [key: string]: Json }[];
  }[];
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
