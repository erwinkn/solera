export type Json =
  null | boolean | number | string | Json[] | { [key: string]: Json };

export type RunStatus =
  "queued" | "running" | "paused" | "succeeded" | "failed" | "canceled";

export type TaskStatus =
  | "waiting"
  | "queued"
  | "claimable"
  | "running"
  | "succeeded"
  | "skipped"
  | "failed"
  | "blocked"
  | "canceled";

export type AssetStatus =
  "not_materialized" | "materialized" | "stale" | "partial";

export type ScopeStatus =
  "complete" | "missing" | "retired" | "running" | "failed";

export interface Diagnostics {
  backend: string;
  state: string;
  objects: string;
  namespace: string;
  project: string;
  revision: string;
  inflight: number;
  /** Runs submitted and not yet finished. */
  active_runs: number;
  postgres: boolean;
  last_error: string | null;
}

export interface Ref {
  output: string;
  store: string;
  handle: Record<string, Json>;
  version: string;
  partition?: string;
  meta?: Record<string, Json>;
}

export interface Head {
  ref: Ref;
  commit: string | null;
  at: number;
  complete: boolean;
  asset: string | null;
  version: string | null;
  batch?: number;
  count?: number;
  elements?: string[];
}

export interface OutputDecl {
  name: string;
  store: string;
  key: string | null;
  revision: string | null;
  incremental: boolean;
  /** Ordered migration names declared on the output (§4). */
  migrations?: string[];
  config: Record<string, Json>;
  partition_set: boolean;
}

export interface Edge {
  kind: "in" | "incremental" | "all_partitions" | "dep";
  output: string;
  param?: string;
  batch_size?: number;
  meta?: Record<string, Json>;
}

export interface PartitionDim {
  kind: "static" | "time" | "set";
  keys?: string[];
  output?: string;
  start?: string;
  every?: string;
  format?: string;
}

export interface Placement {
  kind: string;
  environment: Record<string, Json>;
  placement: Record<string, Json>;
}

export interface CatalogAsset {
  name: string;
  outputs: OutputDecl[];
  inputs: Record<string, Edge>;
  deps: string[];
  partitions: { dims: Record<string, PartitionDim> } | null;
  placement: Placement;
  retries: { n: number; delay: number; backoff: string };
  timeout: number;
  version: string;
  doc: string | null;
  automations: string[];
  heads: Record<string, Record<string, Head>>;
}

export interface SourceDecl {
  name: string;
  store: string;
  key: string | null;
  handle: Record<string, Json>;
  head: Ref;
}

export interface Manifest {
  name: string;
  assets: Record<string, Omit<CatalogAsset, "name" | "heads">>;
  outputs: Record<string, OutputDecl & { asset: string | null }>;
  sources: Record<string, SourceDecl>;
  stores: Record<string, Json>;
  automations: Record<string, AutomationDecl>;
  revision: string;
}

export interface Trigger {
  kind: "every" | "cron" | "onchange" | "ondeploy";
  seconds?: number;
  expression?: string;
  timezone?: string;
  outputs?: string[];
}

export interface AutomationDecl {
  name: string;
  targets: string[];
  trigger: Trigger;
  enabled: boolean;
  partitions: string | string[] | null;
  mode: string;
  upstream: boolean;
  config: Record<string, Json>;
  keys: Record<string, string | string[]> | null;
  watched: string[];
}

export interface AutomationRecord extends AutomationDecl {
  last_at: number | null;
  last_run: string | null;
  last_revision: string | null;
  pending: [string | null, string][];
}

export interface Run {
  id: string;
  targets: string[];
  partitions: string | string[];
  mode: string;
  upstream: boolean;
  config: Record<string, Json>;
  keys: Record<string, string | string[]> | null;
  automation: string | null;
  by?: string | null;
  source?: string;
  tags?: Record<string, string>;
  status: RunStatus;
  paused: boolean;
  tasks: string[];
  created_at: number;
  updated_at: number;
}

export interface Task {
  id: string;
  run: string;
  asset: string;
  scope: string;
  status: TaskStatus;
  deps: string[];
  generation: number;
  attempt_count: number;
  ready_at: number;
  error?: string | null;
}

export interface Attempt {
  id: string;
  task: string;
  generation: number;
  status: string;
  started_at: number;
  finished_at?: number;
  commit?: string;
  error?: {
    type: string;
    message: string;
    traceback?: string;
    retryable?: boolean;
  };
}

export interface RunDetail {
  request: Run;
  tasks: Task[];
  attempts: Record<string, Attempt[]>;
}

export interface ScopeOutcome {
  last_outcome: string | null;
  last_attempt: string | null;
  at: number;
}

export interface PartitionScope {
  scope: string;
  status: ScopeStatus;
  last_outcome?: string | null;
  last_attempt?: string | null;
}

export interface OutputHead {
  scope: string;
  ref: Ref;
  version: string | null;
  key_count: number | null;
  batch: number | null;
  complete: boolean;
  cursor: boolean;
  at: number;
  commit: string | null;
}

export interface Watermark {
  batch: number;
  until?: number;
  after: string | null;
  full: boolean;
  fingerprint: string;
  output: string;
  up: string;
}

export interface AssetDetail {
  asset: Omit<CatalogAsset, "name" | "heads">;
  heads: Record<string, [string, Head][]>;
  cursor: Json;
  watermarks: Record<string, Watermark | null>;
  current_keys: string[][];
  // Output -> partitions a dead attempt may have partly written (the next commit repairs them).
  unsettled: Record<string, string[]>;
  scopes: Record<string, ScopeOutcome>;
  automations: AutomationRecord[];
}

export interface EnvironmentInfo {
  key: string;
  kind: string;
  environment: Record<string, Json>;
  max_concurrent: number | null;
  in_flight: number;
}

export interface PoolWorker {
  id: string;
  pools: string[];
  meta: { cpu?: number; memory?: number | null; gpu?: number | null };
  seen_at: number;
  task: string | null;
}

/** A run as the history lists it (object-store-state.md §7). */
export interface RunRow {
  id: string;
  created_at: number;
  finished_at: number | null;
  status: RunStatus | "skipped";
  trigger: "manual" | "automation" | "commit";
  automation: string | null;
  by: string | null;
  source: string | null;
  targets: string[];
  assets: string[];
  committed: string[];
  mode: string | null;
  partitions: string | string[] | null;
  upstream: boolean;
  tags: Record<string, string>;
  task_count: number;
  failed_count: number;
  error: string | null;
}

export interface RunPage {
  runs: RunRow[];
  /** The `before` cursor of the next page, or null on the last one. */
  next: string | null;
  /** How many runs match, across every page. */
  total: number;
}

export interface FacetValue {
  value: string;
  count: number;
}

export type Facets = Record<
  "status" | "asset" | "tag" | "trigger" | "automation" | "by" | "source",
  FacetValue[]
>;

export interface Histogram {
  bucket: number;
  since: number | null;
  until: number;
  bars: { t: number; counts: Record<string, number> }[];
}

export interface Materialization {
  output: string;
  asset: string | null;
  scope: string;
  version: string | null;
  store: string | null;
  run: string | null;
  attempt: string | null;
  at: number;
  batch: number | null;
  added: number | null;
  removed: number | null;
  rows: number | null;
  complete: boolean;
  metadata: Record<string, Json> | null;
}

export interface AssetHistory {
  asset: string;
  materializations: Materialization[];
  next: string | null;
}

export interface VersionRef {
  output: string;
  scope: string;
  version: string | null;
}

export interface LineageNode extends VersionRef {
  asset?: string | null;
  run?: string | null;
  attempt?: string | null;
  at?: number;
  rows?: number | null;
  /** Still the output's head for that scope. */
  current: boolean;
}

export interface Lineage {
  root: VersionRef;
  direction: "upstream" | "downstream";
  nodes: LineageNode[];
  edges: { from: VersionRef; to: VersionRef; param: string; run: string }[];
}

export interface StatsRow {
  tasks: number;
  skipped: number;
  failed: number;
  p50: number | null;
  p95: number | null;
  wait_p50: number | null;
  wait_p95: number | null;
  hours: number | null;
  cpu_hours: number | null;
  gb_hours: number | null;
  gpu_hours: number | null;
}

export interface Stats {
  assets: (StatsRow & { asset: string })[];
  executors: (StatsRow & { executor: string })[];
}
