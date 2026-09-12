export type Json =
  null | boolean | number | string | Json[] | { [key: string]: Json };

export type RunStatus =
  "queued" | "running" | "paused" | "succeeded" | "failed" | "canceled";

export type TaskStatus =
  | "waiting"
  | "queued"
  | "running"
  | "succeeded"
  | "skipped"
  | "failed"
  | "blocked"
  | "canceled";

export type AssetStatus =
  "materialized" | "stale" | "partial" | "not_materialized";

export interface OutputRef {
  kind?: string;
  complete?: boolean;
  rows?: number;
  [key: string]: Json | undefined;
}

export interface Head {
  ref: OutputRef;
  commit_id: string;
  updated_at: number;
  producer: string;
  partition: string;
  scope_complete: boolean;
  state_version: string;
}

export interface IncrementalSpec {
  input: string;
  key: string;
  revision: string;
  batch_size: number;
}

export interface CatalogAsset {
  name: string;
  producer: string;
  group: string;
  description: string;
  version: string | null;
  partitions: "daily" | null;
  incremental: IncrementalSpec | null;
  inputs: string[];
  heads: Head[];
}

export interface RunRecord {
  id: string;
  targets: string[];
  partitions: string[];
  mode: string;
  config: Record<string, Json>;
  revision: string;
  cause: string;
  created_at: number;
  updated_at: number;
  status: RunStatus;
  paused: boolean;
  tasks: string[];
}

export interface Trigger {
  kind: "interval" | "cron" | "commit";
  seconds?: number;
  expression?: string;
  timezone?: string;
  assets?: string[];
}

export interface AutomationRecord {
  name: string;
  targets: string[];
  enabled: boolean;
  trigger: Trigger;
  next_at: number | null;
  last_at: number | null;
  last_request: string | null;
  pending: string | null;
}

export interface StorageInfo {
  engine: string;
  scheme: string;
  namespace: string;
  sequence: number;
  experimental: boolean;
}

export interface StateResponse {
  assets: CatalogAsset[];
  runs: RunRecord[];
  automations: AutomationRecord[];
  storage: StorageInfo;
  revision: string;
}

export interface AssetDetail {
  head: Head | null;
  checkpoint: Json;
  commit: Json;
  preview: Json;
}

export interface TaskRecord {
  id: string;
  run_id: string;
  producer: string;
  partition: string;
  scope: string;
  status: TaskStatus;
  generation: number;
  attempt_count: number;
  max_attempts: number;
  result: Record<string, Json>;
  error?: string | null;
  deps: Record<string, { asset: string; partition: string; task: string }>;
  pinned_inputs: Record<string, { asset: string; partition: string }> | null;
  ready_at?: number;
}

export interface EventRecord {
  id: string;
  at: number;
  kind: string;
  message: string;
  task_id: string | null;
  data: Json;
}

export interface LogEntry {
  at: number;
  message: string;
  fields: Record<string, Json>;
}

export interface AttemptRecord {
  generation: number;
  status: string;
  commit_id?: string;
  error?: string;
  logs?: string;
  log_entries?: LogEntry[];
}

export interface RunDetail {
  request: RunRecord;
  tasks: TaskRecord[];
  events: EventRecord[];
  attempts: Record<string, AttemptRecord[]>;
}
