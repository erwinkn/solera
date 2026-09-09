export type Json =
  null | boolean | number | string | Json[] | { [key: string]: Json };
export type Status =
  | "missing"
  | "materialized"
  | "stale"
  | "queued"
  | "waiting"
  | "running"
  | "succeeded"
  | "skipped"
  | "failed"
  | "blocked"
  | "canceled";
export interface Incremental {
  kind: "key" | "cursor";
  input?: string;
  key?: string;
  revision?: string;
  batch_size?: number;
  state_version?: string;
}
export interface Asset {
  key: string;
  producer: string;
  description: string;
  group: string;
  store: string;
  inputs: string[];
  outputs: string[];
  incremental: Incremental | null;
  partitions: { start: string } | null;
  status: Status;
  last_materialized: string | null;
  version: string | null;
  rows: number | null;
  partition_count: number;
}
export interface Catalog {
  name: string;
  manifest_id: string;
  assets: Asset[];
  queue: { running: number; queued: number };
}
export interface Reference {
  store: string;
  version: string;
  preview: Json;
  rows: number | null;
  asset?: string;
  commit_id?: string;
}
export interface Commit {
  id: string;
  task_id: string;
  producer: string;
  partition_key: string;
  created_at: string;
  outputs: Record<string, Reference>;
  metadata: Json;
  checkpoint_generation: number | null;
  input_refs: Record<string, Reference>;
}
export interface AssetDetail {
  definition: Asset;
  commits: Commit[];
  preview: Json;
  partitions: { partition_key: string; version: string; updated_at: string }[];
  checkpoint: null | {
    generation: number;
    tracked_items: number;
    cursor_value: Json;
    state_version: string | null;
    updated_at: string;
  };
}
export interface Run {
  id: string;
  status: Status;
  mode: string;
  reason: string;
  targets: string[];
  partitions: string[];
  paused: boolean;
  parent_id: string | null;
  created_at: string;
  finished_at: string | null;
  counts?: Record<string, number>;
}
export interface Task {
  id: string;
  producer: string;
  partition_key: string;
  status: Status;
  attempt: number;
  max_attempts: number;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
  input_refs: Record<string, Reference> | null;
  output_commit: string | null;
}
export interface Event {
  id: number;
  task_id: string | null;
  kind: string;
  message: string;
  detail: Json;
  created_at: string;
}
export interface Attempt {
  token: string;
  task_id: string;
  number: number;
  status: string;
  error: string | null;
  started_at: string;
  finished_at: string | null;
}
export interface RunDetail {
  request: Run;
  tasks: Task[];
  attempts: Attempt[];
  events: Event[];
}
export interface Automation {
  name: string;
  enabled: boolean;
  definition: {
    targets: string[];
    trigger: {
      kind: "interval" | "cron" | "commit";
      seconds?: number;
      expression?: string;
      timezone?: string;
      assets?: string[];
    };
  };
  last_tick: string | null;
  next_tick: string;
  last_request: string | null;
}
