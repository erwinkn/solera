import { infiniteQueryOptions, queryOptions, useSuspenseQuery } from "@tanstack/react-query";
import { api, apiText } from "./client";
import type {
  AssetDetail,
  AssetStatus,
  Attempt,
  AttemptResult,
  AttemptSpec,
  Automation,
  CatalogAsset,
  Diagnostics,
  Input,
  Executor,
  Explain,
  Facets,
  FailedKeys,
  Histogram,
  Repair,
  Cleanup,
  KeyOutcome,
  KeyPage,
  Lineage,
  LogLine,
  Manifest,
  Commit,
  OutputHead,
  PartitionRow,
  RunDetail,
  RunEvent,
  RunPage,
  SensorWorker,
  SensorView,
  StaleKeys,
  Stats,
  Tick,
  Worker,
} from "./types";

/**
 * One factory per resource. Keys run from the resource down, so a mutation
 * invalidates exactly as wide as its effect: ["runs"] for every run view,
 * ["runs", id] for one run and its events, ["assets", name] for one asset.
 *
 * Live views poll; `refetchInterval` is a function of the data where the
 * data says whether it can still change.
 */

const SECOND = 1000;
const LIST = 5 * SECOND;
const LIVE = 2 * SECOND;

const p = (project: string) => `/projects/${encodeURIComponent(project)}`;
const enc = encodeURIComponent;

export const ACTIVE_RUN = new Set(["queued", "running", "waiting"]);
/** An attempt that can still change: preparing, launching (its launch not yet
 * durable), launched, waiting for a pool worker, running. */
export const ACTIVE_ATTEMPT = new Set([
  "preparing",
  "launching",
  "launched",
  "provisioning",
  "claimable",
  "claimed",
  "running",
  "queued",
  "waiting",
]);

/**
 * Whether a run can still change. A canceled run is marked at once, while
 * its launched attempts may still be draining: it is live until they end.
 */
export function runIsLive(detail: RunDetail | undefined): boolean {
  if (!detail) return true;
  return (
    ACTIVE_RUN.has(detail.request.status) ||
    Object.values(detail.attempts).some((list) => list.some((a) => ACTIVE_ATTEMPT.has(a.outcome)))
  );
}

export type RunFilter = {
  status?: string[];
  origin?: string[];
  automation?: string[];
  asset?: string[];
  tag?: string[];
  by?: string[];
  source?: string[];
  q?: string;
  since?: number;
  until?: number;
  /** A trailing window ("24h"): turned into `since` when fetched, so the key stays put. */
  range?: string;
  /** Include the engine's cleanup tasks, which the API leaves out unless `origin` names them. */
  cleanup?: boolean;
};

/** Every origin a run can have: asking for all of them is how a listing includes cleanup tasks. */
export const ORIGINS = ["manual", "automation", "sensor", "commit", "cleanup"];

const RANGE_SECONDS: Record<string, number> = {
  "1h": 3600,
  "6h": 21600,
  "24h": 86400,
  "7d": 604800,
  "30d": 2592000,
};

/** The query string of a run filter: list fields repeat, a range becomes `since`. */
function runQuery({ range, cleanup, ...filter }: RunFilter) {
  const since = range && RANGE_SECONDS[range] ? Date.now() / 1000 - RANGE_SECONDS[range]! : undefined;
  const origin = cleanup && !filter.origin?.length ? ORIGINS : filter.origin;
  return { ...filter, origin, since: filter.since ?? since };
}

export const q = {
  diagnostics: () =>
    queryOptions({
      queryKey: ["diagnostics"],
      queryFn: ({ signal }) => api<Diagnostics>("/diagnostics", { signal }),
      refetchInterval: LIST,
    }),

  health: () =>
    queryOptions({
      queryKey: ["healthz"],
      queryFn: async ({ signal }) => {
        const response = await fetch("/healthz", { signal });
        return {
          ok: response.ok,
          ...((await response.json().catch(() => ({}))) as { status?: string }),
        };
      },
      refetchInterval: LIST,
      retry: false,
    }),

  /** Keyed by deploy: a new one changes the key, and the new manifest loads. */
  manifest: (project: string, deploy: string) =>
    queryOptions({
      queryKey: ["manifest", deploy],
      queryFn: ({ signal }) => api<Manifest>(`${p(project)}/manifest`, { signal }),
      staleTime: Infinity,
    }),

  // -- assets ----------------------------------------------------------------------

  catalog: (project: string) =>
    queryOptions({
      queryKey: ["assets", "catalog"],
      queryFn: async ({ signal }) =>
        (
          await api<{ assets: CatalogAsset[] }>(`${p(project)}/assets`, {
            signal,
          })
        ).assets,
      refetchInterval: LIST,
    }),

  assetStatus: (project: string) =>
    queryOptions({
      queryKey: ["assets", "status"],
      queryFn: async ({ signal }) =>
        (await api<{ assets: Record<string, AssetStatus> }>(`${p(project)}/assets:status`, { signal }))
          .assets,
      refetchInterval: LIVE,
    }),

  asset: (project: string, name: string) =>
    queryOptions({
      queryKey: ["assets", name, "detail"],
      queryFn: ({ signal }) => api<AssetDetail>(`${p(project)}/assets/${enc(name)}`, { signal }),
      refetchInterval: LIST,
    }),

  partitions: (project: string, name: string) =>
    queryOptions({
      queryKey: ["assets", name, "partitions"],
      queryFn: async ({ signal }) =>
        (await api<{ partitions: PartitionRow[] }>(`${p(project)}/partitions/${enc(name)}`, { signal }))
          .partitions,
      refetchInterval: LIVE,
    }),

  heads: (project: string, output: string) =>
    queryOptions({
      queryKey: ["outputs", output, "heads"],
      queryFn: async ({ signal }) =>
        (await api<{ heads: OutputHead[] }>(`${p(project)}/outputs/${enc(output)}/heads`, { signal })).heads,
      refetchInterval: LIST,
    }),

  keys: (project: string, output: string, partition: string) =>
    infiniteQueryOptions({
      queryKey: ["outputs", output, "keys", partition],
      queryFn: ({ signal, pageParam }) =>
        api<KeyPage>(`${p(project)}/outputs/${enc(output)}/keys`, {
          signal,
          query: { partition, after: pageParam, limit: 200 },
        }),
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (page) => page.next ?? undefined,
      refetchInterval: LIST,
    }),

  failures: (project: string, name: string, filter: { partition?: string; outcome?: string[] }) =>
    infiniteQueryOptions({
      queryKey: ["assets", name, "failures", filter],
      queryFn: ({ signal, pageParam }) =>
        api<FailedKeys>(`${p(project)}/assets/${enc(name)}/failed-keys`, {
          signal,
          query: {
            partition: filter.partition,
            outcome: filter.outcome,
            after: pageParam,
            limit: 100,
          },
        }),
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (page) => page.next ?? undefined,
      refetchInterval: LIST,
    }),

  /** One partition's stale keys, a page at a time, each with why. */
  staleKeys: (project: string, name: string, partition: string) =>
    infiniteQueryOptions({
      queryKey: ["assets", name, "stale-keys", partition],
      queryFn: ({ signal, pageParam }) =>
        api<StaleKeys>(`${p(project)}/assets/${enc(name)}/stale-keys`, {
          signal,
          query: { partition, after: pageParam, limit: 200 },
        }),
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (page) => page.next ?? undefined,
      refetchInterval: LIST,
    }),

  keyOutcomes: (
    project: string,
    name: string,
    filter: { partition?: string; q?: string; outcome?: string[]; key?: string },
  ) =>
    infiniteQueryOptions({
      queryKey: ["assets", name, "key-outcomes", filter],
      queryFn: ({ signal, pageParam }) =>
        api<{ outcomes: KeyOutcome[]; next: string | null }>(
          `${p(project)}/assets/${enc(name)}/key-outcomes`,
          {
            signal,
            query: { ...filter, before: pageParam, limit: 100 },
          },
        ),
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (page) => page.next ?? undefined,
      refetchInterval: LIST,
    }),

  explain: (project: string, name: string, key: string, partition: string, input?: string) =>
    queryOptions({
      queryKey: ["assets", name, "explain", partition, key, input ?? null],
      queryFn: ({ signal }) =>
        api<Explain>(`${p(project)}/assets/${enc(name)}/explain`, {
          signal,
          query: { key, partition, input },
        }),
      refetchInterval: LIST,
    }),

  inputs: (project: string, name: string) =>
    queryOptions({
      queryKey: ["assets", name, "inputs"],
      queryFn: async ({ signal }) =>
        (await api<{ inputs: Input[] }>(`${p(project)}/assets/${enc(name)}/inputs`, { signal })).inputs,
      refetchInterval: LIST,
    }),

  history: (project: string, name: string, filter: { output?: string; partition?: string }) =>
    infiniteQueryOptions({
      queryKey: ["assets", name, "history", filter],
      queryFn: ({ signal, pageParam }) =>
        api<{ commits: Commit[]; next: string | null }>(`${p(project)}/assets/${enc(name)}/history`, {
          signal,
          query: { ...filter, before: pageParam, limit: 50 },
        }),
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (page) => page.next ?? undefined,
      refetchInterval: LIST,
    }),

  lineage: (
    project: string,
    output: string,
    partition: string,
    generation: number | undefined,
    direction: "upstream" | "downstream",
  ) =>
    queryOptions({
      queryKey: ["outputs", output, "lineage", partition, generation ?? "head", direction],
      queryFn: ({ signal }) =>
        api<Lineage>(`${p(project)}/outputs/${enc(output)}/lineage`, {
          signal,
          query: { partition, generation, direction, depth: 4 },
        }),
    }),

  stats: (project: string, since?: number, asset?: string) =>
    queryOptions({
      queryKey: ["stats", since ?? null, asset ?? null],
      queryFn: ({ signal }) => api<Stats>(`${p(project)}/stats`, { signal, query: { since, asset } }),
      refetchInterval: 30 * SECOND,
    }),

  // -- runs ----------------------------------------------------------------------

  runs: (project: string, filter: RunFilter, limit = 50) =>
    infiniteQueryOptions({
      queryKey: ["runs", "list", filter, limit],
      queryFn: ({ signal, pageParam }) =>
        api<RunPage>(`${p(project)}/runs`, {
          signal,
          query: { ...runQuery(filter), before: pageParam, limit },
        }),
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (page) => page.next ?? undefined,
      refetchInterval: LIVE,
    }),

  runFacets: (project: string, filter: RunFilter) =>
    queryOptions({
      queryKey: ["runs", "facets", filter],
      queryFn: ({ signal }) =>
        api<Facets>(`${p(project)}/runs:facets`, {
          signal,
          query: runQuery(filter),
        }),
      refetchInterval: LIST,
    }),

  runHistogram: (project: string, filter: RunFilter, bars: number) =>
    queryOptions({
      queryKey: ["runs", "histogram", filter, bars],
      queryFn: ({ signal }) =>
        api<Histogram>(`${p(project)}/runs:histogram`, {
          signal,
          query: { ...runQuery(filter), bars },
        }),
      refetchInterval: LIST,
    }),

  run: (project: string, id: string) =>
    queryOptions({
      queryKey: ["runs", id],
      queryFn: ({ signal }) => api<RunDetail>(`${p(project)}/runs/${enc(id)}`, { signal }),
      refetchInterval: (query) => (runIsLive(query.state.data) ? SECOND : false),
    }),

  /** `live` is in the key: when the run ends, the key changes and the final events are read once. */
  runEvents: (project: string, id: string, live: boolean) =>
    queryOptions({
      queryKey: ["runs", id, "events", live],
      queryFn: ({ signal }) => api<RunEvent[]>(`${p(project)}/runs/${enc(id)}/events`, { signal }),
      refetchInterval: live ? SECOND : false,
    }),

  attemptLogs: (project: string, run: string, attempt: Attempt, tail: number | null) =>
    queryOptions({
      // The status is in the key: an attempt that ends gets one last, complete read.
      queryKey: ["attempts", run, attempt.id, "logs", tail, ACTIVE_ATTEMPT.has(attempt.outcome)],
      queryFn: async ({ signal }) =>
        parseLog(
          await apiText(`${p(project)}/runs/${enc(run)}/attempts/${enc(attempt.id)}/logs`, {
            signal,
            query: { tail },
          }),
        ),
      refetchInterval: ACTIVE_ATTEMPT.has(attempt.outcome) ? SECOND : false,
      staleTime: ACTIVE_ATTEMPT.has(attempt.outcome) ? 0 : Infinity,
    }),

  attemptSpec: (project: string, run: string, attempt: string) =>
    queryOptions({
      queryKey: ["attempts", run, attempt, "spec"],
      queryFn: ({ signal }) =>
        api<AttemptSpec>(`${p(project)}/runs/${enc(run)}/attempts/${enc(attempt)}/spec`, { signal }),
      staleTime: Infinity,
      retry: false,
    }),

  attemptResult: (project: string, run: string, attempt: string) =>
    queryOptions({
      queryKey: ["attempts", run, attempt, "result"],
      queryFn: ({ signal }) =>
        api<AttemptResult>(`${p(project)}/runs/${enc(run)}/attempts/${enc(attempt)}/result`, { signal }),
      staleTime: Infinity,
      retry: false,
    }),

  // -- automations, sensors, sources, executors, health ------------------------------

  automations: (project: string) =>
    queryOptions({
      queryKey: ["automations"],
      queryFn: async ({ signal }) =>
        (await api<{ automations: Automation[] }>(`${p(project)}/automations`, { signal })).automations,
      refetchInterval: LIST,
    }),

  sensors: (project: string) =>
    queryOptions({
      queryKey: ["sensors"],
      queryFn: ({ signal }) =>
        api<{ sensors: SensorView[]; workers: SensorWorker[] }>(`${p(project)}/sensors`, { signal }),
      refetchInterval: LIVE,
    }),

  ticks: (project: string, sensor: string) =>
    queryOptions({
      queryKey: ["sensors", sensor, "ticks"],
      queryFn: async ({ signal }) =>
        (
          await api<{ ticks: Tick[] }>(`${p(project)}/sensors/${enc(sensor)}/ticks`, {
            signal,
            query: { limit: 200 },
          })
        ).ticks,
      refetchInterval: LIST,
    }),

  executors: (project: string) =>
    queryOptions({
      queryKey: ["executors"],
      queryFn: async ({ signal }) =>
        (
          await api<{ executors: Executor[] }>(`${p(project)}/executors`, {
            signal,
          })
        ).executors,
      refetchInterval: LIVE,
    }),

  workers: (project: string) =>
    queryOptions({
      queryKey: ["workers"],
      queryFn: async ({ signal }) =>
        (await api<{ workers: Worker[] }>(`${p(project)}/workers`, { signal })).workers,
      refetchInterval: LIST,
    }),

  repairs: (project: string) =>
    queryOptions({
      queryKey: ["repairs"],
      queryFn: async ({ signal }) =>
        (await api<{ repairs: Repair[] }>(`${p(project)}/repairs`, { signal })).repairs,
      refetchInterval: LIST,
    }),

  cleanups: (project: string) =>
    queryOptions({
      queryKey: ["cleanups"],
      queryFn: async ({ signal }) =>
        (await api<{ cleanups: Cleanup[] }>(`${p(project)}/cleanups`, { signal })).cleanups,
      refetchInterval: LIST,
    }),
};

/** Log lines are NDJSON; a line that isn't JSON is shown as text. */
export function parseLog(text: string): LogLine[] {
  const lines: LogLine[] = [];
  for (const raw of text.split("\n")) {
    if (!raw.trim()) continue;
    try {
      lines.push(JSON.parse(raw) as LogLine);
    } catch {
      lines.push({ at: 0, level: "info", message: raw });
    }
  }
  return lines;
}

// -- hooks every page needs ------------------------------------------------------

export function useProject(): string {
  return useSuspenseQuery(q.diagnostics()).data.project;
}

export function useManifest(): Manifest {
  const { project, deploy } = useSuspenseQuery(q.diagnostics()).data;
  return useSuspenseQuery(q.manifest(project, deploy)).data;
}
