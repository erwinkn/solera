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
  Edge,
  Executor,
  Explain,
  Facets,
  Failures,
  Histogram,
  Holds,
  KeyOutcome,
  KeyPage,
  Lineage,
  LogLine,
  Manifest,
  Materialization,
  OutputHead,
  PartitionRow,
  RunDetail,
  RunEvent,
  RunPage,
  SensorHost,
  SensorView,
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
/** An attempt that can still change: preparing, launched, waiting for a pool worker, running. */
export const ACTIVE_ATTEMPT = new Set([
  "preparing",
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
    Object.values(detail.attempts).some((list) => list.some((a) => ACTIVE_ATTEMPT.has(a.status)))
  );
}

export type RunFilter = {
  status?: string[];
  trigger?: string[];
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
};

const RANGE_SECONDS: Record<string, number> = {
  "1h": 3600,
  "6h": 21600,
  "24h": 86400,
  "7d": 604800,
  "30d": 2592000,
};

/** The query string of a run filter: list fields repeat, a range becomes `since`. */
function runQuery({ range, ...filter }: RunFilter) {
  const since = range && RANGE_SECONDS[range] ? Date.now() / 1000 - RANGE_SECONDS[range]! : undefined;
  return { ...filter, since: filter.since ?? since };
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

  keys: (project: string, output: string, scope: string) =>
    infiniteQueryOptions({
      queryKey: ["outputs", output, "keys", scope],
      queryFn: ({ signal, pageParam }) =>
        api<KeyPage>(`${p(project)}/outputs/${enc(output)}/keys`, {
          signal,
          query: { scope, after: pageParam, limit: 200 },
        }),
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (page) => page.next ?? undefined,
      refetchInterval: LIST,
    }),

  failures: (project: string, name: string, filter: { scope?: string; outcome?: string[] }) =>
    infiniteQueryOptions({
      queryKey: ["assets", name, "failures", filter],
      queryFn: ({ signal, pageParam }) =>
        api<Failures>(`${p(project)}/assets/${enc(name)}/failures`, {
          signal,
          query: {
            scope: filter.scope,
            outcome: filter.outcome,
            after: pageParam,
            limit: 100,
          },
        }),
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (page) => page.next ?? undefined,
      refetchInterval: LIST,
    }),

  keyOutcomes: (
    project: string,
    name: string,
    filter: { scope?: string; q?: string; outcome?: string[]; key?: string },
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

  explain: (project: string, name: string, key: string, scope: string, edge?: string) =>
    queryOptions({
      queryKey: ["assets", name, "explain", scope, key, edge ?? null],
      queryFn: ({ signal }) =>
        api<Explain>(`${p(project)}/assets/${enc(name)}/explain`, {
          signal,
          query: { key, scope, edge },
        }),
      refetchInterval: LIST,
    }),

  edges: (project: string, name: string) =>
    queryOptions({
      queryKey: ["assets", name, "edges"],
      queryFn: async ({ signal }) =>
        (await api<{ edges: Edge[] }>(`${p(project)}/assets/${enc(name)}/edges`, { signal })).edges,
      refetchInterval: LIST,
    }),

  history: (project: string, name: string, filter: { output?: string; scope?: string }) =>
    infiniteQueryOptions({
      queryKey: ["assets", name, "history", filter],
      queryFn: ({ signal, pageParam }) =>
        api<{ materializations: Materialization[]; next: string | null }>(
          `${p(project)}/assets/${enc(name)}/history`,
          {
            signal,
            query: { ...filter, before: pageParam, limit: 50 },
          },
        ),
      initialPageParam: undefined as string | undefined,
      getNextPageParam: (page) => page.next ?? undefined,
      refetchInterval: LIST,
    }),

  lineage: (
    project: string,
    output: string,
    scope: string,
    generation: number | undefined,
    direction: "upstream" | "downstream",
  ) =>
    queryOptions({
      queryKey: ["outputs", output, "lineage", scope, generation ?? "head", direction],
      queryFn: ({ signal }) =>
        api<Lineage>(`${p(project)}/outputs/${enc(output)}/lineage`, {
          signal,
          query: { scope, generation, direction, depth: 4 },
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
      queryKey: ["attempts", run, attempt.id, "logs", tail, ACTIVE_ATTEMPT.has(attempt.status)],
      queryFn: async ({ signal }) =>
        parseLog(
          await apiText(`${p(project)}/runs/${enc(run)}/attempts/${enc(attempt.id)}/logs`, {
            signal,
            query: { tail },
          }),
        ),
      refetchInterval: ACTIVE_ATTEMPT.has(attempt.status) ? SECOND : false,
      staleTime: ACTIVE_ATTEMPT.has(attempt.status) ? 0 : Infinity,
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
        api<{ sensors: SensorView[]; hosts: SensorHost[] }>(`${p(project)}/sensors`, { signal }),
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

  holds: (project: string) =>
    queryOptions({
      queryKey: ["holds"],
      queryFn: ({ signal }) => api<Holds>(`${p(project)}/holds`, { signal }),
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
