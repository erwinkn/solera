import { keepPreviousData, queryOptions, useMutation, useQueries, useQuery, useQueryClient, type QueryKey } from "@tanstack/react-query"
import { createContext, useContext } from "react"
import { CELLS, grid } from "../model/activity"
import { get, post, query, request } from "./client"
import type {
  AssetDetail,
  AssetStatus,
  AttemptResult,
  Automation,
  Cleanup,
  Commit,
  Diagnostics,
  Executor,
  Facets,
  Histogram,
  LogLine,
  Manifest,
  OutcomeEvent,
  Outcomes,
  PartitionRow,
  RunDetail,
  RunEvent,
  RunRequest,
  RunRow,
  Sensor,
  SensorWorker,
  StaleKeys,
  TaskHistogram,
  Tick,
  Worker,
  Head,
} from "./types"

/* Every read the console makes, as a hook. Live data is polled: the API has no
 * push channel. FAST is for what someone is watching change, SLOW for the rest. */

const FAST = 1500
const SLOW = 5000

export const ProjectContext = createContext<string>("")
const useBase = () => `/api/projects/${encodeURIComponent(useContext(ProjectContext))}`
const seg = encodeURIComponent

export type Health = { status: "ok" | "degraded" | "unavailable" | "unreachable" }

export function useHealth() {
  return useQuery({
    queryKey: ["health"],
    queryFn: async (): Promise<Health> => {
      try {
        const response = await fetch("/healthz")
        return await response.json()
      } catch {
        return { status: "unreachable" }
      }
    },
    refetchInterval: SLOW,
  })
}

export function useDiagnostics() {
  return useQuery({ queryKey: ["diagnostics"], queryFn: ({ signal }) => get<Diagnostics>("/api/diagnostics", signal), refetchInterval: SLOW })
}

function useLive<T>(key: QueryKey, path: string | null, every: number | false = SLOW) {
  const base = useBase()
  return useQuery({
    queryKey: [base, ...key],
    queryFn: ({ signal }) => get<T>(base + path, signal),
    enabled: path !== null,
    refetchInterval: every,
    placeholderData: keepPreviousData,
  })
}

export const useManifest = () => useLive<Manifest>(["manifest"], "/manifest", 15000)

export function useStatuses() {
  return useLive<{ assets: Record<string, AssetStatus> }>(["statuses"], "/assets:status", 2500)
}

export const useAsset = (name: string) => useLive<AssetDetail>(["asset", name], `/assets/${seg(name)}`)

export function usePartitions(name: string, enabled: boolean) {
  return useLive<{ asset: string; partitions: PartitionRow[] }>(["partitions", name], enabled ? `/partitions/${seg(name)}` : null)
}

export type RunsFilter = { status?: string[]; asset?: string[]; origin?: string[]; automation?: string[]; q?: string; since?: number }

export function useRuns(filter: RunsFilter = {}, limit = 50, every: number | false = 2500) {
  return useLive<{ runs: RunRow[]; next: string | null; total: number }>(["runs", filter, limit], `/runs${query({ ...filter, limit })}`, every)
}

export const useRunFacets = (filter: RunsFilter = {}) => useLive<Facets>(["facets", filter], `/runs:facets${query(filter)}`, 10000)
export const useRunHistogram = (filter: RunsFilter = {}, bars = 60) =>
  useLive<Histogram>(["histogram", filter, bars], `/runs:histogram${query({ ...filter, bars })}`, SLOW)

/** Tasks in cells of time over the last `windowSeconds`, a series per asset, or per partition of `asset`. */
export function useTaskHistogram(windowSeconds: number, asset?: string) {
  const base = useBase()
  return useQuery({
    queryKey: [base, "task-histogram", windowSeconds, asset ?? null],
    queryFn: ({ signal }) => get<TaskHistogram>(`${base}/tasks:histogram${query({ ...grid(windowSeconds, Date.now() / 1000), asset })}`, signal),
    // a cell is worth redrawing a few times while it fills, not more
    refetchInterval: Math.min(15000, Math.max(3000, (windowSeconds / CELLS) * 250)),
    placeholderData: keepPreviousData,
  })
}

const SETTLED = ["succeeded", "failed", "canceled", "skipped", "blocked"]
/** Nothing more will change: the run ended and so did each of its tasks. A canceled run ends before the tasks it was running do. */
const isSettled = (detail: RunDetail) => SETTLED.includes(detail.request.status) && detail.tasks.every((task) => SETTLED.includes(task.status))

const runQuery = (base: string, id: string, every: number) =>
  queryOptions({
    queryKey: [base, "run", id],
    queryFn: ({ signal }) => get<RunDetail>(`${base}/runs/${seg(id)}`, signal),
    refetchInterval: (q) => (q.state.data && isSettled(q.state.data) ? false : every),
    retry: (count, error) => (error as { status?: number }).status !== 404 && count < 2,
  })

/** A run with its tasks and attempts. Polled while it is in progress, then left alone: a finished run does not change. */
export function useRun(id: string) {
  const base = useBase()
  return useQuery({ ...runQuery(base, id, 1000), placeholderData: keepPreviousData })
}

export function useRunEvents(id: string, enabled: boolean, live: boolean) {
  return useLive<RunEvent[]>(["events", id], enabled ? `/runs/${seg(id)}/events` : null, live ? 2000 : false)
}

export function useAttemptLogs(run: string, attempt: string | null, live: boolean) {
  const base = useBase()
  return useQuery({
    queryKey: [base, "logs", run, attempt],
    enabled: attempt !== null,
    refetchInterval: live ? FAST : false,
    queryFn: async ({ signal }): Promise<LogLine[]> => {
      const response = await request(`${base}/runs/${seg(run)}/attempts/${seg(attempt!)}/logs?tail=2000`, { signal })
      const lines: LogLine[] = []
      for (const line of (await response.text()).split("\n")) {
        if (!line.trim()) continue
        try {
          lines.push(JSON.parse(line))
        } catch {
          lines.push({ at: 0, level: "info", message: line })
        }
      }
      return lines
    },
  })
}

const resultQuery = (base: string, run: string, attempt: string | null) =>
  queryOptions({
    queryKey: [base, "result", run, attempt],
    enabled: attempt !== null,
    staleTime: Infinity, // a finished attempt does not change
    retry: false,
    queryFn: ({ signal }) => get<AttemptResult>(`${base}/runs/${seg(run)}/attempts/${seg(attempt!)}/result`, signal),
  })

/** What a finished attempt reported: for a per-key attempt, each key with its outcome and how long its call took. */
export const useAttemptResult = (run: string, attempt: string | null) => useQuery(resultQuery(useBase(), run, attempt))

/** The same for several attempts, by attempt id. One that kept no result counts as having no keys. */
export function useAttemptResults(run: string, attempts: string[]): Map<string, AttemptResult> {
  const base = useBase()
  const found = useQueries({ queries: attempts.map((attempt) => resultQuery(base, run, attempt)) })
  const out = new Map<string, AttemptResult>()
  attempts.forEach((attempt, i) => {
    const result = found[i]
    if (result?.data) out.set(attempt, result.data)
    else if (result?.isError) out.set(attempt, { status: "unknown", key_outcomes: [] })
  })
  return out
}

export const useAutomations = () => useLive<{ automations: Automation[] }>(["automations"], "/automations", 2500)
export const useSensors = () => useLive<{ sensors: Sensor[]; workers: SensorWorker[] }>(["sensors"], "/sensors", 2500)
export const useTicks = (sensor: string) => useLive<{ ticks: Tick[] }>(["ticks", sensor], `/sensors/${seg(sensor)}/ticks?limit=200`)
export const useExecutors = () => useLive<{ executors: Executor[] }>(["executors"], "/executors")
export const useWorkers = () => useLive<{ workers: Worker[] }>(["workers"], "/workers")
export const useCleanups = () => useLive<{ cleanups: Cleanup[] }>(["cleanups"], "/cleanups")

export function useOutcomes(asset: string, filter: { outcome?: string[]; partition?: string; key?: string }, enabled = true) {
  return useLive<Outcomes>(["outcomes", asset, filter], enabled ? `/assets/${seg(asset)}/outcomes${query({ ...filter, limit: 200 })}` : null)
}

export function useOutcomeHistory(asset: string, filter: { key?: string; q?: string; partition?: string; run?: string }, enabled = true) {
  return useLive<{ outcomes: OutcomeEvent[]; next: string | null }>(
    ["outcome-history", asset, filter],
    enabled ? `/assets/${seg(asset)}/outcomes/history${query({ ...filter, limit: 100 })}` : null,
  )
}

export function useStaleKeys(asset: string, partition: string, enabled: boolean) {
  return useLive<StaleKeys>(["stale-keys", asset, partition], enabled ? `/assets/${seg(asset)}/stale-keys${query({ partition, limit: 200 })}` : null)
}

export const useCommits = (asset: string, partition?: string) =>
  useLive<{ commits: Commit[]; next: string | null }>(["commits", asset, partition], `/assets/${seg(asset)}/history${query({ partition, limit: 60 })}`)

export const useHeads = (output: string) => useLive<{ output: string; heads: Head[] }>(["heads", output], `/outputs/${seg(output)}/heads`)

export function useOutputKeys(output: string, partition: string, enabled = true) {
  return useLive<{ total: number; keys: Record<string, number | string>; next: string | null }>(
    ["keys", output, partition],
    enabled ? `/outputs/${seg(output)}/keys${query({ partition, limit: 200 })}` : null,
  )
}

/* -- actions ---------------------------------------------------------------- */

export type RunInput = {
  targets: string[]
  partitions: string | string[]
  mode: "incremental" | "full"
  upstream: boolean
  keys?: Record<string, string[] | "all">
}

/** Every write goes through here: POST, then refetch what is on screen. */
export function useAction<Input, Output = unknown>(path: (input: Input) => string, body?: (input: Input) => unknown, method: "POST" | "DELETE" = "POST") {
  const base = useBase()
  const client = useQueryClient()
  return useMutation({
    mutationFn: async (input: Input): Promise<Output> => {
      if (method === "DELETE") return (await request(base + path(input), { method })).json()
      return post<Output>(base + path(input), body ? { ...(body(input) as object), by: "console" } : undefined)
    },
    onSettled: () => client.invalidateQueries(),
  })
}

export const useStartRun = () => useAction<RunInput, RunRequest | { status: "skipped-active" }>(() => "/runs", (input) => input)
export const useRunCommand = () => useAction<{ id: string; command: "cancel" | "retry" | "pause" | "resume" }, RunRequest>((i) => `/runs/${seg(i.id)}/${i.command}`)
export const useDeleteRun = () => useAction<string>((id) => `/runs/${seg(id)}`, undefined, "DELETE")
export const useAutomationCommand = () =>
  useAction<{ name: string; command: "enable" | "disable" | "run-now" }, Automation>((i) => `/automations/${seg(i.name)}/${i.command}`)
export const useRetryKeys = () =>
  useAction<{ asset: string; classes: string[]; partition?: string }, { runs: RunRequest[]; partitions: string[] }>(
    (i) => `/assets/${seg(i.asset)}/keys:retry`,
    ({ classes, partition }) => ({ classes, partition: partition || null }),
  )
export const useCommitSource = () =>
  useAction<{ source: string; version?: string; upsert?: string[]; remove?: string[] }, unknown>(
    (i) => `/sources/${seg(i.source)}/commit`,
    ({ version, upsert, remove }) => ({ version: version || null, upsert: upsert?.length ? upsert : null, remove: remove?.length ? remove : null }),
  )
export const useClearCleanups = () =>
  useAction<{ output: string; partition: string }>(() => "/cleanups:clear", ({ output, partition }) => ({ output, partition }))
export const usePruneRuns = () =>
  useAction<{ before: number; dry_run: boolean }, { deleted: string[]; dry_run: boolean }>(() => "/runs:prune", (i) => i)
