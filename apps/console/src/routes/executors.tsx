import { createFileRoute } from "@tanstack/react-router";
import { Bot, Cpu, Layers, MonitorSmartphone } from "lucide-react";
import { useState } from "react";
import { Empty, Eyebrow, PageHeader, Segmented } from "@/components/common";
import { useQuery } from "@/lib/api";
import { count, failures, resources, seconds, time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type { ExecutorInfo, PoolWorker, Stats, StatsRow } from "@/lib/types";

export const Route = createFileRoute("/executors")({
  component: ExecutorsPage,
});

function capacityLabel(meta: PoolWorker["meta"]) {
  return resources(meta) || "unbounded";
}

function envIcon(kind: string) {
  if (kind === "Pool") return Layers;
  if (kind === "Local") return MonitorSmartphone;
  return Cpu;
}

// "AWSECS · lab · us-east-1": the kind, then its settings.
function settings(executor: ExecutorInfo) {
  return [executor.kind, ...Object.values(executor.environment ?? {})]
    .map(String)
    .join(" · ");
}

function ExecutorsPage() {
  const { diagnostics, base, select } = useWorkspace();
  const executors = useQuery<{ executors: ExecutorInfo[] }>(
    base ? `${base}/executors` : null,
    3000,
  );
  const workers = useQuery<{ workers: PoolWorker[] }>(
    base ? `${base}/workers` : null,
    3000,
  );
  if (!diagnostics) return null;
  const envs = executors.data?.executors ?? [];
  const pool = workers.data?.workers ?? [];
  return (
    <section className="flex flex-col gap-5">
      <PageHeader
        eyebrow="Execution"
        title="Executors"
        description="Named environments where tasks run, and how many run at once."
      />
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {envs.map((env) => {
          const Icon = envIcon(env.kind);
          const saturated =
            env.max_concurrent != null && env.in_flight >= env.max_concurrent;
          return (
            <div
              key={env.name}
              data-executor={env.name}
              className="flex flex-col gap-3 rounded-xl border bg-card p-4"
            >
              <div className="flex min-w-0 flex-col gap-0.5">
                <div className="flex items-center gap-2">
                  <Icon className="size-4 text-muted-foreground" />
                  <span className="font-mono text-sm font-semibold">
                    {env.name}
                  </span>
                </div>
                <span className="truncate text-xs text-muted-foreground">
                  {settings(env)}
                </span>
              </div>
              <div className="flex items-end justify-between">
                <div className="flex flex-col">
                  <span className="text-2xl font-semibold tabular-nums">
                    {env.in_flight}
                    {env.max_concurrent != null && (
                      <span className="text-base text-muted-foreground">
                        {" "}
                        / {env.max_concurrent}
                      </span>
                    )}
                  </span>
                  <span className="text-xs text-muted-foreground">
                    in flight
                  </span>
                </div>
                <span
                  className={cn(
                    "rounded-full px-2 py-0.5 text-xs font-medium",
                    saturated
                      ? "bg-sky-500/15 text-sky-700 dark:text-sky-300"
                      : "text-muted-foreground",
                  )}
                >
                  {env.max_concurrent != null
                    ? saturated
                      ? "saturated"
                      : `max ${env.max_concurrent}`
                    : "unbounded"}
                </span>
              </div>
            </div>
          );
        })}
      </div>

      <ExecutorStats />

      <div className="flex flex-col gap-3">
        <div className="flex flex-col gap-0.5">
          <Eyebrow>Pool workers</Eyebrow>
          <p className="text-sm text-muted-foreground">
            Registered with{" "}
            <code className="font-mono text-xs">
              solera worker pool &lt;name&gt;
            </code>
            .
          </p>
        </div>
        {!pool.length ? (
          <Empty title="No pool workers connected">
            Start one with{" "}
            <code className="font-mono text-xs">solera worker pool ingest</code>{" "}
            to pick up pool-placed tasks.
          </Empty>
        ) : (
          <div className="grid gap-3 sm:grid-cols-2">
            {pool.map((worker) => (
              <div
                key={worker.id}
                data-worker={worker.id}
                className="flex items-center gap-3 rounded-xl border bg-card p-3.5"
              >
                <span
                  className={cn(
                    "flex size-9 shrink-0 items-center justify-center rounded-lg",
                    worker.task
                      ? "bg-sky-500/15 text-sky-600 dark:text-sky-400"
                      : "bg-emerald-500/15 text-emerald-600 dark:text-emerald-400",
                  )}
                >
                  <Bot className="size-4.5" />
                </span>
                <div className="min-w-0 flex-1">
                  <div className="font-mono text-xs font-medium">
                    worker {worker.id.slice(0, 12)}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    pools {worker.pools.join(", ")} ·{" "}
                    {capacityLabel(worker.meta)}
                  </div>
                  <div className="text-[0.7rem] text-muted-foreground">
                    seen {time(worker.seen_at)}
                  </div>
                </div>
                {worker.task ? (
                  <button
                    className="flex items-center gap-1.5 rounded-md bg-sky-500/10 px-2 py-1 font-mono text-xs text-sky-700 hover:underline dark:text-sky-300"
                    onClick={() =>
                      select({ kind: "run", id: worker.task!.split("/")[0] })
                    }
                    title={worker.task}
                  >
                    <span className="size-1.5 animate-pulse rounded-full bg-sky-500" />
                    {worker.task.split("/").slice(1).join("/")}
                  </button>
                ) : (
                  <span className="text-xs text-muted-foreground">idle</span>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}

const RANGES = { "24h": 86400, "7d": 7 * 86400, "30d": 30 * 86400 } as const;
type Range = keyof typeof RANGES;

function hours(value: number | null) {
  if (value == null) return "—";
  return value >= 10
    ? count(Math.round(value))
    : value.toFixed(value >= 1 ? 1 : 2);
}

/** Finished tasks from the run history, per executor: volume, failures,
    how long tasks took and waited for a slot, and the compute they used. */
function ExecutorStats() {
  const { base } = useWorkspace();
  const [range, setRange] = useState<Range>("7d");
  // Rounded to the minute so the polled path stays stable.
  const minute = Math.floor(Date.now() / 60000) * 60;
  const stats = useQuery<Stats>(
    base ? `${base}/stats?since=${minute - RANGES[range]}` : null,
    15000,
  );
  const rows = stats.data?.executors ?? [];
  const columns: [string, (r: StatsRow) => string, string?][] = [
    ["tasks", (r) => count(r.tasks)],
    ["failed", (r) => failures(r.failed, r.tasks)],
    ["p50", (r) => seconds(r.p50), "Median duration of succeeded tasks"],
    ["p95", (r) => seconds(r.p95)],
    [
      "wait p50",
      (r) => seconds(r.wait_p50),
      "Ready to start, not counting pauses or engine outages",
    ],
    ["wait p95", (r) => seconds(r.wait_p95)],
    ["hours", (r) => hours(r.hours), "Wall-clock task hours"],
    ["cpu·h", (r) => hours(r.cpu_hours), "Requested cpus × hours"],
    ["GB·h", (r) => hours(r.gb_hours), "Requested memory × hours"],
    ["gpu·h", (r) => hours(r.gpu_hours)],
  ];
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-end justify-between gap-3">
        <div className="flex flex-col gap-0.5">
          <Eyebrow>Workload</Eyebrow>
          <p className="text-sm text-muted-foreground">
            Finished tasks per executor, from the run history.
          </p>
        </div>
        <Segmented
          ariaLabel="Workload range"
          value={range}
          onChange={setRange}
          options={(Object.keys(RANGES) as Range[]).map((r) => ({
            value: r,
            label: r,
          }))}
        />
      </div>
      {stats.data && !rows.length ? (
        <Empty title="No finished tasks">
          Nothing finished in the last {range}.
        </Empty>
      ) : (
        <div className="overflow-x-auto rounded-xl border bg-card">
          <table className="w-full text-xs" aria-label="Workload per executor">
            <thead>
              <tr className="border-b text-muted-foreground">
                <th className="px-3 py-2 text-left font-medium">executor</th>
                {columns.map(([label, , title]) => (
                  <th
                    key={label}
                    title={title}
                    className="px-3 py-2 text-right font-medium whitespace-nowrap"
                  >
                    {label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr
                  key={row.executor}
                  data-executor={row.executor}
                  className="border-b last:border-0"
                >
                  <td className="px-3 py-2 font-mono whitespace-nowrap">
                    {row.executor}
                  </td>
                  {columns.map(([label, cell]) => (
                    <td
                      key={label}
                      className={cn(
                        "px-3 py-2 text-right font-mono whitespace-nowrap tabular-nums",
                        label === "failed" &&
                          row.failed > 0 &&
                          "text-red-600 dark:text-red-400",
                      )}
                    >
                      {cell(row)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
