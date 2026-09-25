import { createFileRoute } from "@tanstack/react-router";
import { Bot, Cpu, Layers, MonitorSmartphone } from "lucide-react";
import { Empty, Eyebrow, PageHeader } from "@/components/common";
import { useQuery } from "@/lib/api";
import { time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type { EnvironmentInfo, PoolWorker } from "@/lib/types";

export const Route = createFileRoute("/executors")({
  component: ExecutorsPage,
});

function bytes(value: number | null | undefined) {
  if (value == null) return null;
  const gb = value / 1024 ** 3;
  return gb >= 1
    ? `${gb.toFixed(gb < 10 ? 1 : 0)} GB`
    : `${Math.round(value / 1024 ** 2)} MB`;
}

function capacityLabel(meta: PoolWorker["meta"]) {
  const parts = [
    meta.cpu != null ? `${meta.cpu} cpu` : null,
    bytes(meta.memory),
    meta.gpu != null ? `${meta.gpu} gpu` : null,
  ].filter(Boolean);
  return parts.length ? parts.join(" · ") : "unbounded";
}

function envIcon(kind: string) {
  if (kind === "Pool") return Layers;
  if (kind === "Local") return MonitorSmartphone;
  return Cpu;
}

// The API keys environments by a serialized spec; show the human form instead.
function envLabel(env: EnvironmentInfo) {
  const environment = env.environment as Record<string, unknown>;
  const detail =
    (environment?.name as string) ??
    (environment?.cluster as string) ??
    (environment?.app as string);
  return detail ? `${env.kind}(${detail})` : env.kind;
}

function ExecutorsPage() {
  const { diagnostics, base, select } = useWorkspace();
  const environments = useQuery<{ environments: EnvironmentInfo[] }>(
    base ? `${base}/environments` : null,
    3000,
  );
  const workers = useQuery<{ workers: PoolWorker[] }>(
    base ? `${base}/workers` : null,
    3000,
  );
  if (!diagnostics) return null;
  const envs = environments.data?.environments ?? [];
  const pool = workers.data?.workers ?? [];
  return (
    <section className="flex flex-col gap-5">
      <PageHeader
        eyebrow="Execution"
        title="Executors"
        description="Environments declare where tasks run and how many run concurrently."
      />
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {envs.map((env) => {
          const Icon = envIcon(env.kind);
          const saturated =
            env.max_concurrent != null && env.in_flight >= env.max_concurrent;
          return (
            <div
              key={env.key}
              data-environment={env.key}
              className="flex flex-col gap-3 rounded-xl border bg-card p-4"
            >
              <div className="flex items-center gap-2">
                <Icon className="size-4 text-muted-foreground" />
                <span className="font-mono text-sm font-semibold">
                  {envLabel(env)}
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
