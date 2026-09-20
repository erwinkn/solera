import { Link, createFileRoute } from "@tanstack/react-router";
import { Bot, Cpu } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Empty } from "@/components/common";
import { useQuery } from "@/lib/api";
import { time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import type { EnvironmentInfo, PoolWorker } from "@/lib/types";

export const Route = createFileRoute("/executors")({
  component: ExecutorsPage,
});

function capacityLabel(meta: PoolWorker["meta"]) {
  const parts = [
    meta.cpu != null ? `${meta.cpu} cpu` : null,
    meta.memory != null ? `${meta.memory} memory` : null,
    meta.gpu != null ? `${meta.gpu} gpu` : null,
  ].filter(Boolean);
  return parts.length ? parts.join(" · ") : "unbounded";
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
  return (
    <section className="flex flex-col gap-4">
      <div>
        <div className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
          Execution
        </div>
        <h1 className="font-heading text-xl font-medium">Executors</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Environments declare where tasks run and how many run concurrently.
        </p>
      </div>
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {(environments.data?.environments ?? []).map((env) => (
          <Card key={env.key} data-environment={env.key}>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 font-mono text-sm">
                <Cpu className="size-4" />
                {env.key}
              </CardTitle>
              <CardDescription>
                kind {env.kind}
                {env.max_concurrent != null
                  ? ` · concurrency ${env.max_concurrent}`
                  : " · unbounded concurrency"}
              </CardDescription>
            </CardHeader>
            <CardContent className="flex flex-col gap-2 text-sm">
              <div className="flex justify-between">
                <span className="text-muted-foreground">In flight</span>
                <Badge
                  variant="outline"
                  className={
                    env.max_concurrent != null &&
                    env.in_flight >= env.max_concurrent
                      ? "text-sky-700"
                      : ""
                  }
                >
                  {env.in_flight}
                  {env.max_concurrent != null ? ` / ${env.max_concurrent}` : ""}
                </Badge>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
      <div>
        <h2 className="font-heading text-lg font-medium">Pool workers</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Workers that registered with{" "}
          <code className="text-xs">dorc worker pool &lt;name&gt;</code>.
        </p>
      </div>
      {!workers.data?.workers.length ? (
        <Empty title="No pool workers connected">
          Start one with `dorc worker pool ingest` to pick up pool-placed tasks.
        </Empty>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {workers.data.workers.map((worker) => (
            <Card key={worker.id} data-worker={worker.id}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 font-mono text-sm">
                  <Bot className="size-4" />
                  {worker.id.slice(0, 12)}
                </CardTitle>
                <CardDescription>
                  pools {worker.pools.join(", ")} · {capacityLabel(worker.meta)}
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-col gap-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Last heartbeat</span>
                  <span>{time(worker.seen_at)}</span>
                </div>
                {worker.task && (
                  <button
                    className="flex justify-between rounded-md bg-sky-50 px-2 py-1 text-left text-xs text-sky-800 hover:underline dark:bg-sky-950 dark:text-sky-200"
                    onClick={() =>
                      select({ kind: "run", id: worker.task!.split("/")[0] })
                    }
                  >
                    <span>Current task</span>
                    <span className="font-mono">
                      {worker.task.split("/").slice(1).join("/")}
                    </span>
                  </button>
                )}
              </CardContent>
            </Card>
          ))}
        </div>
      )}
      <Link to="/storage" className="text-sm text-primary hover:underline">
        Storage diagnostics →
      </Link>
    </section>
  );
}
