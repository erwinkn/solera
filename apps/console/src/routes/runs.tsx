import { Link, createFileRoute } from "@tanstack/react-router";
import { ArrowRight, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Empty, PageHeader, StatusBadge } from "@/components/common";
import { duration, time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import type { Run } from "@/lib/types";

export const Route = createFileRoute("/runs")({
  component: RunsPage,
});

function displayStatus(run: Run) {
  return run.paused && !["succeeded", "failed", "canceled"].includes(run.status)
    ? "paused"
    : run.status;
}

function selectionLabel(run: Run) {
  return Array.isArray(run.partitions)
    ? `${run.partitions.length} scope${run.partitions.length === 1 ? "" : "s"} · ${run.mode}`
    : `${run.partitions} · ${run.mode}`;
}

function RunsPage() {
  const { runs, select, refresh, diagnostics } = useWorkspace();
  if (!diagnostics) return null;
  const active = runs.filter((r) =>
    ["running", "queued"].includes(r.status),
  ).length;
  return (
    <section className="flex flex-col gap-4">
      <PageHeader
        eyebrow="Execution"
        title="Runs"
        description="Durable requests, their tasks, attempts, and materialization history."
        aside={
          <div className="flex items-center gap-4">
            {!!active && (
              <div className="flex flex-col items-end">
                <span className="text-xl font-semibold text-sky-600 tabular-nums dark:text-sky-400">
                  {active}
                </span>
                <span className="text-xs text-muted-foreground">active</span>
              </div>
            )}
            <Button variant="outline" size="sm" onClick={refresh}>
              <RefreshCw />
              Refresh
            </Button>
          </div>
        }
      />
      {!runs.length ? (
        <Empty
          title="No runs yet"
          action={
            <Button variant="outline" render={<Link to="/assets" />}>
              Browse assets
            </Button>
          }
        >
          Materialize an asset to create the first run.
        </Empty>
      ) : (
        <div className="overflow-x-auto rounded-xl border bg-card">
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead>Run</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Targets</TableHead>
                <TableHead>Selection</TableHead>
                <TableHead>Requested</TableHead>
                <TableHead className="text-right">Duration</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.map((run) => (
                <TableRow key={run.id} data-run={run.id}>
                  <TableCell>
                    <button
                      className="flex items-center gap-1 font-mono text-xs font-medium text-primary hover:underline"
                      onClick={() => select({ kind: "run", id: run.id })}
                    >
                      {run.id.slice(0, 8)}
                      <ArrowRight className="size-3" />
                    </button>
                    <span className="block text-xs text-muted-foreground">
                      {run.automation ?? "manual"}
                    </span>
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={displayStatus(run)} />
                  </TableCell>
                  <TableCell>
                    <span
                      className="block max-w-48 truncate font-mono text-xs"
                      title={run.targets.join(", ")}
                    >
                      {run.targets.length === 1
                        ? run.targets[0]
                        : `${run.targets.length} assets`}
                    </span>
                    <span className="block text-xs text-muted-foreground tabular-nums">
                      {run.tasks.length} task{run.tasks.length === 1 ? "" : "s"}
                    </span>
                  </TableCell>
                  <TableCell className="text-sm text-muted-foreground">
                    {selectionLabel(run)}
                    {run.upstream && (
                      <span className="ml-1 rounded bg-muted px-1 text-[0.65rem]">
                        +upstream
                      </span>
                    )}
                  </TableCell>
                  <TableCell className="text-sm text-muted-foreground">
                    {time(run.created_at)}
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs text-muted-foreground tabular-nums">
                    {duration(run.created_at, run.updated_at)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </section>
  );
}
