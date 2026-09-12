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
import { Empty, StatusBadge } from "@/components/common";
import { duration, time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";

export const Route = createFileRoute("/runs")({
  component: RunsPage,
});

function RunsPage() {
  const { state, select, refresh } = useWorkspace();
  if (!state) return null;
  const runs = state.runs;
  return (
    <section className="flex flex-col gap-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
            Execution
          </div>
          <h1 className="font-heading text-xl font-medium">Runs</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Durable requests, attempts, and materialization history.
          </p>
        </div>
        <Button variant="outline" onClick={refresh}>
          <RefreshCw />
          Refresh
        </Button>
      </div>
      {!runs.length ? (
        <Empty
          title="No materializations yet"
          action={
            <Button variant="outline" render={<Link to="/assets" />}>
              Browse assets
            </Button>
          }
        >
          Start with sample_quality to exercise incremental, multi-output
          processing.
        </Empty>
      ) : (
        <div className="overflow-x-auto rounded-xl border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Run</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Selection</TableHead>
                <TableHead>Requested</TableHead>
                <TableHead>Duration</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.map((run) => (
                <TableRow key={run.id}>
                  <TableCell>
                    <button
                      className="flex items-center gap-1 font-mono text-xs font-medium text-primary hover:underline"
                      onClick={() => select({ kind: "run", id: run.id })}
                    >
                      {run.id.slice(0, 8)}
                      <ArrowRight className="size-3" />
                    </button>
                    <span className="block text-xs text-muted-foreground">
                      {run.cause.replaceAll("_", " ")}
                    </span>
                  </TableCell>
                  <TableCell>
                    <StatusBadge
                      status={
                        run.paused &&
                        !["succeeded", "failed", "canceled"].includes(
                          run.status,
                        )
                          ? "paused"
                          : run.status
                      }
                    />
                  </TableCell>
                  <TableCell>
                    <span
                      className="block max-w-48 truncate"
                      title={run.targets.join(", ")}
                    >
                      {run.targets.length === 1
                        ? run.targets[0]
                        : `${run.targets.length} assets`}
                    </span>
                    <span className="block text-xs text-muted-foreground">
                      {run.partitions.length
                        ? `${run.partitions.length} partition${run.partitions.length === 1 ? "" : "s"}`
                        : run.mode.replace("_", " ")}
                    </span>
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {time(run.created_at)}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {duration(run.created_at, run.updated_at)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
      <div className="text-xs text-muted-foreground">
        Showing latest {runs.length} runs
      </div>
    </section>
  );
}
