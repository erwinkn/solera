import { useState } from "react";
import { Ban, Pause, Play, RotateCcw } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Empty, ErrorNotice, StatusBadge } from "@/components/common";
import { request, useAction, useQuery, useQueryText } from "@/lib/api";
import { duration, time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import type { Attempt, RunDetail, Task } from "@/lib/types";

function JsonView({ path }: { path: string }) {
  const { data, error } = useQuery<unknown>(path, 30000);
  if (error)
    return <ErrorNotice message={`${error.message} (not committed yet?)`} />;
  return (
    <pre className="max-h-64 overflow-auto rounded-lg bg-muted p-3 font-mono text-xs">
      {data == null ? "…" : JSON.stringify(data, null, 2)}
    </pre>
  );
}

function LogView({ path }: { path: string }) {
  // The endpoint returns the log's last lines as JSON lines; poll it for a tail.
  const { data } = useQueryText(path, 1000);
  return (
    <pre
      aria-label="Attempt logs"
      className="max-h-64 min-h-16 overflow-auto rounded-lg bg-zinc-950 p-3 font-mono text-xs text-zinc-100"
    >
      {data ? data : "No log lines yet."}
    </pre>
  );
}

function AttemptRow({
  base,
  task,
  attempt,
}: {
  base: string;
  task: Task;
  attempt: Attempt;
}) {
  const attemptId = attempt.id ?? `${task.id}/${attempt.generation}`;
  const path = `${base}/runs/${encodeURIComponent(task.run)}/attempts/${encodeURIComponent(attemptId)}`;
  return (
    <details className="rounded-lg border" data-attempt={attemptId}>
      <summary className="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm">
        <StatusBadge status={attempt.status} />
        <span className="font-mono text-xs">attempt {attempt.generation}</span>
        <span className="ml-auto text-xs text-muted-foreground">
          {time(attempt.started_at)} ·{" "}
          {duration(attempt.started_at, attempt.finished_at)}
        </span>
      </summary>
      <div className="flex flex-col gap-3 border-t px-3 py-3">
        {attempt.error && (
          <ErrorNotice
            message={`${attempt.error.type}: ${attempt.error.message}`}
          />
        )}
        <Tabs defaultValue="logs">
          <TabsList>
            <TabsTrigger value="logs">Logs</TabsTrigger>
            <TabsTrigger value="spec">Spec</TabsTrigger>
            <TabsTrigger value="result">Result</TabsTrigger>
          </TabsList>
          <TabsContent value="logs">
            <LogView path={`${path}/logs?tail=500`} />
          </TabsContent>
          <TabsContent value="spec">
            <JsonView path={`${path}/spec`} />
          </TabsContent>
          <TabsContent value="result">
            <JsonView path={`${path}/result`} />
          </TabsContent>
        </Tabs>
      </div>
    </details>
  );
}

export function RunSheet() {
  const { selection, select, base, refresh } = useWorkspace();
  const action = useAction();
  const [confirmCancel, setConfirmCancel] = useState(false);
  const runId = selection?.kind === "run" ? selection.id : null;
  const detail = useQuery<RunDetail>(
    base && runId ? `${base}/runs/${runId}` : null,
    1500,
  );
  const run = detail.data?.request;
  const live = run && !["succeeded", "failed", "canceled"].includes(run.status);
  return (
    <Sheet open={!!runId} onOpenChange={(open) => !open && select(null)}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-3xl">
        <SheetHeader>
          <SheetTitle className="font-mono text-sm">
            run {runId?.slice(0, 12)}
          </SheetTitle>
          <SheetDescription>
            {run ? (
              <>
                {run.targets.join(", ")} · {run.mode} ·{" "}
                {Array.isArray(run.partitions)
                  ? `${run.partitions.length} scope${run.partitions.length === 1 ? "" : "s"}`
                  : run.partitions}
                {run.automation ? ` · ${run.automation}` : ""}
              </>
            ) : (
              "…"
            )}
          </SheetDescription>
        </SheetHeader>
        <div className="flex flex-col gap-4 px-4 pb-8">
          {action.error && <ErrorNotice message={action.error} />}
          {run && (
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge
                status={run.paused && live ? "paused" : run.status}
              />
              <span className="text-xs text-muted-foreground">
                {run.tasks.length} task{run.tasks.length === 1 ? "" : "s"}
              </span>
              <span className="ml-auto flex gap-1.5">
                {live && (
                  <>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() =>
                        action.run(async () => {
                          await request(
                            `${base}/runs/${run.id}/${run.paused ? "resume" : "pause"}`,
                            { body: {} },
                          );
                          detail.refresh();
                          refresh();
                        })
                      }
                    >
                      {run.paused ? <Play /> : <Pause />}
                      {run.paused ? "Resume" : "Pause"}
                    </Button>
                    {confirmCancel ? (
                      <Button
                        variant="destructive"
                        size="sm"
                        onClick={() =>
                          action.run(async () => {
                            await request(`${base}/runs/${run.id}/cancel`, {
                              body: {},
                            });
                            setConfirmCancel(false);
                            detail.refresh();
                            refresh();
                          })
                        }
                      >
                        <Ban /> Confirm cancel
                      </Button>
                    ) : (
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => setConfirmCancel(true)}
                      >
                        <Ban /> Cancel
                      </Button>
                    )}
                  </>
                )}
                {!live && run.status === "failed" && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      action.run(async () => {
                        await request(`${base}/runs/${run.id}/retry`, {
                          body: {},
                        });
                        detail.refresh();
                        refresh();
                      })
                    }
                  >
                    <RotateCcw /> Retry failed
                  </Button>
                )}
              </span>
            </div>
          )}
          {(detail.data?.tasks ?? []).map((task) => (
            <section key={task.id} className="flex flex-col gap-2">
              <div className="flex items-center gap-2">
                <span className="font-mono text-sm font-medium">
                  {task.asset}
                </span>
                {task.scope && (
                  <Badge variant="outline" className="font-mono text-xs">
                    {task.scope}
                  </Badge>
                )}
                <StatusBadge status={task.status} />
                {task.error && (
                  <span className="text-xs text-red-700">{task.error}</span>
                )}
              </div>
              <div className="flex flex-col gap-2 pl-4">
                {(detail.data?.attempts[task.id] ?? []).map((attempt) => (
                  <AttemptRow
                    key={attempt.generation}
                    base={base!}
                    task={task}
                    attempt={attempt}
                  />
                ))}
                {!(detail.data?.attempts[task.id] ?? []).length &&
                  task.status !== "succeeded" && (
                    <p className="text-xs text-muted-foreground">
                      {task.status === "waiting"
                        ? "Waiting on upstream tasks."
                        : "No attempts yet."}
                    </p>
                  )}
              </div>
            </section>
          ))}
          {detail.data && !detail.data.tasks.length && (
            <Empty title="No tasks">The request produced no work.</Empty>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
