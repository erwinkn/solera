import { useState } from "react";
import { Ban, Pause, Play, RotateCcw } from "lucide-react";
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
    <pre className="max-h-72 overflow-auto rounded-lg border bg-muted/40 p-3 font-mono text-xs">
      {data == null ? "…" : JSON.stringify(data, null, 2)}
    </pre>
  );
}

interface LogLine {
  at?: number;
  message?: string;
  fields?: Record<string, unknown>;
}

function logTime(at?: number) {
  if (!at) return "";
  const d = new Date(at * 1000);
  const pad = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

function LogConsole({ path }: { path: string }) {
  // The endpoint returns the log's last lines as JSON records; poll it for a tail.
  const { data } = useQueryText(path, 1000);
  const lines = (data ?? "")
    .split("\n")
    .map((raw) => raw.trim())
    .filter(Boolean)
    .map((raw): LogLine | { raw: string } => {
      try {
        return JSON.parse(raw) as LogLine;
      } catch {
        return { raw };
      }
    });
  return (
    <pre
      aria-label="Attempt logs"
      className="max-h-72 min-h-16 overflow-auto rounded-lg bg-zinc-950 p-3 font-mono text-[0.7rem] leading-relaxed text-zinc-100"
    >
      {!lines.length && (
        <span className="text-zinc-500">No log lines yet.</span>
      )}
      {lines.map((line, i) =>
        "raw" in line ? (
          <div key={i} className="text-zinc-300">
            {line.raw}
          </div>
        ) : (
          <div key={i} className="flex gap-2">
            <span className="shrink-0 text-zinc-500">{logTime(line.at)}</span>
            <span className="text-zinc-100">{line.message}</span>
            {line.fields && Object.keys(line.fields).length > 0 && (
              <span className="text-sky-400">
                {Object.entries(line.fields)
                  .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
                  .join(" ")}
              </span>
            )}
          </div>
        ),
      )}
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
      <summary className="flex cursor-pointer list-none items-center gap-2 px-3 py-2 text-sm">
        <StatusBadge status={attempt.status} />
        <span className="font-mono text-xs text-muted-foreground">
          attempt {attempt.generation}
        </span>
        <span className="ml-auto text-xs text-muted-foreground tabular-nums">
          {time(attempt.started_at)} ·{" "}
          {duration(attempt.started_at, attempt.finished_at)}
        </span>
      </summary>
      <div className="flex flex-col gap-3 border-t p-3">
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
            <LogConsole path={`${path}/logs?tail=500`} />
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
  const tasks = detail.data?.tasks ?? [];

  async function act(path: string, onSuccess?: () => void) {
    await action.run(async () => {
      await request(`${base}/runs/${run!.id}/${path}`, { body: {} });
      detail.refresh();
      refresh();
      onSuccess?.();
    });
  }

  return (
    <Sheet open={!!runId} onOpenChange={(open) => !open && select(null)}>
      <SheetContent className="w-full gap-0 overflow-y-auto p-0 sm:max-w-3xl">
        <SheetHeader className="border-b p-5 pr-14">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <SheetTitle className="font-mono text-sm">
                run {runId?.slice(0, 12)}
              </SheetTitle>
              <SheetDescription className="text-xs">
                {run?.source ? (
                  `${run.source} · source commit · ${run.by ?? "api"}`
                ) : run ? (
                  <>
                    {run.targets.join(", ")} · {run.mode} ·{" "}
                    {Array.isArray(run.partitions)
                      ? `${run.partitions.length} scope${run.partitions.length === 1 ? "" : "s"}`
                      : run.partitions}
                    {run.automation ? ` · ${run.automation}` : " · manual"}
                  </>
                ) : (
                  "…"
                )}
              </SheetDescription>
            </div>
            {run && (
              <StatusBadge
                status={run.paused && live ? "paused" : run.status}
              />
            )}
          </div>
        </SheetHeader>
        <div className="flex flex-col gap-4 p-5">
          {action.error && <ErrorNotice message={action.error} />}
          {run && (
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-xs text-muted-foreground tabular-nums">
                {tasks.length} task{tasks.length === 1 ? "" : "s"}
              </span>
              {Object.entries(run.tags ?? {}).map(([k, v]) => (
                <span
                  key={k}
                  className="rounded border px-1.5 font-mono text-[0.7rem] text-muted-foreground"
                  data-tag={k}
                >
                  {k}={v}
                </span>
              ))}
              <span className="ml-auto flex gap-1.5">
                {live && (
                  <>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => act(run.paused ? "resume" : "pause")}
                    >
                      {run.paused ? <Play /> : <Pause />}
                      {run.paused ? "Resume" : "Pause"}
                    </Button>
                    {confirmCancel ? (
                      <Button
                        variant="destructive"
                        size="sm"
                        onClick={() =>
                          act("cancel", () => setConfirmCancel(false))
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
                    onClick={() => act("retry")}
                  >
                    <RotateCcw /> Retry failed
                  </Button>
                )}
              </span>
            </div>
          )}
          {tasks.map((task) => (
            <section key={task.id} className="flex flex-col gap-2">
              <div className="flex flex-wrap items-center gap-2">
                <span className="font-mono text-sm font-medium">
                  {task.asset}
                </span>
                {task.scope && (
                  <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs text-muted-foreground">
                    {task.scope}
                  </span>
                )}
                <StatusBadge status={task.status} />
                {task.error && (
                  <span className="text-xs text-red-600 dark:text-red-400">
                    {task.error}
                  </span>
                )}
              </div>
              <div className="flex flex-col gap-2 border-l pl-3">
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
          {detail.data && !tasks.length && (
            <Empty title="No tasks">The request produced no work.</Empty>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
