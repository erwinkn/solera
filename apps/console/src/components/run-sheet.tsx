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
import { cn } from "cn";
import { bytes, count, duration, resources, seconds, time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import type { Attempt, RunDetail, RunEvent, Task } from "@/lib/types";

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

// An attempt's phases, in order, each from one milestone to the next.
const PHASES = [
  ["preparing", "bg-zinc-400"],
  ["provisioning", "bg-amber-400"],
  ["importing", "bg-violet-400"],
  ["loading", "bg-sky-400"],
  ["computing", "bg-emerald-500"],
  ["writing", "bg-indigo-400"],
  ["settling", "bg-zinc-300 dark:bg-zinc-600"],
] as const;

function Phases({ attempt }: { attempt: Attempt }) {
  const spent = PHASES.flatMap(([phase, fill]) => {
    const value = attempt[phase];
    return value ? [{ phase, fill, value }] : [];
  });
  const total = spent.reduce((sum, p) => sum + p.value, 0);
  const usage = [
    attempt.cpu_seconds != null && `${seconds(attempt.cpu_seconds)} cpu`,
    attempt.peak_memory != null && `${bytes(attempt.peak_memory)} peak`,
  ].filter(Boolean);
  if (!total && !usage.length) return null;
  return (
    <div className="flex flex-col gap-1.5" data-phases>
      {total > 0 && (
        <div className="flex h-1.5 gap-px overflow-hidden rounded-full">
          {spent.map(({ phase, fill, value }) => (
            <div
              key={phase}
              className={fill}
              style={{ width: `${(value / total) * 100}%` }}
              title={`${phase} ${seconds(value)}`}
            />
          ))}
        </div>
      )}
      <div className="flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-muted-foreground tabular-nums">
        {spent.map(({ phase, fill, value }) => (
          <span key={phase} className="flex items-center gap-1">
            <span className={cn("size-1.5 rounded-full", fill)} />
            {phase} {seconds(value)}
          </span>
        ))}
        {usage.length > 0 && (
          <span className="ml-auto">{usage.join(" · ")}</span>
        )}
      </div>
    </div>
  );
}

const HELD: Record<string, string> = {
  lock: "an output it writes is locked by another attempt",
  engine: "the engine is at its concurrency",
};

/** What an event says beyond its type. */
function eventDetail(e: RunEvent) {
  const parts: string[] = [];
  if (e.type === "held")
    parts.push(HELD[e.reason ?? ""] ?? `executor ${e.name} is full`);
  else if (e.type === "outage" && e.until != null)
    parts.push(`the engine was down ${seconds(e.until - e.at)}`);
  else if (e.type === "retry_scheduled" && e.until != null)
    parts.push(`in ${seconds(e.until - e.at)}`);
  else {
    if (e.name) parts.push(e.name);
    if (e.rows != null) parts.push(`${count(e.rows)} rows`);
    if (e.reason) parts.push(e.reason);
  }
  if (e.by !== "engine" && e.by !== "worker") parts.push(`by ${e.by}`);
  return parts.join(" · ");
}

const EVENT_TONE: Record<string, string> = {
  succeeded: "text-emerald-700 dark:text-emerald-300",
  committed: "text-emerald-700 dark:text-emerald-300",
  failed: "text-red-700 dark:text-red-300",
  lost: "text-red-700 dark:text-red-300",
  blocked: "text-red-700 dark:text-red-300",
  aborted: "text-amber-700 dark:text-amber-300",
  held: "text-amber-700 dark:text-amber-300",
  outage: "text-amber-700 dark:text-amber-300",
  paused: "text-amber-700 dark:text-amber-300",
};

function Timeline({
  path,
  live,
  tasks,
  attempts,
}: {
  path: string;
  live: boolean;
  tasks: Task[];
  attempts: Record<string, Attempt[]>;
}) {
  const { data, error } = useQuery<RunEvent[]>(path, live ? 1500 : 60000);
  if (error) return <ErrorNotice message={error.message} />;
  if (!data) return <p className="text-xs text-muted-foreground">…</p>;
  const byId = new Map(tasks.map((t) => [t.id, t]));
  const generation = new Map(
    Object.values(attempts)
      .flat()
      .map((a) => [a.id, a.generation]),
  );
  const start = data[0]?.at ?? 0;
  return (
    // One line per event on wide screens, in columns; wrapped on narrow ones.
    <ol className="text-xs" aria-label="Run timeline">
      {data.map((e) => {
        const task = e.task ? byId.get(e.task) : undefined;
        const subject =
          (task
            ? `${task.asset}${task.scope ? ` ${task.scope}` : ""}`
            : (e.task ?? "run")) +
          (e.attempt ? ` #${generation.get(e.attempt) ?? "?"}` : "");
        return (
          <li
            key={e.n}
            className="grid grid-cols-[4rem_1fr] gap-x-3 border-b py-1 last:border-0"
            data-event={e.type}
          >
            <span
              className="text-right whitespace-nowrap text-muted-foreground tabular-nums"
              title={time(e.at)}
            >
              +{seconds(e.at - start)}
            </span>
            <span className="flex min-w-0 flex-wrap gap-x-3 sm:grid sm:grid-cols-[11rem_6.5rem_1fr]">
              <span
                className={cn(
                  "truncate font-mono",
                  e.attempt && "pl-3 text-muted-foreground",
                  !e.task && "font-medium",
                )}
                title={subject}
              >
                {subject}
              </span>
              <span className={cn("whitespace-nowrap", EVENT_TONE[e.type])}>
                {e.type.replace("_", " ")}
              </span>
              <span className="min-w-0 text-muted-foreground">
                {eventDetail(e)}
              </span>
            </span>
          </li>
        );
      })}
    </ol>
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
        {attempt.executor && (
          <div className="text-xs text-muted-foreground" data-execution>
            on{" "}
            <span className="font-mono text-foreground">
              {attempt.executor}
            </span>
            {[
              resources(attempt),
              ...Object.entries(attempt.options ?? {}).map(
                ([k, v]) => `${k} ${v}`,
              ),
            ]
              .filter(Boolean)
              .map((part) => ` · ${part}`)
              .join("")}
          </div>
        )}
        <Phases attempt={attempt} />
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
          {detail.data && tasks.length > 0 && (
            <Tabs defaultValue="tasks">
              <TabsList>
                <TabsTrigger value="tasks">Tasks</TabsTrigger>
                <TabsTrigger value="timeline">Timeline</TabsTrigger>
              </TabsList>
              <TabsContent value="tasks" className="flex flex-col gap-4 pt-2">
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
              </TabsContent>
              <TabsContent value="timeline" className="pt-2">
                <Timeline
                  path={`${base}/runs/${runId}/events`}
                  live={!!live}
                  tasks={tasks}
                  attempts={detail.data.attempts}
                />
              </TabsContent>
            </Tabs>
          )}
          {detail.data && !tasks.length && (
            <Empty title="No tasks">The request produced no work.</Empty>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
