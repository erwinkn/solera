import type { ReactNode } from "react";
import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { Ban, CirclePause, CirclePlay, RotateCcw, Trash2 } from "lucide-react";
import { ACTIVE_ATTEMPT, ACTIVE_RUN, q, runIsLive, useManifest, useProject } from "@/api/queries";
import { useDeleteRun, useRetryRun, useRunAction } from "@/api/mutations";
import type { Attempt, AttemptError, Json, RunDetail, RunEvent, Task } from "@/api/types";
import { CLEANUP, PartitionsLabel, runTitle, TriggerLabel } from "@/features/runs";
import {
  BatchRange,
  batchLabel,
  describeProgress,
  KeyClasses,
  ProgressBar,
  walkOf,
  type Walk,
} from "@/features/batches";
import { Logs, type LogLevel } from "@/features/logs";
import { workerOf } from "@/features/worker";
import { PHASE_LABEL, PhaseBar, PhaseLegend, phaseColor, phasesOf, Waterfall } from "@/features/timeline";
import { useNow } from "@/lib/clock";
import { cn } from "@/lib/cn";
import { bytes, duration, firstLine, plural, shortId } from "@/lib/format";
import { label, tone, toneSoft } from "@/lib/status";
import { Button } from "@/ui/button";
import { rove } from "@/ui/form";
import { CopyButton, Elapsed, Empty, ErrorNote, Id, JsonView, Skeleton, Time } from "@/ui/data";
import { Card, CardHeader, Crumb, Fact, Facts, Meta, Page, PageHeader } from "@/ui/layout";
import { Confirm, Tooltip } from "@/ui/overlay";
import { StatusBadge, StatusIcon } from "@/ui/status";

const route = getRouteApi("/runs/$run");

/** Which task to show when the URL names none: what failed, else what runs, else the first. */
function defaultTask(tasks: Task[]): Task | undefined {
  return (
    tasks.find((t) => tone(t.status) === "fail") ??
    tasks.find((t) => ACTIVE_RUN.has(t.status) || t.status === "running") ??
    tasks[0]
  );
}

export function Run() {
  const { run: id } = route.useParams();
  const search = route.useSearch();
  const project = useProject();
  const { data } = useSuspenseQuery(q.run(project, id));
  const { request, tasks, attempts } = data;
  const live = runIsLive(data);
  const events = useQuery(q.runEvents(project, id, live)).data;
  const end = live ? null : (request.finished_at ?? request.updated_at);

  // A link may name only an attempt (from a key outcome, a hold): its task owns it.
  const owner = search.attempt
    ? tasks.find((t) => attempts[t.id]?.some((a) => a.id === search.attempt))
    : undefined;
  const task = tasks.find((t) => t.id === search.task) ?? owner ?? defaultTask(tasks);
  const taskAttempts = task ? (attempts[task.id] ?? []) : [];
  const attempt = taskAttempts.find((a) => a.id === search.attempt) ?? taskAttempts[taskAttempts.length - 1];
  const failed = tasks.filter((t) => tone(t.status) === "fail");

  return (
    <Page>
      <PageHeader
        ident
        eyebrow={
          <>
            <Crumb>
              <Link to="/runs" className="hover:text-fg">
                Runs
              </Link>
            </Crumb>
            <Crumb last>
              <span className="font-mono">{shortId(id)}</span>
            </Crumb>
          </>
        }
        title={runTitle({ ...request, origin: request.source ? "commit" : undefined })}
        badge={<StatusBadge status={request.paused && live ? "paused" : request.status} />}
        actions={<RunActions detail={data} />}
        meta={
          <>
            <Meta label="Run">
              <span className="inline-flex items-center gap-1 font-mono">
                {id}
                <CopyButton value={id} label="Copy run id" />
              </span>
            </Meta>
            <Meta label="Trigger">
              <TriggerLabel
                run={{
                  origin: request.source
                    ? "commit"
                    : request.tags?.cleanup
                      ? "cleanup"
                      : request.automation
                        ? "automation"
                        : "manual",
                  ...request,
                  automation: request.automation ?? null,
                  by: request.by ?? null,
                  source: request.source ?? null,
                }}
              />
            </Meta>
            {request.retry_of && (
              <Meta label="Retries">
                <Link
                  to="/runs/$run"
                  params={{ run: request.retry_of }}
                  className="font-mono text-link hover:underline"
                >
                  {shortId(request.retry_of)}
                </Link>
              </Meta>
            )}
            <Meta label="Partitions">
              <PartitionsLabel partitions={request.partitions} />
            </Meta>
            <Meta label="Mode">
              {request.mode}
              {request.upstream && " · with upstream"}
            </Meta>
            {live && tasks.length > 1 && (
              <Meta label="Tasks">
                {tasks.filter((t) => !ACTIVE_RUN.has(t.status) && t.status !== "running").length} of{" "}
                {tasks.length} done
              </Meta>
            )}
            <Meta label="Started">
              <Time at={request.created_at} />
            </Meta>
            <Meta label="Took">
              <Elapsed start={request.created_at} end={end} />
            </Meta>
            {Object.entries(request.tags ?? {}).map(([k, v]) => (
              <Meta key={k} label={k}>
                {v}
              </Meta>
            ))}
          </>
        }
      />

      {request.status === "failed" && failed.length > 0 && (
        <div
          role="alert"
          className="flex items-start gap-2.5 rounded-md border-theme border-fail bg-fail-soft px-4 py-3 text-sm text-fail-fg"
        >
          <StatusIcon tone="fail" className="mt-0.5 size-4" />
          <div className="min-w-0">
            <p className="font-medium">
              {plural(failed.length, "task")} failed
              {failed.length === 1
                ? `: ${failed[0]!.asset}${failed[0]!.partition ? ` · ${failed[0]!.partition}` : ""}`
                : ""}
            </p>
            {failed[0]?.error && <p className="mt-0.5 truncate opacity-90">{firstLine(failed[0].error)}</p>}
          </div>
        </div>
      )}

      {tasks.length === 0 ? (
        <Card>
          <Empty title={request.source ? "A source commit" : "No tasks"}>
            {request.source
              ? `This run records a commit to ${request.source}; it planned no tasks.`
              : "This run planned nothing: every target partition was already current or running."}
          </Empty>
        </Card>
      ) : (
        <Card>
          <CardHeader
            title="Timeline"
            description={`${plural(tasks.length, "task")} · bars are batches (one for most tasks) and their retries, split into phases; a dashed line is time waiting to run`}
            actions={<PhaseLegend className="hidden md:flex" />}
          />
          <Waterfall
            events={events}
            run={id}
            tasks={tasks}
            attempts={attempts}
            start={request.created_at}
            end={end}
            selected={{ task: task?.id, attempt: attempt?.id }}
          />
        </Card>
      )}

      {task && <TaskPanel run={id} task={task} attempts={taskAttempts} attempt={attempt} live={live} />}
    </Page>
  );
}

function RunActions({ detail }: { detail: RunDetail }) {
  const { request } = detail;
  const act = useRunAction(request.id);
  const retry = useRetryRun(request.id);
  const remove = useDeleteRun();
  const live = ACTIVE_RUN.has(request.status);
  const retryable = request.status === "failed" || request.status === "canceled";
  return (
    <>
      {live &&
        (request.paused ? (
          <Button icon={<CirclePlay />} onClick={() => act.mutate("resume")} disabled={act.isPending}>
            Resume
          </Button>
        ) : (
          <Button icon={<CirclePause />} onClick={() => act.mutate("pause")} disabled={act.isPending}>
            Pause
          </Button>
        ))}
      {live && (
        <Confirm
          trigger={
            <Button variant="danger" icon={<Ban />}>
              Cancel run
            </Button>
          }
          title="Cancel this run?"
          description="Attempts stop starting work and drain: finished keys are written and committed, the rest are recorded as canceled. Nothing resumes by itself."
          action="Cancel run"
          danger
          onConfirm={() => act.mutate("cancel")}
        />
      )}
      {retryable && (
        <Button
          title="Runs this run's failed, canceled and blocked work again, as a new run"
          variant="primary"
          icon={<RotateCcw />}
          onClick={() => retry.mutate()}
          disabled={retry.isPending}
        >
          Retry
        </Button>
      )}
      {!runIsLive(detail) && (
        <Confirm
          trigger={
            <Button variant="ghost" icon={<Trash2 />}>
              Delete
            </Button>
          }
          title="Delete this run?"
          description="Its attempt files, logs and history rows go. Heads, key indexes and cursors stay: current state never depends on runs."
          action="Delete run"
          danger
          onConfirm={() => remove.mutate(request.id)}
        />
      )}
    </>
  );
}

// -- the selected task --------------------------------------------------------------

function TaskPanel({
  run,
  task,
  attempts,
  attempt,
  live,
}: {
  run: string;
  task: Task;
  attempts: Attempt[];
  attempt?: Attempt;
  live: boolean;
}) {
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const tab = search.tab ?? "logs";
  // The run is the picture (D173): batches show only for a walk of several, attempts only once retried.
  const walk = walkOf(attempts);
  const progress = walk.multi ? describeProgress(task.progress, walk.planned) : null;
  const tabs: { id: typeof tab; label: string }[] = [
    { id: "logs", label: "Logs" },
    { id: "result", label: "Result" },
    { id: "spec", label: "Spec" },
    { id: "events", label: "Events" },
  ];
  return (
    <Card>
      <CardHeader
        ident
        title={
          <span className="flex flex-wrap items-center gap-2">
            {task.asset === CLEANUP ? (
              <span>cleanup</span>
            ) : (
              <Link
                to="/assets/$asset"
                params={{ asset: task.asset }}
                search={{ partition: task.partition || undefined }}
                className="hover:underline"
              >
                {task.asset}
              </Link>
            )}
            {task.partition && <span className="font-mono text-sm text-fg-muted">{task.partition}</span>}
            <StatusBadge status={task.status} className="font-sans" />
          </span>
        }
        description={
          <span className="inline-flex flex-wrap items-center gap-x-1.5">
            {walk.multi && <ProgressBar progress={task.progress} planned={walk.planned} />}
            {[
              progress,
              walk.retried && plural(attempts.length, "attempt"),
              task.wait != null && task.wait >= 1 && `waited ${duration(task.wait)}`,
              task.held && `held: ${task.held[0]}${task.held[1] ? ` (${task.held[1]})` : ""}`,
            ]
              .filter(Boolean)
              .join(" · ")}
          </span>
        }
        actions={
          !walk.multi &&
          walk.retried && (
            <AttemptChips
              select={(a) => ({
                to: "/runs/$run",
                params: { run },
                search: (s) => ({ ...s, task: task.id, attempt: a.id }),
                replace: true,
              })}
              attempts={attempts}
              selected={attempt}
            />
          )
        }
      />
      {walk.multi && <Batches run={run} task={task} walk={walk} selected={attempt} />}
      {!attempt ? (
        <Empty compact title={task.status === "blocked" ? "Blocked" : "Not started yet"}>
          {task.status === "blocked"
            ? "An upstream task of this run failed, so this one never ran."
            : "No attempt has been claimed for this task."}
        </Empty>
      ) : (
        <>
          <AttemptSummary run={run} attempt={attempt} walk={walk} live={live} />
          <div
            role="tablist"
            aria-label="Attempt details"
            className="flex gap-1 border-b border-line px-4"
            onKeyDown={(e) => rove(e, "tab")}
          >
            {tabs.map((t) => (
              <button
                key={t.id}
                role="tab"
                type="button"
                aria-selected={tab === t.id}
                tabIndex={tab === t.id ? 0 : -1}
                onClick={() =>
                  navigate({
                    search: (s) => ({
                      ...s,
                      tab: t.id === "logs" ? undefined : t.id,
                    }),
                    replace: true,
                  })
                }
                className={cn(
                  "-mb-px border-b-2 px-2.5 py-2 text-sm motion-1 transition-colors",
                  tab === t.id
                    ? "border-fg font-medium text-fg"
                    : "border-transparent text-fg-muted hover:text-fg",
                )}
              >
                {t.label}
              </button>
            ))}
          </div>
          {tab === "logs" && (
            <Logs
              key={attempt.id}
              run={run}
              attempt={attempt}
              level={(search.level ?? "all") as LogLevel}
              text={search.lq ?? ""}
              onFilter={({ level, text }) =>
                navigate({
                  search: (s) => ({
                    ...s,
                    ...(level !== undefined && {
                      level: level === "all" ? undefined : level,
                    }),
                    ...(text !== undefined && { lq: text || undefined }),
                  }),
                  replace: true,
                })
              }
            />
          )}
          {tab === "result" && <ResultTab run={run} attempt={attempt} />}
          {tab === "spec" && <SpecTab run={run} attempt={attempt} />}
          {tab === "events" && <EventsTab run={run} attempt={attempt} live={live} />}
        </>
      )}
    </Card>
  );
}

function errorOf(error: Attempt["error"]): AttemptError | null {
  if (!error) return null;
  return typeof error === "string" ? { message: error } : error;
}

/**
 * A task's walk, batch by batch (docs/observed-set.md, "A run"): the keys
 * each batch covered, what it held per class, and its attempts — a retry is
 * another attempt of the same batch, so it sits on the same row.
 */
function Batches({ run, task, walk, selected }: { run: string; task: Task; walk: Walk; selected?: Attempt }) {
  const committed = task.progress ? task.progress.batch : -1;
  const final = task.progress?.key === null;
  const select = (a: Attempt) => ({
    to: "/runs/$run" as const,
    params: { run },
    search: (s: Record<string, unknown>) => ({ ...s, task: task.id, attempt: a.id }),
    replace: true,
  });
  return (
    <div className="max-h-72 overflow-auto border-t border-line">
      <table className="w-full text-sm whitespace-nowrap">
        <thead className="sticky top-0 z-10 bg-surface text-left text-2xs font-medium tracking-wide text-fg-subtle uppercase">
          <tr>
            <th className="py-1.5 pr-3 pl-4 font-medium">Batch</th>
            <th className="px-3 py-1.5 font-medium">Keys</th>
            <th className="px-3 py-1.5 font-medium">Changes</th>
            {walk.retried && <th className="py-1.5 pr-4 pl-3 font-medium">Attempts</th>}
          </tr>
        </thead>
        <tbody>
          {walk.groups.map(({ batch, attempts: tries }) => {
            const last = tries[tries.length - 1]!;
            const current = tries.some((a) => a.id === selected?.id);
            const done = batch != null && batch.index <= committed;
            return (
              <tr
                key={batch ? `b${batch.index}` : last.id}
                className={cn("border-t border-line", current && "bg-select")}
              >
                <td className="py-1.5 pr-3 pl-4 whitespace-nowrap">
                  <Link
                    {...select(last)}
                    aria-current={current || undefined}
                    className="inline-flex items-center gap-2 hover:underline"
                  >
                    <StatusIcon status={done ? "committed" : last.outcome} />
                    <span className="tabular">{batch ? batchLabel(batch, final) : label(last.outcome)}</span>
                  </Link>
                </td>
                <td className="max-w-80 px-3 py-1.5">{batch && <BatchRange batch={batch} />}</td>
                <td className="px-3 py-1.5">{batch && <KeyClasses {...batch} />}</td>
                {walk.retried && (
                  <td className="py-1.5 pr-4 pl-3">
                    {/* Attempts only where the batch was retried: one attempt says nothing. */}
                    {tries.length > 1 && (
                      <AttemptChips select={select} attempts={tries} selected={selected} />
                    )}
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** A retried batch's (or task's) attempts, each viewable: the failed tries too. */
function AttemptChips({
  select,
  attempts,
  selected,
}: {
  select: (a: Attempt) => {
    to: "/runs/$run";
    params: { run: string };
    search: (s: Record<string, unknown>) => Record<string, unknown>;
    replace: boolean;
  };
  attempts: Attempt[];
  selected?: Attempt;
}) {
  return (
    <span
      role="tablist"
      aria-label="Attempts"
      className="flex flex-wrap gap-1"
      onKeyDown={(e) => rove(e, "tab")}
    >
      {attempts.map((a, i) => (
        <Link
          key={a.id}
          {...select(a)}
          role="tab"
          aria-selected={a.id === selected?.id}
          tabIndex={a.id === selected?.id ? 0 : -1}
          title={`attempt ${i + 1} · ${label(a.outcome)}`}
          className={cn(
            "inline-flex h-6 items-center gap-1 rounded-sm px-1.5 text-xs font-medium",
            a.id === selected?.id ? "bg-fg text-fg-inverse" : "text-fg-muted hover:bg-accent-soft",
          )}
        >
          <StatusIcon status={a.outcome} className={a.id === selected?.id ? "text-current" : undefined} />
          attempt {i + 1}
        </Link>
      ))}
    </span>
  );
}

function AttemptSummary({
  run,
  attempt,
  walk,
  live,
}: {
  run: string;
  attempt: Attempt;
  walk: Walk;
  live: boolean;
}) {
  const now = useNow();
  const end =
    attempt.finished_at ?? (ACTIVE_ATTEMPT.has(attempt.outcome) ? now : (attempt.started_at ?? now));
  const error = errorOf(attempt.error);
  const phases = phasesOf(attempt);
  const worker = workerOf(attempt, useManifest());
  return (
    <div className="flex flex-col gap-5 border-y border-line px-4 py-4">
      <Facts>
        <Fact label="Outcome">
          <StatusBadge status={attempt.outcome} />
        </Fact>
        {walk.retried && (
          <Fact label="Attempt">
            <Id value={attempt.id} copy />
          </Fact>
        )}

        <Fact label="Executor">{attempt.executor ?? "—"}</Fact>
        {worker && (
          <Fact label="Worker">
            {worker.href ? (
              <a href={worker.href} target="_blank" rel="noreferrer" className="text-link hover:underline">
                {worker.label}
              </a>
            ) : (
              <span className="font-mono text-xs">{worker.label}</span>
            )}
          </Fact>
        )}
        <Fact label="Started">
          <Time at={attempt.started_at} />
        </Fact>
        <Fact label="Duration">
          <Elapsed
            start={attempt.started_at}
            end={attempt.finished_at ?? (ACTIVE_ATTEMPT.has(attempt.outcome) ? null : attempt.started_at)}
          />
        </Fact>
        {attempt.cpu_seconds != null && <Fact label="CPU">{duration(attempt.cpu_seconds)}</Fact>}
        {attempt.peak_memory != null && <Fact label="Peak memory">{bytes(attempt.peak_memory)}</Fact>}
        {(attempt.cpu != null || attempt.memory != null || attempt.gpu != null) && (
          <Fact label="Asked for">
            {[
              attempt.cpu != null && `${attempt.cpu} cpu`,
              attempt.memory != null && bytes(attempt.memory),
              attempt.gpu ? `${attempt.gpu} gpu` : null,
            ]
              .filter(Boolean)
              .join(" · ")}
          </Fact>
        )}
      </Facts>

      {phases.length > 0 && (
        <div className="flex flex-col gap-2">
          <div className="h-3">
            <PhaseBar attempt={attempt} end={end} />
          </div>
          <dl className="flex flex-wrap gap-x-5 gap-y-1 text-xs">
            {phases.map(({ phase, seconds }) => (
              <div key={phase} className="flex items-center gap-1.5">
                <span className="size-2 rounded-mark" style={phaseColor(phase)} />
                <dt className="text-fg-muted">{PHASE_LABEL[phase]}</dt>
                <dd className="text-fg tabular">{duration(seconds)}</dd>
              </div>
            ))}
          </dl>
        </div>
      )}

      {/* With several batches, the batch table above already says this for the selected row. */}
      {attempt.batch && !walk.multi && (
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs">
          <span className="text-fg-subtle">Keys</span>
          <KeyClasses {...attempt.batch} />
        </div>
      )}

      {/* Per-key calls say something only when one didn't end ok (D173). */}
      {attempt.keys &&
        Object.entries(attempt.keys).some(([outcome, n]) => outcome !== "ok" && (n ?? 0) > 0) && (
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <span className="text-fg-subtle">Calls</span>
            {Object.entries(attempt.keys).map(([outcome, n]) => (
              <span
                key={outcome}
                className={cn(
                  "inline-flex h-5 items-center gap-1 rounded-full px-2 font-medium",
                  toneSoft[tone(outcome)],
                )}
              >
                {n} {label(outcome)}
              </span>
            ))}
          </div>
        )}

      {attempt.outputs && attempt.outputs.length > 0 && (
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs">
          <span className="text-fg-subtle">Committed</span>
          {attempt.outputs.map((output) => (
            <span key={output} className="inline-flex items-center gap-1.5">
              <span className="text-fg">{output}</span>
              <span className="font-mono text-fg-muted">g{attempt.generation}</span>
            </span>
          ))}
        </div>
      )}

      <CancelNote run={run} attempt={attempt} live={live} />
      {/* A cancel's "error" is the cancel itself, which the note above explains. */}
      {error && error.message !== "canceled" && <ErrorBlock error={error} run={run} attempt={attempt} />}
    </div>
  );
}

const REASON: Record<string, string> = {
  user: "a user canceled the run",
  canceled: "a user canceled the run",
  timeout: "it ran past its timeout",
  provisioning: "no worker reported before the provisioning deadline",
};

/**
 * How a cancel or a timeout ended this attempt (docs/lifecycle.md §7): asked
 * to stop, a worker drains — finished work is written and committed — or,
 * past the grace period, the engine aborts it and nothing of it commits.
 */
function CancelNote({ run, attempt, live }: { run: string; attempt: Attempt; live: boolean }) {
  const project = useProject();
  const ended = ["canceled", "aborted", "timed_out"].includes(attempt.outcome);
  const result = useQuery({ ...q.attemptResult(project, run, attempt.id), enabled: ended }).data;
  const closing = useQuery({ ...q.runEvents(project, run, live), enabled: ended }).data?.find(
    (e) => e.attempt === attempt.id && (e.type === "aborted" || e.type === "canceled"),
  );
  if (!ended && !result?.cancel) return null;
  const record = result?.cancel;
  const reason = record?.reason ?? closing?.reason ?? "user";
  const drained = result?.status === "canceled" && record?.phase === "requested";
  return (
    <div className="flex items-start gap-2.5 rounded-md border-theme border-line bg-surface-2 px-3 py-2.5 text-sm">
      <StatusIcon status="canceled" className="mt-0.5 size-4" />
      <div className="flex flex-col gap-0.5">
        <p className="text-fg">
          {drained
            ? "Stopped and drained: the work it finished was written and committed."
            : record
              ? "Stopped before it reached its writes: it committed nothing."
              : "Aborted: it never drained, so nothing of it committed."}
        </p>
        <p className="text-xs text-fg-muted">
          Cancel {record ? `${record.phase} — ` : ""}because {REASON[reason] ?? reason}.
          {reason === "user" || reason === "canceled"
            ? " Keys it didn't finish are recorded as canceled and wait for a retry."
            : reason === "timeout"
              ? " Keys it didn't finish count a try and come due again."
              : ""}
        </p>
      </div>
    </div>
  );
}

/** The error as the engine summarised it, completed from the sealed result: type, class, traceback. */
function ErrorBlock({
  error: summary,
  run,
  attempt,
}: {
  error: AttemptError;
  run: string;
  attempt: Attempt;
}) {
  const project = useProject();
  const sealed = useQuery({
    ...q.attemptResult(project, run, attempt.id),
    enabled: !ACTIVE_ATTEMPT.has(attempt.outcome),
  }).data?.error;
  const error: AttemptError = sealed ? { ...summary, ...sealed } : summary;
  return (
    <div className="flex flex-col gap-2 rounded-md bg-fail-soft p-3 text-fail-fg">
      <p className="flex flex-wrap items-baseline gap-x-2 text-sm">
        {error.type && <span className="font-mono font-medium">{error.type}</span>}
        <span className="break-words">{error.message}</span>
      </p>
      <p className="flex flex-wrap gap-x-3 text-xs opacity-85">
        {error.class && <span>class: {error.class}</span>}
        {error.retryable !== undefined && <span>{error.retryable ? "retryable" : "not retryable"}</span>}
        {error.retry_after != null && <span>retry after {duration(error.retry_after)}</span>}
      </p>
      {error.traceback && (
        <details>
          <summary className="cursor-pointer text-xs font-medium select-none">Traceback</summary>
          <pre className="mt-2 max-h-80 overflow-auto rounded-sm bg-surface p-3 font-mono text-xs leading-relaxed whitespace-pre text-fg">
            {error.traceback}
          </pre>
        </details>
      )}
    </div>
  );
}

function ResultTab({ run, attempt }: { run: string; attempt: Attempt }) {
  const project = useProject();
  const { data, error, isPending } = useQuery({
    ...q.attemptResult(project, run, attempt.id),
    enabled: !ACTIVE_ATTEMPT.has(attempt.outcome),
  });
  if (ACTIVE_ATTEMPT.has(attempt.outcome))
    return (
      <Empty compact title="Still running">
        The result appears once the worker seals it.
      </Empty>
    );
  if (isPending) return <Skeleton className="m-4 h-40" />;
  if (error)
    return (
      <Empty compact title="No result">
        {error.message}. A lost or aborted attempt never sealed one.
      </Empty>
    );
  const {
    key_outcomes,
    events: _events,
    log: _log,
    ...rest
  } = data as typeof data & { events?: Json; log?: Json };
  return (
    <div className="flex flex-col gap-4 p-4">
      <Facts>
        <Fact label="Status">
          <StatusBadge status={data.status} />
        </Fact>
        <Fact label="Write">
          <Tooltip content="Where the attempt's store writes got to: none (no store call), writing (a call may still land), complete (every call returned).">
            <span
              className={cn(
                "underline decoration-dotted underline-offset-2",
                data.write === "writing" && "text-warn-fg",
              )}
            >
              {data.write ?? "—"}
            </span>
          </Tooltip>
        </Fact>
        {data.cancel && (
          <Fact label="Cancel">
            {data.cancel.phase} · {data.cancel.reason}
          </Fact>
        )}
        {data.usage &&
          Object.entries(data.usage).map(([k, v]) => (
            <Fact key={k} label={k.replaceAll("_", " ")}>
              {k.includes("memory") ? bytes(v) : k.includes("seconds") ? duration(v) : String(v)}
            </Fact>
          ))}
      </Facts>
      {key_outcomes && key_outcomes.length > 0 && (
        <div className="overflow-hidden rounded-md border-theme border-line">
          <table className="w-full text-xs tabular">
            <thead className="bg-surface-2 text-left text-2xs text-fg-subtle uppercase">
              <tr>
                <th className="px-3 py-1.5 font-medium">Key</th>
                <th className="px-3 py-1.5 font-medium">Outcome</th>
                <th className="px-3 py-1.5 font-medium">Generation</th>
                <th className="px-3 py-1.5 font-medium">Error</th>
                <th className="px-3 py-1.5 text-right font-medium">Time</th>
              </tr>
            </thead>
            <tbody>
              {key_outcomes.map((k) => (
                <tr key={k.key} className="border-t border-line">
                  <td className="px-3 py-1.5 font-mono">{k.key}</td>
                  <td className="px-3 py-1.5">
                    <StatusBadge status={k.outcome} />
                  </td>
                  <td className="px-3 py-1.5 font-mono text-fg-muted">
                    {k.generation != null ? `g${k.generation}` : "—"}
                  </td>
                  <td className="max-w-80 truncate px-3 py-1.5 text-fail-fg" title={k.error ?? undefined}>
                    {k.error}
                  </td>
                  <td className="px-3 py-1.5 text-right text-fg-muted">{duration(k.duration)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <JsonView value={rest as Json} className="max-h-[28rem]" />
    </div>
  );
}

function SpecTab({ run, attempt }: { run: string; attempt: Attempt }) {
  const project = useProject();
  const { data, error, isPending } = useQuery(q.attemptSpec(project, run, attempt.id));
  if (isPending) return <Skeleton className="m-4 h-40" />;
  if (error)
    return (
      <div className="p-4">
        <ErrorNote error={error} title="No spec" />
      </div>
    );
  const inputs = (data.inputs ?? {}) as Record<
    string,
    {
      ref?: { output: string; generation: number; partition: string };
      batch?: Record<string, Json>;
      refs?: Record<string, Json>;
    }
  >;
  return (
    <div className="flex flex-col gap-4 p-4">
      {Object.keys(inputs).length > 0 && (
        <div className="flex flex-col gap-1.5">
          <h3 className="text-xs font-medium text-fg-muted">Pinned inputs</h3>
          <ul className="flex flex-col divide-y divide-line rounded-md border-theme border-line text-xs">
            {Object.entries(inputs).map(([param, pin]) => (
              <li key={param} className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 px-3 py-2">
                <span className="font-mono font-medium text-fg">{param}</span>
                {pin.ref && (
                  <span className="text-fg-muted">
                    {pin.ref.output}
                    {pin.ref.partition && ` · ${pin.ref.partition}`} @{" "}
                    <span className="font-mono">g{pin.ref.generation}</span>
                  </span>
                )}
                {pin.refs && (
                  <span className="text-fg-muted">
                    {plural(Object.keys(pin.refs).length, "partition")} (all partitions)
                  </span>
                )}
                {pin.batch && (
                  <span className="text-fg-subtle">{describeBatch(pin.batch, attempt.batch)}</span>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}
      <JsonView value={data as Json} className="max-h-[32rem]" />
    </div>
  );
}

/** What an incremental pin reads: its batch, from the attempt's own record, else the spec's index and keys. */
function describeBatch(pin: Record<string, Json>, batch: Attempt["batch"]): string {
  const index = batch?.index ?? (typeof pin.index === "number" ? pin.index : null);
  const count = batch?.count ?? (typeof pin.count === "number" ? pin.count : null);
  const parts: string[] = [];
  if (index != null) parts.push(batchLabel({ index, count }, pin.final === true));
  if (batch) parts.push(`${batch.after == null ? "first" : `after ${batch.after}`} → ${batch.last ?? "end"}`);
  if (Array.isArray(pin.keys)) parts.push(plural(pin.keys.length, "key"));
  return parts.join(" · ");
}

function EventsTab({ run, attempt, live }: { run: string; attempt: Attempt; live: boolean }) {
  const project = useProject();
  const { data, isPending } = useQuery(q.runEvents(project, run, live));
  if (isPending) return <Skeleton className="m-4 h-40" />;
  const events = data ?? [];
  const start = events[0]?.at ?? 0;
  return (
    <ol className="flex flex-col py-2 font-mono text-xs">
      {events.map((e) => (
        <EventRow
          key={e.n}
          event={e}
          start={start}
          mine={e.attempt === attempt.id || e.task === attempt.task}
        />
      ))}
    </ol>
  );
}

function EventRow({ event, start, mine }: { event: RunEvent; start: number; mine: boolean }) {
  const subject = event.task ? event.task.split("/").slice(1).join("/") : "run";
  const detail: ReactNode[] = [
    event.by && event.by !== "engine" ? `by ${event.by}` : null,
    event.name,
    event.reason,
    event.rows != null ? `${event.rows} rows` : null,
  ].filter(Boolean);
  return (
    <li
      className={cn(
        "grid grid-cols-[4.5rem_minmax(8rem,14rem)_7rem_minmax(0,1fr)] gap-x-3 px-4 py-0.5",
        mine ? "text-fg" : "text-fg-subtle",
      )}
    >
      <span className="text-right tabular">+{(event.at - start).toFixed(2)}</span>
      <span className="truncate">{subject}</span>
      <span className={cn(mine && tone(event.type) !== "idle" && toneSoft[tone(event.type)].split(" ")[1])}>
        {label(event.type)}
      </span>
      <span className="truncate font-sans">{detail.join(" · ")}</span>
    </li>
  );
}
