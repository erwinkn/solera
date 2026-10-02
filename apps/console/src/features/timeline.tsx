import type { CSSProperties } from "react";
import { Link } from "@tanstack/react-router";
import { PHASES, type Attempt, type Phase, type Task } from "@/api/types";
import { useNow } from "@/lib/clock";
import { cn } from "@/lib/cn";
import { duration } from "@/lib/format";
import { label, tone, toneSolid } from "@/lib/status";
import { Tooltip } from "@/ui/overlay";
import { StatusIcon } from "@/ui/status";

/**
 * Attempt phases (docs/object-store-state.md §7): from one milestone to the
 * next — claimed → launched → booted → imported → computing → computed →
 * finished → the engine's end. A categorical palette validated for colour-
 * vision deficiency in this order; every use pairs it with names.
 */
export const PHASE_LABEL: Record<Phase, string> = {
  preparing: "Preparing",
  provisioning: "Provisioning",
  importing: "Importing",
  loading: "Loading",
  computing: "Computing",
  writing: "Writing",
  settling: "Settling",
};
export const phaseColor = (phase: Phase): CSSProperties => ({
  backgroundColor: `var(--ph-${PHASES.indexOf(phase) + 1})`,
});

const ACTIVE = new Set(["preparing", "launched", "provisioning", "claimed", "running"]);

export function phasesOf(attempt: Attempt): { phase: Phase; seconds: number }[] {
  return PHASES.map((phase) => ({
    phase,
    seconds: attempt[phase] ?? 0,
  })).filter((p) => p.seconds > 0);
}

function PhaseTip({ attempt, end }: { attempt: Attempt; end: number }) {
  const phases = phasesOf(attempt);
  const total = (end ?? 0) - (attempt.started_at ?? end);
  return (
    <div className="flex min-w-44 flex-col gap-1">
      <span className="flex items-center justify-between gap-4 font-medium">
        <span>
          Attempt {attempt.generation} · {label(attempt.status)}
        </span>
        <span className="tabular">{duration(total)}</span>
      </span>
      {phases.length === 0 && <span className="opacity-80">No phases recorded yet</span>}
      {phases.map(({ phase, seconds }) => (
        <span key={phase} className="flex items-center justify-between gap-4 tabular">
          <span className="flex items-center gap-1.5">
            <span className="size-2 rounded-[2px]" style={phaseColor(phase)} />
            {PHASE_LABEL[phase]}
          </span>
          {duration(seconds)}
        </span>
      ))}
    </div>
  );
}

/** One attempt's phases as a bar; an attempt still running fills to now. */
export function PhaseBar({ attempt, end, className }: { attempt: Attempt; end: number; className?: string }) {
  const phases = phasesOf(attempt);
  const total = phases.reduce((sum, p) => sum + p.seconds, 0);
  const live = ACTIVE.has(attempt.status);
  const span = Math.max(total, end - (attempt.started_at ?? end));
  return (
    <div className={cn("flex h-full w-full gap-[2px] overflow-hidden rounded-[4px]", className)}>
      {phases.map(({ phase, seconds }) => (
        <span
          key={phase}
          className="h-full min-w-[2px]"
          style={{
            ...phaseColor(phase),
            flexGrow: seconds / (span || 1),
            flexBasis: 0,
          }}
        />
      ))}
      {(live || phases.length === 0) && (
        <span
          className={cn(
            "h-full min-w-[3px] flex-1",
            live
              ? "bg-[repeating-linear-gradient(135deg,var(--run)_0_4px,var(--run-soft)_4px_8px)] bg-[length:16px_100%] animate-[march_0.8s_linear_infinite]"
              : toneSolid[tone(attempt.status)],
          )}
          style={{
            flexGrow: live ? Math.max(0.02, (span - total) / (span || 1)) : 1,
            flexBasis: 0,
          }}
        />
      )}
    </div>
  );
}

export function PhaseLegend({ className }: { className?: string }) {
  return (
    <div className={cn("flex flex-wrap items-center gap-x-3 gap-y-1 text-2xs text-fg-subtle", className)}>
      {PHASES.map((phase) => (
        <span key={phase} className="inline-flex items-center gap-1">
          <span className="size-2 rounded-[2px]" style={phaseColor(phase)} />
          {PHASE_LABEL[phase]}
        </span>
      ))}
    </div>
  );
}

/**
 * The run's waterfall: one row per task, its attempts placed on the run's
 * clock and split into phases. The gap before a task's first attempt is its
 * wait. Rows select a task; bars select an attempt.
 */
export function Waterfall({
  run,
  tasks,
  attempts,
  start,
  end,
  selected,
}: {
  run: string;
  tasks: Task[];
  attempts: Record<string, Attempt[]>;
  start: number;
  end: number | null;
  selected: { task?: string; attempt?: string };
}) {
  const now = useNow();
  const finish = end ?? now;
  const latest = Math.max(
    finish,
    ...Object.values(attempts)
      .flat()
      .map(
        (a) => a.finished_at ?? (a.started_at != null && ACTIVE.has(a.status) ? now : (a.started_at ?? 0)),
      ),
  );
  const span = Math.max(0.001, latest - start);
  const ticks = niceTicks(span);
  const x = (t: number) => `${(100 * Math.min(Math.max(t - start, 0), span)) / span}%`;
  return (
    <div className="flex flex-col">
      <div className="grid grid-cols-[minmax(9rem,14rem)_minmax(0,1fr)] gap-x-3 border-b border-line pb-1.5 text-2xs text-fg-subtle tabular">
        <span className="pl-4">Task</span>
        <div className="relative mr-4 h-4">
          {ticks.map((t) => (
            <span
              key={t}
              className="absolute -translate-x-1/2 first:translate-x-0"
              style={{ left: `${(100 * t) / span}%` }}
            >
              {duration(t)}
            </span>
          ))}
        </div>
      </div>
      <ol className="flex flex-col py-1">
        {tasks.map((task) => {
          const list = attempts[task.id] ?? [];
          const isSelected = selected.task === task.id;
          return (
            <li
              key={task.id}
              className={cn(
                "group relative grid grid-cols-[minmax(9rem,14rem)_minmax(0,1fr)] items-center gap-x-3",
                isSelected ? "bg-select" : "hover:bg-surface-2",
              )}
            >
              <Link
                to="/runs/$run"
                params={{ run }}
                search={(s) => ({ ...s, task: task.id, attempt: undefined })}
                replace
                aria-current={isSelected || undefined}
                className="flex h-8 min-w-0 items-center gap-2 pl-4 text-sm"
              >
                <StatusIcon status={task.status} />
                <span className="truncate">
                  <span className="text-fg">{task.asset}</span>
                  {task.scope && <span className="font-mono text-xs text-fg-subtle"> · {task.scope}</span>}
                </span>
              </Link>
              <div className="relative mr-4 h-8">
                {ticks.map((t) => (
                  <span
                    key={t}
                    aria-hidden
                    className="absolute inset-y-0 w-px bg-line"
                    style={{ left: `${(100 * t) / span}%` }}
                  />
                ))}
                {list[0]?.started_at != null && list[0].started_at > start && (
                  <span
                    aria-hidden
                    title={`waited ${duration(task.wait ?? list[0].started_at - start)}`}
                    className="absolute top-1/2 h-px border-t border-dashed border-fg-subtle"
                    style={{ left: 0, width: x(list[0].started_at) }}
                  />
                )}
                {list.slice(1).map((attempt, i) => {
                  // The retry wait: from one attempt's end to the next one's start.
                  const before = list[i]!;
                  if (attempt.started_at == null || before.finished_at == null) return null;
                  return (
                    <span
                      key={`gap-${attempt.id}`}
                      aria-hidden
                      title={`retried after ${duration(attempt.started_at - before.finished_at)}`}
                      className="absolute top-1/2 h-px border-t border-dotted border-fail"
                      style={{ left: x(before.finished_at), width: `calc(${x(attempt.started_at)} - ${x(before.finished_at)})` }}
                    />
                  );
                })}
                {list.map((attempt) => {
                  if (attempt.started_at == null) return null;
                  const stop = attempt.finished_at ?? (ACTIVE.has(attempt.status) ? now : attempt.started_at);
                  const left = (100 * (attempt.started_at - start)) / span;
                  const width = Math.max(0.6, (100 * (stop - attempt.started_at)) / span);
                  const active =
                    selected.attempt === attempt.id ||
                    (isSelected && !selected.attempt && attempt === list[list.length - 1]);
                  return (
                    <Tooltip key={attempt.id} content={<PhaseTip attempt={attempt} end={stop} />}>
                      <Link
                        to="/runs/$run"
                        params={{ run }}
                        search={(s) => ({
                          ...s,
                          task: task.id,
                          attempt: attempt.id,
                        })}
                        replace
                        aria-label={`${task.asset} ${task.scope} attempt ${attempt.generation}: ${label(attempt.status)}, ${duration(stop - attempt.started_at)}`}
                        className={cn(
                          "absolute top-1/2 h-3.5 -translate-y-1/2 rounded-[5px] p-[1.5px]",
                          active ? "ring-2 ring-fg" : "hover:ring-2 hover:ring-line-strong",
                        )}
                        style={{
                          left: `${left}%`,
                          width: `max(${width}%, 4px)`,
                        }}
                      >
                        <PhaseBar attempt={attempt} end={stop} />
                      </Link>
                    </Tooltip>
                  );
                })}
              </div>
            </li>
          );
        })}
      </ol>
    </div>
  );
}

function niceTicks(span: number): number[] {
  const steps = [
    0.1, 0.2, 0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 43200, 86400,
  ];
  const step = steps.find((s) => span / s <= 6) ?? 86400;
  const out: number[] = [];
  for (let t = 0; t <= span + 1e-9; t += step) out.push(Number(t.toFixed(3)));
  return out;
}
