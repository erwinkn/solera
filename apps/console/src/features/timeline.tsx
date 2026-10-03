import type { CSSProperties } from "react";
import { Link } from "@tanstack/react-router";
import { ACTIVE_ATTEMPT } from "@/api/queries";
import { PHASES, type Attempt, type Phase, type RunEvent, type Task } from "@/api/types";
import { useNow } from "@/lib/clock";
import { cn } from "@/lib/cn";
import { duration } from "@/lib/format";
import { label, tone, toneSolid } from "@/lib/status";
import { Tooltip } from "@/ui/overlay";
import { StatusIcon } from "@/ui/status";

/**
 * Attempt phases (docs/object-store-state.md §7): from one milestone to the
 * next — claimed → launched → booted → imported → computing → computed →
 * finished → the engine's end. Colours (themes.css): the overhead before the
 * work as one ramp, computing as the accent; every use pairs them with names.
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

const ACTIVE = ACTIVE_ATTEMPT;

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
            <span className="size-2 rounded-mark" style={phaseColor(phase)} />
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
    <div className={cn("flex h-full w-full gap-[2px] overflow-hidden rounded-mark", className)}>
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
          <span className="size-2 rounded-mark" style={phaseColor(phase)} />
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
  events = [],
}: {
  run: string;
  tasks: Task[];
  attempts: Record<string, Attempt[]>;
  start: number;
  end: number | null;
  selected: { task?: string; attempt?: string };
  /** The run's timeline: its own cancels, pauses and outages are drawn across every row. */
  events?: RunEvent[];
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
  const intervals = Object.values(attempts)
    .flat()
    .filter((a) => a.started_at != null)
    .map(
      (a) =>
        [a.started_at!, a.finished_at ?? (ACTIVE.has(a.status) ? now : a.started_at!)] as [number, number],
    );
  const scale = timeScale(start, start + span, intervals);
  const pos = (t: number) => scale.at(t);
  const x = (t: number) => `${pos(t)}%`;
  const marks = events.filter((e) => e.task === null && MARKS[e.type]);
  const across = (
    <>
      {marks.map((m) =>
        m.type === "outage" && m.until ? (
          <span
            key={m.n}
            aria-hidden
            className="absolute inset-y-0 bg-[repeating-linear-gradient(135deg,var(--idle-soft)_0_3px,transparent_3px_6px)]"
            style={{ left: x(m.at), width: `calc(${x(m.until)} - ${x(m.at)})` }}
          />
        ) : (
          <span
            key={m.n}
            aria-hidden
            className={cn("absolute inset-y-0 w-0 border-l-[1.5px] border-dashed", MARKS[m.type]!.line)}
            style={{ left: x(m.at) }}
          />
        ),
      )}
    </>
  );
  return (
    <div className="flex flex-col">
      <div className="grid grid-cols-[minmax(9rem,14rem)_minmax(0,1fr)] gap-x-3 border-b border-line pb-1.5 text-2xs text-fg-subtle tabular">
        <span className="pl-4">Task</span>
        <div className={cn("relative mr-4 h-4", marks.length > 0 && "mt-5")}>
          {scale.ticks.map((tick) => (
            <span
              key={tick.pos}
              className={cn(
                "absolute",
                tick.pos > 92 ? "-translate-x-full" : tick.pos > 2 && "-translate-x-1/2",
              )}
              style={{ left: `${tick.pos}%` }}
            >
              {tick.label}
            </span>
          ))}
          {scale.breaks.map((b) => (
            <span
              key={b.pos}
              title={`${duration(b.seconds)} with no attempt running, compressed`}
              className="absolute -translate-x-1/2 text-fg-subtle"
              style={{ left: `${b.pos + b.width / 2}%` }}
            >
              ⫽
            </span>
          ))}
          {marks.map((m) => (
            <span
              key={m.n}
              className={cn(
                "absolute -top-4 rounded-xs px-1 font-medium whitespace-nowrap",
                pos(m.at) > 70 ? "-translate-x-full" : pos(m.at) > 10 && "-translate-x-1/2",
                MARKS[m.type]!.label,
              )}
              style={{ left: x(m.at) }}
            >
              {MARKS[m.type]!.text}
              {m.by && m.by !== "engine" ? ` · ${m.by}` : ""}
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
                  {task.partition && (
                    <span className="font-mono text-xs text-fg-subtle"> · {task.partition}</span>
                  )}
                </span>
              </Link>
              <div className="relative mr-4 h-8">
                {scale.ticks.map((tick) => (
                  <span
                    key={tick.pos}
                    aria-hidden
                    className="absolute inset-y-0 w-px bg-line"
                    style={{ left: `${tick.pos}%` }}
                  />
                ))}
                {scale.breaks.map((b) => (
                  <span
                    key={b.pos}
                    aria-hidden
                    className="absolute inset-y-0 bg-[repeating-linear-gradient(120deg,color-mix(in_srgb,var(--fg)_14%,transparent)_0_1px,transparent_1px_5px)]"
                    style={{ left: `${b.pos}%`, width: `${b.width}%` }}
                  />
                ))}
                {across}
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
                      style={{
                        left: x(before.finished_at),
                        width: `calc(${x(attempt.started_at)} - ${x(before.finished_at)})`,
                      }}
                    />
                  );
                })}
                {list.map((attempt) => {
                  if (attempt.started_at == null) return null;
                  const stop = attempt.finished_at ?? (ACTIVE.has(attempt.status) ? now : attempt.started_at);
                  const left = pos(attempt.started_at);
                  const width = Math.max(0.6, pos(stop) - left);
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
                        aria-label={`${task.asset} ${task.partition} attempt ${attempt.generation}: ${label(attempt.status)}, ${duration(stop - attempt.started_at)}`}
                        className={cn(
                          "absolute top-1/2 h-3.5 -translate-y-1/2 rounded-mark p-[1.5px]",
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

const MARKS: Record<string, { text: string; line: string; label: string }> = {
  canceled: { text: "cancel requested", line: "border-fail", label: "bg-fail-soft text-fail-fg" },
  paused: { text: "paused", line: "border-wait", label: "bg-wait-soft text-wait-fg" },
  resumed: { text: "resumed", line: "border-wait", label: "bg-wait-soft text-wait-fg" },
  outage: { text: "engine down", line: "border-idle", label: "bg-idle-soft text-idle-fg" },
};

const BREAK = 4; // percent of the axis a compressed gap takes

/**
 * The run's clock, with the waits squeezed out. Spans when attempts run keep
 * their proportions; a gap with nothing running that is long next to the
 * work (a retry's backoff, a queue) collapses to a fixed sliver, marked, so
 * the attempts are what you see. Labels give real elapsed time.
 */
export function timeScale(start: number, end: number, intervals: [number, number][]) {
  const span = Math.max(end - start, 0.001);
  const merged: [number, number][] = [];
  for (const [a, b] of intervals
    .map(([a, b]) => [Math.max(a, start), Math.min(Math.max(a, b), end)] as [number, number])
    .sort((p, q) => p[0] - q[0])) {
    const last = merged[merged.length - 1];
    if (last && a <= last[1]) last[1] = Math.max(last[1], b);
    else merged.push([a, b]);
  }
  const active = merged.reduce((sum, [a, b]) => sum + (b - a), 0);
  const long = Math.max(0.5, active * 0.25);
  // Pieces of the axis in time order: real spans, and gaps long enough to compress.
  const pieces: { from: number; to: number; squeezed: boolean }[] = [];
  let cursor = start;
  for (const [a, b] of [...merged, [end, end] as [number, number]]) {
    if (a > cursor) pieces.push({ from: cursor, to: a, squeezed: active > 0 && a - cursor > long });
    if (b > a) pieces.push({ from: a, to: b, squeezed: false });
    cursor = Math.max(cursor, b);
  }
  const squeezed = pieces.filter((p) => p.squeezed).length;
  const real = pieces.filter((p) => !p.squeezed).reduce((sum, p) => sum + (p.to - p.from), 0) || span;
  const share = 100 - BREAK * squeezed;
  let offset = 0;
  const placed = pieces.map((p) => {
    const width = p.squeezed ? BREAK : (share * (p.to - p.from)) / real;
    const piece = { ...p, pos: offset, width };
    offset += width;
    return piece;
  });
  const at = (t: number) => {
    if (t <= start) return 0;
    const piece = placed.find((p) => t <= p.to) ?? placed[placed.length - 1];
    if (!piece) return Math.min(100, (100 * (t - start)) / span);
    const within = piece.to > piece.from ? (t - piece.from) / (piece.to - piece.from) : 0;
    return Math.min(100, piece.pos + piece.width * Math.min(1, within));
  };
  // Labels: the start, where time resumes after each break, and the end — none crowding another.
  const ticks: { pos: number; label: string }[] = [];
  const candidates = [
    start,
    ...placed.filter((_, i) => i > 0 && placed[i - 1]!.squeezed).map((piece) => piece.from),
    end,
  ];
  for (const t of candidates) {
    const p = at(t);
    if (ticks.every((k) => Math.abs(k.pos - p) > 9)) ticks.push({ pos: p, label: duration(t - start) });
  }
  if (squeezed === 0) {
    // No breaks: plain round ticks read better.
    ticks.length = 0;
    const steps = [
      0.1, 0.2, 0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 43200, 86400,
    ];
    const step = steps.find((x) => span / x <= 6) ?? 86400;
    for (let t = 0; t <= span + 1e-9; t += step) ticks.push({ pos: (100 * t) / span, label: duration(t) });
  }
  const breaks = placed
    .filter((p) => p.squeezed)
    .map((p) => ({ pos: p.pos, width: p.width, seconds: p.to - p.from }));
  return { at, ticks, breaks };
}
