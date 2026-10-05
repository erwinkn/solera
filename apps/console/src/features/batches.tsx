import type { Attempt, Batch, Progress } from "@/api/types";
import { cn } from "@/lib/cn";
import { count } from "@/lib/format";

/**
 * A task walks the keys its partition owes in key order, a batch at a time:
 * up to `batch_size` keys, committed once. An attempt is one execution of a
 * batch; a retry is another attempt of the same batch (docs/observed-set.md,
 * "A run"). These helpers group a task's attempts by batch and say how far
 * its walk has got.
 */

/** A task's attempts grouped by the batch they executed, in batch order. */
export interface BatchGroup {
  /** Null for attempts that carry no batch: an unkeyed or non-incremental task, or an engine that predates batches. */
  batch: Batch | null;
  attempts: Attempt[];
}

export function groupByBatch(attempts: Attempt[]): BatchGroup[] {
  const groups: BatchGroup[] = [];
  const byIndex = new Map<number, BatchGroup>();
  for (const attempt of attempts) {
    const batch = attempt.batch ?? null;
    if (batch == null) {
      groups.push({ batch: null, attempts: [attempt] });
      continue;
    }
    const group = byIndex.get(batch.index);
    if (group) {
      group.attempts.push(attempt);
      // The latest attempt's view of the batch wins: a retry may class keys again at a newer head.
      group.batch = batch;
    } else {
      const fresh = { batch, attempts: [attempt] };
      byIndex.set(batch.index, fresh);
      groups.push(fresh);
    }
  }
  return groups;
}

/** Whether any attempt of a task says which batch it ran: then the task panel shows batches. */
export const hasBatches = (attempts: Attempt[]) => attempts.some((a) => a.batch != null);

/** "batch 2 of 3", or "of ~3" while the planned count is an estimate the run hasn't reached. */
export function batchLabel(batch: Pick<Batch, "index" | "count">, done?: boolean): string {
  const n = batch.index + 1;
  if (batch.count == null) return `batch ${n}`;
  return `batch ${n} of ${done ? "" : "~"}${count(Math.max(batch.count, n))}`;
}

/** The keys a batch covers, `(after, last]`, as an operator reads it; a null `last` runs to the end. */
export function BatchRange({
  batch,
  className,
}: {
  batch: Pick<Batch, "after" | "last">;
  className?: string;
}) {
  const { after, last } = batch;
  return (
    <span className={cn("inline-flex min-w-0 items-baseline gap-1 font-mono text-xs", className)}>
      {after == null && last == null ? (
        <span className="font-sans text-fg-subtle">every key</span>
      ) : (
        <>
          <span className="truncate text-fg-subtle" title={after ?? "the first key"}>
            {after == null ? "first" : `after ${after}`}
          </span>
          <span aria-hidden className="text-fg-subtle">
            →
          </span>
          <span className="truncate text-fg" title={last ?? "to the last key"}>
            {last ?? "end"}
          </span>
        </>
      )}
    </span>
  );
}

/**
 * What a batch (or a partition's debt) holds per class: added, updated,
 * removed, and — only when a run asked for more than was owed — unchanged.
 * Zero classes are left out; all zero reads "no keys".
 */
export function KeyClasses({
  added,
  updated,
  removed,
  unchanged = 0,
  className,
}: {
  added: number;
  updated: number;
  removed: number;
  unchanged?: number;
  className?: string;
}) {
  const classes = (
    [
      ["added", added],
      ["updated", updated],
      ["removed", removed],
      ["unchanged", unchanged],
    ] as const
  ).filter(([, n]) => n > 0);
  if (classes.length === 0) return <span className={cn("text-xs text-fg-subtle", className)}>no keys</span>;
  return (
    <span className={cn("inline-flex flex-wrap items-baseline gap-x-2.5 text-xs", className)}>
      {classes.map(([name, n]) => (
        <span key={name} className="whitespace-nowrap">
          <span className="font-medium text-fg tabular">{count(n)}</span>{" "}
          <span className="text-fg-muted">{name}</span>
        </span>
      ))}
    </span>
  );
}

/**
 * How far a task's walk has got: its last committed batch, and the key it
 * reached. `progress.key` null means the walk is done.
 */
export function describeProgress(
  progress: Progress | null | undefined,
  planned: number | null,
): string | null {
  if (progress === undefined) return null;
  if (progress === null) return "no batch committed yet";
  const n = progress.batch + 1;
  if (progress.key === null) return n === 1 ? "1 batch, done" : `${count(n)} batches, done`;
  const of = planned != null ? ` of ~${count(Math.max(planned, n + 1))}` : "";
  return `${count(n)}${of} committed, through ${progress.key}`;
}

/** A thin bar of a walk's committed batches against the planned count. */
export function ProgressBar({
  progress,
  planned,
  className,
}: {
  progress: Progress | null | undefined;
  planned: number | null;
  className?: string;
}) {
  if (progress === undefined) return null;
  const done = progress ? progress.batch + 1 : 0;
  const final = progress?.key === null;
  const total = final ? done : Math.max(planned ?? done + 1, done + 1);
  const ratio = total ? done / total : 0;
  return (
    <span
      role="meter"
      aria-label="Batches committed"
      aria-valuemin={0}
      aria-valuemax={total}
      aria-valuenow={done}
      className={cn("inline-block h-1.5 w-16 overflow-hidden rounded-full bg-sunken", className)}
    >
      <span
        className={cn(
          "block h-full rounded-full motion-2 transition-[width]",
          final ? "bg-viz-ok" : "bg-run",
        )}
        style={{ width: `${Math.round(100 * ratio)}%` }}
      />
    </span>
  );
}

/** Which try of its batch an attempt is: 1 for the first, 2 for its first retry. Without batches, its place in the task. */
export function tryOf(attempt: Attempt, attempts: Attempt[]): number {
  const index = attempt.batch?.index;
  const same = attempts.filter((a) => (index == null ? true : a.batch?.index === index));
  return same.findIndex((a) => a.id === attempt.id) + 1;
}

/** An attempt's name for people: "batch 2, try 2", or "attempt 3" where there are no batches. */
export function attemptName(attempt: Attempt, attempts: Attempt[]): string {
  if (!attempt.batch) return `attempt ${attempt.generation}`;
  const n = tryOf(attempt, attempts);
  return `batch ${attempt.batch.index + 1}${n > 1 ? `, try ${n}` : ""}`;
}
