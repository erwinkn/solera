import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import { CalendarClock, GitCommitHorizontal, Hand, Radar, RotateCcw } from "lucide-react";
import type { Histogram, RunRow } from "@/api/types";
import { cn } from "@/lib/cn";
import { clock, count, dateTime, firstLine, plural, shortId } from "@/lib/format";
import { label, toneSolid, type Tone } from "@/lib/status";
import { useNow } from "@/lib/clock";
import { Elapsed, Id, Time } from "@/ui/data";
import { Tooltip } from "@/ui/overlay";
import { StatusBadge, StatusIcon } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

/** Who or what asked for a run, in a few words. */
export function TriggerLabel({
  run,
}: {
  run: Pick<RunRow, "trigger" | "automation" | "by" | "source" | "tags" | "retry_of">;
}) {
  const sensor = run.tags?.sensor;
  let icon: ReactNode;
  let text: ReactNode;
  if (run.trigger === "commit") {
    icon = <GitCommitHorizontal />;
    text = (
      <>
        commit <span className="text-fg">{run.source}</span>
      </>
    );
  } else if (sensor || run.trigger === "sensor") {
    icon = <Radar />;
    text = <span className="text-fg">{sensor ?? run.by}</span>;
  } else if (run.retry_of) {
    icon = <RotateCcw />;
    text = (
      <>
        retry of <span className="font-mono text-fg">{shortId(run.retry_of)}</span>
      </>
    );
  } else if (run.automation) {
    icon = <CalendarClock />;
    text = <span className="text-fg">{run.automation}</span>;
  } else if (run.by === "retry clock") {
    icon = <RotateCcw />;
    text = "retry clock";
  } else {
    icon = <Hand />;
    text = (
      <>
        manual
        {run.by && run.by !== "api" ? <span className="text-fg"> · {run.by}</span> : null}
      </>
    );
  }
  return (
    <span className="inline-flex min-w-0 items-center gap-1.5 text-fg-muted [&_svg]:size-3.5 [&_svg]:shrink-0">
      {icon}
      <span className="truncate">{text}</span>
    </span>
  );
}

export function partitionsOf(partitions: RunRow["partitions"]): string[] | string {
  if (Array.isArray(partitions)) return partitions;
  if (typeof partitions === "string" && partitions.startsWith("[")) {
    try {
      return JSON.parse(partitions) as string[];
    } catch {
      return partitions;
    }
  }
  return partitions ?? "";
}

export function PartitionsLabel({ partitions }: { partitions: RunRow["partitions"] }) {
  const value = partitionsOf(partitions);
  if (typeof value === "string") return <span className="text-fg-muted">{value || "—"}</span>;
  if (value.length === 0 || (value.length === 1 && value[0] === ""))
    return <span className="text-fg-subtle">unpartitioned</span>;
  if (value.length === 1) return <span className="font-mono text-xs">{value[0]}</span>;
  return (
    <Tooltip content={<span className="font-mono">{value.join("\n")}</span>}>
      <span className="text-fg-muted">{plural(value.length, "partition")}</span>
    </Tooltip>
  );
}

export function RunsTable({
  runs,
  compact,
  empty,
}: {
  runs: RunRow[];
  compact?: boolean;
  empty?: ReactNode;
}) {
  if (runs.length === 0) return <>{empty}</>;
  if (compact) return <RunList runs={runs} />;
  return (
    <TableScroll>
      <Table>
        <thead>
          <tr>
            <Th className="w-28">Status</Th>
            <Th>Run</Th>
            <Th>Trigger</Th>
            <Th>Partitions</Th>
            <Th>Started</Th>
            <Th className="text-right">Duration</Th>
            <Th className="text-right">Tasks</Th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <Tr key={run.id} className="relative">
              <Td>
                <StatusBadge status={run.status} />
              </Td>
              <Td className="max-w-[28rem]">
                <Link
                  to="/runs/$run"
                  params={{ run: run.id }}
                  className="flex min-w-0 items-baseline gap-2 after:absolute after:inset-0 after:content-['']"
                >
                  <span className="truncate font-medium text-fg">{run.targets.join(", ")}</span>
                  <Id value={run.id} className="text-fg-subtle" />
                </Link>
                {run.status === "failed" && run.error && (
                  <p className="mt-0.5 truncate text-xs text-fail-fg">{firstLine(run.error)}</p>
                )}
              </Td>
              <Td className="max-w-56">
                <TriggerLabel run={run} />
              </Td>
              <Td>
                <PartitionsLabel partitions={run.partitions} />
              </Td>
              <Td className="text-fg-muted">
                <Time at={run.created_at} className="relative z-10" />
              </Td>
              <Td className="text-right text-fg-muted">
                <Elapsed start={run.created_at} end={run.finished_at} />
              </Td>
              <Td className="text-right whitespace-nowrap">
                <TaskCount total={run.task_count} failed={run.failed_count} />
              </Td>
            </Tr>
          ))}
        </tbody>
      </Table>
    </TableScroll>
  );
}

/** Two lines a run, for narrow cards: what and why on the left, when on the right. */
function RunList({ runs }: { runs: RunRow[] }) {
  return (
    <ul className="flex flex-col pb-1.5">
      {runs.map((run) => (
        <li
          key={run.id}
          className="relative grid grid-cols-[auto_minmax(0,1fr)_auto] items-start gap-x-3 px-4 py-2 hover:bg-surface-2"
        >
          <StatusIcon status={run.status} className="mt-0.5 size-4" />
          <div className="flex min-w-0 flex-col gap-0.5">
            <Link
              to="/runs/$run"
              params={{ run: run.id }}
              className="flex min-w-0 items-baseline gap-2 after:absolute after:inset-0 after:content-['']"
            >
              <span className="truncate text-sm font-medium text-fg">{run.targets.join(", ")}</span>
              <Id value={run.id} className="text-xs text-fg-subtle" />
            </Link>
            {run.status === "failed" && run.error ? (
              <p className="truncate text-xs text-fail-fg">{firstLine(run.error)}</p>
            ) : (
              <span className="flex min-w-0 items-center gap-2 text-xs">
                <TriggerLabel run={run} />
                <span className="text-fg-subtle">·</span>
                <PartitionsLabel partitions={run.partitions} />
              </span>
            )}
          </div>
          <div className="flex flex-col items-end gap-0.5 text-xs text-fg-muted">
            <Time at={run.created_at} className="relative z-10" />
            <Elapsed start={run.created_at} end={run.finished_at} className="text-fg-subtle" />
          </div>
        </li>
      ))}
    </ul>
  );
}

function TaskCount({ total, failed }: { total: number | null; failed: number | null }) {
  if (total == null) return <span className="text-fg-subtle">—</span>;
  return (
    <span className="text-fg-muted">
      {failed ? (
        <>
          <span className="font-medium text-fail-fg">{count(failed)} failed</span> of {count(total)}
        </>
      ) : (
        count(total)
      )}
    </span>
  );
}

// -- histogram -------------------------------------------------------------------

/** Stack order from the baseline up: what finished well, then what didn't. */
const STACK: { status: string; tone: Tone }[] = [
  { status: "succeeded", tone: "ok" },
  { status: "skipped", tone: "idle" },
  { status: "canceled", tone: "idle" },
  { status: "failed", tone: "fail" },
  { status: "queued", tone: "wait" },
  { status: "running", tone: "run" },
];

/**
 * Runs over time, one column per bucket, stacked by status. Status colors
 * with a legend; every column has a tooltip with its numbers; clicking one
 * narrows the filter to that bucket.
 */
export function RunHistogram({
  data,
  onSelect,
  height = 72,
}: {
  data: Histogram;
  onSelect?: (since: number, until: number) => void;
  height?: number;
}) {
  const now = useNow();
  const totals = data.bars.map((b) => Object.values(b.counts).reduce((a, n) => a + n, 0));
  const max = Math.max(1, ...totals);
  const slots = Math.max(1, Math.ceil((data.until - data.since) / data.bucket));
  const byT = new Map(data.bars.map((b) => [b.t, b]));
  const columns = Array.from({ length: slots }, (_, i) => {
    const t = data.since + i * data.bucket;
    return { t, counts: byT.get(t)?.counts ?? {} };
  });
  const present = STACK.filter((s) => data.bars.some((b) => (b.counts[s.status] ?? 0) > 0));
  return (
    <figure className="flex flex-col gap-2">
      <div className="flex items-end gap-[2px]" style={{ height }}>
        {columns.map(({ t, counts }) => {
          const total = Object.values(counts).reduce((a, n) => a + n, 0);
          const parts = STACK.filter((s) => (counts[s.status] ?? 0) > 0);
          const content = (
            <div className="flex flex-col gap-1">
              <span className="font-medium">
                {clock(t)} – {clock(t + data.bucket)}
              </span>
              {total === 0 ? (
                <span className="opacity-80">no runs</span>
              ) : (
                Object.entries(counts).map(([status, n]) => (
                  <span key={status} className="flex items-center justify-between gap-4 tabular">
                    <span>{label(status)}</span>
                    <span>{count(n)}</span>
                  </span>
                ))
              )}
            </div>
          );
          return (
            <Tooltip key={t} content={content}>
              <button
                type="button"
                aria-label={`${clock(t)}: ${total} runs`}
                onClick={() => onSelect?.(t, t + data.bucket)}
                className="group flex h-full max-w-6 min-w-0 flex-1 flex-col-reverse gap-[2px] rounded-t-[4px] hover:bg-accent-soft"
              >
                {parts.map((part, i) => (
                  <span
                    key={part.status}
                    className={cn(
                      "w-full shrink-0",
                      toneSolid[part.tone],
                      i === parts.length - 1 && "rounded-t-[4px]",
                    )}
                    style={{
                      height: `${(100 * (counts[part.status] ?? 0)) / max}%`,
                      minHeight: 2,
                    }}
                  />
                ))}
              </button>
            </Tooltip>
          );
        })}
      </div>
      <figcaption className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 text-2xs text-fg-subtle tabular">
        <span>{dateTime(data.since, now)}</span>
        <span className="flex flex-wrap items-center gap-3">
          {present.map((s) => (
            <span key={s.status} className="inline-flex items-center gap-1">
              <StatusIcon status={s.status} className="size-3" />
              {label(s.status)}
            </span>
          ))}
          <span>{data.bucket >= 3600 ? `${data.bucket / 3600}h` : `${data.bucket / 60}m`} buckets</span>
        </span>
        <span>{dateTime(data.until, now)}</span>
      </figcaption>
    </figure>
  );
}
