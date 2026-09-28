import { useRef, type KeyboardEvent, type ReactNode } from "react";
import { Badge } from "@/components/ui/badge";
import { cn } from "cn";
import type { AssetStatus, RunStatus, TaskStatus } from "@/lib/types";

type Status = AssetStatus | RunStatus | TaskStatus | string;

const labels: Record<string, string> = {
  not_materialized: "Not materialized",
  materialized: "Materialized",
  stale: "Stale",
  partial: "Partial",
  queued: "Queued",
  claimable: "Claimable",
  waiting: "Waiting",
  running: "Running",
  paused: "Paused",
  succeeded: "Succeeded",
  skipped: "Unchanged",
  failed: "Failed",
  blocked: "Blocked",
  canceled: "Canceled",
  complete: "Complete",
  missing: "Missing",
  retired: "Retired",
};

// One colour language across every surface. Each tone reads in light and dark.
const EMERALD =
  "border-emerald-600/30 bg-emerald-500/10 text-emerald-700 dark:border-emerald-400/25 dark:bg-emerald-400/10 dark:text-emerald-300";
const SKY =
  "border-sky-600/30 bg-sky-500/10 text-sky-700 dark:border-sky-400/25 dark:bg-sky-400/10 dark:text-sky-300";
const AMBER =
  "border-amber-600/30 bg-amber-500/10 text-amber-700 dark:border-amber-400/25 dark:bg-amber-400/10 dark:text-amber-300";
const RED =
  "border-red-600/30 bg-red-500/10 text-red-700 dark:border-red-400/25 dark:bg-red-400/10 dark:text-red-300";
const ZINC = "border-border bg-muted/40 text-muted-foreground";

const tones: Record<string, string> = {
  not_materialized: ZINC,
  materialized: EMERALD,
  complete: EMERALD,
  succeeded: EMERALD,
  stale: AMBER,
  partial: AMBER,
  missing: AMBER,
  paused: AMBER,
  queued: ZINC,
  claimable: ZINC,
  waiting: ZINC,
  running: SKY,
  skipped: ZINC,
  retired: ZINC,
  failed: RED,
  blocked: RED,
  canceled: ZINC,
};

const dots: Record<string, string> = {
  not_materialized: "bg-muted-foreground/50",
  materialized: "bg-emerald-500",
  complete: "bg-emerald-500",
  succeeded: "bg-emerald-500",
  stale: "bg-amber-500",
  partial: "bg-amber-500",
  missing: "bg-amber-500",
  paused: "bg-amber-500",
  queued: "bg-muted-foreground/50",
  claimable: "bg-muted-foreground/50",
  waiting: "bg-muted-foreground/50",
  running: "bg-sky-500 animate-pulse",
  skipped: "bg-muted-foreground/50",
  retired: "bg-muted-foreground/40",
  failed: "bg-red-500",
  blocked: "bg-red-500",
  canceled: "bg-muted-foreground/50",
};

/** Solid fills for charts, in the badges' colour language. */
export const statusFill: Record<string, string> = {
  succeeded: "bg-emerald-500",
  failed: "bg-red-500",
  canceled: "bg-zinc-400 dark:bg-zinc-500",
  running: "bg-sky-500",
  queued: "bg-zinc-300 dark:bg-zinc-600",
  paused: "bg-amber-500",
  skipped: "bg-zinc-200 dark:bg-zinc-700",
};

/** The order statuses stack in, bottom first. */
export const statusOrder = [
  "succeeded",
  "failed",
  "canceled",
  "running",
  "queued",
  "paused",
  "skipped",
];

export function StatusBadge({
  status,
  className,
}: {
  status: Status;
  className?: string;
}) {
  return (
    <Badge
      variant="outline"
      data-status={status}
      className={cn("gap-1.5 font-medium", tones[status] ?? ZINC, className)}
    >
      <span className={cn("size-1.5 rounded-full", dots[status])} />
      {labels[status] ?? status.replaceAll("_", " ")}
    </Badge>
  );
}

/** A small uppercase overline used above titles and section headings. */
export function Eyebrow({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "text-[0.65rem] font-semibold tracking-[0.09em] text-muted-foreground uppercase",
        className,
      )}
    >
      {children}
    </div>
  );
}

/** The page header block: eyebrow + title + optional description, with room
    for aside content (stat clusters, actions) on the right. */
export function PageHeader({
  eyebrow,
  title,
  description,
  aside,
}: {
  eyebrow: string;
  title: string;
  description?: string;
  aside?: ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-3">
      <div className="flex flex-col gap-1">
        <Eyebrow>{eyebrow}</Eyebrow>
        <h1 className="font-heading text-xl font-semibold tracking-tight">
          {title}
        </h1>
        {description && (
          <p className="text-sm text-muted-foreground">{description}</p>
        )}
      </div>
      {aside}
    </div>
  );
}

export interface SegmentOption<T extends string> {
  value: T;
  label: string;
  icon?: ReactNode;
}

/** A compact segmented control — the workhorse for latest/missing/all/pick,
    incremental/full, table/graph, and the light/dark theme switch.
    Radio-group semantics: one tabbable option, arrows/Home/End move and
    select. */
export function Segmented<T extends string>({
  value,
  onChange,
  options,
  ariaLabel,
  className,
}: {
  value: T;
  onChange: (value: T) => void;
  options: SegmentOption<T>[];
  ariaLabel: string;
  className?: string;
}) {
  const refs = useRef<(HTMLButtonElement | null)[]>([]);
  function onKeyDown(event: KeyboardEvent, index: number) {
    const last = options.length - 1;
    let next: number | null = null;
    if (event.key === "ArrowRight" || event.key === "ArrowDown")
      next = index === last ? 0 : index + 1;
    else if (event.key === "ArrowLeft" || event.key === "ArrowUp")
      next = index === 0 ? last : index - 1;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = last;
    if (next === null || next === index) return;
    event.preventDefault();
    onChange(options[next].value);
    refs.current[next]?.focus();
  }
  return (
    <div
      role="radiogroup"
      aria-label={ariaLabel}
      className={cn(
        "flex items-center gap-0.5 rounded-lg bg-muted p-0.5",
        className,
      )}
    >
      {options.map((option, index) => {
        const active = option.value === value;
        return (
          <button
            key={option.value}
            ref={(el) => {
              refs.current[index] = el;
            }}
            type="button"
            role="radio"
            aria-checked={active}
            tabIndex={active ? 0 : -1}
            data-active={active || undefined}
            onClick={() => onChange(option.value)}
            onKeyDown={(event) => onKeyDown(event, index)}
            className={cn(
              "flex flex-1 items-center justify-center gap-1.5 rounded-md px-2.5 py-1 text-xs font-medium whitespace-nowrap transition-colors",
              active
                ? "bg-background text-foreground shadow-xs"
                : "text-muted-foreground hover:text-foreground",
            )}
          >
            {option.icon}
            {option.label}
          </button>
        );
      })}
    </div>
  );
}

export function Empty({
  title,
  children,
  action,
}: {
  title: string;
  children?: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center gap-2 rounded-xl border border-dashed px-6 py-10 text-center">
      <h3 className="font-heading text-sm font-medium">{title}</h3>
      {children && (
        <p className="max-w-sm text-sm text-muted-foreground">{children}</p>
      )}
      {action}
    </div>
  );
}

export function ErrorNotice({ message }: { message: string }) {
  return (
    <div
      role="alert"
      className="rounded-lg border border-red-600/30 bg-red-500/10 px-3 py-2 text-sm text-red-700 dark:border-red-400/25 dark:bg-red-400/10 dark:text-red-300"
    >
      {message}
    </div>
  );
}

export function Loading({
  label = "Loading workspace data…",
}: {
  label?: string;
}) {
  return (
    <div
      role="status"
      className="flex items-center gap-2 py-8 text-sm text-muted-foreground"
    >
      <span className="size-4 animate-spin rounded-full border-2 border-muted-foreground/30 border-t-foreground" />
      {label}
    </div>
  );
}

export function JsonBlock({ value }: { value: unknown }) {
  return (
    <pre className="max-h-80 overflow-auto rounded-lg border bg-muted/40 p-3 font-mono text-xs break-words whitespace-pre-wrap">
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

export function Properties({ entries }: { entries: [string, ReactNode][] }) {
  return (
    <dl className="grid grid-cols-[minmax(7rem,auto)_1fr] gap-x-4 gap-y-1.5 text-sm">
      {entries.map(([key, value]) => (
        <div key={key} className="contents">
          <dt className="text-muted-foreground">{key}</dt>
          <dd className="min-w-0 break-words">{value}</dd>
        </div>
      ))}
    </dl>
  );
}
