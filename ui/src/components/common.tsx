import type { ReactNode } from "react";
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
  waiting: "Waiting",
  running: "Running",
  paused: "Paused",
  succeeded: "Succeeded",
  skipped: "Unchanged",
  failed: "Failed",
  blocked: "Blocked",
  canceled: "Canceled",
};

const tones: Record<string, string> = {
  not_materialized: "border-border text-muted-foreground",
  materialized: "border-emerald-600/30 bg-emerald-500/10 text-emerald-700",
  stale: "border-amber-600/30 bg-amber-500/10 text-amber-700",
  partial: "border-amber-600/30 bg-amber-500/10 text-amber-700",
  queued: "border-border text-muted-foreground",
  waiting: "border-border text-muted-foreground",
  running: "border-sky-600/30 bg-sky-500/10 text-sky-700",
  paused: "border-amber-600/30 bg-amber-500/10 text-amber-700",
  succeeded: "border-emerald-600/30 bg-emerald-500/10 text-emerald-700",
  skipped: "border-border text-muted-foreground",
  failed: "border-red-600/30 bg-red-500/10 text-red-700",
  blocked: "border-red-600/30 bg-red-500/10 text-red-700",
  canceled: "border-border text-muted-foreground",
};

const dots: Record<string, string> = {
  not_materialized: "bg-muted-foreground/50",
  materialized: "bg-emerald-600",
  stale: "bg-amber-500",
  partial: "bg-amber-500",
  queued: "bg-muted-foreground/50",
  waiting: "bg-muted-foreground/50",
  running: "bg-sky-500 animate-pulse",
  paused: "bg-amber-500",
  succeeded: "bg-emerald-600",
  skipped: "bg-muted-foreground/50",
  failed: "bg-red-500",
  blocked: "bg-red-500",
  canceled: "bg-muted-foreground/50",
};

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
      className={cn("gap-1.5 font-medium", tones[status], className)}
    >
      <span className={cn("size-1.5 rounded-full", dots[status])} />
      {labels[status] ?? status.replaceAll("_", " ")}
    </Badge>
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
      className="rounded-lg border border-red-600/30 bg-red-500/10 px-3 py-2 text-sm text-red-700"
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
