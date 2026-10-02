import { useState, type ReactNode } from "react";
import { Check, Copy } from "lucide-react";
import type { Json } from "@/api/types";
import { useNow } from "@/lib/clock";
import { cn } from "@/lib/cn";
import { ago, duration, shortHash, shortId, stamp } from "@/lib/format";
import { markSolid, type Tone } from "@/lib/status";
import { Barrels } from "./art";
import { Tooltip } from "./overlay";

/** A relative time that stays current, with the absolute one on hover. */
export function Time({ at, className }: { at: number | null | undefined; className?: string }) {
  const now = useNow();
  if (at == null) return <span className={cn("text-fg-subtle", className)}>never</span>;
  return (
    <Tooltip content={stamp(at)}>
      <time
        dateTime={new Date(at * 1000).toISOString()}
        className={cn("whitespace-nowrap tabular", className)}
      >
        {ago(at, now)}
      </time>
    </Tooltip>
  );
}

/** A duration that keeps counting while `end` is unknown. */
export function Elapsed({
  start,
  end,
  className,
}: {
  start: number | null | undefined;
  end?: number | null;
  className?: string;
}) {
  if (start == null) return <span className="text-fg-subtle">—</span>;
  return end != null ? (
    <span className={cn("tabular whitespace-nowrap", className)}>{duration(end - start)}</span>
  ) : (
    <Ticking start={start} className={className} />
  );
}

function Ticking({ start, className }: { start: number; className?: string }) {
  const now = useNow();
  return <span className={cn("tabular whitespace-nowrap", className)}>{duration(now - start)}</span>;
}

export function CopyButton({ value, label = "Copy" }: { value: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      aria-label={copied ? "Copied" : label}
      title={copied ? "Copied" : label}
      onClick={(event) => {
        event.preventDefault();
        event.stopPropagation();
        void navigator.clipboard?.writeText(value).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1200);
        });
      }}
      className="inline-grid size-5 place-items-center rounded-xs text-fg-subtle motion-1 transition-colors hover:bg-accent-soft hover:text-fg [&_svg]:size-3"
    >
      {copied ? <Check className="text-ok-fg" /> : <Copy />}
    </button>
  );
}

/** A ULID, shortened to its random tail, full value on hover and copy. */
export function Id({
  value,
  copy = false,
  className,
}: {
  value: string;
  copy?: boolean;
  className?: string;
}) {
  return (
    <span className={cn("inline-flex items-center gap-0.5", className)}>
      <Tooltip content={<span className="font-mono">{value}</span>}>
        <span className="font-mono text-[0.92em] tracking-tight">{shortId(value)}</span>
      </Tooltip>
      {copy && <CopyButton value={value} label="Copy id" />}
    </span>
  );
}

export function Hash({ value, className }: { value: string | null | undefined; className?: string }) {
  if (!value) return <span className="text-fg-subtle">—</span>;
  return (
    <Tooltip content={<span className="font-mono break-all">{value}</span>}>
      <span className={cn("font-mono text-[0.92em] text-fg-muted", className)}>{shortHash(value)}</span>
    </Tooltip>
  );
}

/** A version: the generation of the write that made it (docs/versions.md). */
export function Generation({ value, className }: { value: number | null | undefined; className?: string }) {
  if (value == null) return <span className="text-fg-subtle">—</span>;
  return (
    <Tooltip content="The generation of the write that made this version">
      <span className={cn("font-mono text-[0.92em] text-fg-muted", className)}>g{value}</span>
    </Tooltip>
  );
}

/** Proportions as one bar of tone segments; the label says the numbers. */
export function SegmentBar({
  parts,
  className,
  label,
}: {
  parts: { tone: Tone; value: number; label: string }[];
  className?: string;
  label?: string;
}) {
  const total = parts.reduce((sum, p) => sum + p.value, 0);
  const text =
    label ??
    parts
      .filter((p) => p.value)
      .map((p) => `${p.value} ${p.label}`)
      .join(", ");
  return (
    <div
      role="img"
      aria-label={text}
      title={text}
      className={cn("flex h-1.5 min-w-12 overflow-hidden rounded-full bg-sunken", className)}
    >
      {total > 0 &&
        parts
          .filter((p) => p.value > 0)
          .map((p) => (
            <span
              key={p.label}
              className={cn("h-full border-r border-surface last:border-r-0", markSolid[p.tone])}
              style={{ width: `${(100 * p.value) / total}%` }}
            />
          ))}
    </div>
  );
}

export function Empty({
  title,
  children,
  action,
  compact,
}: {
  title: ReactNode;
  children?: ReactNode;
  action?: ReactNode;
  compact?: boolean;
}) {
  return (
    <div className={cn("flex flex-col items-center gap-2 px-6 text-center", compact ? "py-6" : "py-12")}>
      {!compact && <Barrels className="mb-2 w-28" />}
      <p className="font-display text-base text-fg">{title}</p>
      {children && <div className="max-w-md text-sm text-fg-muted">{children}</div>}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

export function Skeleton({ className }: { className?: string }) {
  return (
    <div
      aria-hidden
      className={cn(
        "rounded-sm bg-sunken bg-[linear-gradient(90deg,transparent,var(--surface-2),transparent)] bg-[length:200px_100%] bg-no-repeat",
        "animate-[shimmer_1.4s_linear_infinite]",
        className,
      )}
    />
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  return (
    <kbd className="inline-grid h-5 min-w-5 place-items-center rounded-xs border-theme border-line-strong bg-surface px-1 font-mono text-2xs text-fg-muted">
      {children}
    </kbd>
  );
}

/** Pretty JSON with keys, strings and numbers told apart. */
export function JsonView({ value, className }: { value: Json | undefined; className?: string }) {
  return (
    <pre
      className={cn(
        "overflow-auto rounded-md bg-sunken p-3 font-mono text-xs leading-relaxed text-fg",
        className,
      )}
    >
      <JsonNode value={value ?? null} indent={0} />
    </pre>
  );
}

function JsonNode({ value, indent }: { value: Json; indent: number }): ReactNode {
  const pad = "  ".repeat(indent + 1);
  const end = "  ".repeat(indent);
  if (value === null) return <span className="text-fg-subtle">null</span>;
  if (typeof value === "string") return <span className="text-ok-fg">{JSON.stringify(value)}</span>;
  if (typeof value === "number" || typeof value === "boolean")
    return <span className="text-run-fg">{String(value)}</span>;
  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    return (
      <>
        {"[\n"}
        {value.map((item, i) => (
          <span key={i}>
            {pad}
            <JsonNode value={item} indent={indent + 1} />
            {i < value.length - 1 ? ",\n" : "\n"}
          </span>
        ))}
        {end}]
      </>
    );
  }
  const entries = Object.entries(value);
  if (entries.length === 0) return "{}";
  return (
    <>
      {"{\n"}
      {entries.map(([k, v], i) => (
        <span key={k}>
          {pad}
          <span className="text-wait-fg">{JSON.stringify(k)}</span>:{" "}
          <JsonNode value={v} indent={indent + 1} />
          {i < entries.length - 1 ? ",\n" : "\n"}
        </span>
      ))}
      {end}
      {"}"}
    </>
  );
}

export function ErrorNote({ error, title = "Couldn't load this" }: { error: unknown; title?: string }) {
  return (
    <div
      role="alert"
      className="rounded-md border-theme border-fail bg-fail-soft px-4 py-3 text-sm text-fail-fg"
    >
      <p className="font-medium">{title}</p>
      <p className="mt-0.5 opacity-90">{error instanceof Error ? error.message : String(error)}</p>
    </div>
  );
}

/** The footer of a paged list: how much is shown, and a way to the next page. */
export function LoadMore({
  query,
  shown,
}: {
  query: { hasNextPage: boolean; isFetchingNextPage: boolean; fetchNextPage: () => unknown };
  shown?: string;
}) {
  if (!query.hasNextPage) return null;
  return (
    <div className="flex items-center justify-between gap-3 border-t border-line px-4 py-2.5 text-xs text-fg-subtle">
      <span className="tabular">{shown}</span>
      <button
        type="button"
        onClick={() => query.fetchNextPage()}
        disabled={query.isFetchingNextPage}
        className="pressable h-7 rounded-sm border-theme border-line-strong bg-surface px-2.5 text-xs font-medium text-fg hover:bg-surface-2 disabled:opacity-50"
      >
        {query.isFetchingNextPage ? "Loading…" : "Load more"}
      </button>
    </div>
  );
}
