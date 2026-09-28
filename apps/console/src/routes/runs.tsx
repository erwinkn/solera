import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Link, createFileRoute, useNavigate } from "@tanstack/react-router";
import { ArrowRight, RefreshCw, Search, X, ZoomOut } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Empty,
  ErrorNotice,
  Eyebrow,
  PageHeader,
  Segmented,
  StatusBadge,
  statusFill,
  statusOrder,
} from "@/components/common";
import { useQuery } from "@/lib/api";
import { bucketLabel, count, seconds, time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type { Facets, Histogram, RunPage, RunRow } from "@/lib/types";

// The fields a run listing filters on; each takes several values (any matches).
const FIELDS = [
  "status",
  "trigger",
  "asset",
  "tag",
  "automation",
  "by",
  "source",
] as const;
type Field = (typeof FIELDS)[number];

const RANGES = {
  "1h": 3600,
  "24h": 86400,
  "7d": 7 * 86400,
  "30d": 30 * 86400,
  all: null,
} as const;
type Range = keyof typeof RANGES;

type RunSearch = Partial<Record<Field, string[]>> & {
  q?: string;
  range?: Range;
  /** A window picked on the histogram; overrides `range`. */
  window?: [number, number];
};

const PAGE = 50;
const MAX = 500;

export const Route = createFileRoute("/runs")({
  component: RunsPage,
  validateSearch: (raw: Record<string, unknown>): RunSearch => {
    const out: RunSearch = {};
    for (const field of FIELDS) {
      const value = raw[field];
      const list = (Array.isArray(value) ? value : value ? [value] : []).map(
        String,
      );
      if (list.length) out[field] = list;
    }
    if (typeof raw.q === "string" && raw.q) out.q = raw.q;
    if (typeof raw.range === "string" && raw.range in RANGES)
      out.range = raw.range as Range;
    if (
      Array.isArray(raw.window) &&
      raw.window.length === 2 &&
      raw.window.every((n) => typeof n === "number")
    )
      out.window = raw.window as [number, number];
    return out;
  },
});

/** The API query string for a filter. `since` is rounded to the minute so the
    polled paths stay stable between renders. */
function filterQuery(search: RunSearch) {
  const params = new URLSearchParams();
  for (const field of FIELDS)
    for (const value of search[field] ?? []) params.append(field, value);
  if (search.q) params.set("q", search.q);
  if (search.window) {
    params.set("since", String(search.window[0]));
    params.set("until", String(search.window[1]));
  } else {
    const span = RANGES[search.range ?? "7d"];
    if (span !== null) {
      const minute = Math.floor(Date.now() / 60000) * 60;
      params.set("since", String(minute - span));
    }
  }
  return params;
}

function withParams(path: string, params: URLSearchParams) {
  const text = params.toString();
  return text ? `${path}?${text}` : path;
}

function RunsPage() {
  const { base, diagnostics, select, refresh } = useWorkspace();
  const search = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const [limit, setLimit] = useState(PAGE);
  const params = filterQuery(search);
  const key = params.toString();
  useEffect(() => setLimit(PAGE), [key]);

  const listParams = new URLSearchParams(params);
  listParams.set("limit", String(limit));
  const page = useQuery<RunPage>(
    base ? withParams(`${base}/runs`, listParams) : null,
    3000,
  );
  const facets = useQuery<Facets>(
    base ? withParams(`${base}/runs:facets`, params) : null,
    5000,
  );
  const histogram = useQuery<Histogram>(
    base ? withParams(`${base}/runs:histogram`, params) : null,
    5000,
  );

  function update(next: Partial<RunSearch>) {
    void navigate({
      search: (current: RunSearch) => {
        const merged: RunSearch = { ...current, ...next };
        for (const [k, v] of Object.entries(merged))
          if (v === undefined || (Array.isArray(v) && !v.length))
            delete merged[k as keyof RunSearch];
        return merged;
      },
      replace: true,
    });
  }

  function toggle(field: Field, value: string) {
    const current = search[field] ?? [];
    update({
      [field]: current.includes(value)
        ? current.filter((v) => v !== value)
        : [...current, value],
    });
  }

  if (!diagnostics) return null;
  const runs = page.data?.runs ?? [];
  const chips = FIELDS.flatMap((field) =>
    (search[field] ?? []).map((value) => ({ field, value })),
  );
  const filtered = chips.length > 0 || !!search.q || !!search.window;

  return (
    <section className="flex flex-col gap-4">
      <PageHeader
        eyebrow="Execution"
        title="Runs"
        description="Every run and source commit, from the run history."
        aside={
          <div className="flex items-center gap-4">
            {!!diagnostics.active_runs && (
              <div className="flex flex-col items-end">
                <span className="text-xl font-semibold text-sky-600 tabular-nums dark:text-sky-400">
                  {diagnostics.active_runs}
                </span>
                <span className="text-xs text-muted-foreground">active</span>
              </div>
            )}
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                refresh();
                page.refresh();
                facets.refresh();
                histogram.refresh();
              }}
            >
              <RefreshCw />
              Refresh
            </Button>
          </div>
        }
      />

      <div className="flex flex-wrap items-center gap-3">
        <SearchBox value={search.q ?? ""} onChange={(q) => update({ q })} />
        <Segmented<Range>
          ariaLabel="Time range"
          value={search.window ? ("" as Range) : (search.range ?? "7d")}
          onChange={(range) => update({ range, window: undefined })}
          options={(Object.keys(RANGES) as Range[]).map((r) => ({
            value: r,
            label: r === "all" ? "All" : r,
          }))}
        />
      </div>

      {(chips.length > 0 || search.window) && (
        <div
          className="flex flex-wrap items-center gap-1.5"
          aria-label="Filters"
        >
          {search.window && (
            <Chip
              label={`${time(search.window[0])} – ${time(search.window[1])}`}
              icon={<ZoomOut className="size-3" />}
              onRemove={() => update({ window: undefined })}
            />
          )}
          {chips.map(({ field, value }) => (
            <Chip
              key={`${field}:${value}`}
              label={`${field}: ${field === "status" && value === "skipped" ? "unchanged" : value}`}
              onRemove={() => toggle(field, value)}
            />
          ))}
          <button
            className="px-1 text-xs text-muted-foreground hover:text-foreground"
            onClick={() =>
              update({
                ...Object.fromEntries(FIELDS.map((f) => [f, undefined])),
                q: undefined,
                window: undefined,
              })
            }
          >
            Clear all
          </button>
        </div>
      )}

      {histogram.data && (
        <RunHistogram
          data={histogram.data}
          onPick={(t) => update({ window: [t, t + histogram.data!.bucket] })}
        />
      )}

      <div className="grid items-start gap-4 lg:grid-cols-[15rem_minmax(0,1fr)]">
        <FacetPanel facets={facets.data} search={search} onToggle={toggle} />
        <div className="flex min-w-0 flex-col gap-3">
          {page.error && <ErrorNotice message={page.error.message} />}
          {page.data && !runs.length ? (
            <Empty
              title={filtered ? "No matching runs" : "No runs yet"}
              action={
                !filtered && (
                  <Button variant="outline" render={<Link to="/assets" />}>
                    Browse assets
                  </Button>
                )
              }
            >
              {filtered
                ? "Loosen a filter or widen the time range."
                : "Materialize an asset to create the first run."}
            </Empty>
          ) : (
            <RunTable
              runs={runs}
              onOpen={(id) => select({ kind: "run", id })}
              onTag={(tag) => toggle("tag", tag)}
              tagged={search.tag ?? []}
            />
          )}
          {page.data?.next &&
            (limit < MAX ? (
              <Button
                variant="outline"
                size="sm"
                className="self-center"
                onClick={() => setLimit((n) => Math.min(MAX, n + PAGE))}
              >
                Show more
              </Button>
            ) : (
              <p className="text-center text-xs text-muted-foreground">
                Showing the newest {MAX}. Narrow the time range to see older
                runs.
              </p>
            ))}
        </div>
      </div>
    </section>
  );
}

function SearchBox({
  value,
  onChange,
}: {
  value: string;
  onChange: (value: string | undefined) => void;
}) {
  const [text, setText] = useState(value);
  useEffect(() => setText(value), [value]);
  useEffect(() => {
    if (text === value) return;
    const timer = window.setTimeout(() => onChange(text || undefined), 300);
    return () => window.clearTimeout(timer);
  }, [text]);
  return (
    <div className="relative min-w-52 flex-1">
      <Search className="pointer-events-none absolute top-2.5 left-2.5 size-4 text-muted-foreground" />
      <Input
        type="search"
        className="pl-8"
        placeholder="Search errors or run ids…"
        aria-label="Search runs"
        value={text}
        onChange={(event) => setText(event.target.value)}
      />
    </div>
  );
}

function Chip({
  label,
  icon,
  onRemove,
}: {
  label: string;
  icon?: ReactNode;
  onRemove: () => void;
}) {
  return (
    <span className="flex items-center gap-1 rounded-full border bg-muted/40 py-0.5 pr-1 pl-2 text-xs">
      {icon}
      <span className="font-mono">{label}</span>
      <button
        aria-label={`Remove ${label}`}
        className="rounded-full p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
        onClick={onRemove}
      >
        <X className="size-3" />
      </button>
    </span>
  );
}

/** Runs per time bucket, stacked by status. Click a bar to zoom into it. */
function RunHistogram({
  data,
  onPick,
}: {
  data: Histogram;
  onPick: (t: number) => void;
}) {
  const bars = useMemo(() => {
    if (data.since === null) return [];
    const byT = new Map(data.bars.map((b) => [b.t, b.counts]));
    const out = [];
    for (let t = data.since; t <= data.until; t += data.bucket)
      out.push({ t, counts: byT.get(t) ?? {} });
    return out;
  }, [data]);
  const total = (counts: Record<string, number>) =>
    Object.values(counts).reduce((a, b) => a + b, 0);
  const peak = Math.max(1, ...bars.map((b) => total(b.counts)));
  const runs = bars.reduce((n, b) => n + total(b.counts), 0);
  if (!bars.length) return null;
  return (
    <div className="flex flex-col gap-1.5 rounded-xl border bg-card p-3">
      <div className="flex items-baseline justify-between gap-2">
        <Eyebrow>Runs over time</Eyebrow>
        <span className="text-xs text-muted-foreground tabular-nums">
          {count(runs)} runs · {seconds(data.bucket)} buckets
        </span>
      </div>
      <div
        className="flex h-20 items-end gap-px"
        role="img"
        aria-label="Runs over time"
      >
        {bars.map(({ t, counts }) => {
          const n = total(counts);
          return (
            <button
              key={t}
              data-bar={t}
              title={`${bucketLabel(t, data.bucket)}\n${
                statusOrder
                  .filter((s) => counts[s])
                  .map((s) => `${counts[s]} ${s}`)
                  .join("\n") || "no runs"
              }`}
              onClick={() => n && onPick(t)}
              className={cn(
                "flex h-full min-w-0 flex-1 flex-col-reverse rounded-[2px]",
                n ? "cursor-zoom-in hover:opacity-80" : "cursor-default",
              )}
            >
              {statusOrder
                .filter((s) => counts[s])
                .map((s) => (
                  <span
                    key={s}
                    className={cn(
                      "w-full first:rounded-b-[2px] last:rounded-t-[2px]",
                      statusFill[s],
                    )}
                    style={{ height: `${(counts[s] / peak) * 100}%` }}
                  />
                ))}
              {!n && <span className="h-px w-full bg-border" />}
            </button>
          );
        })}
      </div>
      <div className="flex justify-between text-[0.65rem] text-muted-foreground tabular-nums">
        <span>{bucketLabel(bars[0].t, data.bucket)}</span>
        <span>{bucketLabel(bars[bars.length - 1].t, data.bucket)}</span>
      </div>
    </div>
  );
}

const FACET_LABELS: Record<Field, string> = {
  status: "Status",
  trigger: "Trigger",
  asset: "Asset",
  tag: "Tag",
  automation: "Automation",
  by: "Requested by",
  source: "Source",
};

/** Value counts per field, each counted with every other filter applied. */
function FacetPanel({
  facets,
  search,
  onToggle,
}: {
  facets: Facets | null;
  search: RunSearch;
  onToggle: (field: Field, value: string) => void;
}) {
  const [open, setOpen] = useState<Partial<Record<Field, boolean>>>({});
  if (!facets) return <div className="hidden lg:block" />;
  const groups = FIELDS.filter(
    (f) => facets[f]?.length || (search[f] ?? []).length,
  );
  return (
    <aside
      aria-label="Facets"
      className="flex flex-col gap-4 rounded-xl border bg-card p-3 lg:sticky lg:top-4"
    >
      {groups.map((field) => {
        const selected = search[field] ?? [];
        const values = [...(facets[field] ?? [])];
        for (const value of selected)
          if (!values.some((v) => v.value === value))
            values.push({ value, count: 0 });
        const peak = Math.max(1, ...values.map((v) => v.count));
        const shown = open[field] ? values : values.slice(0, 6);
        return (
          <div key={field} className="flex flex-col gap-1" data-facet={field}>
            <Eyebrow>{FACET_LABELS[field]}</Eyebrow>
            {shown.map(({ value, count: n }) => {
              const active = selected.includes(value);
              return (
                <button
                  key={value}
                  aria-pressed={active}
                  onClick={() => onToggle(field, value)}
                  className={cn(
                    "relative flex items-center gap-2 overflow-hidden rounded-md px-2 py-1 text-left text-xs transition-colors",
                    active
                      ? "bg-primary/10 text-foreground ring-1 ring-primary/30"
                      : "text-muted-foreground hover:bg-muted hover:text-foreground",
                  )}
                >
                  <span
                    className="absolute inset-y-0 left-0 bg-muted/70"
                    style={{ width: `${(n / peak) * 100}%` }}
                    aria-hidden
                  />
                  {field === "status" && (
                    <span
                      className={cn(
                        "relative size-1.5 shrink-0 rounded-full",
                        statusFill[value] ?? "bg-muted-foreground/50",
                      )}
                    />
                  )}
                  <span className="relative min-w-0 flex-1 truncate font-mono">
                    {field === "status" && value === "skipped"
                      ? "unchanged"
                      : value}
                  </span>
                  <span className="relative tabular-nums">{count(n)}</span>
                </button>
              );
            })}
            {values.length > 6 && (
              <button
                className="self-start px-2 text-[0.7rem] text-muted-foreground hover:text-foreground"
                onClick={() => setOpen((o) => ({ ...o, [field]: !o[field] }))}
              >
                {open[field] ? "Show fewer" : `Show all ${values.length}`}
              </button>
            )}
          </div>
        );
      })}
      {!groups.length && (
        <p className="text-xs text-muted-foreground">Nothing to filter yet.</p>
      )}
    </aside>
  );
}

function selectionLabel(run: RunRow) {
  if (run.trigger === "commit") return "source commit";
  const scopes = Array.isArray(run.partitions)
    ? `${run.partitions.length} scope${run.partitions.length === 1 ? "" : "s"}`
    : run.partitions;
  return [scopes, run.mode].filter(Boolean).join(" · ");
}

function origin(run: RunRow) {
  if (run.trigger === "automation") return run.automation;
  if (run.trigger === "commit") return `${run.source} · ${run.by ?? "api"}`;
  return run.by ? `manual · ${run.by}` : "manual";
}

function RunTable({
  runs,
  onOpen,
  onTag,
  tagged,
}: {
  runs: RunRow[];
  onOpen: (id: string) => void;
  onTag: (tag: string) => void;
  tagged: string[];
}) {
  const now = Date.now() / 1000;
  return (
    <div className="overflow-x-auto rounded-xl border bg-card">
      <Table>
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead>Run</TableHead>
            <TableHead>Status</TableHead>
            <TableHead>Targets</TableHead>
            <TableHead>Requested</TableHead>
            <TableHead className="text-right">Duration</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {runs.map((run) => (
            <TableRow key={run.id} data-run={run.id}>
              <TableCell className="align-top">
                <button
                  className="flex items-center gap-1 font-mono text-xs font-medium text-primary hover:underline"
                  onClick={() => onOpen(run.id)}
                >
                  {run.id.slice(0, 8)}
                  <ArrowRight className="size-3" />
                </button>
                <span className="block max-w-44 truncate text-xs text-muted-foreground">
                  {origin(run)}
                </span>
              </TableCell>
              <TableCell className="max-w-72 align-top">
                <StatusBadge status={run.status} />
                {run.error && (
                  <span
                    className="mt-1 block truncate text-xs text-red-600 dark:text-red-400"
                    title={run.error}
                  >
                    {run.failed_count > 1 && `${run.failed_count} failed · `}
                    {run.error}
                  </span>
                )}
              </TableCell>
              <TableCell className="align-top">
                <span
                  className="block max-w-56 truncate font-mono text-xs"
                  title={run.targets.join(", ")}
                >
                  {run.targets.length === 1
                    ? run.targets[0]
                    : `${run.targets.length} assets`}
                </span>
                <span className="block text-xs text-muted-foreground tabular-nums">
                  {run.trigger === "commit"
                    ? selectionLabel(run)
                    : `${run.task_count} task${run.task_count === 1 ? "" : "s"} · ${selectionLabel(run)}`}
                  {run.upstream && (
                    <span className="ml-1 rounded bg-muted px-1 text-[0.65rem]">
                      +upstream
                    </span>
                  )}
                </span>
                {Object.keys(run.tags).length > 0 && (
                  <span className="mt-1 flex flex-wrap gap-1">
                    {Object.entries(run.tags).map(([k, v]) => {
                      const tag = `${k}=${v}`;
                      return (
                        <button
                          key={k}
                          onClick={() => onTag(tag)}
                          title={`Filter by ${tag}`}
                          className={cn(
                            "rounded border px-1 font-mono text-[0.65rem] text-muted-foreground hover:text-foreground",
                            tagged.includes(tag) &&
                              "border-primary/40 bg-primary/10 text-foreground",
                          )}
                        >
                          {tag}
                        </button>
                      );
                    })}
                  </span>
                )}
              </TableCell>
              <TableCell className="align-top text-sm text-muted-foreground">
                {time(run.created_at)}
              </TableCell>
              <TableCell className="text-right align-top font-mono text-xs text-muted-foreground tabular-nums">
                {seconds((run.finished_at ?? now) - run.created_at)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
