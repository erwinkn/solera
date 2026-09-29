import { useMemo, useState } from "react";
import { ArrowLeft, GitBranch, Play } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Eyebrow, Segmented } from "@/components/common";
import { useQuery } from "@/lib/api";
import { count, failures, seconds, time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type {
  AssetHistory as History,
  CatalogAsset,
  Json,
  Lineage,
  LineageNode,
  Materialization,
  PartitionScope,
  Stats,
  VersionRef,
} from "@/lib/types";

const ALL = "*";
const WEEK = 7 * 86400;

function short(version: string | null | undefined) {
  if (!version) return "—";
  return version.length > 10 ? version.slice(0, 10) : version;
}

function display(value: Json) {
  if (typeof value === "number") return count(value);
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

/** An asset's versions over time: how it has been running, a chart per
    numeric metadata field, and the versions themselves, each with its
    lineage. */
export function AssetHistory({
  asset,
  scopes,
}: {
  asset: CatalogAsset;
  scopes: PartitionScope[];
}) {
  const { base } = useWorkspace();
  const outputs = asset.outputs.map((o) => o.name);
  const [output, setOutput] = useState(outputs[0]);
  const [scope, setScope] = useState(ALL);
  const [lineage, setLineage] = useState<VersionRef | null>(null);
  const partitioned = !!asset.partitions;
  const params = new URLSearchParams({ output: output ?? "", limit: "200" });
  if (partitioned && scope !== ALL) params.set("scope", scope);
  if (!partitioned) params.set("scope", "");
  const history = useQuery<History>(
    base && output ? `${base}/assets/${asset.name}/history?${params}` : null,
    5000,
  );
  const minute = Math.floor(Date.now() / 60000) * 60;
  const statsParams = new URLSearchParams({
    asset: asset.name,
    since: String(minute - WEEK),
  });
  if (partitioned && scope !== ALL) statsParams.set("scope", scope);
  const stats = useQuery<Stats>(
    base ? `${base}/stats?${statsParams}` : null,
    15000,
  );
  const made = history.data?.materializations ?? [];
  const week = stats.data?.assets[0];
  const charted = !partitioned || scope !== ALL;

  return (
    <section className="flex flex-col gap-3" data-section="history">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <Eyebrow>History</Eyebrow>
        <div className="flex flex-wrap items-center gap-2">
          {outputs.length > 1 && (
            <Segmented
              ariaLabel="Output"
              value={output}
              onChange={setOutput}
              options={outputs.map((o) => ({ value: o, label: o }))}
            />
          )}
          {partitioned && (
            <Select
              value={scope}
              onValueChange={(value) => setScope(String(value ?? ALL))}
              items={[
                { value: ALL, label: "All partitions" },
                ...scopes.map((s) => ({ value: s.scope, label: s.scope })),
              ]}
            >
              <SelectTrigger size="sm" aria-label="Partition">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL}>All partitions</SelectItem>
                {scopes.map((s) => (
                  <SelectItem key={s.scope} value={s.scope}>
                    {s.scope}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          )}
        </div>
      </div>

      {week && week.tasks + week.skipped > 0 && (
        <dl
          className="grid grid-cols-2 gap-px overflow-hidden rounded-lg border bg-border text-xs"
          aria-label="Last 7 days"
        >
          {(
            [
              [
                "runs · 7 days",
                `${count(week.tasks)}${week.skipped ? ` +${count(week.skipped)} skipped` : ""}`,
              ],
              ["failed", failures(week.failed, week.tasks)],
              ["took · p50 p95", `${seconds(week.p50)} ${seconds(week.p95)}`],
              [
                "waited · p50 p95",
                `${seconds(week.wait_p50)} ${seconds(week.wait_p95)}`,
              ],
            ] as const
          ).map(([label, value]) => (
            <div
              key={label}
              className="flex flex-col gap-0.5 bg-card px-3 py-2"
            >
              <dt className="text-[0.65rem] whitespace-nowrap text-muted-foreground">
                {label}
              </dt>
              <dd className="font-mono whitespace-nowrap tabular-nums">
                {value}
              </dd>
            </div>
          ))}
        </dl>
      )}

      {charted && <MetadataCharts versions={made} />}
      {!charted && made.length > 0 && (
        <p className="text-[0.7rem] text-muted-foreground">
          Pick a partition to chart its metadata.
        </p>
      )}

      <VersionTimeline
        versions={made}
        showScope={partitioned && scope === ALL}
        onLineage={setLineage}
      />
      {history.data && !made.length && (
        <p className="text-xs text-muted-foreground">
          No versions recorded yet.
        </p>
      )}
      <LineageDialog start={lineage} onClose={() => setLineage(null)} />
    </section>
  );
}

/** One sparkline per numeric field across versions, oldest to newest. */
function MetadataCharts({ versions }: { versions: Materialization[] }) {
  const series = useMemo(() => {
    const ordered = [...versions].reverse();
    const fields = new Map<string, { at: number; value: number }[]>();
    const add = (name: string, at: number, value: unknown) => {
      if (typeof value !== "number" || !Number.isFinite(value)) return;
      if (!fields.has(name)) fields.set(name, []);
      fields.get(name)!.push({ at, value });
    };
    for (const v of ordered) {
      add("rows", v.at, v.rows);
      for (const [name, value] of Object.entries(v.metadata ?? {}))
        if (name !== "rows") add(name, v.at, value);
    }
    return [...fields.entries()].filter(([, points]) => points.length >= 2);
  }, [versions]);
  if (!series.length) return null;
  return (
    <div className="grid gap-2 sm:grid-cols-2" aria-label="Metadata charts">
      {series.map(([name, points]) => (
        <Sparkline key={name} name={name} points={points} />
      ))}
    </div>
  );
}

function Sparkline({
  name,
  points,
}: {
  name: string;
  points: { at: number; value: number }[];
}) {
  const W = 240;
  const H = 44;
  const values = points.map((p) => p.value);
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  // One step per version, not per second: versions come in bursts, and a
  // time axis would crush a busy afternoon against the right edge.
  const x = (i: number) => (i / (points.length - 1)) * W;
  const y = (v: number) =>
    hi === lo ? H / 2 : H - 3 - ((v - lo) / (hi - lo)) * (H - 6);
  const path = points
    .map((p, i) => `${x(i).toFixed(1)},${y(p.value).toFixed(1)}`)
    .join(" ");
  const last = points[points.length - 1];
  const previous = points[points.length - 2];
  const change = previous.value
    ? (last.value - previous.value) / Math.abs(previous.value)
    : null;
  return (
    <figure
      className="flex flex-col gap-1 rounded-lg border px-3 py-2"
      data-chart={name}
    >
      <figcaption className="flex items-baseline justify-between gap-2">
        <span className="truncate font-mono text-xs">{name}</span>
        <span className="flex items-baseline gap-1.5 font-mono text-sm tabular-nums">
          {count(last.value)}
          {change !== null && Math.abs(change) >= 0.005 && (
            <span
              className={cn(
                "text-[0.65rem]",
                change > 0
                  ? "text-emerald-600 dark:text-emerald-400"
                  : "text-amber-600 dark:text-amber-400",
              )}
            >
              {change > 0 ? "+" : ""}
              {Math.round(change * 100)}%
            </span>
          )}
        </span>
      </figcaption>
      <div className="relative text-primary">
        <svg
          viewBox={`0 0 ${W} ${H}`}
          preserveAspectRatio="none"
          className="h-11 w-full overflow-visible"
          role="img"
          aria-label={`${name} over ${points.length} versions, from ${count(values[0])} to ${count(last.value)}`}
        >
          <polyline
            points={path}
            fill="none"
            stroke="currentColor"
            strokeWidth={1.5}
            vectorEffect="non-scaling-stroke"
            strokeLinejoin="round"
          />
        </svg>
        <span
          className="absolute size-1.5 -translate-1/2 rounded-full bg-current"
          style={{ left: "100%", top: `${(y(last.value) / H) * 100}%` }}
        />
      </div>
      <div className="flex justify-between text-[0.6rem] text-muted-foreground tabular-nums">
        <span>
          {points.length} versions since {time(points[0].at)}
        </span>
        <span>
          {count(lo)} – {count(hi)}
        </span>
      </div>
    </figure>
  );
}

function VersionTimeline({
  versions,
  showScope,
  onLineage,
}: {
  versions: Materialization[];
  showScope: boolean;
  onLineage: (ref: VersionRef) => void;
}) {
  const { select } = useWorkspace();
  const [all, setAll] = useState(false);
  const shown = all ? versions : versions.slice(0, 12);
  if (!versions.length) return null;
  return (
    <div className="flex flex-col">
      <ol className="flex flex-col" aria-label="Versions">
        {shown.map((v, i) => {
          const metadata = Object.entries(v.metadata ?? {}).filter(
            ([k]) => k !== "rows",
          );
          return (
            <li
              key={`${v.at}:${v.output}:${v.scope}`}
              className="relative flex gap-3 pb-3 pl-4 last:pb-0"
              data-version={v.version ?? ""}
            >
              <span
                className={cn(
                  "absolute top-1.5 left-0 size-2 rounded-full",
                  i === 0 ? "bg-primary" : "bg-muted-foreground/40",
                )}
              />
              {i < shown.length - 1 && (
                <span className="absolute top-4 bottom-0 left-[3.5px] w-px bg-border" />
              )}
              <div className="flex min-w-0 flex-1 flex-col gap-1">
                <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-xs">
                  <span
                    className="font-mono font-medium"
                    title={v.version ?? ""}
                  >
                    {short(v.version)}
                  </span>
                  {showScope && v.scope && (
                    <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-[0.7rem] text-muted-foreground">
                      {v.scope}
                    </span>
                  )}
                  <span className="text-muted-foreground">{time(v.at)}</span>
                  {v.rows != null && (
                    <span className="text-muted-foreground tabular-nums">
                      {count(v.rows)} rows
                    </span>
                  )}
                  {(!!v.added || !!v.removed) && (
                    <span className="font-mono text-[0.7rem] tabular-nums">
                      <span className="text-emerald-600 dark:text-emerald-400">
                        +{count(v.added ?? 0)}
                      </span>{" "}
                      <span className="text-red-600 dark:text-red-400">
                        −{count(v.removed ?? 0)}
                      </span>
                    </span>
                  )}
                </div>
                {metadata.length > 0 && (
                  <div className="flex flex-wrap gap-1">
                    {metadata.map(([k, value]) => (
                      <span
                        key={k}
                        className="max-w-60 truncate rounded border px-1.5 font-mono text-[0.65rem] text-muted-foreground"
                        title={`${k}: ${JSON.stringify(value)}`}
                      >
                        {k}{" "}
                        <span className="text-foreground">
                          {display(value)}
                        </span>
                      </span>
                    ))}
                  </div>
                )}
              </div>
              <span className="-mt-1 flex shrink-0 gap-0.5">
                <Button
                  variant="ghost"
                  size="icon-xs"
                  title="Lineage"
                  aria-label={`Lineage of ${short(v.version)}`}
                  onClick={() =>
                    onLineage({
                      output: v.output,
                      scope: v.scope,
                      version: v.version,
                    })
                  }
                >
                  <GitBranch />
                </Button>
                {v.run && (
                  <Button
                    variant="ghost"
                    size="icon-xs"
                    title="Open run"
                    aria-label={`Open run ${v.run}`}
                    onClick={() => select({ kind: "run", id: v.run! })}
                  >
                    <Play />
                  </Button>
                )}
              </span>
            </li>
          );
        })}
      </ol>
      {versions.length > shown.length && (
        <button
          className="mt-2 self-start text-xs text-muted-foreground hover:text-foreground"
          onClick={() => setAll(true)}
        >
          Show all {versions.length} versions
        </button>
      )}
    </div>
  );
}

const refKey = (r: VersionRef) =>
  `${r.output}\u0000${r.scope}\u0000${r.version ?? ""}`;

interface TreeNode {
  node: LineageNode;
  param: string | null;
  children: TreeNode[];
}

/** Edges as a tree rooted at `root`: inputs for upstream, consumers for
    downstream. A version met twice is shown once, where it is nearest. */
function tree(data: Lineage | null): TreeNode[] {
  if (!data) return [];
  const nodes = new Map(data.nodes.map((n) => [refKey(n), n]));
  const next = new Map<string, { ref: VersionRef; param: string }[]>();
  for (const e of data.edges) {
    const [near, far] =
      data.direction === "upstream" ? [e.to, e.from] : [e.from, e.to];
    const k = refKey(near);
    if (!next.has(k)) next.set(k, []);
    next.get(k)!.push({ ref: far, param: e.param });
  }
  const seen = new Set([refKey(data.root)]);
  const grow = (k: string): TreeNode[] => {
    const out: TreeNode[] = [];
    for (const { ref, param } of next.get(k) ?? []) {
      const fk = refKey(ref);
      if (seen.has(fk)) continue;
      seen.add(fk);
      out.push({
        node: nodes.get(fk) ?? { ...ref, current: false },
        param,
        children: [],
      });
    }
    for (const child of out) child.children = grow(refKey(child.node));
    return out;
  };
  return grow(refKey(data.root));
}

/** What a version was built from, and what was built from it. Clicking a
    version re-centres the view on it. */
function LineageDialog({
  start,
  onClose,
}: {
  start: VersionRef | null;
  onClose: () => void;
}) {
  const { base, select } = useWorkspace();
  const [trail, setTrail] = useState<VersionRef[]>([]);
  const [opened, setOpened] = useState<VersionRef | null>(null);
  if (start !== opened) {
    setOpened(start);
    setTrail(start ? [start] : []);
  }
  const focus = trail[trail.length - 1] ?? null;
  const path = (direction: string) =>
    base && focus
      ? `${base}/outputs/${encodeURIComponent(focus.output)}/lineage?${new URLSearchParams(
          {
            scope: focus.scope,
            ...(focus.version ? { version: focus.version } : {}),
            direction,
            depth: "4",
          },
        )}`
      : null;
  const up = useQuery<Lineage>(path("upstream"), 30000);
  const down = useQuery<Lineage>(path("downstream"), 30000);
  const root =
    up.data?.nodes.find((n) => focus && refKey(n) === refKey(focus)) ??
    (focus ? { ...focus, current: false } : null);

  const open = (ref: VersionRef) => setTrail((t) => [...t, ref]);

  return (
    <Dialog open={!!start} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-h-[85dvh] overflow-y-auto sm:max-w-4xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            {trail.length > 1 && (
              <Button
                variant="ghost"
                size="icon-xs"
                aria-label="Back"
                onClick={() => setTrail((t) => t.slice(0, -1))}
              >
                <ArrowLeft />
              </Button>
            )}
            Lineage
          </DialogTitle>
          <DialogDescription>
            The versions this one was built from, and the versions built from
            it.
          </DialogDescription>
        </DialogHeader>
        {root && (
          <div className="grid gap-4 md:grid-cols-3 md:items-start">
            <LineageSide
              label="Built from"
              trees={tree(up.data)}
              loading={!up.data}
              empty="Read no recorded inputs."
              onOpen={open}
            />
            <div className="flex min-w-0 flex-col gap-1">
              <Eyebrow>This version</Eyebrow>
              <VersionCard
                node={root}
                focused
                onRun={(id) => select({ kind: "run", id })}
              />
            </div>
            <LineageSide
              label="Used by"
              trees={tree(down.data)}
              loading={!down.data}
              empty="Nothing has read it yet."
              onOpen={open}
            />
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

function LineageSide({
  label,
  trees,
  loading,
  empty,
  onOpen,
}: {
  label: string;
  trees: TreeNode[];
  loading: boolean;
  empty: string;
  onOpen: (ref: VersionRef) => void;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-1" aria-label={label}>
      <Eyebrow>{label}</Eyebrow>
      {loading ? (
        <p className="text-xs text-muted-foreground">Loading…</p>
      ) : trees.length ? (
        <TreeList trees={trees} onOpen={onOpen} />
      ) : (
        <p className="text-xs text-muted-foreground">{empty}</p>
      )}
    </div>
  );
}

function TreeList({
  trees,
  onOpen,
}: {
  trees: TreeNode[];
  onOpen: (ref: VersionRef) => void;
}) {
  return (
    <ul className="flex flex-col gap-1.5">
      {trees.map((t) => (
        <li key={refKey(t.node)} className="flex flex-col gap-1.5">
          <button className="text-left" onClick={() => onOpen(t.node)}>
            <VersionCard node={t.node} param={t.param} />
          </button>
          {t.children.length > 0 && (
            <div className="border-l pl-3">
              <TreeList trees={t.children} onOpen={onOpen} />
            </div>
          )}
        </li>
      ))}
    </ul>
  );
}

function VersionCard({
  node,
  param,
  focused,
  onRun,
}: {
  node: LineageNode;
  param?: string | null;
  focused?: boolean;
  onRun?: (id: string) => void;
}) {
  return (
    <div
      className={cn(
        "flex flex-col gap-0.5 rounded-lg border px-2.5 py-1.5 text-xs",
        focused ? "border-primary/40 bg-primary/5" : "hover:bg-muted/60",
      )}
      data-lineage={node.output}
    >
      <div className="flex items-start gap-1.5">
        <span className="min-w-0 font-mono font-medium break-all">
          {node.output}
        </span>
        {node.scope && (
          <span className="rounded bg-muted px-1 font-mono text-[0.65rem] text-muted-foreground">
            {node.scope}
          </span>
        )}
        <span
          className={cn(
            "ml-auto shrink-0 rounded-full px-1.5 text-[0.6rem]",
            node.current
              ? "bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
              : "bg-muted text-muted-foreground",
          )}
        >
          {node.current ? "current" : "superseded"}
        </span>
      </div>
      <div className="flex flex-wrap items-center gap-x-2 text-muted-foreground">
        <span className="font-mono" title={node.version ?? ""}>
          {short(node.version)}
        </span>
        {node.at && <span>{time(node.at)}</span>}
        {node.rows != null && (
          <span className="tabular-nums">{count(node.rows)} rows</span>
        )}
        {param && param !== node.output && (
          <span className="font-mono">as {param}</span>
        )}
        {onRun && node.run && (
          <button
            className="text-primary hover:underline"
            onClick={() => onRun(node.run!)}
          >
            run {node.run.slice(0, 8)}
          </button>
        )}
      </div>
    </div>
  );
}
