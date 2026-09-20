import { useEffect, useMemo, useRef, useState } from "react";
import { createFileRoute, useNavigate } from "@tanstack/react-router";
import {
  Braces,
  Calendar,
  Inbox,
  LayoutGrid,
  Network,
  Play,
  Search,
  Table as TableIcon,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { assetStatus, upstreamNames } from "@/components/asset-sheet";
import {
  Eyebrow,
  PageHeader,
  Segmented,
  StatusBadge,
} from "@/components/common";
import { useQuery } from "@/lib/api";
import { time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type { CatalogAsset, Manifest, SourceDecl } from "@/lib/types";

export const Route = createFileRoute("/assets")({
  component: AssetsPage,
});

function placementLabel(asset: CatalogAsset) {
  const p = asset.placement;
  const options = Object.entries(p.placement)
    .map(([k, v]) => `${k}=${v}`)
    .join(" ");
  return p.kind + (options ? ` · ${options}` : "");
}

function partitionLabel(asset: CatalogAsset) {
  return asset.partitions
    ? Object.keys(asset.partitions.dims).join(" × ")
    : "—";
}

function assetIcon(asset: CatalogAsset) {
  if (!asset.outputs.length) return Braces; // a job
  if (asset.outputs.some((o) => o.partition_set)) return Calendar;
  return LayoutGrid;
}

// The dot beside a node / row: the asset's materialization state.
const NODE_DOT: Record<string, string> = {
  materialized: "bg-emerald-500",
  complete: "bg-emerald-500",
  stale: "bg-amber-500",
  partial: "bg-amber-500",
  not_materialized: "bg-muted-foreground/40",
  running: "bg-sky-500",
};

type EdgeKind = "whole" | "bykey" | "all_partitions" | "dep";

interface GraphNode {
  // IDs are namespaced by kind: a source and an asset may share a name.
  id: string;
  name: string;
  kind: "asset" | "source";
  asset?: CatalogAsset;
  source?: SourceDecl;
}

interface GraphEdge {
  from: string;
  to: string;
  kind: EdgeKind;
}

// Each edge kind gets a distinct stroke so lineage reads at a glance.
const EDGE_STYLE: Record<
  EdgeKind,
  { stroke: string; dash?: string; width: number; opacity: number }
> = {
  whole: { stroke: "var(--color-muted-foreground)", width: 1.5, opacity: 0.5 },
  bykey: {
    stroke: "var(--color-primary)",
    dash: "5 4",
    width: 1.6,
    opacity: 0.9,
  },
  all_partitions: {
    stroke: "oklch(0.606 0.25 292)",
    width: 2.25,
    opacity: 0.85,
  },
  dep: {
    stroke: "var(--color-muted-foreground)",
    dash: "1.5 4",
    width: 1.5,
    opacity: 0.55,
  },
};

function EdgeLegend() {
  const items: [EdgeKind, string][] = [
    ["whole", "whole"],
    ["bykey", "ByKey"],
    ["all_partitions", "AllPartitions"],
    ["dep", "dep"],
  ];
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-xs text-muted-foreground">
      <span className="font-medium text-foreground">Lineage</span>
      {items.map(([kind, label]) => {
        const s = EDGE_STYLE[kind];
        return (
          <span key={kind} className="flex items-center gap-1.5">
            <svg width="24" height="6" aria-hidden="true">
              <line
                x1="0"
                y1="3"
                x2="24"
                y2="3"
                stroke={s.stroke}
                strokeWidth={s.width}
                strokeDasharray={s.dash}
                opacity={s.opacity}
              />
            </svg>
            {label}
          </span>
        );
      })}
    </div>
  );
}

function Graph({
  assets,
  sources,
}: {
  assets: CatalogAsset[];
  sources: SourceDecl[];
}) {
  const { select } = useWorkspace();
  const navigate = useNavigate();

  const { nodes, edges, positions, width, height } = useMemo(() => {
    const nodes: GraphNode[] = [
      ...sources.map<GraphNode>((s) => ({
        id: `source:${s.name}`,
        name: s.name,
        kind: "source",
        source: s,
      })),
      ...assets.map<GraphNode>((a) => ({
        id: `asset:${a.name}`,
        name: a.name,
        kind: "asset",
        asset: a,
      })),
    ];
    const byId = new Map(nodes.map((n) => [n.id, n]));

    // output name -> owning node id (asset that declares it, or a source)
    const ownerOf = new Map<string, string>();
    for (const s of sources) ownerOf.set(s.name, `source:${s.name}`);
    for (const a of assets)
      for (const o of a.outputs) ownerOf.set(o.name, `asset:${a.name}`);

    const edges: GraphEdge[] = [];
    const seen = new Set<string>();
    const push = (from: string, to: string, kind: EdgeKind) => {
      if (!byId.has(from) || from === to) return;
      const key = `${from}|${to}|${kind}`;
      if (seen.has(key)) return;
      seen.add(key);
      edges.push({ from, to, kind });
    };
    for (const a of assets) {
      const to = `asset:${a.name}`;
      for (const edge of Object.values(a.inputs)) {
        const owner = ownerOf.get(edge.output);
        if (!owner) continue;
        const kind: EdgeKind =
          edge.kind === "bykey"
            ? "bykey"
            : edge.kind === "all_partitions"
              ? "all_partitions"
              : "whole";
        push(owner, to, kind);
      }
      for (const dep of a.deps) {
        const owner = ownerOf.get(dep);
        if (owner) push(owner, to, "dep");
      }
      for (const dim of Object.values(a.partitions?.dims ?? {})) {
        if (dim.kind === "set" && dim.output) {
          const owner = ownerOf.get(dim.output);
          if (owner) push(owner, to, "dep");
        }
      }
    }

    // Longest-path layering for left-to-right columns.
    const upstreamOf = new Map<string, string[]>();
    for (const n of nodes) upstreamOf.set(n.id, []);
    for (const e of edges) upstreamOf.get(e.to)!.push(e.from);
    const level = new Map<string, number>();
    const depth = (id: string, stack = new Set<string>()): number => {
      if (level.has(id)) return level.get(id)!;
      if (stack.has(id)) return 0;
      stack.add(id);
      const value = Math.max(
        0,
        ...(upstreamOf.get(id) ?? []).map((u) => depth(u, stack) + 1),
      );
      stack.delete(id);
      level.set(id, value);
      return value;
    };
    for (const n of nodes) depth(n.id);

    const columns = new Map<number, GraphNode[]>();
    for (const n of nodes) {
      const l = level.get(n.id)!;
      columns.set(l, [...(columns.get(l) ?? []), n]);
    }
    const NODE_W = 176;
    const NODE_H = 60;
    const COL_GAP = 108;
    const ROW_GAP = 26;
    const height = Math.max(
      360,
      ...[...columns.values()].map((c) => c.length * (NODE_H + ROW_GAP) + 40),
    );
    const width =
      (Math.max(0, ...level.values()) + 1) * (NODE_W + COL_GAP) + 24;
    const positions = new Map<string, { x: number; y: number }>();
    for (const [l, items] of columns) {
      const colHeight = items.length * (NODE_H + ROW_GAP) - ROW_GAP;
      const top = (height - colHeight) / 2;
      items.forEach((n, i) =>
        positions.set(n.id, {
          x: 24 + l * (NODE_W + COL_GAP),
          y: top + i * (NODE_H + ROW_GAP),
        }),
      );
    }
    return { nodes, edges, positions, width, height, NODE_W, NODE_H };
  }, [assets, sources]);

  const NODE_W = 176;
  const NODE_H = 60;

  return (
    <div
      className="graph-grid overflow-auto rounded-xl border bg-card"
      aria-label="Asset lineage graph"
    >
      <div className="relative" style={{ width, height, minWidth: "100%" }}>
        <svg
          className="absolute inset-0"
          width={width}
          height={height}
          aria-hidden="true"
        >
          <defs>
            {(Object.keys(EDGE_STYLE) as EdgeKind[]).map((kind) => (
              <marker
                key={kind}
                id={`arrow-${kind}`}
                viewBox="0 0 10 10"
                refX="8"
                refY="5"
                markerWidth="6"
                markerHeight="6"
                orient="auto"
              >
                <path
                  d="M0 0 10 5 0 10z"
                  fill={EDGE_STYLE[kind].stroke}
                  opacity={EDGE_STYLE[kind].opacity}
                />
              </marker>
            ))}
          </defs>
          {edges.map((edge) => {
            const from = positions.get(edge.from);
            const to = positions.get(edge.to);
            if (!from || !to) return null;
            const x1 = from.x + NODE_W;
            const y1 = from.y + NODE_H / 2;
            const x2 = to.x;
            const y2 = to.y + NODE_H / 2;
            const mid = (x1 + x2) / 2;
            const s = EDGE_STYLE[edge.kind];
            return (
              <path
                key={`${edge.from}:${edge.to}:${edge.kind}`}
                d={`M${x1},${y1} C${mid},${y1} ${mid},${y2} ${x2},${y2}`}
                fill="none"
                stroke={s.stroke}
                strokeWidth={s.width}
                strokeDasharray={s.dash}
                opacity={s.opacity}
                markerEnd={`url(#arrow-${edge.kind})`}
              />
            );
          })}
        </svg>
        {nodes.map((node) => {
          const pos = positions.get(node.id)!;
          const status =
            node.kind === "source" ? "materialized" : assetStatus(node.asset!);
          const Icon = node.kind === "source" ? Inbox : assetIcon(node.asset!);
          const meta =
            node.kind === "source"
              ? `${node.source!.store} · ${
                  node.source!.key === "<elements>"
                    ? "set"
                    : node.source!.key
                      ? "keyed"
                      : "value"
                }`
              : node.asset!.partitions
                ? partitionLabel(node.asset!)
                : node.asset!.placement.kind;
          return (
            <button
              key={node.id}
              className={cn(
                "absolute flex flex-col justify-center gap-1.5 rounded-xl border bg-popover px-3 text-left shadow-xs transition hover:-translate-y-px hover:border-primary/40 hover:shadow-sm focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
                node.kind === "source" && "border-dashed",
              )}
              style={{ left: pos.x, top: pos.y, width: NODE_W, height: NODE_H }}
              onClick={() =>
                node.kind === "source"
                  ? navigate({
                      to: "/sources",
                      hash: `source-${node.name}`,
                    })
                  : select({ kind: "asset", name: node.name })
              }
              aria-label={`Inspect ${node.name}`}
            >
              <span className="flex items-center gap-2">
                <Icon className="size-4 shrink-0 text-muted-foreground" />
                <strong
                  className="min-w-0 flex-1 truncate font-mono text-xs"
                  title={node.name}
                >
                  {node.name}
                </strong>
                <span
                  className={cn(
                    "size-2 shrink-0 rounded-sm",
                    NODE_DOT[status] ?? "bg-muted-foreground/40",
                  )}
                />
              </span>
              <span className="truncate text-[0.7rem] text-muted-foreground">
                {meta}
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function AssetsPage() {
  const { assets, select, openMaterialize, checked, setChecked, diagnostics } =
    useWorkspace();
  const manifest = useQuery<Manifest>(
    diagnostics ? `/projects/${diagnostics.project}/manifest` : null,
    30000,
  );
  const [search, setSearch] = useState("");
  const [view, setView] = useState<"table" | "graph">("table");
  const input = useRef<HTMLInputElement>(null);
  const filtered = assets.filter((asset) =>
    `${asset.name} ${asset.doc ?? ""}`
      .toLowerCase()
      .includes(search.toLowerCase()),
  );
  useEffect(() => {
    function shortcut(event: KeyboardEvent) {
      if (
        event.key === "/" &&
        !(
          event.target instanceof HTMLInputElement ||
          event.target instanceof HTMLTextAreaElement
        )
      ) {
        event.preventDefault();
        input.current?.focus();
      }
    }
    window.addEventListener("keydown", shortcut);
    return () => window.removeEventListener("keydown", shortcut);
  }, []);
  if (!diagnostics) return null;

  const counts = { materialized: 0, stale: 0, idle: 0 };
  for (const asset of assets) {
    const status = assetStatus(asset);
    if (status === "materialized") counts.materialized += 1;
    else if (status === "stale" || status === "partial") counts.stale += 1;
    else counts.idle += 1;
  }

  function toggle(name: string, on: boolean) {
    setChecked((current) =>
      on ? [...current, name] : current.filter((v) => v !== name),
    );
  }
  const ownerOf = (() => {
    const owners = new Map<string, string>();
    for (const asset of assets)
      for (const output of asset.outputs) owners.set(output.name, asset.name);
    return (output: string) => owners.get(output) ?? null;
  })();
  function headCount(asset: CatalogAsset) {
    return Object.values(asset.heads).reduce(
      (n, scopes) => n + Object.keys(scopes).length,
      0,
    );
  }
  function updatedAt(asset: CatalogAsset) {
    const times = Object.values(asset.heads).flatMap((scopes) =>
      Object.values(scopes).map((head) => head.at),
    );
    return times.length ? Math.max(...times) : null;
  }

  return (
    <section className="flex flex-col gap-4">
      <PageHeader
        eyebrow="Workspace"
        title="Assets"
        description="Data products, their dependencies, and what needs to run."
        aside={
          <div className="flex gap-5">
            {(
              [
                ["materialized", counts.materialized, "text-foreground"],
                ["stale", counts.stale, "text-amber-600 dark:text-amber-400"],
                ["idle", counts.idle, "text-muted-foreground"],
              ] as const
            ).map(([label, value, tone]) => (
              <div key={label} className="flex flex-col items-end">
                <span
                  className={cn("text-xl font-semibold tabular-nums", tone)}
                >
                  {value}
                </span>
                <span className="text-xs text-muted-foreground">{label}</span>
              </div>
            ))}
          </div>
        }
      />
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative min-w-52 flex-1">
          <Search className="pointer-events-none absolute top-2.5 left-2.5 size-4 text-muted-foreground" />
          <Input
            ref={input}
            type="search"
            className="pl-8"
            placeholder="Filter assets…  (press /)"
            aria-label="Filter assets"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </div>
        <Segmented
          ariaLabel="View"
          value={view}
          onChange={setView}
          options={[
            {
              value: "table",
              label: "Table",
              icon: <TableIcon className="size-3.5" />,
            },
            {
              value: "graph",
              label: "Graph",
              icon: <Network className="size-3.5" />,
            },
          ]}
        />
        <span className="text-xs text-muted-foreground tabular-nums">
          {filtered.length} assets
        </span>
      </div>
      {view === "graph" ? (
        <div className="flex flex-col gap-2">
          <EdgeLegend />
          <Graph
            assets={filtered}
            sources={Object.values(manifest.data?.sources ?? {})}
          />
        </div>
      ) : !filtered.length ? (
        <div className="rounded-xl border border-dashed px-6 py-10 text-center text-sm text-muted-foreground">
          No assets match “{search}”.
        </div>
      ) : (
        <div className="overflow-x-auto rounded-xl border bg-card">
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="w-8">
                  <Checkbox
                    aria-label="Select all listed assets"
                    checked={
                      !!filtered.length &&
                      filtered.every((asset) => checked.includes(asset.name))
                    }
                    onCheckedChange={(value) =>
                      setChecked(
                        value === true
                          ? Array.from(
                              new Set([
                                ...checked,
                                ...filtered.map((asset) => asset.name),
                              ]),
                            )
                          : checked.filter(
                              (name) =>
                                !filtered.some((asset) => asset.name === name),
                            ),
                      )
                    }
                  />
                </TableHead>
                <TableHead>Asset</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Partitions</TableHead>
                <TableHead>Placement</TableHead>
                <TableHead>Published</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((asset) => {
                const Icon = assetIcon(asset);
                const upstream = upstreamNames(asset, ownerOf).length;
                const heads = headCount(asset);
                return (
                  <TableRow key={asset.name} data-asset={asset.name}>
                    <TableCell>
                      <Checkbox
                        aria-label={`Select ${asset.name}`}
                        checked={checked.includes(asset.name)}
                        onCheckedChange={(value) =>
                          toggle(asset.name, value === true)
                        }
                      />
                    </TableCell>
                    <TableCell>
                      <button
                        className="flex items-center gap-2.5 text-left"
                        aria-label={asset.name}
                        onClick={() =>
                          select({ kind: "asset", name: asset.name })
                        }
                      >
                        <span className="flex size-7 items-center justify-center rounded-md bg-muted text-muted-foreground">
                          <Icon className="size-4" />
                        </span>
                        <span className="min-w-0">
                          <span className="block truncate font-mono text-xs font-medium">
                            {asset.name}
                          </span>
                          <span className="block text-xs text-muted-foreground">
                            {asset.outputs.length
                              ? upstream
                                ? `${upstream} upstream`
                                : "root asset"
                              : "job"}
                          </span>
                        </span>
                      </button>
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={assetStatus(asset)} />
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {partitionLabel(asset)}
                    </TableCell>
                    <TableCell className="font-mono text-xs text-muted-foreground">
                      {placementLabel(asset)}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {heads
                        ? `${heads} scope${heads > 1 ? "s" : ""} · ${time(updatedAt(asset))}`
                        : "—"}
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="icon-sm"
                        aria-label={`Materialize ${asset.name}`}
                        title="Materialize asset"
                        onClick={() => openMaterialize([asset.name])}
                      >
                        <Play />
                      </Button>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span className="tabular-nums">
          {filtered.length} of {assets.length} assets
        </span>
        <span className="flex items-center gap-1.5">
          <Eyebrow>Revision</Eyebrow>
          <code className="font-mono">{diagnostics.revision.slice(0, 10)}</code>
        </span>
      </div>
    </section>
  );
}
