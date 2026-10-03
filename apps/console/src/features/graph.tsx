import { useState, type ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import { Boxes, Briefcase, Database, KeyRound, ListTree } from "lucide-react";
import type { AssetDecl, AssetStatus, Manifest } from "@/api/types";
import { cn } from "@/lib/cn";
import { plural } from "@/lib/format";
import { toneSoft, toneText, worst, type Tone } from "@/lib/status";
import { SegmentBar, Time } from "@/ui/data";
import { StatusDot } from "@/ui/status";
import { layout } from "./dag";

// -- the model ---------------------------------------------------------------------

export type NodeKind = "asset" | "job" | "dynamic-partitions" | "source";
export type EdgeKind = "in" | "incremental" | "each" | "all_partitions" | "dep" | "partitions";

export interface GraphNode {
  id: string;
  kind: NodeKind;
  asset?: AssetDecl;
}

export interface GraphEdge {
  from: string;
  to: string;
  kind: EdgeKind;
  label?: string;
  patterns?: boolean;
}

export function kindOf(asset: AssetDecl): NodeKind {
  if (asset.outputs.length === 0) return "job";
  if (asset.outputs.some((o) => o.dynamic_partitions)) return "dynamic-partitions";
  return "asset";
}

export const KIND_LABEL: Record<NodeKind, string> = {
  asset: "asset",
  job: "job",
  "dynamic-partitions": "dynamic partitions",
  source: "source",
};

export const KIND_ICON: Record<NodeKind, ReactNode> = {
  asset: <Boxes />,
  job: <Briefcase />,
  "dynamic-partitions": <ListTree />,
  source: <Database />,
};

/** Assets and sources as nodes; inputs, deps and dynamic partitions as edges, by kind. */
export function graphOf(manifest: Manifest): {
  nodes: GraphNode[];
  edges: GraphEdge[];
} {
  const owner = (output: string) => manifest.outputs[output]?.asset ?? output;
  const nodes: GraphNode[] = [
    ...Object.entries(manifest.assets).map(([id, asset]) => ({
      id,
      kind: kindOf(asset),
      asset,
    })),
    ...Object.keys(manifest.sources).map((id) => ({
      id,
      kind: "source" as const,
    })),
  ];
  const edges: GraphEdge[] = [];
  const seen = new Set<string>();
  const add = (edge: GraphEdge) => {
    const key = `${edge.from}→${edge.to}`;
    if (seen.has(key) || edge.from === edge.to) return;
    seen.add(key);
    edges.push(edge);
  };
  for (const [id, asset] of Object.entries(manifest.assets)) {
    for (const edge of Object.values(asset.inputs)) {
      const kind: EdgeKind = edge.each ? "each" : edge.kind;
      add({
        from: owner(edge.output),
        to: id,
        kind,
        patterns: !!edge.patterns,
      });
    }
    for (const dep of asset.deps) add({ from: owner(dep), to: id, kind: "dep" });
    for (const dim of Object.values(asset.partitions?.dims ?? {})) {
      if (dim.kind === "dynamic") add({ from: owner(dim.output), to: id, kind: "partitions" });
    }
  }
  return { nodes, edges };
}

/** One tone for an asset: the worst thing about it. */
export function assetTone(status: AssetStatus | undefined): Tone {
  if (!status) return "idle";
  const tones: Tone[] = [];
  const p = status.partitions;
  if (p.failed) tones.push("fail");
  if (Object.entries(status.failures ?? {}).some(([k, n]) => k !== "canceled" && (n ?? 0) > 0))
    tones.push("warn");
  if (p.running) tones.push("run");
  if (p.complete) tones.push("ok");
  if (!tones.length) tones.push("idle");
  return worst(tones);
}

// -- the drawing -------------------------------------------------------------------

const W = 224;
const H = 84;

const EDGE_STYLE: Record<
  EdgeKind,
  { dash?: string; width: number; label: string; tone: "strong" | "faint" }
> = {
  in: { width: 1.5, label: "whole value", tone: "strong" },
  incremental: {
    dash: "6 4",
    width: 1.5,
    label: "incremental",
    tone: "strong",
  },
  each: { dash: "2 3", width: 2, label: "each key", tone: "strong" },
  all_partitions: { width: 3, label: "all partitions", tone: "strong" },
  dep: {
    dash: "1 4",
    width: 1.5,
    label: "dep (pinned, not loaded)",
    tone: "faint",
  },
  partitions: {
    dash: "1 4",
    width: 1.5,
    label: "partition keys (on hover)",
    tone: "faint",
  },
};

export function AssetGraph({
  manifest,
  status,
  match,
}: {
  manifest: Manifest;
  status: Record<string, AssetStatus> | undefined;
  match: (id: string) => boolean;
}) {
  const [hover, setHover] = useState<string | null>(null);
  const { nodes, edges } = graphOf(manifest);
  const { positions, width, height } = layout({
    nodes: nodes.map((n) => n.id),
    edges,
    width: W,
    height: H,
    gapX: 72,
    gapY: 24,
  });
  const near = hover
    ? new Set([hover, ...edges.flatMap((e) => (e.from === hover ? [e.to] : e.to === hover ? [e.from] : []))])
    : null;
  const pad = 32;

  return (
    <div className="canvas-dots relative overflow-auto rounded-lg border-theme border-line-strong bg-surface-2 shadow-2">
      <div className="relative" style={{ width: width + pad * 2, height: height + pad * 2 + 40 }}>
        <svg className="absolute inset-0" width={width + pad * 2} height={height + pad * 2} aria-hidden>
          <defs>
            <marker
              id="arrow"
              viewBox="0 0 8 8"
              refX="7"
              refY="4"
              markerWidth="8"
              markerHeight="8"
              markerUnits="userSpaceOnUse"
              orient="auto"
            >
              <path d="M0 0 L8 4 L0 8 z" fill="var(--fg-subtle)" />
            </marker>
          </defs>
          {edges.map((edge) => {
            const a = positions.get(edge.from);
            const b = positions.get(edge.to);
            if (!a || !b) return null;
            const style = EDGE_STYLE[edge.kind];
            const x1 = a.x + W + pad;
            const y1 = a.y + H / 2 + pad;
            const x2 = b.x + pad - 2;
            const y2 = b.y + H / 2 + pad;
            const dx = Math.max(40, (x2 - x1) / 2);
            const lit = near ? edge.from === hover || edge.to === hover : null;
            // Dynamic partitions feed the key sets of many assets: drawn only for the node in hand.
            if (edge.kind === "partitions" && !lit) return null;
            return (
              <path
                key={`${edge.from}-${edge.to}`}
                d={`M${x1} ${y1} C${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`}
                fill="none"
                stroke={lit ? "var(--fg)" : style.tone === "faint" ? "var(--fg-subtle)" : "var(--fg-muted)"}
                strokeOpacity={lit === false ? 0.15 : style.tone === "faint" ? 0.6 : 0.85}
                strokeWidth={style.width}
                strokeDasharray={style.dash}
                strokeLinecap="round"
                markerEnd="url(#arrow)"
                className="motion-2 transition-[stroke-opacity]"
              />
            );
          })}
        </svg>
        {nodes.map((node) => {
          const pos = positions.get(node.id);
          if (!pos) return null;
          return (
            <div
              key={node.id}
              className={cn(
                "absolute motion-2 transition-opacity",
                (near && !near.has(node.id)) || !match(node.id) ? "opacity-35" : "opacity-100",
              )}
              style={{
                left: pos.x + pad,
                top: pos.y + pad,
                width: W,
                height: H,
              }}
              onPointerEnter={() => setHover(node.id)}
              onPointerLeave={() => setHover(null)}
            >
              <Node node={node} status={status?.[node.id]} />
            </div>
          );
        })}
        <Legend className="absolute bottom-3 left-4" />
      </div>
    </div>
  );
}

function Node({ node, status }: { node: GraphNode; status: AssetStatus | undefined }) {
  const tone = node.kind === "source" ? "idle" : assetTone(status);
  const p = status?.partitions;
  const failing = Object.entries(status?.failures ?? {}).reduce(
    (s, [k, n]) => s + (k === "canceled" ? 0 : (n ?? 0)),
    0,
  );
  const body = (
    <>
      <span className="flex items-center gap-2">
        <span
          className={cn(
            "grid size-6 shrink-0 place-items-center rounded-sm [&_svg]:size-3.5",
            tone === "ok" ? "bg-sunken text-fg-muted" : toneSoft[tone],
          )}
        >
          {KIND_ICON[node.kind]}
        </span>
        <span className="min-w-0 flex-1 truncate text-sm font-medium text-fg">{node.id}</span>
        {node.kind !== "source" && <StatusDot tone={tone} pulse={!!p?.running} title={tone} />}
      </span>
      {node.kind === "source" ? (
        <span className="text-xs text-fg-subtle">external source</span>
      ) : p && status?.partitioned ? (
        <SegmentBar
          className="h-1.5"
          parts={[
            { tone: "ok", value: p.complete, label: "complete" },
            { tone: "run", value: p.running, label: "running" },
            { tone: "fail", value: p.failed, label: "failed" },
            { tone: "idle", value: p.missing, label: "missing" },
          ]}
        />
      ) : (
        <span className={cn("text-xs", toneText[tone])}>
          {!p
            ? " "
            : p.running
              ? "running"
              : p.failed
                ? "failed"
                : p.complete
                  ? "materialized"
                  : node.kind === "job"
                    ? "not run yet"
                    : "never materialized"}
        </span>
      )}
      <span className="flex items-center gap-2 text-2xs text-fg-subtle">
        <span className="truncate">
          {KIND_LABEL[node.kind]}
          {p && status?.partitioned && ` · ${p.complete}/${p.total} partitions`}
        </span>
        <span className="ml-auto flex shrink-0 items-center gap-1.5">
          {failing > 0 && (
            <span
              className={cn(
                "inline-flex items-center gap-0.5 rounded-full px-1.5 font-medium",
                toneSoft.warn,
              )}
              title={`${plural(failing, "failing key")}`}
            >
              <KeyRound className="size-2.5" />
              {failing}
            </span>
          )}
          {status?.updated_at != null && <Time at={status.updated_at} />}
        </span>
      </span>
    </>
  );
  const className =
    "corner-ticks flex h-full flex-col justify-between gap-1.5 rounded-md border-theme border-line-strong bg-surface p-2.5 shadow-1 motion-2 transition-[transform,box-shadow] hover:-translate-y-px hover:shadow-2 focus-visible:outline-2";
  return node.kind === "source" ? (
    <Link to="/sources/$source" params={{ source: node.id }} className={cn(className, "border-dashed")}>
      {body}
    </Link>
  ) : (
    <Link to="/assets/$asset" params={{ asset: node.id }} className={className}>
      {body}
    </Link>
  );
}

function Legend({ className }: { className?: string }) {
  return (
    <div
      className={cn(
        "flex flex-wrap items-center gap-x-4 gap-y-1 rounded-sm bg-surface/90 px-2.5 py-1.5 text-2xs text-fg-muted",
        className,
      )}
    >
      {(Object.keys(EDGE_STYLE) as EdgeKind[]).map((kind) => {
        const style = EDGE_STYLE[kind];
        return (
          <span key={kind} className="inline-flex items-center gap-1.5">
            <svg width="26" height="6" aria-hidden>
              <line
                x1="1"
                y1="3"
                x2="25"
                y2="3"
                stroke={style.tone === "faint" ? "var(--fg-subtle)" : "var(--fg-muted)"}
                strokeWidth={style.width}
                strokeDasharray={style.dash}
                strokeLinecap="round"
              />
            </svg>
            {style.label}
          </span>
        );
      })}
    </div>
  );
}
