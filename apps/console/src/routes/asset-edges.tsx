import { useSuspenseQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { q, useProject } from "@/api/queries";
import type { Edge, EdgeScope, EdgeState } from "@/api/types";
import { PatternList } from "@/features/patterns";
import { cn } from "@/lib/cn";
import { count, plural } from "@/lib/format";
import { label } from "@/lib/status";
import { Empty } from "@/ui/data";
import { Card, CardHeader } from "@/ui/layout";
import { Tooltip } from "@/ui/overlay";
import { StatusBadge } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

const route = getRouteApi("/assets/$asset/edges");

const KIND: Record<Edge["kind"], { name: string; means: string }> = {
  in: {
    name: "In",
    means:
      "The whole value at its pinned head. A new version of it changes the fingerprint, which resets this asset's incremental edges.",
  },
  incremental: {
    name: "Incremental",
    means: "Only what changed since this asset's watermark, a page at a time.",
  },
  each: {
    name: "Each",
    means: "One call per changed key, its outcome kept per key in the failure index.",
  },
  all_partitions: {
    name: "AllPartitions",
    means: "Every committed partition of the upstream, as a dict, at pin time. Never waits for missing ones.",
  },
  dep: {
    name: "dep",
    means: "Pinned in lineage and the fingerprint, watched by AutoRefresh, never loaded.",
  },
};

const STATE_HINT: Record<EdgeState, string> = {
  never: "Nothing delivered yet: the first delivery is the whole head.",
  caught_up: "Every commit delivered.",
  behind: "Batches committed upstream wait to be delivered.",
  paging: "A window is being delivered over several attempts.",
  full: "A full delivery (a reset or a full run) is in progress.",
  rescope: "The edge's patterns changed: finishing old deltas, then diffing membership.",
  reconcile: "After a full delivery: removing keys the upstream no longer names.",
};

export function AssetEdges() {
  const { asset: name } = route.useParams();
  const { scope } = route.useSearch();
  const project = useProject();
  const { data: edges } = useSuspenseQuery(q.edges(project, name));
  if (edges.length === 0)
    return (
      <Card>
        <Empty title="No inputs">This asset reads nothing: it is a root of the graph.</Empty>
      </Card>
    );
  return (
    <div className="flex flex-col gap-4">
      {edges.map((edge) => (
        <EdgeCard key={edge.param} edge={edge} scope={scope} />
      ))}
    </div>
  );
}

function EdgeCard({ edge, scope }: { edge: Edge; scope?: string }) {
  const kind = KIND[edge.kind];
  const scopes = edge.scopes.filter((s) => scope === undefined || s.scope === scope);
  const behind = edge.scopes.filter((s) => (s.lag ?? 0) > 0).length;
  return (
    <Card>
      <CardHeader
        ident
        title={
          <span className="flex flex-wrap items-baseline gap-x-2">
            <span className="font-mono text-sm">{edge.param}</span>
            <span className="text-sm font-normal text-fg-muted">
              {kind.name}(
              {edge.upstream_asset ? (
                <Link
                  to="/assets/$asset"
                  params={{ asset: edge.upstream_asset }}
                  className="text-link hover:underline"
                >
                  {edge.output}
                </Link>
              ) : (
                <Link
                  to="/sources/$source"
                  params={{ source: edge.output }}
                  className="text-link hover:underline"
                >
                  {edge.output}
                </Link>
              )}
              )
            </span>
          </span>
        }
        description={kind.means}
        actions={
          <div className="flex flex-wrap items-center gap-2 text-xs text-fg-muted">
            {edge.page_size != null && <span>{edge.page_size} keys a page</span>}
            {edge.concurrency != null && <span>· {edge.concurrency} at a time</span>}
            {edge.scopes.length > 0 && <span>· {behind ? `${behind} behind` : "all caught up"}</span>}
          </div>
        }
      />
      {edge.patterns && (
        <div className="flex flex-wrap items-center gap-2 px-4 pb-3 text-xs text-fg-muted">
          Keys taken: <PatternList patterns={edge.patterns} />
        </div>
      )}
      {scopes.length > 0 && (
        <TableScroll className="border-t border-line">
          <Table>
            <thead>
              <tr>
                <Th>Partition</Th>
                <Th>Delivery</Th>
                <Th className="text-right">Delivered to</Th>
                <Th className="text-right">Upstream head</Th>
                <Th>Lag</Th>
                <Th>Position</Th>
              </tr>
            </thead>
            <tbody>
              {scopes.map((s) => (
                <ScopeRow key={s.scope} s={s} />
              ))}
            </tbody>
          </Table>
        </TableScroll>
      )}
    </Card>
  );
}

function ScopeRow({ s }: { s: EdgeScope }) {
  const wm = s.watermark;
  const delivered = wm ? wm.next - 1 : null;
  const lag = s.lag ?? 0;
  const head = s.head_commit ?? 0;
  const done = head + 1 - lag;
  const d = wm?.delivery;
  const at =
    typeof d?.at === "string" ? `after ${d.at}` : typeof d?.at === "number" ? `commit ${count(d.at)}` : null;
  const position = d ? [d.mode, at, `page ${d.page + 1} of ${d.pages}`].filter(Boolean).join(" · ") : null;
  return (
    <Tr>
      <Td className="font-mono text-xs">
        {s.scope || "—"}
        {s.up_scope !== s.scope && <span className="text-fg-subtle"> ← {s.up_scope || "unpartitioned"}</span>}
      </Td>
      <Td>
        <Tooltip content={STATE_HINT[s.state]}>
          <span>
            <StatusBadge status={s.state} text={label(s.state)} />
          </span>
        </Tooltip>
      </Td>
      <Td className="text-right text-fg-muted">
        {delivered != null && delivered >= 0 ? `commit ${count(delivered)}` : "—"}
      </Td>
      <Td className="text-right text-fg-muted">
        {s.head_commit != null ? `commit ${count(s.head_commit)}` : "—"}
      </Td>
      <Td>
        {s.state === "never" && !s.head_commit ? (
          <span className="text-xs text-fg-subtle">—</span>
        ) : (
          <span className="flex items-center gap-2">
            <span className="h-1.5 w-20 overflow-hidden rounded-full bg-sunken" aria-hidden>
              <span
                className={cn("block h-full rounded-full", lag ? "bg-warn" : "bg-viz-ok")}
                style={{
                  width: `${head + 1 > 0 ? (100 * Math.max(0, done)) / (head + 1) : 100}%`,
                }}
              />
            </span>
            <span className={cn("text-xs tabular", lag ? "font-medium text-warn-fg" : "text-fg-subtle")}>
              {lag ? plural(lag, "commit") : "none"}
            </span>
          </span>
        )}
      </Td>
      <Td className="max-w-64 truncate font-mono text-xs text-fg-muted">{position ?? "—"}</Td>
    </Tr>
  );
}
