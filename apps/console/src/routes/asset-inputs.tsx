import { useSuspenseQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { q, useProject } from "@/api/queries";
import type { Input, InputPartition, InputState } from "@/api/types";
import { PatternList } from "@/features/patterns";
import { cn } from "@/lib/cn";
import { count, plural } from "@/lib/format";
import { label } from "@/lib/status";
import { Empty } from "@/ui/data";
import { Card, CardHeader } from "@/ui/layout";
import { Tooltip } from "@/ui/overlay";
import { StatusBadge } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

const route = getRouteApi("/assets/$asset/inputs");

const KIND: Record<Input["kind"], { name: string; means: string }> = {
  in: {
    name: "In",
    means:
      "The whole value at its pinned head. A new version of it changes the fingerprint, which resets this asset's incremental inputs.",
  },
  incremental: {
    name: "Incremental",
    means: "Only what changed since this asset's bookmark, a batch at a time.",
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

const STATE_HINT: Record<InputState, string> = {
  never: "Nothing delivered yet: the first delivery is the whole head.",
  caught_up: "Every commit delivered.",
  behind: "Batches committed upstream wait to be delivered.",
  paging: "A window is being delivered over several attempts.",
  full: "A full delivery (a reset or a full run) is in progress.",
  pattern_change: "The input's patterns changed: finishing old deltas, then diffing membership.",
  reconcile: "After a full delivery: removing keys the upstream no longer names.",
};

export function AssetInputs() {
  const { asset: name } = route.useParams();
  const { partition } = route.useSearch();
  const project = useProject();
  const { data: inputs } = useSuspenseQuery(q.inputs(project, name));
  if (inputs.length === 0)
    return (
      <Card>
        <Empty title="No inputs">This asset reads nothing: it is a root of the graph.</Empty>
      </Card>
    );
  return (
    <div className="flex flex-col gap-4">
      {inputs.map((input) => (
        <EdgeCard key={input.param} input={input} partition={partition} />
      ))}
    </div>
  );
}

function EdgeCard({ input, partition }: { input: Input; partition?: string }) {
  const kind = KIND[input.kind];
  const partitions = input.partitions.filter((s) => partition === undefined || s.partition === partition);
  const behind = input.partitions.filter((s) => (s.lag ?? 0) > 0).length;
  return (
    <Card>
      <CardHeader
        ident
        title={
          <span className="flex flex-wrap items-baseline gap-x-2">
            <span className="font-mono text-sm">{input.param}</span>
            <span className="text-sm font-normal text-fg-muted">
              {kind.name}(
              {input.upstream_asset ? (
                <Link
                  to="/assets/$asset"
                  params={{ asset: input.upstream_asset }}
                  className="text-link hover:underline"
                >
                  {input.output}
                </Link>
              ) : (
                <Link
                  to="/sources/$source"
                  params={{ source: input.output }}
                  className="text-link hover:underline"
                >
                  {input.output}
                </Link>
              )}
              )
            </span>
          </span>
        }
        description={kind.means}
        actions={
          <div className="flex flex-wrap items-center gap-2 text-xs text-fg-muted">
            {input.batch_size != null && <span>{input.batch_size} keys a batch</span>}
            {input.concurrency != null && <span>· {input.concurrency} at a time</span>}
            {input.partitions.length > 0 && <span>· {behind ? `${behind} behind` : "all caught up"}</span>}
          </div>
        }
      />
      {input.patterns && (
        <div className="flex flex-wrap items-center gap-2 px-4 pb-3 text-xs text-fg-muted">
          Keys taken: <PatternList patterns={input.patterns} />
        </div>
      )}
      {partitions.length > 0 && (
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
              {partitions.map((s) => (
                <PartitionRow key={s.partition} s={s} />
              ))}
            </tbody>
          </Table>
        </TableScroll>
      )}
    </Card>
  );
}

function PartitionRow({ s }: { s: InputPartition }) {
  const wm = s.bookmark;
  const delivered = wm ? wm.next - 1 : null;
  const lag = s.lag ?? 0;
  const head = s.head_commit ?? 0;
  const done = head + 1 - lag;
  const d = wm?.pass;
  const at =
    typeof d?.at === "string" ? `after ${d.at}` : typeof d?.at === "number" ? `commit ${count(d.at)}` : null;
  const position = d ? [d.mode, at, `page ${d.page + 1} of ${d.pages}`].filter(Boolean).join(" · ") : null;
  return (
    <Tr>
      <Td className="font-mono text-xs">
        {s.partition || "—"}
        {s.upstream_partition !== s.partition && (
          <span className="text-fg-subtle"> ← {s.upstream_partition || "unpartitioned"}</span>
        )}
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
