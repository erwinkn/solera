import { useSuspenseQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { q, useProject } from "@/api/queries";
import type { Input, InputPartition, Observed } from "@/api/types";
import { KeyClasses } from "@/features/batches";
import { PatternList } from "@/features/patterns";
import { count, plural } from "@/lib/format";
import { Empty } from "@/ui/data";
import { Card, CardHeader } from "@/ui/layout";
import { Tooltip } from "@/ui/overlay";
import { StatusBadge } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

const route = getRouteApi("/assets/$asset/inputs");

/** Each input kind as it is declared (`call(output, flag)`), and what it means. */
const KIND: Record<Input["kind"], { name: string; flag?: string; means: string }> = {
  in: {
    name: "In",
    means:
      "The whole value at its pinned head, or, over upstream dimensions this asset lacks, a dict of every materialized partition across them. A new version of it is an input change.",
  },
  incremental: {
    name: "Incremental",
    means:
      "What changed since each partition last read it, as added, updated and removed keys, a batch at a time.",
  },
  each: {
    name: "Incremental",
    flag: "each=True",
    means:
      "One call per changed key, a batch at a time. A key that fails is kept with its retry record and retried on its own.",
  },
  all_partitions: {
    name: "In",
    flag: "all_partitions=True",
    means:
      "Every materialized partition of the upstream, the shared dimensions too, as a dict at pin time. Never waits for missing ones.",
  },
  dep: {
    name: "dep",
    means:
      "Pinned in lineage and watched by OnChange(), never loaded. A new version of it is an input change.",
  },
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
        <InputCard key={input.param} input={input} partition={partition} />
      ))}
    </div>
  );
}

const owes = (o: Observed | null | undefined) =>
  !!o && (o.full_run_due != null || o.owed == null || o.owed.added + o.owed.updated + o.owed.removed > 0);

function InputCard({ input, partition }: { input: Input; partition?: string }) {
  const kind = KIND[input.kind];
  const partitions = input.partitions.filter((s) => partition === undefined || s.partition === partition);
  // What each partition owes comes from the observed set; an engine that predates it says nothing.
  const observed = input.partitions.some((s) => s.observed !== undefined);
  const heads = input.partitions.some((s) => s.head_commit != null);
  const owing = input.partitions.filter((s) => owes(s.observed)).length;
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
              {kind.flag && `, ${kind.flag}`})
            </span>
          </span>
        }
        description={kind.means}
        actions={
          <div className="flex flex-wrap items-center gap-2 text-xs text-fg-muted">
            {input.batch_size != null && (
              <span>
                {input.batch_size.toLocaleString("en-US")} keys a batch
                {input.concurrency != null && `, ${input.concurrency.toLocaleString("en-US")} at once`}
              </span>
            )}
            {observed && input.partitions.length > 0 && (
              <span>
                · {owing ? `${plural(owing, "partition")} owe${owing === 1 ? "s" : ""} work` : "nothing owed"}
              </span>
            )}
          </div>
        }
      />
      {input.patterns && (
        <div className="flex flex-wrap items-center gap-2 px-4 pb-3 text-xs text-fg-muted">
          Keys taken: <PatternList patterns={input.patterns} />
        </div>
      )}
      {partitions.length > 0 && (observed || heads) && (
        <TableScroll className="border-t border-line">
          <Table>
            <thead>
              <tr>
                <Th>Partition</Th>
                {observed && <Th>Owed to it</Th>}
                {observed && <Th className="text-right">Observed through</Th>}
                {heads && <Th className="text-right">Upstream head</Th>}
              </tr>
            </thead>
            <tbody>
              {partitions.map((s) => (
                <PartitionRow key={s.partition} s={s} observed={observed} heads={heads} />
              ))}
            </tbody>
          </Table>
        </TableScroll>
      )}
    </Card>
  );
}

function PartitionRow({ s, observed, heads }: { s: InputPartition; observed: boolean; heads: boolean }) {
  const o = s.observed;
  return (
    <Tr>
      <Td className="font-mono text-xs">
        {s.partition || "—"}
        {s.upstream_partition !== s.partition && (
          <span className="text-fg-subtle"> ← {s.upstream_partition || "unpartitioned"}</span>
        )}
      </Td>
      {observed && (
        <Td>
          <Owed observed={o} />
        </Td>
      )}
      {observed && (
        <Td className="text-right text-fg-muted">
          {o?.observed_at != null ? (
            <Tooltip content="Every key this partition read was observed at this upstream commit or later.">
              <span className="tabular">commit {count(o.observed_at)}</span>
            </Tooltip>
          ) : (
            "—"
          )}
        </Td>
      )}
      {heads && (
        <Td className="text-right text-fg-muted tabular">
          {s.head_commit != null ? `commit ${count(s.head_commit)}` : "—"}
        </Td>
      )}
    </Tr>
  );
}

/** What a partition owes this input: a full run, a count per class, nothing, or not known yet. */
function Owed({ observed: o }: { observed: Observed | null | undefined }) {
  if (!o) return <span className="text-xs text-fg-subtle">never read</span>;
  if (o.full_run_due)
    return (
      <span className="flex flex-wrap items-center gap-2">
        <StatusBadge status="stale" text="full run due" />
        <span className="text-xs text-fg-muted">{o.full_run_due.replaceAll("_", " ")}</span>
      </span>
    );
  if (!o.owed)
    return (
      <Tooltip content="The comparison with upstream isn't computed yet, as after a pattern or definition change at large scale. It is neither stale nor fresh until it is.">
        <span>
          <StatusBadge status="pending" />
        </span>
      </Tooltip>
    );
  const { added, updated, removed } = o.owed;
  if (added + updated + removed === 0) return <span className="text-xs text-fg-subtle">nothing</span>;
  return <KeyClasses added={added} updated={updated} removed={removed} />;
}
