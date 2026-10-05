import { useSuspenseQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { Play } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import type { AssetDetail, PartitionRow, PartitionStatus } from "@/api/types";
import { RunButton } from "@/features/run-dialog";
import { reasonsText, StaleKeysCard, StaleReasons } from "@/features/stale";
import { cn } from "@/lib/cn";
import { count, plural } from "@/lib/format";
import { label, tone, toneSolid, toneSoft } from "@/lib/status";
import { Empty, Generation, Time } from "@/ui/data";
import { Card, CardHeader, Fact, Facts } from "@/ui/layout";
import { Tooltip } from "@/ui/overlay";
import { StatusBadge, StatusIcon } from "@/ui/status";

const route = getRouteApi("/assets/$asset/partitions");
/** The legend's order: what needs a look first. */
const ORDER: PartitionStatus[] = ["failed", "stale", "running", "missing", "materialized", "removed"];

/** `day=2026-09-01,site=alpha` → {day: …, site: …}; a one-dimension key is itself. */
function parse(partition: string, dims: string[]): Record<string, string> {
  if (dims.length < 2) return { [dims[0] ?? ""]: partition };
  return Object.fromEntries(partition.split(",").map((part) => part.split("=") as [string, string]));
}

export function AssetPartitions() {
  const { asset: name } = route.useParams();
  const { partition } = route.useSearch();
  const project = useProject();
  const manifest = useManifest();
  const { data: rows } = useSuspenseQuery(q.partitions(project, name));
  const { data: detail } = useSuspenseQuery(q.asset(project, name));
  const dims = Object.keys(manifest.assets[name]?.partitions?.dims ?? {});
  const keyed = manifest.assets[name]?.outputs.some((o) => o.key) ?? false;
  const counts = Object.fromEntries(
    ORDER.map((s) => [s, rows.filter((r) => r.status === s).length]),
  ) as Record<PartitionStatus, number>;
  const selected = rows.find((r) => r.partition === partition);

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_22rem]">
      <Card>
        <CardHeader
          title="Partitions"
          description={`${plural(rows.length - counts.removed, "partition")}${counts.removed ? `, and ${counts.removed} removed` : ""} · select one for its heads, staleness and last attempt`}
          actions={
            <div className="flex flex-wrap gap-1.5">
              {ORDER.filter((s) => counts[s]).map((s) => (
                <span
                  key={s}
                  className={cn(
                    "inline-flex h-6 items-center gap-1 rounded-full px-2 text-xs font-medium",
                    toneSoft[tone(s)],
                  )}
                >
                  <StatusIcon status={s} className="size-3" />
                  {count(counts[s])} {label(s)}
                </span>
              ))}
            </div>
          }
        />
        <div className="px-4 pb-4">
          {rows.length === 0 ? (
            <Empty compact title="No partition keys yet">
              The dynamic partitions this asset is bound to have none yet.
            </Empty>
          ) : dims.length === 2 ? (
            <Matrix rows={rows} dims={dims} selected={partition} />
          ) : (
            <Strip rows={rows} selected={partition} />
          )}
        </div>
      </Card>
      <PartitionPanel name={name} row={selected} detail={detail} />
      {selected?.status === "stale" && keyed && (
        <StaleKeysCard name={name} partition={selected.partition} className="self-start xl:col-start-2" />
      )}
    </div>
  );
}

function Cell({ row, selected, compact }: { row: PartitionRow; selected: boolean; compact?: boolean }) {
  const t = tone(row.status === "missing" ? "missing" : row.status);
  return (
    <Tooltip
      content={
        <span className="flex flex-col">
          <span className="font-mono">{row.partition}</span>
          <span>
            {label(row.status)}
            {row.status === "stale" && row.reasons?.length ? `: ${reasonsText(row.reasons)}` : ""}
            {row.last_outcome &&
              row.last_outcome !== "succeeded" &&
              ` · last attempt ${label(row.last_outcome)}`}
          </span>
        </span>
      }
    >
      <Link
        from="/assets/$asset/partitions"
        to="."
        search={(s) => ({ ...s, partition: selected ? undefined : row.partition })}
        replace
        aria-label={`${row.partition}: ${label(row.status)}`}
        aria-pressed={selected}
        className={cn(
          "block rounded-xs motion-1 transition-transform hover:scale-110",
          compact ? "size-4" : "h-8",
          row.status === "missing"
            ? "border-theme border-dashed border-line-strong bg-surface"
            : toneSolid[t],
          row.status === "removed" && "opacity-40",
          row.status === "running" && "animate-[pulse-dot_1.6s_ease-in-out_infinite]",
          selected && "ring-2 ring-fg ring-offset-2 ring-offset-surface",
        )}
      >
        {!compact && <span className="sr-only">{row.partition}</span>}
      </Link>
    </Tooltip>
  );
}

function Strip({ rows, selected }: { rows: PartitionRow[]; selected?: string }) {
  return (
    <ul className="grid grid-cols-[repeat(auto-fill,minmax(7.5rem,1fr))] gap-2">
      {rows.map((row) => (
        <li key={row.partition} className="flex flex-col gap-1">
          <Cell row={row} selected={row.partition === selected} />
          <span
            className={cn(
              "truncate font-mono text-2xs",
              row.partition === selected ? "text-fg" : "text-fg-subtle",
            )}
          >
            {row.partition}
          </span>
        </li>
      ))}
    </ul>
  );
}

/** Two dimensions: the smaller one down the side, the other across. */
function Matrix({ rows, dims, selected }: { rows: PartitionRow[]; dims: string[]; selected?: string }) {
  const parsed = rows.map((row) => ({ row, keys: parse(row.partition, dims) }));
  const values = dims.map((d) => [...new Set(parsed.map((p) => p.keys[d] ?? ""))].sort());
  const [down, across] = values[0]!.length <= values[1]!.length ? [0, 1] : [1, 0];
  const downDim = dims[down]!;
  const acrossDim = dims[across]!;
  const index = new Map(parsed.map((p) => [`${p.keys[downDim]}|${p.keys[acrossDim]}`, p.row]));
  const columns = values[across]!;
  return (
    <div className="overflow-x-auto">
      <table className="border-separate border-spacing-[3px]">
        <thead>
          <tr>
            <th className="pr-2 text-left align-bottom text-2xs font-medium whitespace-nowrap text-fg-subtle">
              {downDim} ╲ {acrossDim}
            </th>
            {columns.map((c, i) => (
              <th key={c} className="relative h-16 w-4 min-w-4 p-0 text-2xs font-normal text-fg-subtle">
                {(i % 7 === 0 || i === columns.length - 1) && (
                  <span className="absolute bottom-1 left-1 origin-bottom-left -rotate-45 font-mono whitespace-nowrap">
                    {c}
                  </span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {values[down]!.map((r) => (
            <tr key={r}>
              <th scope="row" className="pr-2 text-left font-mono text-xs font-normal text-fg-muted">
                {r}
              </th>
              {columns.map((c) => {
                const row = index.get(`${r}|${c}`);
                return (
                  <td key={c}>
                    {row ? (
                      <Cell row={row} selected={row.partition === selected} compact />
                    ) : (
                      <span className="block size-4" />
                    )}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function PartitionPanel({ name, row, detail }: { name: string; row?: PartitionRow; detail: AssetDetail }) {
  if (!row) {
    return (
      <Card className="self-start">
        <Empty compact title="No partition selected">
          Select a cell to see its heads and the attempt that last ran it.
        </Empty>
      </Card>
    );
  }
  const heads = Object.entries(detail.heads).flatMap(([output, list]) =>
    list.filter(([s]) => s === row.partition).map(([, head]) => ({ output, head })),
  );
  const [run, attempt] = row.last_attempt?.split("/") ?? [];
  return (
    <Card className="self-start">
      <CardHeader
        ident
        title={<span className="font-mono text-sm">{row.partition}</span>}
        actions={<StatusBadge status={row.status} />}
      />
      <div className="flex flex-col gap-4 px-4 pb-4">
        {row.status === "stale" && (
          <div className="flex flex-col gap-1.5">
            <span className="text-2xs font-medium tracking-wide text-fg-subtle uppercase">Stale because</span>
            <StaleReasons reasons={row.reasons} />
          </div>
        )}
        <Facts className="grid-cols-2">
          <Fact label="Last outcome">{row.last_outcome ? label(row.last_outcome) : "—"}</Fact>
          <Fact label="Last attempt">
            {run && attempt ? (
              <Link
                to="/runs/$run"
                params={{ run }}
                search={{ attempt }}
                className="font-mono text-link hover:underline"
              >
                {attempt.slice(-7)}
              </Link>
            ) : (
              "—"
            )}
          </Fact>
        </Facts>
        {heads.length > 0 && (
          <ul className="flex flex-col divide-y divide-line rounded-md border-theme border-line text-sm">
            {heads.map(({ output, head }) => (
              <li key={output} className="flex flex-wrap items-center gap-x-3 gap-y-0.5 px-3 py-2">
                <span className="font-medium">{output}</span>
                <Generation value={head.ref.generation} />
                {head.key_count != null && (
                  <span className="text-xs text-fg-muted">{plural(head.key_count, "key")}</span>
                )}
                <span className="ml-auto text-xs text-fg-subtle">
                  <Time at={head.at} />
                </span>
              </li>
            ))}
          </ul>
        )}
        <RunButton
          targets={[name]}
          partition={row.partition}
          icon={<Play />}
          label="Run this partition"
          variant="secondary"
        />
      </div>
    </Card>
  );
}
