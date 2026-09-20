import { Play } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Switch } from "@/components/ui/switch";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ErrorNotice } from "@/components/common";
import { request, useAction, useQuery } from "@/lib/api";
import { time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type {
  AssetDetail,
  CatalogAsset,
  Head,
  PartitionScope,
} from "@/lib/types";

export function upstreamNames(
  asset: Pick<CatalogAsset, "inputs" | "deps" | "partitions">,
  ownerOf: (output: string) => string | null,
): string[] {
  const names = new Set<string>();
  for (const edge of Object.values(asset.inputs)) {
    const owner = ownerOf(edge.output);
    if (owner) names.add(owner);
  }
  for (const dep of asset.deps) {
    const owner = ownerOf(dep);
    if (owner) names.add(owner);
  }
  for (const dim of Object.values(asset.partitions?.dims ?? {})) {
    if (dim.kind === "set" && dim.output) {
      const owner = ownerOf(dim.output);
      if (owner) names.add(owner);
    }
  }
  return [...names];
}

export function assetStatus(asset: CatalogAsset) {
  const heads = Object.values(asset.heads).flatMap((scopes) =>
    Object.values(scopes),
  );
  if (!heads.length) return "not_materialized";
  if (heads.some((head) => head.version !== asset.version)) return "stale";
  if (heads.every((head) => head.complete)) return "materialized";
  return "partial";
}

const SCOPE_TONE: Record<string, string> = {
  complete: "bg-emerald-500/80 border-emerald-600/30 text-emerald-950",
  missing: "bg-amber-500/15 border-amber-600/30 text-amber-800",
  retired: "bg-muted border-border text-muted-foreground line-through",
  running: "bg-sky-500/20 border-sky-600/30 text-sky-800 animate-pulse",
  failed: "bg-red-500/15 border-red-600/30 text-red-800",
};

function scopeKey(dims: string[], parts: Record<string, string>) {
  if (dims.length === 1) return parts[dims[0]];
  return dims
    .slice()
    .sort()
    .map((name) => `${name}=${encodeURIComponent(parts[name])}`)
    .join(",");
}

function PartitionGrid({
  asset,
  detail,
  scopes,
  onPick,
}: {
  asset: CatalogAsset;
  detail: AssetDetail | null;
  scopes: PartitionScope[];
  onPick: (scope: string) => void;
}) {
  const { select } = useWorkspace();
  const dims = Object.keys(asset.partitions?.dims ?? {});
  const byScope = new Map(scopes.map((s) => [s.scope, s]));
  if (!dims.length) {
    const head = Object.values(asset.heads)[0]?.[""];
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <span
          className={cn(
            "rounded-md border px-2 py-0.5 text-xs",
            SCOPE_TONE[
              head?.complete ? "complete" : head ? "failed" : "missing"
            ],
          )}
        >
          unpartitioned
        </span>
        {head && <span>v{String(head.version).slice(0, 12)}</span>}
      </div>
    );
  }
  const keys = detail?.current_keys ?? [];
  const cells: { scope: string; label: string; cell: PartitionScope }[] = [];
  const missingCell = (scope: string): PartitionScope => ({
    scope,
    status: "missing",
  });
  if (dims.length === 1) {
    for (const key of keys[0] ?? []) {
      const scope = scopeKey(dims, { [dims[0]]: key });
      cells.push({
        scope,
        label: key,
        cell: byScope.get(scope) ?? missingCell(scope),
      });
    }
    for (const s of scopes)
      if (!cells.some((c) => c.scope === s.scope))
        cells.push({ scope: s.scope, label: s.scope, cell: s });
  } else {
    const [first, second, ...rest] = keys;
    for (const row of first ?? [])
      for (const col of second ?? []) {
        const parts: Record<string, string> = {
          [dims[0]]: row,
          [dims[1]]: col,
        };
        rest.forEach((dimKeys, i) => {
          parts[dims[i + 2]] = dimKeys[0] ?? "";
        });
        const scope = scopeKey(dims, parts);
        cells.push({
          scope,
          label: `${row} × ${col}`,
          cell: byScope.get(scope) ?? missingCell(scope),
        });
      }
  }
  const headsByScope = new Map<string, Head>();
  if (detail) {
    for (const pairs of Object.values(detail.heads))
      for (const [scope, head] of pairs) headsByScope.set(scope, head);
  } else {
    for (const heads of Object.values(asset.heads))
      for (const [scope, head] of Object.entries(heads))
        headsByScope.set(scope, head);
  }
  return (
    <div className="flex flex-col gap-2">
      <div
        className="flex flex-wrap gap-1.5"
        role="list"
        aria-label="Partitions"
      >
        {cells.map(({ scope, label, cell }) => {
          const head = headsByScope.get(scope);
          const keyCount = (
            head?.ref.meta as Record<string, { count?: number }> | undefined
          )?.keys?.count;
          const attemptRun = cell.last_attempt?.split("/")[0];
          return (
            <span key={scope} role="listitem" className="inline-flex">
              <button
                data-scope={scope}
                data-status={cell.status}
                title={`${scope} · ${cell.status}${cell.last_outcome ? ` · last ${cell.last_outcome}` : ""}${keyCount != null ? ` · ${keyCount} keys` : ""}`}
                className={cn(
                  "rounded-md border px-2 py-1 font-mono text-xs transition-transform hover:scale-105",
                  SCOPE_TONE[cell.status],
                )}
                onClick={() => onPick(scope)}
              >
                {label}
                {keyCount != null && (
                  <span className="ml-1 opacity-70">{keyCount}</span>
                )}
              </button>
              {attemptRun && (
                <button
                  aria-label={`Open last attempt for ${scope}`}
                  title={`last attempt ${cell.last_attempt}`}
                  className="ml-0.5 rounded-sm px-0.5 text-muted-foreground hover:text-foreground"
                  onClick={(event) => {
                    event.stopPropagation();
                    select({ kind: "run", id: attemptRun });
                  }}
                >
                  <Play className="size-3" />
                </button>
              )}
            </span>
          );
        })}
        {!cells.length && (
          <span className="text-xs text-muted-foreground">
            No partition keys committed yet.
          </span>
        )}
      </div>
      <p className="text-xs text-muted-foreground">
        {dims.join(" × ")} ·{" "}
        {cells.filter((c) => c.cell.status === "complete").length}/
        {cells.length} complete
        {detail?.cursor != null && " · cursor present"}
      </p>
    </div>
  );
}

export function AssetSheet() {
  const { selection, select, base, assets, openMaterialize, refresh } =
    useWorkspace();
  const action = useAction();
  const name = selection?.kind === "asset" ? selection.name : null;
  const asset = assets.find((a) => a.name === name) ?? null;
  const detail = useQuery<AssetDetail>(
    base && name ? `${base}/assets/${name}` : null,
    2000,
  );
  const partitions = useQuery<{ partitions: PartitionScope[] }>(
    base && name && asset?.partitions ? `${base}/partitions/${name}` : null,
    2000,
  );
  if (!asset) {
    return (
      <Sheet open={!!name} onOpenChange={(open) => !open && select(null)}>
        <SheetContent />
      </Sheet>
    );
  }
  return (
    <Sheet open={!!name} onOpenChange={(open) => !open && select(null)}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-2xl">
        <SheetHeader>
          <SheetTitle className="font-mono">{asset.name}</SheetTitle>
          <SheetDescription>{asset.doc ?? "Asset"}</SheetDescription>
        </SheetHeader>
        <div className="flex flex-col gap-6 px-4 pb-8">
          {action.error && <ErrorNotice message={action.error} />}
          <section className="flex flex-col gap-2">
            <h3 className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
              Outputs
            </h3>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Store</TableHead>
                  <TableHead>Key</TableHead>
                  <TableHead>Mode</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {asset.outputs.map((output) => (
                  <TableRow key={output.name}>
                    <TableCell className="font-mono text-xs">
                      {output.name}
                    </TableCell>
                    <TableCell>{output.store}</TableCell>
                    <TableCell className="font-mono text-xs">
                      {output.key ??
                        (output.partition_set ? "<elements>" : "—")}
                      {output.revision ? ` @${output.revision}` : ""}
                    </TableCell>
                    <TableCell>{output.mode ?? "replace"}</TableCell>
                  </TableRow>
                ))}
                {!asset.outputs.length && (
                  <TableRow>
                    <TableCell colSpan={4} className="text-muted-foreground">
                      A job — no declared outputs.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </section>
          <section className="flex flex-col gap-2">
            <h3 className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
              Partitions
            </h3>
            <PartitionGrid
              asset={asset}
              detail={detail.data}
              scopes={partitions.data?.partitions ?? []}
              onPick={(scope) => openMaterialize([asset.name], [scope])}
            />
          </section>
          <section className="flex flex-col gap-2">
            <h3 className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
              Placement
            </h3>
            <p className="font-mono text-xs">
              {asset.placement.kind}{" "}
              {Object.entries(asset.placement.placement)
                .map(([k, v]) => `${k}=${v}`)
                .join(" ")}
            </p>
          </section>
          <section className="flex flex-col gap-2">
            <h3 className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
              Automations
            </h3>
            {(detail.data?.automations ?? []).length ? (
              (detail.data?.automations ?? []).map((auto) => (
                <div
                  key={auto.name}
                  className="flex items-center gap-3 rounded-lg border px-3 py-2"
                  data-automation={auto.name}
                >
                  <span className="min-w-0 flex-1">
                    <span className="block font-mono text-xs">{auto.name}</span>
                    <span className="block text-xs text-muted-foreground">
                      {auto.trigger.kind}
                      {auto.last_at ? ` · fired ${time(auto.last_at)}` : ""}
                    </span>
                  </span>
                  <Switch
                    aria-label={`Enable ${auto.name}`}
                    checked={auto.enabled}
                    onCheckedChange={(enabled) =>
                      action.run(async () => {
                        await request(
                          `${base}/automations/${auto.name}/${enabled ? "enable" : "disable"}`,
                          { body: {} },
                        );
                        detail.refresh();
                        refresh();
                      })
                    }
                  />
                </div>
              ))
            ) : (
              <p className="text-xs text-muted-foreground">None declared.</p>
            )}
          </section>
          <Button onClick={() => openMaterialize([asset.name])}>
            <Play /> Materialize
          </Button>
        </div>
      </SheetContent>
    </Sheet>
  );
}
