import {
  Bookmark,
  Braces,
  Check,
  Clock,
  Database,
  LayoutGrid,
  Package,
  Play,
  Table as TableIcon,
  Zap,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Switch } from "@/components/ui/switch";
import { Eyebrow, ErrorNotice } from "@/components/common";
import { request, useAction, useQuery } from "@/lib/api";
import { time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type {
  AssetDetail,
  CatalogAsset,
  Head,
  OutputDecl,
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

// Grid cells are filled by status so a scope's state reads without a legend.
export const CELL_TONE: Record<string, string> = {
  complete:
    "border-emerald-600/30 bg-emerald-500/15 text-emerald-700 dark:border-emerald-400/25 dark:bg-emerald-400/12 dark:text-emerald-300",
  missing:
    "border-dashed border-amber-600/40 bg-amber-500/5 text-amber-700 dark:border-amber-400/30 dark:text-amber-300",
  retired:
    "border-border bg-muted/50 text-muted-foreground line-through decoration-muted-foreground/40",
  running:
    "border-sky-600/30 bg-sky-500/15 text-sky-700 dark:border-sky-400/25 dark:bg-sky-400/12 dark:text-sky-300",
  failed:
    "border-red-600/30 bg-red-500/12 text-red-700 dark:border-red-400/25 dark:bg-red-400/12 dark:text-red-300",
};

const STORE_ICON: Record<string, typeof Database> = {
  json: Braces,
  postgres: TableIcon,
  blob: Package,
};

function storeIcon(store: string) {
  return STORE_ICON[store] ?? Database;
}

function headEntries(
  detail: AssetDetail | null,
  asset: CatalogAsset,
  output: string,
): [string, Head][] {
  if (detail?.heads?.[output]) return detail.heads[output];
  return Object.entries(asset.heads[output] ?? {});
}

// Migrations are declared on the output; the last applied name travels in the
// head's handle as `schema`. Applied count = its position in the declared list.
function appliedCount(output: OutputDecl, heads: [string, Head][]): number {
  const declared = output.migrations ?? [];
  if (!declared.length) return 0;
  let best = 0;
  for (const [, head] of heads) {
    const schema = (head?.ref?.handle as Record<string, unknown> | undefined)
      ?.schema;
    if (typeof schema === "string") {
      const index = declared.indexOf(schema);
      if (index >= 0) best = Math.max(best, index + 1);
    }
  }
  return best;
}

function MigrationCell({
  output,
  heads,
}: {
  output: OutputDecl;
  heads: [string, Head][];
}) {
  const declared = output.migrations ?? [];
  if (!declared.length) return <span className="text-muted-foreground">—</span>;
  const applied = appliedCount(output, heads);
  const done = applied >= declared.length;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[0.7rem] font-semibold",
        done
          ? "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300"
          : "bg-amber-500/15 text-amber-700 dark:text-amber-300",
      )}
      title={declared
        .map((name, i) => `${i < applied ? "✓" : "·"} ${name}`)
        .join("\n")}
    >
      {done ? <Check className="size-3" /> : <Clock className="size-3" />}
      {applied}/{declared.length} {done ? "applied" : "pending"}
    </span>
  );
}

function scopeKey(dims: string[], parts: Record<string, string>) {
  if (dims.length === 1) return parts[dims[0]];
  return dims
    .slice()
    .sort()
    .map((name) => `${name}=${encodeURIComponent(parts[name])}`)
    .join(",");
}

function Cell({
  scope,
  label,
  cell,
  keyCount,
  onPick,
}: {
  scope: string;
  label: string;
  cell: PartitionScope;
  keyCount?: number | null;
  onPick: (scope: string) => void;
}) {
  const { select } = useWorkspace();
  const attemptRun = cell.last_attempt?.split("/")[0];
  return (
    <span
      data-scope={scope}
      data-status={cell.status}
      className={cn(
        "flex min-w-0 items-stretch rounded-md border font-mono text-[0.7rem] transition",
        CELL_TONE[cell.status] ?? CELL_TONE.missing,
      )}
    >
      <button
        type="button"
        title={`${scope} · ${cell.status}${cell.last_outcome ? ` · last ${cell.last_outcome}` : ""}${keyCount != null ? ` · ${keyCount} keys` : ""}`}
        className="flex min-w-0 flex-1 items-center justify-center gap-1 rounded-l-md px-2 py-1.5 hover:brightness-105 focus-visible:z-10 focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
        onClick={(event) => {
          if (attemptRun && (event.metaKey || event.ctrlKey)) {
            select({ kind: "run", id: attemptRun });
            return;
          }
          onPick(scope);
        }}
      >
        {label && <span className="truncate">{label}</span>}
        {keyCount != null ? (
          <span className="shrink-0 tabular-nums">{keyCount}</span>
        ) : (
          !label &&
          cell.status === "complete" && <Check className="size-3 shrink-0" />
        )}
      </button>
      {attemptRun && (
        <button
          type="button"
          aria-label={`Open last attempt for ${label || scope || "scope"}`}
          title={`Open last attempt\n${attemptRun}`}
          className="flex shrink-0 items-center rounded-r-md px-1.5 opacity-50 hover:opacity-100 focus-visible:z-10 focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
          onClick={() => select({ kind: "run", id: attemptRun })}
        >
          <Play className="size-2.5" />
        </button>
      )}
    </span>
  );
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
  const dims = Object.keys(asset.partitions?.dims ?? {});
  const byScope = new Map(scopes.map((s) => [s.scope, s]));
  const missingCell = (scope: string): PartitionScope => ({
    scope,
    status: "missing",
  });

  // key count per scope, from the head's meta.
  const headsByScope = new Map<string, Head>();
  for (const output of asset.outputs)
    for (const [scope, head] of headEntries(detail, asset, output.name))
      headsByScope.set(scope, head);
  const keyCountOf = (scope: string) =>
    (headsByScope.get(scope)?.ref.meta as { keys?: { count?: number } })?.keys
      ?.count;

  if (!dims.length) {
    const scope = "";
    const head = headsByScope.get(scope) ?? Object.values(asset.heads)[0]?.[""];
    const cell = byScope.get(scope) ?? {
      scope,
      status: head?.complete ? "complete" : head ? "failed" : "missing",
    };
    return (
      <div className="flex flex-wrap items-center gap-2">
        <div className="w-40">
          <Cell
            scope={scope}
            label="unpartitioned"
            cell={cell as PartitionScope}
            keyCount={keyCountOf(scope)}
            onPick={onPick}
          />
        </div>
        {head && (
          <span className="font-mono text-xs text-muted-foreground">
            v{String(head.version).slice(0, 12)}
          </span>
        )}
      </div>
    );
  }

  const keys = detail?.current_keys ?? [];

  if (dims.length >= 2) {
    // Two-dimensional grid: dim0 rows × dim1 columns.
    const rows = keys[0] ?? [];
    const cols = keys[1] ?? [];
    const rest = keys.slice(2);
    return (
      <div className="overflow-x-auto rounded-lg border">
        <table className="w-full border-collapse text-xs">
          <thead>
            <tr>
              <th className="sticky left-0 z-10 border-b border-r bg-muted/40 px-2.5 py-1.5 text-left text-[0.65rem] font-semibold text-muted-foreground">
                {dims[0]} \ {dims[1]}
              </th>
              {cols.map((col) => (
                <th
                  key={col}
                  className="border-b px-2 py-1.5 text-center font-mono text-[0.7rem] font-normal text-muted-foreground"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row}>
                <th className="sticky left-0 z-10 border-r bg-card px-2.5 py-1 text-left font-mono text-[0.7rem] font-normal text-muted-foreground">
                  {row}
                </th>
                {cols.map((col) => {
                  const parts: Record<string, string> = {
                    [dims[0]]: row,
                    [dims[1]]: col,
                  };
                  rest.forEach((dimKeys, i) => {
                    parts[dims[i + 2]] = dimKeys[0] ?? "";
                  });
                  const scope = scopeKey(dims, parts);
                  const cell = byScope.get(scope) ?? missingCell(scope);
                  const glyph =
                    cell.status === "running"
                      ? "run"
                      : cell.status === "failed"
                        ? "fail"
                        : "";
                  return (
                    <td key={col} className="p-1">
                      <Cell
                        scope={scope}
                        label={glyph}
                        cell={cell}
                        keyCount={keyCountOf(scope)}
                        onPick={onPick}
                      />
                    </td>
                  );
                })}
              </tr>
            ))}
            {!rows.length && (
              <tr>
                <td
                  colSpan={Math.max(1, cols.length + 1)}
                  className="px-3 py-4 text-center text-muted-foreground"
                >
                  No partition keys committed yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    );
  }

  // One-dimensional: wrap of cells, ordered by the current key set then any
  // extra scopes (e.g. retired).
  const cells: { scope: string; label: string; cell: PartitionScope }[] = [];
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
  return (
    <div className="flex flex-wrap gap-1.5" role="list" aria-label="Partitions">
      {cells.map(({ scope, label, cell }) => (
        <div key={scope} role="listitem" className="max-w-[10rem]">
          <Cell
            scope={scope}
            label={label}
            cell={cell}
            keyCount={keyCountOf(scope)}
            onPick={onPick}
          />
        </div>
      ))}
      {!cells.length && (
        <span className="text-xs text-muted-foreground">
          No partition keys committed yet.
        </span>
      )}
    </div>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return <Eyebrow>{children}</Eyebrow>;
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
  const scopes = partitions.data?.partitions ?? [];
  const complete = scopes.filter((s) => s.status === "complete").length;
  const dims = Object.keys(asset.partitions?.dims ?? {});
  const cursorPresent = detail.data?.cursor != null;

  return (
    <Sheet open={!!name} onOpenChange={(open) => !open && select(null)}>
      <SheetContent className="w-full gap-0 overflow-y-auto p-0 sm:max-w-2xl">
        <SheetHeader className="flex-row items-start gap-3 border-b p-5 pr-14">
          <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary">
            <LayoutGrid className="size-4.5" />
          </span>
          <div className="min-w-0 flex-1">
            <SheetTitle className="truncate font-mono text-base">
              {asset.name}
            </SheetTitle>
            <SheetDescription className="text-xs">
              {asset.doc ?? "Asset"}
            </SheetDescription>
          </div>
          <Button size="sm" onClick={() => openMaterialize([asset.name])}>
            <Play /> Materialize
          </Button>
        </SheetHeader>
        <div className="flex flex-col gap-6 p-5">
          {action.error && <ErrorNotice message={action.error} />}

          <section className="flex flex-col gap-2">
            <SectionLabel>Outputs</SectionLabel>
            <div className="overflow-hidden rounded-lg border">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b bg-muted/40 text-[0.7rem] text-muted-foreground">
                    <th className="px-3 py-2 text-left font-semibold">Name</th>
                    <th className="px-3 py-2 text-left font-semibold">Store</th>
                    <th className="px-3 py-2 text-left font-semibold">Key</th>
                    <th className="px-3 py-2 text-left font-semibold">
                      Migrations
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {asset.outputs.map((output) => {
                    const StoreIcon = storeIcon(output.store);
                    const heads = headEntries(detail.data, asset, output.name);
                    return (
                      <tr key={output.name} className="border-b last:border-0">
                        <td className="px-3 py-2 font-mono">{output.name}</td>
                        <td className="px-3 py-2">
                          <span className="flex items-center gap-1.5">
                            <StoreIcon className="size-3.5 text-muted-foreground" />
                            {output.store}
                          </span>
                        </td>
                        <td className="px-3 py-2 font-mono text-muted-foreground">
                          {output.key ??
                            (output.partition_set ? "<elements>" : "—")}
                          {output.revision ? ` @${output.revision}` : ""}
                          {output.mode ? ` · ${output.mode}` : ""}
                        </td>
                        <td className="px-3 py-2">
                          <MigrationCell output={output} heads={heads} />
                        </td>
                      </tr>
                    );
                  })}
                  {!asset.outputs.length && (
                    <tr>
                      <td
                        colSpan={4}
                        className="px-3 py-3 text-muted-foreground"
                      >
                        A job — no declared outputs.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </section>

          <section className="grid grid-cols-2 gap-5">
            <div className="flex flex-col gap-1.5">
              <SectionLabel>Placement</SectionLabel>
              <span className="font-mono text-xs">
                {asset.placement.kind}
                {Object.entries(asset.placement.placement).length > 0 && (
                  <span className="text-muted-foreground">
                    {" "}
                    {Object.entries(asset.placement.placement)
                      .map(([k, v]) => `${k}=${v}`)
                      .join(" ")}
                  </span>
                )}
              </span>
            </div>
            <div className="flex flex-col gap-1.5">
              <SectionLabel>Dependencies</SectionLabel>
              {asset.deps.length ? (
                <div className="flex flex-wrap gap-1.5">
                  {asset.deps.map((dep) => (
                    <span
                      key={dep}
                      className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs"
                    >
                      {dep}
                    </span>
                  ))}
                </div>
              ) : (
                <span className="text-xs text-muted-foreground">None</span>
              )}
            </div>
          </section>

          <section className="flex flex-col gap-2.5">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <SectionLabel>
                Partitions{dims.length ? ` · ${dims.join(" × ")}` : ""}
              </SectionLabel>
              <span className="flex items-center gap-3 text-xs text-muted-foreground">
                {dims.length > 0 && (
                  <span className="flex items-center gap-1 tabular-nums">
                    <Check className="size-3 text-emerald-600 dark:text-emerald-400" />
                    {complete}/{scopes.length} complete
                  </span>
                )}
                {cursorPresent && (
                  <span className="flex items-center gap-1">
                    <Bookmark className="size-3 text-primary" />
                    cursor
                  </span>
                )}
              </span>
            </div>
            <PartitionGrid
              asset={asset}
              detail={detail.data}
              scopes={scopes}
              onPick={(scope) => openMaterialize([asset.name], [scope])}
            />
            <p className="text-[0.7rem] text-muted-foreground">
              Click a cell to materialize it · the play icon (or ⌘/Ctrl-click)
              opens its last attempt.
            </p>
          </section>

          <section className="flex flex-col gap-2">
            <SectionLabel>Automations</SectionLabel>
            {(detail.data?.automations ?? []).length ? (
              (detail.data?.automations ?? []).map((auto) => (
                <div
                  key={auto.name}
                  className="flex items-center gap-3 rounded-lg border px-3 py-2"
                  data-automation={auto.name}
                >
                  <span className="flex size-7 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground">
                    <Zap className="size-3.5" />
                  </span>
                  <span className="min-w-0 flex-1">
                    <span className="block truncate font-mono text-xs">
                      {auto.name}
                    </span>
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
        </div>
      </SheetContent>
    </Sheet>
  );
}
