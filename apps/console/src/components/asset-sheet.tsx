import { useState } from "react";
import { ArrowRight, LayoutGrid, Play } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useQuery } from "@/lib/api";
import { useWorkspace } from "@/lib/workspace";
import type { AssetDetail, CatalogAsset } from "@/lib/types";
import { time } from "@/lib/format";
import {
  Empty,
  ErrorNotice,
  JsonBlock,
  Loading,
  Properties,
  StatusBadge,
} from "./common";
import { DataPreview } from "./data-preview";

export function assetStatus(asset: CatalogAsset) {
  if (!asset.heads.length) return "not_materialized";
  if (asset.heads.some((head) => head.state_version !== asset.version))
    return "stale";
  if (asset.heads.some((head) => !head.scope_complete)) return "partial";
  return "materialized";
}

export function updateModel(asset: CatalogAsset) {
  if (asset.incremental) return "Keyed incremental";
  if (asset.partitions) return "Daily partitions";
  return "Snapshot";
}

export function AssetSheet() {
  const { selection, select } = useWorkspace();
  const open = selection?.kind === "asset";
  return (
    <Sheet
      open={open}
      modal={false}
      onOpenChange={(next) => {
        if (!next) select(null);
      }}
    >
      <SheetContent
        side="right"
        hideOverlay
        className="w-full overflow-y-auto sm:max-w-xl"
      >
        {open && <AssetDetailView key={selection.name} name={selection.name} />}
      </SheetContent>
    </Sheet>
  );
}

function AssetDetailView({ name }: { name: string }) {
  const { state, select, openMaterialize } = useWorkspace();
  const asset = state?.assets.find((entry) => entry.name === name);
  const [partition, setPartition] = useState(
    () => asset?.heads[0]?.partition ?? "",
  );
  const query = useQuery<AssetDetail>(
    `/assets/${encodeURIComponent(name)}?partition=${encodeURIComponent(partition)}`,
  );
  const detail = query.data;
  return (
    <>
      <SheetHeader>
        <SheetTitle className="font-mono">{name}</SheetTitle>
      </SheetHeader>
      <div className="flex flex-col gap-4 px-4 pb-6">
        {query.error && <ErrorNotice message={query.error.message} />}
        {!detail ? (
          <Loading label="Loading asset…" />
        ) : (
          <>
            <div className="flex items-center justify-between gap-3">
              {asset && <StatusBadge status={assetStatus(asset)} />}
              <Button
                size="sm"
                onClick={() => {
                  select(null);
                  openMaterialize([name]);
                }}
              >
                <Play />
                Materialize
              </Button>
            </div>
            <p className="text-sm text-muted-foreground">
              {asset?.description ||
                "No description provided in the asset definition."}
            </p>
            {asset?.partitions && asset.heads.length > 0 && (
              <div className="grid w-56 gap-1.5">
                <Select
                  value={partition}
                  onValueChange={(value) => setPartition(value as string)}
                >
                  <SelectTrigger aria-label="Partition" className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {asset.heads.map((head) => (
                      <SelectItem key={head.partition} value={head.partition}>
                        {head.partition}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
            <Tabs defaultValue="overview">
              <TabsList>
                <TabsTrigger value="overview">Overview</TabsTrigger>
                <TabsTrigger value="data">Data</TabsTrigger>
              </TabsList>
              <TabsContent
                value="overview"
                className="flex flex-col gap-4 pt-3"
              >
                {asset && (
                  <>
                    <section>
                      <h3 className="mb-2 text-sm font-medium">Definition</h3>
                      <Properties
                        entries={[
                          [
                            "Producer",
                            <code key="p" className="font-mono text-xs">
                              {asset.producer}
                            </code>,
                          ],
                          ["Update model", updateModel(asset)],
                          ["Group", asset.group],
                          [
                            "Data version",
                            <code key="v" className="font-mono text-xs">
                              {asset.version?.slice(0, 16) ||
                                "Not materialized"}
                            </code>,
                          ],
                        ]}
                      />
                    </section>
                    {asset.incremental && (
                      <p className="rounded-lg bg-muted/50 px-3 py-2 text-xs text-muted-foreground">
                        Tracks <code>{asset.incremental.key}</code> by{" "}
                        <code>{asset.incremental.revision}</code>, committing up
                        to {asset.incremental.batch_size} source changes per
                        batch.
                      </p>
                    )}
                    <section>
                      <h3 className="mb-2 text-sm font-medium">Dependencies</h3>
                      <div className="flex flex-col gap-1">
                        {asset.inputs.length ? (
                          asset.inputs.map((input) => (
                            <button
                              key={input}
                              className="flex items-center gap-2 rounded-lg border px-3 py-2 text-left font-mono text-xs transition-colors hover:bg-muted"
                              onClick={() =>
                                select({ kind: "asset", name: input })
                              }
                            >
                              <LayoutGrid className="size-3.5 shrink-0 text-muted-foreground" />
                              <span className="min-w-0 flex-1 truncate">
                                {input}
                              </span>
                              <ArrowRight className="size-3.5 shrink-0 text-muted-foreground" />
                            </button>
                          ))
                        ) : (
                          <p className="text-sm text-muted-foreground">
                            Source asset · no upstream dependencies
                          </p>
                        )}
                      </div>
                    </section>
                    <section>
                      <h3 className="mb-2 text-sm font-medium">Downstream</h3>
                      <div className="flex flex-col gap-1">
                        {state?.assets
                          .filter((entry) => entry.inputs.includes(name))
                          .map((entry) => (
                            <button
                              key={entry.name}
                              className="flex items-center gap-2 rounded-lg border px-3 py-2 text-left font-mono text-xs transition-colors hover:bg-muted"
                              onClick={() =>
                                select({ kind: "asset", name: entry.name })
                              }
                            >
                              <LayoutGrid className="size-3.5 shrink-0 text-muted-foreground" />
                              <span className="min-w-0 flex-1 truncate">
                                {entry.name}
                              </span>
                              <ArrowRight className="size-3.5 shrink-0 text-muted-foreground" />
                            </button>
                          ))}
                        {!state?.assets.some((entry) =>
                          entry.inputs.includes(name),
                        ) && (
                          <p className="text-sm text-muted-foreground">
                            No downstream assets
                          </p>
                        )}
                      </div>
                    </section>
                    {asset.partitions && asset.heads.length > 0 && (
                      <section>
                        <h3 className="mb-2 text-sm font-medium">
                          Materialized partitions{" "}
                          <span className="text-muted-foreground">
                            {asset.heads.length}
                          </span>
                        </h3>
                        <div className="flex flex-wrap gap-1.5">
                          {asset.heads.map((head) => (
                            <button
                              key={head.partition}
                              title={`${head.partition} · ${time(head.updated_at)}`}
                              aria-label={`Inspect partition ${head.partition}`}
                              className="rounded-md border px-2 py-1 font-mono text-xs transition-colors hover:bg-muted"
                              onClick={() => setPartition(head.partition)}
                            >
                              {head.partition.slice(5)}
                            </button>
                          ))}
                        </div>
                      </section>
                    )}
                  </>
                )}
                {detail.checkpoint && (
                  <section>
                    <h3 className="mb-2 text-sm font-medium">
                      Incremental checkpoint
                    </h3>
                    <JsonBlock value={detail.checkpoint} />
                  </section>
                )}
              </TabsContent>
              <TabsContent value="data" className="flex flex-col gap-4 pt-3">
                {!detail.head ? (
                  <Empty title="No committed data">
                    Materialize this asset to inspect its output.
                  </Empty>
                ) : (
                  <>
                    <section>
                      <h3 className="mb-2 text-sm font-medium">
                        Data preview · first 100 rows
                      </h3>
                      <DataPreview value={detail.preview} />
                    </section>
                    <section>
                      <h3 className="mb-2 text-sm font-medium">
                        Immutable output
                      </h3>
                      <JsonBlock value={detail.head} />
                    </section>
                    {detail.commit && (
                      <section>
                        <h3 className="mb-2 text-sm font-medium">
                          Materialization commit
                        </h3>
                        <JsonBlock value={detail.commit} />
                      </section>
                    )}
                  </>
                )}
              </TabsContent>
            </Tabs>
          </>
        )}
      </div>
    </>
  );
}
