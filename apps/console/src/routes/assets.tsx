import { useEffect, useRef, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { LayoutGrid, Network, Play } from "lucide-react";
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
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { assetStatus, updateModel } from "@/components/asset-sheet";
import { Empty, StatusBadge } from "@/components/common";
import { time } from "@/lib/format";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type { CatalogAsset } from "@/lib/types";

export const Route = createFileRoute("/assets")({
  component: AssetsPage,
});

function Graph({ assets }: { assets: CatalogAsset[] }) {
  const { select } = useWorkspace();
  const byName = new Map(assets.map((asset) => [asset.name, asset]));
  const levels = new Map<string, number>();
  function depth(name: string): number {
    if (levels.has(name)) return levels.get(name)!;
    const value = Math.max(
      0,
      ...(byName
        .get(name)
        ?.inputs.filter((input) => byName.has(input))
        .map((input) => depth(input) + 1) || []),
    );
    levels.set(name, value);
    return value;
  }
  assets.forEach((asset) => depth(asset.name));
  const columns = new Map<number, CatalogAsset[]>();
  assets.forEach((asset) => {
    const level = depth(asset.name);
    columns.set(level, [...(columns.get(level) || []), asset]);
  });
  const height = Math.max(
    440,
    ...Array.from(columns.values()).map((items) => items.length * 124 + 80),
  );
  const width = Math.max(760, (Math.max(0, ...levels.values()) + 1) * 284 + 32);
  const positions = new Map<string, { x: number; y: number }>();
  columns.forEach((items, level) =>
    items.forEach((asset, index) =>
      positions.set(asset.name, {
        x: 32 + level * 284,
        y: (height - items.length * 124) / 2 + index * 124,
      }),
    ),
  );
  const statusColor: Record<string, string> = {
    materialized: "bg-emerald-500",
    stale: "bg-amber-500",
    partial: "bg-amber-500",
    not_materialized: "bg-muted-foreground/40",
  };
  return (
    <div
      className="overflow-x-auto rounded-xl border bg-card"
      aria-label="Asset lineage graph"
    >
      <div className="relative" style={{ width, height }}>
        <svg
          className="absolute inset-0 text-border"
          width={width}
          height={height}
          aria-hidden="true"
        >
          <defs>
            <marker
              id="edge-arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="5"
              markerHeight="5"
              orient="auto"
            >
              <path d="M0 0 10 5 0 10z" fill="currentColor" />
            </marker>
          </defs>
          {assets.flatMap((asset) =>
            asset.inputs
              .filter((input) => positions.has(input))
              .map((input) => {
                const from = positions.get(input)!;
                const to = positions.get(asset.name)!;
                const x = from.x + 216;
                const y = from.y + 44;
                return (
                  <path
                    key={`${input}:${asset.name}`}
                    d={`M${x},${y} C${x + 34},${y} ${to.x - 34},${to.y + 44} ${to.x},${to.y + 44}`}
                    fill="none"
                    stroke="currentColor"
                    strokeWidth={1.5}
                    markerEnd="url(#edge-arrow)"
                  />
                );
              }),
          )}
        </svg>
        {assets.map((asset) => (
          <button
            key={asset.name}
            className="absolute flex w-[216px] flex-col gap-2 rounded-xl border bg-popover p-3 text-left shadow-xs transition-colors hover:border-foreground/30 focus-visible:ring-2 focus-visible:ring-ring"
            style={{
              left: positions.get(asset.name)!.x,
              top: positions.get(asset.name)!.y,
            }}
            onClick={() => select({ kind: "asset", name: asset.name })}
            aria-label={`Inspect ${asset.name}`}
          >
            <span className="flex items-center gap-2">
              <LayoutGrid className="size-4 shrink-0 text-muted-foreground" />
              <strong
                className="min-w-0 flex-1 truncate font-mono text-xs"
                title={asset.name}
              >
                {asset.name}
              </strong>
            </span>
            <span className="flex items-center justify-between text-xs text-muted-foreground">
              <span>{updateModel(asset)}</span>
              <span
                className={cn(
                  "size-2 rounded-full",
                  statusColor[assetStatus(asset)],
                )}
              />
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}

function AssetsPage() {
  const { state, select, openMaterialize, checked, setChecked } =
    useWorkspace();
  const [search, setSearch] = useState("");
  const [view, setView] = useState("table");
  const input = useRef<HTMLInputElement>(null);
  const assets = (state?.assets ?? []).filter((asset) =>
    `${asset.name} ${asset.description}`
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
  if (!state) return null;
  function toggle(name: string, on: boolean) {
    setChecked((current) =>
      on ? [...current, name] : current.filter((v) => v !== name),
    );
  }
  return (
    <section className="flex flex-col gap-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
            Workspace
          </div>
          <h1 className="font-heading text-xl font-medium">Asset catalog</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Data products, their dependencies, and what needs to run.
          </p>
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative min-w-52 flex-1">
          <Input
            ref={input}
            type="search"
            placeholder="Filter assets…"
            aria-label="Filter assets"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </div>
        <Tabs value={view} onValueChange={(value) => setView(value as string)}>
          <TabsList>
            <TabsTrigger value="table" aria-label="Table view">
              <LayoutGrid />
              Table
            </TabsTrigger>
            <TabsTrigger value="graph" aria-label="Graph view">
              <Network />
              Graph
            </TabsTrigger>
          </TabsList>
        </Tabs>
        <span className="text-xs text-muted-foreground">
          {assets.length} assets ·{" "}
          {new Set(assets.map((asset) => asset.group)).size} groups
        </span>
      </div>
      {view === "graph" ? (
        <Graph assets={assets} />
      ) : !assets.length ? (
        <Empty title="No matching assets">
          Adjust the filter to see assets in this workspace.
        </Empty>
      ) : (
        <div className="overflow-x-auto rounded-xl border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-8">
                  <Checkbox
                    aria-label="Select all listed assets"
                    checked={
                      !!assets.length &&
                      assets.every((asset) => checked.includes(asset.name))
                    }
                    onCheckedChange={(value) =>
                      setChecked(
                        value === true
                          ? Array.from(
                              new Set([
                                ...checked,
                                ...assets.map((asset) => asset.name),
                              ]),
                            )
                          : checked.filter(
                              (name) =>
                                !assets.some((asset) => asset.name === name),
                            ),
                      )
                    }
                  />
                </TableHead>
                <TableHead>Asset</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Update model</TableHead>
                <TableHead>Published</TableHead>
                <TableHead className="w-10" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {assets.map((asset) => (
                <TableRow key={asset.name}>
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
                      className="flex items-center gap-2 text-left"
                      aria-label={asset.name}
                      onClick={() =>
                        select({ kind: "asset", name: asset.name })
                      }
                    >
                      <LayoutGrid className="size-4 shrink-0 text-muted-foreground" />
                      <span className="min-w-0">
                        <span className="block truncate font-mono text-xs font-medium">
                          {asset.name}
                        </span>
                        <span className="block text-xs text-muted-foreground">
                          {asset.inputs.length
                            ? `${asset.inputs.length} upstream asset${asset.inputs.length > 1 ? "s" : ""}`
                            : "Source asset"}
                        </span>
                      </span>
                    </button>
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={assetStatus(asset)} />
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {updateModel(asset)}
                  </TableCell>
                  <TableCell className="text-muted-foreground">
                    {asset.heads.length
                      ? `${asset.heads.length} scope${asset.heads.length > 1 ? "s" : ""} · ${time(Math.max(...asset.heads.map((h) => h.updated_at)))}`
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
              ))}
            </TableBody>
          </Table>
        </div>
      )}
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>
          {assets.length} of {state.assets.length} assets
        </span>
        <span>
          Definition <code>{state.revision.slice(0, 10)}</code>
        </span>
      </div>
    </section>
  );
}
