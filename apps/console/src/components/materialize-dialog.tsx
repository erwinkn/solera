import { useEffect, useMemo, useState } from "react";
import { Search, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { CELL_TONE } from "@/components/asset-sheet";
import { ErrorNotice, Segmented } from "@/components/common";
import { request, useAction, useQuery } from "@/lib/api";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type { CatalogAsset, PartitionScope, Run } from "@/lib/types";

type PartitionMode = "latest" | "missing" | "all" | "pick";

// When exactly one partitioned target is chosen, pick individual scopes off its
// grid — the same cell colours as the asset sheet, toggled into the selection.
// Retired scopes can't be materialized, so they render disabled.
function ScopePicker({
  scopes,
  selected,
  onToggle,
}: {
  scopes: PartitionScope[];
  selected: Set<string>;
  onToggle: (scope: string) => void;
}) {
  if (!scopes.length)
    return (
      <p className="text-xs text-muted-foreground">
        No partition keys committed yet.
      </p>
    );
  return (
    <div className="flex max-h-32 flex-wrap gap-1.5 overflow-y-auto rounded-lg border p-2">
      {scopes.map((scope) => {
        const on = selected.has(scope.scope);
        const retired = scope.status === "retired";
        return (
          <button
            key={scope.scope}
            type="button"
            data-scope={scope.scope}
            data-status={scope.status}
            aria-pressed={on}
            disabled={retired}
            title={retired ? "Retired scopes can't be materialized" : undefined}
            onClick={() => onToggle(scope.scope)}
            className={cn(
              "rounded-md border px-2 py-1 font-mono text-[0.7rem] transition",
              CELL_TONE[scope.status] ?? CELL_TONE.missing,
              on && "ring-2 ring-primary ring-offset-1 ring-offset-background",
              retired
                ? "cursor-not-allowed opacity-60"
                : "hover:brightness-105",
            )}
          >
            {scope.scope}
          </button>
        );
      })}
    </div>
  );
}

export function MaterializeDialog() {
  const {
    materializeTargets,
    materializeScopes,
    closeMaterialize,
    assets,
    base,
    select,
  } = useWorkspace();
  const open = materializeTargets !== null;
  const [targets, setTargets] = useState<string[]>([]);
  const [partitions, setPartitions] = useState<PartitionMode>("latest");
  const [picked, setPicked] = useState<Set<string>>(new Set());
  const [mode, setMode] = useState<"incremental" | "full">("incremental");
  const [upstream, setUpstream] = useState(false);
  const [config, setConfig] = useState("{}");
  const [keys, setKeys] = useState("");
  const [search, setSearch] = useState("");
  const [formError, setFormError] = useState<string | null>(null);
  const action = useAction();

  useEffect(() => {
    if (open) {
      setTargets(materializeTargets ?? []);
      setPartitions(materializeScopes.length ? "pick" : "latest");
      setPicked(new Set(materializeScopes));
      setMode("incremental");
      setUpstream(false);
      setConfig("{}");
      setKeys("");
      setSearch("");
      setFormError(null);
    }
  }, [open, materializeTargets, materializeScopes]);

  const selectedAssets = targets
    .map((name) => assets.find((a) => a.name === name))
    .filter((a): a is CatalogAsset => !!a);

  const incrementalEdges = useMemo(() => {
    const edges = new Set<string>();
    for (const asset of selectedAssets)
      for (const edge of Object.values(asset.inputs))
        if (edge.kind === "incremental") edges.add(edge.output);
    return [...edges];
  }, [selectedAssets]);

  // The grid picker only works when exactly one partitioned target is
  // selected — with several targets its scopes would not match them all.
  const pickTarget =
    selectedAssets.length === 1 && selectedAssets[0].partitions
      ? selectedAssets[0].name
      : null;

  const scopeList = useQuery<{ partitions: PartitionScope[] }>(
    partitions === "pick" && pickTarget && base
      ? `${base}/partitions/${pickTarget}`
      : null,
    4000,
  );
  const pickScopes = useMemo(
    () => scopeList.data?.partitions ?? [],
    [scopeList.data],
  );

  // Retired scopes can't be materialized — drop any that sneak into picked
  // (e.g. seeded from a grid cell click) once the scope list loads.
  useEffect(() => {
    const retired = new Set(
      pickScopes.filter((s) => s.status === "retired").map((s) => s.scope),
    );
    if (retired.size)
      setPicked((current) => {
        const next = new Set([...current].filter((s) => !retired.has(s)));
        return next.size === current.size ? current : next;
      });
  }, [pickScopes]);

  const filteredAssets = assets.filter((a) =>
    a.name.toLowerCase().includes(search.toLowerCase()),
  );

  function toggle(name: string, on: boolean) {
    setTargets((current) =>
      on ? [...current, name] : current.filter((t) => t !== name),
    );
  }
  function togglePick(scope: string) {
    setPicked((current) => {
      const next = new Set(current);
      if (next.has(scope)) next.delete(scope);
      else next.add(scope);
      return next;
    });
  }

  async function submit() {
    let parsedConfig: Record<string, unknown>;
    try {
      parsedConfig = JSON.parse(config || "{}");
      if (typeof parsedConfig !== "object" || parsedConfig === null)
        throw new Error("not an object");
    } catch {
      setFormError("Config must be a JSON object");
      return;
    }
    let parsedKeys: Record<string, string | string[]> | undefined;
    if (keys.trim()) {
      parsedKeys = {};
      for (const line of keys.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const [edge, value] = trimmed.split("=", 2);
        if (!edge || !value) {
          setFormError(`Keys line must be EDGE=full or EDGE=k1,k2: ${trimmed}`);
          return;
        }
        parsedKeys[edge.trim()] =
          value.trim() === "full"
            ? "full"
            : value
                .split(",")
                .map((k) => k.trim())
                .filter(Boolean);
      }
    }
    setFormError(null);
    const retired = new Set(
      pickScopes.filter((s) => s.status === "retired").map((s) => s.scope),
    );
    const partitionSelection =
      partitions === "pick"
        ? [...picked].filter((s) => !retired.has(s))
        : partitions;
    if (partitions === "pick" && !partitionSelection.length) {
      setFormError("Pick at least one materializable scope.");
      return;
    }
    const run = await action.run(() =>
      request<Run>(`${base}/runs`, {
        body: {
          targets,
          partitions: partitionSelection,
          mode,
          upstream,
          config: parsedConfig,
          keys: parsedKeys ?? null,
        },
      }),
    );
    if (run) {
      closeMaterialize();
      if (run.id) select({ kind: "run", id: run.id });
    }
  }

  return (
    <Dialog open={open} onOpenChange={(o) => !o && closeMaterialize()}>
      <DialogContent className="gap-0 p-0 sm:max-w-xl">
        <DialogHeader className="p-5 pb-3">
          <DialogTitle>Materialize</DialogTitle>
          <DialogDescription>
            One task per asset and partition scope; upstream planned on request.
          </DialogDescription>
        </DialogHeader>
        <div className="flex max-h-[64dvh] flex-col gap-5 overflow-y-auto px-5 pb-2">
          {(formError || action.error) && (
            <ErrorNotice message={formError ?? action.error!} />
          )}

          <div className="flex flex-col gap-2">
            <div className="flex items-center justify-between">
              <Label>Targets</Label>
              <span className="text-xs text-muted-foreground tabular-nums">
                {targets.length} selected
              </span>
            </div>
            {targets.length > 0 && (
              <div className="flex flex-wrap gap-1.5">
                {targets.map((name) => (
                  <span
                    key={name}
                    className="flex items-center gap-1.5 rounded-md border border-primary/40 bg-primary/10 px-2 py-1 font-mono text-xs font-medium text-primary"
                  >
                    {name}
                    <button
                      type="button"
                      aria-label={`Remove ${name}`}
                      onClick={() => toggle(name, false)}
                    >
                      <X className="size-3" />
                    </button>
                  </span>
                ))}
              </div>
            )}
            <div className="relative">
              <Search className="pointer-events-none absolute top-2.5 left-2.5 size-3.5 text-muted-foreground" />
              <Input
                className="h-8 pl-8 text-xs"
                placeholder="Filter targets…"
                aria-label="Filter targets"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>
            <div className="grid max-h-36 grid-cols-1 gap-0.5 overflow-y-auto rounded-lg border p-1 sm:grid-cols-2">
              {filteredAssets.map((asset) => (
                <label
                  key={asset.name}
                  className="flex cursor-pointer items-center gap-2 rounded-md px-2 py-1 text-sm hover:bg-muted"
                >
                  <Checkbox
                    aria-label={asset.name}
                    checked={targets.includes(asset.name)}
                    onCheckedChange={(v) => toggle(asset.name, v === true)}
                  />
                  <span className="truncate font-mono text-xs">
                    {asset.name}
                  </span>
                </label>
              ))}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1.5">
              <Label>Partitions</Label>
              <Segmented
                ariaLabel="Partitions"
                value={partitions}
                onChange={setPartitions}
                options={[
                  { value: "latest", label: "latest" },
                  { value: "missing", label: "missing" },
                  { value: "all", label: "all" },
                  { value: "pick", label: "pick" },
                ]}
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label>Mode</Label>
              <Segmented
                ariaLabel="Mode"
                value={mode}
                onChange={setMode}
                options={[
                  { value: "incremental", label: "incremental" },
                  { value: "full", label: "full" },
                ]}
              />
            </div>
          </div>

          {partitions === "pick" && (
            <div className="flex flex-col gap-1.5">
              <Label>
                Pick cells
                {picked.size > 0 ? ` — ${picked.size} selected` : ""}
              </Label>
              {pickTarget && base ? (
                <ScopePicker
                  scopes={pickScopes}
                  selected={picked}
                  onToggle={togglePick}
                />
              ) : (
                <Input
                  className="font-mono text-xs"
                  placeholder="alpha, bravo"
                  aria-label="Scopes"
                  value={[...picked].join(", ")}
                  onChange={(e) =>
                    setPicked(
                      new Set(
                        e.target.value
                          .split(",")
                          .map((s) => s.trim())
                          .filter(Boolean),
                      ),
                    )
                  }
                />
              )}
            </div>
          )}

          <label className="flex items-center gap-3">
            <Switch
              aria-label="Materialize upstream first"
              checked={upstream}
              onCheckedChange={(v) => setUpstream(v === true)}
            />
            <span className="flex flex-col">
              <span className="text-sm font-medium">
                Materialize upstream first
              </span>
              <span className="text-xs text-muted-foreground">
                plan the input closure, not just the targets
              </span>
            </span>
          </label>

          <div
            className={cn(
              "grid gap-3",
              incrementalEdges.length ? "grid-cols-2" : "grid-cols-1",
            )}
          >
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="md-config">Run config (JSON)</Label>
              <Textarea
                id="md-config"
                rows={2}
                className="font-mono text-xs"
                value={config}
                onChange={(e) => setConfig(e.target.value)}
              />
            </div>
            {!!incrementalEdges.length && (
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="md-keys">Incremental override</Label>
                <Textarea
                  id="md-keys"
                  rows={2}
                  className="font-mono text-xs"
                  placeholder={incrementalEdges
                    .map((e) => `${e}=full`)
                    .join("\n")}
                  value={keys}
                  onChange={(e) => setKeys(e.target.value)}
                />
              </div>
            )}
          </div>
        </div>
        <DialogFooter className="border-t bg-muted/30 p-4">
          <Button variant="outline" onClick={closeMaterialize}>
            Cancel
          </Button>
          <Button onClick={submit} disabled={action.pending || !targets.length}>
            {action.pending ? "Starting…" : "Start materialization"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
