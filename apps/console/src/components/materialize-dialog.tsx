import { useEffect, useMemo, useState } from "react";
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { ErrorNotice } from "@/components/common";
import { request, useAction } from "@/lib/api";
import { useWorkspace } from "@/lib/workspace";
import type { Run } from "@/lib/types";

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
  const [partitions, setPartitions] = useState("latest");
  const [scopeList, setScopeList] = useState("");
  const [mode, setMode] = useState("incremental");
  const [upstream, setUpstream] = useState(false);
  const [config, setConfig] = useState("{}");
  const [keys, setKeys] = useState("");
  const [configError, setConfigError] = useState<string | null>(null);
  const action = useAction();

  useEffect(() => {
    if (open) {
      setTargets(materializeTargets ?? []);
      setPartitions(materializeScopes.length ? "explicit" : "latest");
      setScopeList(materializeScopes.join(", "));
      setMode("incremental");
      setUpstream(false);
      setConfig("{}");
      setKeys("");
      setConfigError(null);
    }
  }, [open, materializeTargets, materializeScopes]);

  const bykeyEdges = useMemo(() => {
    const edges = new Set<string>();
    for (const name of targets)
      for (const edge of Object.values(
        assets.find((a) => a.name === name)?.inputs ?? {},
      ))
        if (edge.kind === "bykey") edges.add(edge.output);
    return [...edges];
  }, [targets, assets]);

  function toggle(name: string, on: boolean) {
    setTargets((current) =>
      on ? [...current, name] : current.filter((t) => t !== name),
    );
  }

  async function submit() {
    let parsedConfig: Record<string, unknown>;
    try {
      parsedConfig = JSON.parse(config || "{}");
      if (typeof parsedConfig !== "object" || parsedConfig === null)
        throw new Error("not an object");
      setConfigError(null);
    } catch {
      setConfigError("Config must be a JSON object");
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
          setConfigError(
            `Keys line must be EDGE=full or EDGE=k1,k2: ${trimmed}`,
          );
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
    const partitionSelection =
      partitions === "explicit"
        ? scopeList
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean)
        : partitions;
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
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Materialize assets</DialogTitle>
          <DialogDescription>
            One task per asset and partition scope; upstream work is planned
            when requested.
          </DialogDescription>
        </DialogHeader>
        <div className="flex max-h-[60dvh] flex-col gap-4 overflow-y-auto py-2">
          {action.error && <ErrorNotice message={action.error} />}
          {configError && <ErrorNotice message={configError} />}
          <div className="flex flex-col gap-2">
            <Label>Targets</Label>
            <div className="grid max-h-40 grid-cols-1 gap-1 overflow-y-auto rounded-lg border p-2 sm:grid-cols-2">
              {assets.map((asset) => (
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
              <Label htmlFor="md-partitions">Partitions</Label>
              <Select
                value={partitions}
                onValueChange={(v) => setPartitions(v ?? "latest")}
              >
                <SelectTrigger id="md-partitions">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="latest">latest</SelectItem>
                  <SelectItem value="missing">missing</SelectItem>
                  <SelectItem value="all">all</SelectItem>
                  <SelectItem value="explicit">pick scopes…</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="md-mode">Mode</Label>
              <Select
                value={mode}
                onValueChange={(v) => setMode(v ?? "incremental")}
              >
                <SelectTrigger id="md-mode">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="incremental">incremental</SelectItem>
                  <SelectItem value="recompute">recompute</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          {partitions === "explicit" && (
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="md-scopes">Scopes (comma-separated)</Label>
              <Input
                id="md-scopes"
                placeholder="alpha, bravo"
                value={scopeList}
                onChange={(e) => setScopeList(e.target.value)}
              />
            </div>
          )}
          <label className="flex items-center gap-2 text-sm">
            <Checkbox
              aria-label="Include upstream"
              checked={upstream}
              onCheckedChange={(v) => setUpstream(v === true)}
            />
            Materialize upstream first
          </label>
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
          {!!bykeyEdges.length && (
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="md-keys">
                ByKey overrides — one per line: EDGE=full or EDGE=k1,k2
              </Label>
              <Textarea
                id="md-keys"
                rows={2}
                className="font-mono text-xs"
                placeholder={bykeyEdges.map((e) => `${e}=full`).join("\n")}
                value={keys}
                onChange={(e) => setKeys(e.target.value)}
              />
            </div>
          )}
        </div>
        <DialogFooter>
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
