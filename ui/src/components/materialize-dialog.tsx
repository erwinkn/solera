import { useMemo, useRef, useState } from "react";
import { Play } from "lucide-react";
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
import { request, useAction } from "@/lib/api";
import { useWorkspace } from "@/lib/workspace";
import { ErrorNotice } from "./common";

const modes = [
  { value: "incremental", label: "Incremental — process changes" },
  { value: "fill_missing", label: "Fill missing — retain complete scopes" },
  { value: "recompute", label: "Recompute — rebuild selected scopes" },
];

export function MaterializeDialog() {
  const { state, materializeTargets, closeMaterialize, select, refresh } =
    useWorkspace();
  const open = materializeTargets !== null && !!state;
  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) closeMaterialize();
      }}
    >
      <DialogContent className="sm:max-w-lg" showCloseButton>
        {open && (
          <MaterializeForm
            key={materializeTargets.join(",")}
            initial={materializeTargets}
            done={(runId) => {
              closeMaterialize();
              refresh();
              select({ kind: "run", id: runId });
            }}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function MaterializeForm({
  initial,
  done,
}: {
  initial: string[];
  done: (runId: string) => void;
}) {
  const { state, closeMaterialize } = useWorkspace();
  const assets = state?.assets ?? [];
  const [selected, setSelected] = useState<string[]>(initial);
  const [mode, setMode] = useState("incremental");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [configText, setConfigText] = useState("{}");
  const [formError, setFormError] = useState<string | null>(null);
  const action = useAction();
  // One idempotency key per distinct request body — a retry after a failed
  // submit replays safely, while any edit mints a fresh key.
  const idempotency = useRef<{ body: string; key: string } | null>(null);
  const partitioned = useMemo(
    () =>
      selected.some(
        (name) => assets.find((asset) => asset.name === name)?.partitions,
      ),
    [assets, selected],
  );

  function toggle(name: string, checked: boolean) {
    setSelected((current) =>
      checked ? [...current, name] : current.filter((value) => value !== name),
    );
  }

  async function submit() {
    setFormError(null);
    try {
      if (!selected.length) throw new Error("Select at least one asset");
      const partitions: string[] = [];
      if (partitioned) {
        if (!from || !to || from > to)
          throw new Error("Choose a valid inclusive date range");
        for (
          let day = new Date(`${from}T00:00:00Z`),
            last = new Date(`${to}T00:00:00Z`);
          day <= last;
          day.setUTCDate(day.getUTCDate() + 1)
        ) {
          partitions.push(day.toISOString().slice(0, 10));
          if (partitions.length > 1000)
            throw new Error("Maximum 1,000 partitions");
        }
      }
      const config: unknown = JSON.parse(configText);
      if (!config || Array.isArray(config) || typeof config !== "object")
        throw new Error("Configuration must be a JSON object");
      const body = JSON.stringify({
        targets: selected,
        partitions,
        mode,
        config,
      });
      if (idempotency.current?.body !== body)
        idempotency.current = { body, key: crypto.randomUUID() };
      const run = await action.run(() =>
        request<{ id: string }>("/runs", {
          body: JSON.parse(body),
          headers: { "Idempotency-Key": idempotency.current!.key },
        }),
      );
      if (run) done(run.id);
    } catch (failure) {
      setFormError(
        failure instanceof Error ? failure.message : String(failure),
      );
    }
  }

  return (
    <>
      <DialogHeader>
        <DialogTitle>Materialize assets</DialogTitle>
        <DialogDescription>
          Upstream dependencies are included automatically. Each producer
          publishes its outputs together.
        </DialogDescription>
      </DialogHeader>
      <div className="flex max-h-[60vh] flex-col gap-4 overflow-y-auto">
        <fieldset>
          <legend className="mb-2 text-sm font-medium">
            Targets{" "}
            <span className="text-muted-foreground">{selected.length}</span>
          </legend>
          <div className="grid max-h-44 grid-cols-1 gap-1 overflow-y-auto rounded-lg border p-2 sm:grid-cols-2">
            {assets.map((asset) => (
              <Label
                key={asset.name}
                className="flex cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 font-normal hover:bg-muted"
              >
                <Checkbox
                  aria-label={asset.name}
                  checked={selected.includes(asset.name)}
                  onCheckedChange={(checked) =>
                    toggle(asset.name, checked === true)
                  }
                />
                <span className="min-w-0 flex-1 truncate font-mono text-xs">
                  {asset.name}
                </span>
                <span className="text-xs text-muted-foreground">
                  {asset.group}
                </span>
              </Label>
            ))}
          </div>
        </fieldset>
        <div className="grid gap-1.5">
          <Label>Mode</Label>
          <Select
            value={mode}
            onValueChange={(value) => setMode(value as string)}
          >
            <SelectTrigger className="w-full" aria-label="Mode">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {modes.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        {partitioned && (
          <div className="grid gap-1.5">
            <div className="grid grid-cols-2 gap-3">
              <div className="grid gap-1.5">
                <Label htmlFor="from-date">From</Label>
                <Input
                  id="from-date"
                  type="date"
                  value={from}
                  onChange={(event) => setFrom(event.target.value)}
                />
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="to-date">Through</Label>
                <Input
                  id="to-date"
                  type="date"
                  value={to}
                  onChange={(event) => setTo(event.target.value)}
                />
              </div>
            </div>
            <p className="text-xs text-muted-foreground">
              Inclusive daily partitions. Maximum 1,000.
            </p>
          </div>
        )}
        <div className="grid gap-1.5">
          <Label htmlFor="run-config">Request configuration (JSON)</Label>
          <Textarea
            id="run-config"
            rows={3}
            className="font-mono text-xs"
            value={configText}
            onChange={(event) => setConfigText(event.target.value)}
          />
        </div>
        {(formError || action.error) && (
          <ErrorNotice message={formError ?? action.error!} />
        )}
      </div>
      <DialogFooter showCloseButton={false}>
        <Button variant="outline" onClick={closeMaterialize}>
          Cancel
        </Button>
        <Button onClick={submit} disabled={action.pending}>
          <Play />
          {action.pending ? "Submitting…" : "Start materialization"}
        </Button>
      </DialogFooter>
    </>
  );
}
