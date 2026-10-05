import { useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { useManifest, useProject, q } from "@/api/queries";
import { useSubmitRun, type RunInput } from "@/api/mutations";
import { cn } from "@/lib/cn";
import { Button } from "@/ui/button";
import { Field, Input, SearchInput, Segmented, Switch, Textarea } from "@/ui/form";
import { Dialog } from "@/ui/overlay";

type Selection = "latest" | "missing" | "all" | "pick";

/** "Run" opens a run form, prefilled with the assets in view. */
export function RunButton({
  targets = [],
  partition,
  icon,
  label = "Run",
  variant = "primary",
}: {
  targets?: string[];
  partition?: string;
  icon?: ReactNode;
  label?: string;
  variant?: "primary" | "secondary";
}) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog
      open={open}
      onOpenChange={setOpen}
      trigger={
        <Button variant={variant} icon={icon}>
          {label}
        </Button>
      }
      title="Run"
      description="Submit a run. Inputs are pinned to their current heads unless you also build upstream."
    >
      {open && <RunForm initial={targets} partition={partition} onDone={() => setOpen(false)} />}
    </Dialog>
  );
}

type Mode = "incremental" | "all" | "full";

/** What each mode does, in one line: "All keys" and "Full" both read everything, only Full starts over. */
const MODE_MEANS: Record<Mode, string> = {
  incremental: "Reads only what changed since each partition last read it.",
  all: "Reads every key of its keyed inputs again and writes over what they made; nothing is reset first.",
  full: "Starts over: outputs reset, no cursor, and every input read from scratch.",
};

function RunForm({
  initial,
  partition,
  onDone,
}: {
  initial: string[];
  partition?: string;
  onDone: () => void;
}) {
  const manifest = useManifest();
  const project = useProject();
  const submit = useSubmitRun();
  const [targets, setTargets] = useState<string[]>(initial);
  const [filter, setFilter] = useState("");
  const [selection, setSelection] = useState<Selection>(partition !== undefined ? "pick" : "latest");
  const [picked, setPicked] = useState(partition ?? "");
  const [mode, setMode] = useState<Mode>("incremental");
  const [upstream, setUpstream] = useState(false);
  const [config, setConfig] = useState("");
  const [tags, setTags] = useState("");

  const names = Object.keys(manifest.assets).sort();
  const visible = names.filter((n) => n.toLowerCase().includes(filter.toLowerCase()));
  const single = targets.length === 1 ? targets[0] : undefined;
  const partitioned = targets.some((t) => manifest.assets[t]?.partitions);
  const partitions = useQuery({
    ...q.partitions(project, single ?? ""),
    enabled: !!single && !!manifest.assets[single]?.partitions && selection === "pick",
  }).data;

  const configError = (() => {
    if (!config.trim()) return null;
    try {
      const value: unknown = JSON.parse(config);
      return value && typeof value === "object" && !Array.isArray(value)
        ? null
        : "Config must be a JSON object.";
    } catch {
      return "Config isn't valid JSON.";
    }
  })();
  const tagPairs = tags
    .split(",")
    .map((t) => t.trim())
    .filter(Boolean)
    .map((t) => t.split("=") as [string, string | undefined]);
  const tagError = tagPairs.some(([k, v]) => !k || v === undefined) ? "Tags are name=value pairs." : null;
  const keys = picked
    .split("\n")
    .map((k) => k.trim())
    .filter(Boolean);
  const invalid =
    targets.length === 0 ||
    !!configError ||
    !!tagError ||
    (selection === "pick" && partitioned && keys.length === 0);

  // The keyed incremental inputs of the targets, by upstream output: what "All keys" reads again.
  const keyedInputs = [
    ...new Set(
      targets.flatMap((t) =>
        Object.values(manifest.assets[t]?.inputs ?? {})
          .filter((i) => i.kind === "incremental" && manifest.outputs[i.output]?.key != null)
          .map((i) => i.output),
      ),
    ),
  ].sort();
  const effectiveMode: Mode = mode === "all" && keyedInputs.length === 0 ? "incremental" : mode;

  const run = (): RunInput => ({
    targets,
    partitions: selection === "pick" ? (partitioned ? keys : "latest") : selection,
    mode: effectiveMode === "full" ? "full" : "incremental",
    keys:
      effectiveMode === "all"
        ? Object.fromEntries(keyedInputs.map((output) => [output, "all" as const]))
        : undefined,
    upstream,
    config: config.trim() ? (JSON.parse(config) as Record<string, unknown>) : {},
    tags: Object.fromEntries(tagPairs.map(([k, v]) => [k, v ?? ""])),
  });

  return (
    <form
      className="flex flex-col gap-5 p-5"
      onSubmit={(event) => {
        event.preventDefault();
        if (!invalid) submit.mutate(run(), { onSuccess: onDone });
      }}
    >
      <fieldset className="flex flex-col gap-2">
        <legend className="mb-1.5 text-xs font-medium text-fg-muted">
          Targets <span className="text-fg-subtle">· {targets.length} selected</span>
        </legend>
        <SearchInput
          aria-label="Filter assets"
          placeholder="Filter assets"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
        <div className="grid max-h-44 grid-cols-1 gap-0.5 overflow-y-auto rounded-sm border-theme border-line p-1 sm:grid-cols-2">
          {visible.map((name) => {
            const checked = targets.includes(name);
            return (
              <label
                key={name}
                className={cn(
                  "flex h-7 cursor-pointer items-center gap-2 rounded-xs px-2 text-sm",
                  checked ? "bg-select text-fg" : "text-fg-muted hover:bg-surface-2",
                )}
              >
                <input
                  type="checkbox"
                  aria-label={name}
                  checked={checked}
                  onChange={() =>
                    setTargets(checked ? targets.filter((t) => t !== name) : [...targets, name])
                  }
                  className="accent-[var(--accent)]"
                />
                <span className="truncate font-mono text-xs">{name}</span>
              </label>
            );
          })}
        </div>
      </fieldset>

      {partitioned && (
        <div className="flex flex-col gap-2">
          <span className="text-xs font-medium text-fg-muted">Partitions</span>
          <Segmented
            label="Partitions"
            value={selection}
            onChange={setSelection}
            options={[
              {
                value: "latest",
                label: "Latest",
                title: "The newest window of each time dimension, every key of the others",
              },
              {
                value: "missing",
                label: "Missing",
                title: "Every key without a complete head",
              },
              {
                value: "all",
                label: "All",
                title: "The whole current key set",
              },
              { value: "pick", label: "Pick…" },
            ]}
          />
          {selection === "pick" && (
            <>
              <Textarea
                aria-label="Partition keys"
                placeholder={"alpha\nday=2026-09-01,site=alpha  (one per line)"}
                value={picked}
                onChange={(e) => setPicked(e.target.value)}
                className="min-h-16"
              />
              {partitions && (
                <div className="flex max-h-24 flex-wrap gap-1 overflow-y-auto">
                  {partitions
                    .filter((p) => p.status !== "removed")
                    .map((p) => (
                      <button
                        key={p.partition}
                        type="button"
                        onClick={() =>
                          setPicked(
                            keys.includes(p.partition)
                              ? keys.filter((k) => k !== p.partition).join("\n")
                              : [...keys, p.partition].join("\n"),
                          )
                        }
                        className={cn(
                          "rounded-full border-theme px-2 py-0.5 font-mono text-2xs",
                          keys.includes(p.partition)
                            ? "border-fg bg-fg text-fg-inverse"
                            : "border-line-strong text-fg-muted hover:text-fg",
                        )}
                      >
                        {p.partition}
                      </button>
                    ))}
                </div>
              )}
            </>
          )}
        </div>
      )}

      <div className="grid gap-4 sm:grid-cols-2">
        <div className="flex flex-col gap-2">
          <span className="text-xs font-medium text-fg-muted">Mode</span>
          <Segmented
            label="Mode"
            value={effectiveMode}
            onChange={setMode}
            options={[
              { value: "incremental", label: "Incremental" },
              // Only where a target reads a keyed input: there is nothing else to read again.
              ...(keyedInputs.length ? [{ value: "all" as const, label: "All keys" }] : []),
              { value: "full", label: "Full" },
            ]}
          />
          <p className="text-xs text-fg-muted">{MODE_MEANS[effectiveMode]}</p>
        </div>
        <label className="flex items-center justify-between gap-3 self-end rounded-sm px-1 py-1.5">
          <span className="text-sm text-fg">Run upstream first</span>
          <Switch checked={upstream} onCheckedChange={setUpstream} label="Run upstream first" />
        </label>
      </div>

      <details className="group rounded-sm">
        <summary className="cursor-pointer text-xs font-medium text-fg-muted select-none hover:text-fg">
          Config and tags
        </summary>
        <div className="mt-3 flex flex-col gap-3">
          <Field label="Run config (JSON, as ctx.config)" hint={configError ?? undefined}>
            <Textarea
              value={config}
              onChange={(e) => setConfig(e.target.value)}
              placeholder='{"feed_tick_seconds": 300}'
            />
          </Field>
          <Field label="Tags" hint={tagError ?? "name=value, comma-separated"}>
            <Input
              value={tags}
              onChange={(e) => setTags(e.target.value)}
              placeholder="env=dev, ticket=OPS-42"
            />
          </Field>
        </div>
      </details>

      <div className="flex items-center justify-end gap-2 border-t border-line pt-4">
        <Button variant="ghost" onClick={onDone}>
          Cancel
        </Button>
        <Button type="submit" variant="primary" disabled={invalid || submit.isPending}>
          {submit.isPending ? "Submitting…" : "Start the run"}
        </Button>
      </div>
    </form>
  );
}
