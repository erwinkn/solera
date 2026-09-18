import { createFileRoute } from "@tanstack/react-router";
import { useWorkspace } from "@/lib/workspace";

export const Route = createFileRoute("/storage")({
  component: StoragePage,
});

function StoragePage() {
  const { state } = useWorkspace();
  if (!state) return null;
  const entries: [string, string][] = [
    ["State engine", `${state.storage.engine} 0.16`],
    [
      "Object store",
      state.storage.scheme === "file"
        ? "Local filesystem"
        : state.storage.scheme.toUpperCase(),
    ],
    ["Namespace", state.storage.namespace],
    ["Last local acknowledgement", String(state.storage.sequence)],
    ["Publication", "Object-store durability awaited before acknowledgement"],
    ["Coordinator", "One active writer; replacement fences the old writer"],
    ["Definition", state.revision.slice(0, 16)],
  ];
  return (
    <section className="flex flex-col gap-4">
      <div>
        <div className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
          Workspace
        </div>
        <h1 className="font-heading text-xl font-medium">Storage</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Object storage is the source of truth. Local caches are disposable.
        </p>
      </div>
      <dl className="grid gap-x-6 gap-y-3 rounded-xl border bg-card p-4 sm:grid-cols-2">
        {entries.map(([key, value]) => (
          <div key={key}>
            <dt className="text-xs text-muted-foreground">{key}</dt>
            <dd className="mt-0.5 text-sm">{value}</dd>
          </div>
        ))}
      </dl>
      <p className="rounded-lg bg-muted/50 px-3 py-2 text-xs text-muted-foreground">
        Experimental backend. SlateDB owns the log, compaction, recovery, and
        writer fencing. Data files are immutable; output references,
        checkpoints, task completion, and change notifications commit together.
      </p>
      <h2 className="mt-2 text-sm font-medium">Current boundaries</h2>
      <p className="text-sm text-muted-foreground">
        JSON snapshots and local subprocess execution. No cross-destination
        transactions, historical code bundles, or artifact garbage collection.
        External side effects may repeat after a crash. The filesystem mode is a
        development backend, not a multi-host object store.
      </p>
    </section>
  );
}
