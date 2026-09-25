import { createFileRoute } from "@tanstack/react-router";
import { Database, FolderTree } from "lucide-react";
import { PageHeader } from "@/components/common";
import { useWorkspace } from "@/lib/workspace";
import type { ReactNode } from "react";

export const Route = createFileRoute("/storage")({
  component: StoragePage,
});

function Row({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex items-center justify-between gap-4 border-b py-2 text-sm last:border-0">
      <span className="text-muted-foreground">{label}</span>
      <span className="min-w-0 truncate text-right font-mono text-xs">
        {value}
      </span>
    </div>
  );
}

function StoragePage() {
  const { diagnostics } = useWorkspace();
  if (!diagnostics) return null;
  return (
    <section className="flex flex-col gap-4">
      <PageHeader
        eyebrow="State"
        title="Storage"
        description="Durable state and object storage backing this deployment."
      />
      <div className="grid gap-4 md:grid-cols-2">
        <div className="flex flex-col gap-1 rounded-xl border bg-card p-4">
          <div className="mb-1 flex items-center gap-2">
            <span className="flex size-8 items-center justify-center rounded-lg bg-primary/10 text-primary">
              <Database className="size-4" />
            </span>
            <div>
              <div className="text-sm font-semibold">Durable state</div>
              <div className="text-xs text-muted-foreground">
                Heads, cursors, tasks, leases
              </div>
            </div>
          </div>
          <Row label="Backend" value={diagnostics.backend} />
          <Row label="URL" value={diagnostics.state} />
          <Row label="Namespace" value={diagnostics.namespace} />
          <Row
            label="Postgres store"
            value={
              <span
                className={
                  diagnostics.postgres
                    ? "text-emerald-600 dark:text-emerald-400"
                    : "text-muted-foreground"
                }
              >
                {diagnostics.postgres ? "available" : "not configured"}
              </span>
            }
          />
        </div>
        <div className="flex flex-col gap-1 rounded-xl border bg-card p-4">
          <div className="mb-1 flex items-center gap-2">
            <span className="flex size-8 items-center justify-center rounded-lg bg-primary/10 text-primary">
              <FolderTree className="size-4" />
            </span>
            <div>
              <div className="text-sm font-semibold">Objects</div>
              <div className="text-xs text-muted-foreground">
                Attempt specs, results, and logs
              </div>
            </div>
          </div>
          <Row label="URL" value={diagnostics.objects} />
          <Row label="Project" value={diagnostics.project} />
          <Row label="Revision" value={diagnostics.revision.slice(0, 16)} />
          <Row
            label="In flight"
            value={<span className="tabular-nums">{diagnostics.inflight}</span>}
          />
        </div>
      </div>
      {diagnostics.last_error && (
        <div className="rounded-lg border border-red-600/30 bg-red-500/10 px-3 py-2 text-sm text-red-700 dark:text-red-300">
          Last error: {diagnostics.last_error}
        </div>
      )}
    </section>
  );
}
