import { useQuery } from "@tanstack/react-query";
import { q, useProject } from "@/api/queries";
import { plural } from "@/lib/format";
import { StatusIcon } from "@/ui/status";

/**
 * Pool executors with attempts waiting and no worker polling them: work
 * that will sit queued until someone starts `solera worker pool NAME`.
 */
export function useStarvedPools() {
  const project = useProject();
  const executors = useQuery(q.executors(project)).data ?? [];
  const workers = useQuery(q.workers(project)).data;
  if (!workers) return [];
  const polled = new Set(workers.flatMap((w) => (w.pools as string[] | undefined) ?? []));
  return executors.filter((e) => e.kind === "Pool" && e.in_flight > 0 && !polled.has(e.name));
}

export function StarvedPools() {
  const starved = useStarvedPools();
  if (starved.length === 0) return null;
  return (
    <div role="status" className="flex flex-col gap-2">
      {starved.map((e) => (
        <div
          key={e.name}
          className="flex items-start gap-2.5 rounded-md border-theme border-warn bg-warn-soft px-4 py-3 text-sm text-warn-fg"
        >
          <StatusIcon tone="warn" className="mt-0.5 size-4" />
          <p>
            {plural(e.in_flight, "attempt")} wait{e.in_flight === 1 ? "s" : ""} on pool{" "}
            <strong>{e.name}</strong>, and no worker is polling it. Start one with{" "}
            <code className="rounded-xs bg-surface/70 px-1 font-mono text-xs">
              solera worker pool {e.name}
            </code>
            .
          </p>
        </div>
      ))}
    </div>
  );
}
