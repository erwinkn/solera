import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { q, useManifest, useProject } from "@/api/queries";
import { StarvedPools } from "@/features/starved";
import type { Executor, Json } from "@/api/types";
import { cn } from "@/lib/cn";
import { count, duration } from "@/lib/format";
import { Empty, Time } from "@/ui/data";
import { Card, CardHeader, Fact, Facts, Page, PageHeader } from "@/ui/layout";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

/**
 * Executors (architecture.md §10): named environments where attempts run.
 * Pool executors are the pull path: work waits until a worker claims it.
 */
export function Executors() {
  const project = useProject();
  const manifest = useManifest();
  const { data: executors } = useSuspenseQuery(q.executors(project));
  const stats = useQuery(q.stats(project)).data;
  const workers = useQuery(q.workers(project)).data ?? [];
  return (
    <Page>
      <PageHeader
        title="Executors"
        description="Where attempts run. Each counts its attempts in flight against its own limit."
      />
      <StarvedPools />
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {executors.map((e) => {
          const s = stats?.executors?.find((x) => x.executor === e.name) as
            Record<string, number | null> | undefined;
          const assets = Object.entries(manifest.assets)
            .filter(([, a]) => a.placement.executor === e.name)
            .map(([n]) => n);
          return (
            <Card key={e.name}>
              <CardHeader
                ident
                title={e.name}
                description={`${e.kind}${e.kind === "Pool" ? " · workers pull its attempts" : ""}`}
                actions={<Load executor={e} />}
              />
              <Facts className="px-4 pb-4">
                <Fact label="In flight">
                  {count(e.in_flight)}
                  {e.max_concurrent != null && <span className="text-fg-subtle"> of {e.max_concurrent}</span>}
                </Fact>
                {s && <Fact label="Tasks">{count(s.tasks ?? 0)}</Fact>}
                {s && <Fact label="p95">{duration(s.p95)}</Fact>}
                {s && <Fact label="Wait p95">{duration(s.wait_p95)}</Fact>}
                {Object.entries(e.config ?? {}).map(([k, v]) => (
                  <Fact key={k} label={k}>
                    {String(v as Json)}
                  </Fact>
                ))}
              </Facts>
              <p className="border-t border-line px-4 py-2.5 text-xs text-fg-muted">
                {assets.length ? (
                  <>
                    Runs{" "}
                    {assets.map((a, i) => (
                      <span key={a}>
                        {i > 0 && ", "}
                        <Link to="/assets/$asset" params={{ asset: a }} className="text-link hover:underline">
                          {a}
                        </Link>
                      </span>
                    ))}
                  </>
                ) : (
                  "No asset is placed here."
                )}
              </p>
            </Card>
          );
        })}
      </div>
      <Card>
        <CardHeader
          title="Pool workers"
          description="Processes long-polling a pool for attempts that fit their capacity (`solera worker pool NAME`)"
        />
        {workers.length === 0 ? (
          <Empty compact title="No pool worker polling">
            {executors.some((e) => e.kind === "Pool")
              ? "Attempts placed on a pool wait, queued, until a worker claims them."
              : "This project has no pool executor."}
          </Empty>
        ) : (
          <TableScroll className="border-t border-line">
            <Table>
              <thead>
                <tr>
                  <Th>Worker</Th>
                  <Th>Pools</Th>
                  <Th>Capacity</Th>
                  <Th>Last poll</Th>
                </tr>
              </thead>
              <tbody>
                {workers.map((w) => (
                  <Tr key={w.id}>
                    <Td className="font-mono text-xs">{w.id}</Td>
                    <Td>{(w.pools as string[] | undefined)?.join(", ")}</Td>
                    <Td className="text-xs text-fg-muted">
                      {Object.entries((w.capacity as Record<string, number> | undefined) ?? {})
                        .map(([k, v]) => `${k} ${v}`)
                        .join(" · ") || "any"}
                    </Td>
                    <Td className="text-fg-muted">
                      <Time at={w.seen_at as number} />
                    </Td>
                  </Tr>
                ))}
              </tbody>
            </Table>
          </TableScroll>
        )}
      </Card>
    </Page>
  );
}

function Load({ executor }: { executor: Executor }) {
  const max = executor.max_concurrent;
  if (max == null) return <span className="text-xs text-fg-subtle">no limit</span>;
  const share = Math.min(1, executor.in_flight / max);
  return (
    <span className="flex items-center gap-2" title={`${executor.in_flight} of ${max} in flight`}>
      <span className="h-1.5 w-16 overflow-hidden rounded-full bg-sunken">
        <span
          className={cn("block h-full", share >= 1 ? "bg-warn" : "bg-run")}
          style={{ width: `${share * 100}%` }}
        />
      </span>
    </span>
  );
}
