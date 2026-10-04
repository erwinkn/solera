import type { ReactNode } from "react";
import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { Eraser } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import { useClearCleanups } from "@/api/mutations";
import { cleanupStuck } from "@/api/read";
import type { Cleanup, Repair, RetiredCleanup } from "@/api/types";
import { useNow } from "@/lib/clock";
import { plural, shortId, until } from "@/lib/format";
import { Button } from "@/ui/button";
import { CopyButton, Empty } from "@/ui/data";
import { Card, CardHeader, Fact, Facts, Page, PageHeader } from "@/ui/layout";
import { Confirm } from "@/ui/overlay";
import { StatusBadge, StatusIcon } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

/**
 * The engine's own state, and what needs an operator (docs/lifecycle.md §9):
 * repairs a dead writer left, which the partition's next attempt makes or
 * which get stuck; and cleanups, stuck or waiting for their cleanup task.
 */
export function Health() {
  const project = useProject();
  const { data: repairs } = useSuspenseQuery(q.repairs(project));
  const { data: cleanups } = useSuspenseQuery(q.cleanups(project));
  const total = repairs.length + cleanups.filter(cleanupStuck).length;
  return (
    <Page>
      <PageHeader
        title="Health"
        description="The engine, and what needs an operator: repairs a dead writer left, and stuck cleanups."
        meta={
          <span>
            {total
              ? `${plural(total, "item")} need${total === 1 ? "s" : ""} attention`
              : "Nothing left behind"}
          </span>
        }
      />
      <Engine />
      <div className="grid items-start gap-4 lg:grid-cols-2">
        <Repairs rows={repairs} />
        <Cleanups rows={cleanups} />
      </div>
      <Stores />
    </Page>
  );
}

function Engine() {
  const { data: health, isError } = useQuery(q.health());
  const { data: d } = useSuspenseQuery(q.diagnostics());
  const manifest = useManifest();
  const ok = !isError && health?.ok !== false;
  return (
    <Card>
      <CardHeader
        title="Engine"
        actions={
          <StatusBadge
            status={!ok ? "failed" : d.last_error ? "blocked" : "succeeded"}
            text={!ok ? "unavailable" : d.last_error ? "degraded" : "healthy"}
          />
        }
      />
      {d.last_error && (
        <div className="mx-4 mb-3 flex items-start gap-2 rounded-md bg-warn-soft px-3 py-2 text-sm text-warn-fg">
          <StatusIcon tone="warn" className="mt-0.5" />
          <span className="break-words">{d.last_error}</span>
        </div>
      )}
      <Facts className="px-4 pb-4">
        <Fact label="Project">{d.project}</Fact>
        <Fact label="Namespace">{d.namespace}</Fact>
        <Fact label="Deploy">
          <span className="inline-flex items-center gap-1 font-mono text-xs">
            {d.deploy.slice(0, 12)}
            <CopyButton value={d.deploy} />
          </span>
        </Fact>
        <Fact label="Build">
          {manifest.build ? (
            <span className="font-mono text-xs">
              {manifest.build.commit ? manifest.build.commit.slice(0, 8) : manifest.build.id.slice(0, 12)}
              {manifest.build.dirty && " · dirty"}{" "}
              <span className="text-fg-subtle">({manifest.build.source})</span>
            </span>
          ) : (
            "—"
          )}
        </Fact>
        <Fact label="Active runs">{d.active_runs}</Fact>
        <Fact label="Attempts in flight">{d.inflight}</Fact>
        <Fact label="Postgres">{d.postgres ? "configured" : "not configured"}</Fact>
        <Fact label="Backend">{d.backend}</Fact>
        <Fact label="State" className="col-span-2">
          <span className="font-mono text-xs break-all">{d.state}</span>
        </Fact>
        <Fact label="Objects" className="col-span-2">
          <span className="font-mono text-xs break-all">{d.objects}</span>
        </Fact>
      </Facts>
    </Card>
  );
}

function Repairs({ rows }: { rows: Repair[] }) {
  return (
    <Card>
      <CardHeader
        title="Repairs owed"
        description="An attempt died after it began writing. The partition's next attempt asks the store which of its keys landed and commits them; until then, readers of the partition wait."
      />
      {rows.length === 0 ? (
        <Empty compact title="No repair owed" />
      ) : (
        <ul className="flex flex-col divide-y divide-line border-t border-line">
          {rows.map((r) => (
            <li key={`${r.output}/${r.partition}`} className="flex flex-col gap-1 px-4 py-2.5 text-sm">
              <span className="flex flex-wrap items-center gap-2">
                <span className="font-medium">{r.output}</span>
                {r.partition && <span className="font-mono text-xs text-fg-muted">{r.partition}</span>}
                <StatusBadge status={r.stuck ? "failed" : "waiting"} text={r.stuck ? "stuck" : "owed"} />
              </span>
              <span className="text-xs text-fg-muted">
                {r.stuck
                  ? `${plural(r.repair_runs ?? 0, "run")} came due without repairing it: look at the partition's last attempt.`
                  : r.repair_runs
                    ? `${plural(r.repair_runs, "run")} came due since it began owing.`
                    : "Its next attempt repairs it."}
              </span>
              <span className="text-xs text-fg-muted">
                {r.intents.map((i) => (
                  <Link
                    key={i.attempt}
                    to="/runs/$run"
                    params={{ run: i.run }}
                    search={{ attempt: i.attempt }}
                    className="mr-3 font-mono text-link hover:underline"
                  >
                    {shortId(i.attempt)}
                    {i.files?.length ? ` · ${plural(i.files.length, "file")}` : ""}
                  </Link>
                ))}
              </span>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}

const isRetired = (c: Cleanup): c is RetiredCleanup => "due" in c;

/**
 * Two kinds of cleanup (glossary, "cleanup"): a partition's store cleanup,
 * stuck after three failed tries, for an operator to clear; and what a
 * removed or moved output left in its store, waiting for its cleanup task.
 */
function Cleanups({ rows }: { rows: Cleanup[] }) {
  const clear = useClearCleanups();
  const now = useNow();
  return (
    <Card>
      <CardHeader
        title="Cleanups"
        description="Deleting what nothing references any more. A store cleanup is stuck after three failed tries; a removed or moved output's data waits for its cleanup task."
        actions={
          <Link
            to="/runs"
            search={{ origin: "cleanup" }}
            className="text-xs whitespace-nowrap text-link hover:underline"
          >
            Cleanup tasks →
          </Link>
        }
      />
      {rows.length === 0 ? (
        <Empty compact title="Nothing waiting" />
      ) : (
        <ul className="flex flex-col divide-y divide-line border-t border-line">
          {rows.map((r) => {
            const retired = isRetired(r);
            const stuck = cleanupStuck(r);
            return (
              <li
                key={retired ? r.id : `${r.output}/${r.partition}`}
                className="flex items-center gap-3 px-4 py-2.5 text-sm"
              >
                <span className="flex min-w-0 flex-1 flex-col gap-0.5">
                  <span className="flex flex-wrap items-center gap-2">
                    <span className="font-medium">{r.output}</span>
                    {!retired && r.partition && (
                      <span className="font-mono text-xs text-fg-muted">{r.partition}</span>
                    )}
                    {stuck ? (
                      <StatusBadge status="failed" text="stuck" />
                    ) : (
                      <StatusBadge status="queued" text="cleanup task due" />
                    )}
                  </span>
                  <span className="text-xs text-fg-subtle">
                    {retired ? (
                      <>
                        removed or moved: what it wrote to {r.store} before g{r.before}
                        {r.stuck
                          ? ` · ${typeof r.stuck === "string" ? r.stuck : "its cleanup task failed three times"}`
                          : r.due > now
                            ? ` · deleted ${until(r.due, now)}`
                            : " · being deleted"}
                      </>
                    ) : (
                      <>
                        {plural(r.stuck.length, "stuck entry", "stuck entries")}
                        {r.pending ? `, ${r.pending} pending` : ""}
                      </>
                    )}
                  </span>
                </span>
                {stuck && (
                  <Confirm
                    trigger={
                      <Button size="sm" icon={<Eraser />}>
                        Clear
                      </Button>
                    }
                    title={`Clear stuck cleanups of ${r.output}?`}
                    description="The engine forgets these entries; their objects stay where they are. Same as `solera cleanups OUTPUT PARTITION --clear`."
                    action="Clear"
                    onConfirm={() =>
                      clear.mutate({ output: r.output, partition: retired ? "" : r.partition })
                    }
                  />
                )}
              </li>
            );
          })}
        </ul>
      )}
    </Card>
  );
}

const KIND_RULE: Record<string, ReactNode> = {
  immutable: "Writes only new names, so an abandoned write is unreferenced, and cleaned up later.",
  fenced: "Every write checks a generation: the next attempt's acquisition fences the old writer out.",
};

function Stores() {
  const manifest = useManifest();
  return (
    <Card>
      <CardHeader
        title="Stores"
        description="How each store's writes stay correct when an attempt dies mid-write"
      />
      <TableScroll className="border-t border-line">
        <Table>
          <thead>
            <tr>
              <Th>Store</Th>
              <Th>Writes</Th>
              <Th>When a writer dies mid-write</Th>
              <Th>Version</Th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(manifest.stores).map(([name, s]) => (
              <Tr key={name}>
                <Td className="font-medium">{name}</Td>
                <Td>{s.writes}</Td>
                <Td className="text-xs text-fg-muted">{KIND_RULE[s.writes]}</Td>
                <Td className="font-mono text-xs text-fg-muted">{s.version}</Td>
              </Tr>
            ))}
          </tbody>
        </Table>
      </TableScroll>
    </Card>
  );
}
