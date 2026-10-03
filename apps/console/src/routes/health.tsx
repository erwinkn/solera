import type { ReactNode } from "react";
import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { Eraser } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import { useClearCleanups } from "@/api/mutations";
import { plural, shortId } from "@/lib/format";
import { Button } from "@/ui/button";
import { CopyButton, Empty } from "@/ui/data";
import { Card, CardHeader, Fact, Facts, Page, PageHeader } from "@/ui/layout";
import { Confirm } from "@/ui/overlay";
import { StatusBadge, StatusIcon } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

/**
 * The engine's own state, and what writers that died left behind
 * (docs/lifecycle.md §9): repairs, which the partition's next attempt makes,
 * and stuck cleanups, whose names couldn't be read.
 */
export function Health() {
  const project = useProject();
  const { data: repairs } = useSuspenseQuery(q.repairs(project));
  const { data: cleanups } = useSuspenseQuery(q.cleanups(project));
  const total = repairs.length + cleanups.length;
  return (
    <Page>
      <PageHeader
        title="Health"
        description="The engine, and what writers that died left behind."
        meta={<span>{total ? `${plural(total, "item")} need attention` : "Nothing left behind"}</span>}
      />
      <Engine />
      <div className="grid gap-4 lg:grid-cols-2">
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

function Repairs({
  rows,
}: {
  rows: {
    output: string;
    partition: string;
    intents: { run: string; attempt: string; files?: string[] }[];
  }[];
}) {
  return (
    <Card>
      <CardHeader
        title="Repairs owed"
        description="Outputs an attempt died writing. The next attempt of the partition reads the keys back and folds what landed into its commit."
      />
      {rows.length === 0 ? (
        <Empty compact title="No repair owed" />
      ) : (
        <ul className="flex flex-col divide-y divide-line border-t border-line">
          {rows.map((r) => (
            <li key={`${r.output}/${r.partition}`} className="flex flex-col gap-1 px-4 py-2.5 text-sm">
              <span>
                <span className="font-medium">{r.output}</span>
                {r.partition && <span className="ml-2 font-mono text-xs text-fg-muted">{r.partition}</span>}
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

function Cleanups({
  rows,
}: {
  rows: {
    output: string;
    partition: string;
    pending: number;
    stuck: { id: string }[];
  }[];
}) {
  const clear = useClearCleanups();
  return (
    <Card>
      <CardHeader
        title="Stuck cleanups"
        description="Data garbage whose names couldn't be read after three tries. It stays on storage; clearing only stops the engine trying."
      />
      {rows.length === 0 ? (
        <Empty compact title="No stuck cleanups" />
      ) : (
        <ul className="flex flex-col divide-y divide-line border-t border-line">
          {rows.map((r) => (
            <li key={`${r.output}/${r.partition}`} className="flex items-center gap-3 px-4 py-2.5 text-sm">
              <span className="min-w-0 flex-1">
                <span className="font-medium">{r.output}</span>
                {r.partition && <span className="ml-2 font-mono text-xs text-fg-muted">{r.partition}</span>}
                <span className="block text-xs text-fg-subtle">
                  {plural(r.stuck.length, "stuck entry", "stuck entries")}
                  {r.pending ? `, ${r.pending} pending` : ""}
                </span>
              </span>
              <Confirm
                trigger={
                  <Button size="sm" icon={<Eraser />}>
                    Clear
                  </Button>
                }
                title={`Clear stuck cleanups of ${r.output}?`}
                description="The engine forgets these entries; their objects stay where they are. Same as `solera partitions cleanups OUTPUT SCOPE --clear`."
                action="Clear"
                onConfirm={() => clear.mutate({ output: r.output, partition: r.partition })}
              />
            </li>
          ))}
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
