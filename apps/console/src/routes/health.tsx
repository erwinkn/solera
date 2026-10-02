import type { ReactNode } from "react";
import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { Eraser, Unlock } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import { useClearDiscards, useReleaseScope } from "@/api/mutations";
import { useNow } from "@/lib/clock";
import { plural, shortId, until } from "@/lib/format";
import { Button } from "@/ui/button";
import { CopyButton, Empty, Time } from "@/ui/data";
import { Card, CardHeader, Fact, Facts, Page, PageHeader } from "@/ui/layout";
import { Confirm } from "@/ui/overlay";
import { StatusBadge, StatusIcon } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

/**
 * What an operator has to clear by hand (docs/lifecycle.md §9), and the
 * engine's own state. Holds keep a scope from running while an ended
 * attempt's writes might still land; stuck discards are garbage whose names
 * couldn't be read.
 */
export function Health() {
  const project = useProject();
  const { data: holds } = useSuspenseQuery(q.holds(project));
  const total = holds.holds.length + holds.unsettled.length + holds.discards.length;
  return (
    <Page>
      <PageHeader
        title="Health"
        description="The engine, and what is waiting on an operator."
        meta={<span>{total ? `${plural(total, "item")} need attention` : "Nothing needs an operator"}</span>}
      />
      <Engine />
      <Holds holds={holds.holds} />
      <div className="grid gap-4 lg:grid-cols-2">
        <Unsettled rows={holds.unsettled} />
        <Discards rows={holds.discards} />
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
        <Fact label="Revision">
          <span className="inline-flex items-center gap-1 font-mono text-xs">
            {d.revision.slice(0, 12)}
            <CopyButton value={d.revision} />
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

function Holds({
  holds,
}: {
  holds: {
    asset: string;
    scope: string;
    attempt: string;
    run: string;
    mode: "grace" | "strict";
    at: number;
    grace: number | null;
    releases_at: number | null;
  }[];
}) {
  const release = useReleaseScope();
  const now = useNow();
  return (
    <Card>
      <CardHeader
        title="Held scopes"
        description="An attempt ended while its writes to an overwrite store might still land. Nothing runs on the scope until it is released: after a grace period, when the writer's result arrives, or by you."
      />
      {holds.length === 0 ? (
        <Empty compact title="No held scopes" />
      ) : (
        <TableScroll className="border-t border-line">
          <Table>
            <thead>
              <tr>
                <Th>Scope</Th>
                <Th>Hold</Th>
                <Th>Held since</Th>
                <Th>Releases</Th>
                <Th>Writer</Th>
                <Th />
              </tr>
            </thead>
            <tbody>
              {holds.map((h) => (
                <Tr key={`${h.asset}/${h.scope}`}>
                  <Td>
                    <Link
                      to="/assets/$asset"
                      params={{ asset: h.asset }}
                      search={{ scope: h.scope || undefined }}
                      className="font-medium hover:underline"
                    >
                      {h.asset}
                    </Link>
                    {h.scope && <span className="ml-2 font-mono text-xs text-fg-muted">{h.scope}</span>}
                  </Td>
                  <Td>
                    <StatusBadge
                      status={h.mode}
                      text={h.mode === "strict" ? "strict: waiting for the writer" : "grace period"}
                    />
                  </Td>
                  <Td className="text-fg-muted">
                    <Time at={h.at} />
                  </Td>
                  <Td className="text-fg-muted">
                    {h.releases_at ? until(h.releases_at, now) : "only when completion is known"}
                  </Td>
                  <Td>
                    <Link
                      to="/runs/$run"
                      params={{ run: h.run }}
                      search={{ attempt: h.attempt }}
                      className="font-mono text-xs text-link hover:underline"
                    >
                      {shortId(h.attempt)}
                    </Link>
                  </Td>
                  <Td className="text-right">
                    <Confirm
                      trigger={
                        <Button size="sm" icon={<Unlock />}>
                          Release
                        </Button>
                      }
                      title={`Release ${h.asset}${h.scope ? ` · ${h.scope}` : ""}?`}
                      description="The next attempt runs at once and repairs the writer's intents. If the old writer is still writing, its writes may land after the next commit. Recorded with your name, as `solera scopes release` is."
                      action="Release scope"
                      danger
                      onConfirm={() => release.mutate({ asset: h.asset, scope: h.scope })}
                    />
                  </Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        </TableScroll>
      )}
    </Card>
  );
}

function Unsettled({
  rows,
}: {
  rows: {
    output: string;
    scope: string;
    intents: { run: string; attempt: string; files?: string[] }[];
  }[];
}) {
  return (
    <Card>
      <CardHeader
        title="Unsettled writes"
        description="Outputs an attempt died writing. The next attempt of the scope reads the keys back and folds what landed into its commit."
      />
      {rows.length === 0 ? (
        <Empty compact title="Nothing unsettled" />
      ) : (
        <ul className="flex flex-col divide-y divide-line border-t border-line">
          {rows.map((r) => (
            <li key={`${r.output}/${r.scope}`} className="flex flex-col gap-1 px-4 py-2.5 text-sm">
              <span>
                <span className="font-medium">{r.output}</span>
                {r.scope && <span className="ml-2 font-mono text-xs text-fg-muted">{r.scope}</span>}
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

function Discards({
  rows,
}: {
  rows: {
    output: string;
    scope: string;
    pending: number;
    stuck: { id: string }[];
  }[];
}) {
  const clear = useClearDiscards();
  return (
    <Card>
      <CardHeader
        title="Stuck discards"
        description="Data garbage whose names couldn't be read after three tries. It stays on storage; clearing only stops the engine trying."
      />
      {rows.length === 0 ? (
        <Empty compact title="No stuck discards" />
      ) : (
        <ul className="flex flex-col divide-y divide-line border-t border-line">
          {rows.map((r) => (
            <li key={`${r.output}/${r.scope}`} className="flex items-center gap-3 px-4 py-2.5 text-sm">
              <span className="min-w-0 flex-1">
                <span className="font-medium">{r.output}</span>
                {r.scope && <span className="ml-2 font-mono text-xs text-fg-muted">{r.scope}</span>}
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
                title={`Clear stuck discards of ${r.output}?`}
                description="The engine forgets these entries; their objects stay where they are. Same as `solera scopes discards OUTPUT SCOPE --clear`."
                action="Clear"
                onConfirm={() => clear.mutate({ output: r.output, scope: r.scope })}
              />
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}

const KIND_RULE: Record<string, ReactNode> = {
  immutable: "Writes only new names; abandoned writes are unreferenced. Never held.",
  fenced:
    "Every write checks a generation; the next attempt's acquisition fences the old writer. Never held.",
  overwrite: "Gate, intents and repair. An uncertain writer holds its scope for a grace period.",
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
              <Th>When a writer is uncertain</Th>
              <Th>Version</Th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(manifest.stores).map(([name, s]) => (
              <Tr key={name}>
                <Td className="font-medium">{name}</Td>
                <Td>
                  {s.writes}
                  {s.strict && (
                    <span className="ml-1.5 rounded-full bg-warn-soft px-1.5 text-2xs text-warn-fg">
                      strict
                    </span>
                  )}
                </Td>
                <Td className="text-xs text-fg-muted">
                  {s.writes === "overwrite" && s.strict
                    ? "Held until the writer's result proves completion, or an operator releases it."
                    : s.writes === "overwrite"
                      ? `Held for ${Math.round(s.late_write_grace)}s, then released.`
                      : KIND_RULE[s.writes]}
                </Td>
                <Td className="font-mono text-xs text-fg-muted">{s.version}</Td>
              </Tr>
            ))}
          </tbody>
        </Table>
      </TableScroll>
    </Card>
  );
}
