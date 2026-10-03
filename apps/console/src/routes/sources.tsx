import { useState } from "react";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { GitCommitHorizontal } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import { useCommitSource } from "@/api/mutations";
import type { Manifest, SourceDecl } from "@/api/types";
import { RunsTable } from "@/features/runs";
import { count, plural } from "@/lib/format";
import { Button } from "@/ui/button";
import { Empty, ErrorNote, Generation, LoadMore, Skeleton, Time } from "@/ui/data";
import { Field, Input, Segmented, Textarea } from "@/ui/form";
import { Card, CardHeader, Crumb, Fact, Facts, Page, PageHeader } from "@/ui/layout";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

/**
 * Sources (architecture.md §5): outputs with no producer, advanced from
 * outside through the commit API or by a sensor. A keyed source and a
 * dynamic partitions are consumed exactly like keyed outputs.
 */

type Kind = "dynamic partitions" | "keyed" | "unkeyed";
const kindOf = (s: SourceDecl, manifest: Manifest): Kind =>
  manifest.outputs[s.name]?.dynamic_partitions ? "dynamic partitions" : s.key ? "keyed" : "unkeyed";

function consumers(manifest: Manifest, source: string): string[] {
  return Object.entries(manifest.assets)
    .filter(
      ([, a]) =>
        Object.values(a.inputs).some((e) => e.output === source) ||
        a.deps.includes(source) ||
        Object.values(a.partitions?.dims ?? {}).some((d) => d.kind === "dynamic" && d.output === source),
    )
    .map(([name]) => name);
}

export function Sources() {
  const manifest = useManifest();
  const project = useProject();
  const sensors = useQuery(q.sensors(project)).data?.sensors ?? [];
  const sources = Object.values(manifest.sources);
  return (
    <Page>
      <PageHeader
        title="Sources"
        description="Outputs with no producer: advanced from outside by the commit API, or polled by a sensor."
      />
      <Card>
        {sources.length === 0 ? (
          <Empty title="No sources">Declare `sources=` on the project to feed the graph from outside.</Empty>
        ) : (
          <TableScroll>
            <Table>
              <thead>
                <tr>
                  <Th>Source</Th>
                  <Th>Kind</Th>
                  <Th>Head</Th>
                  <Th className="text-right">Keys</Th>
                  <Th>Fed by</Th>
                  <Th>Read by</Th>
                </tr>
              </thead>
              <tbody>
                {sources.map((s) => (
                  <SourceRow
                    key={s.name}
                    source={s}
                    manifest={manifest}
                    feeders={sensors.filter((x) => x.commits.includes(s.name)).map((x) => x.name)}
                  />
                ))}
              </tbody>
            </Table>
          </TableScroll>
        )}
      </Card>
    </Page>
  );
}

function SourceRow({
  source,
  manifest,
  feeders,
}: {
  source: SourceDecl;
  manifest: Manifest;
  feeders: string[];
}) {
  const project = useProject();
  const head = useQuery(q.heads(project, source.name)).data?.[0];
  const kind = kindOf(source, manifest);
  return (
    <Tr className="relative">
      <Td className="py-2">
        <Link
          to="/sources/$source"
          params={{ source: source.name }}
          className="font-medium after:absolute after:inset-0 after:content-['']"
        >
          {source.name}
        </Link>
      </Td>
      <Td className="text-fg-muted">{kind}</Td>
      <Td>
        <span className="flex items-center gap-2">
          <Generation value={head?.ref.generation ?? source.head.generation} />
          {head && (
            <span className="text-xs text-fg-subtle">
              <Time at={head.at} />
            </span>
          )}
        </span>
      </Td>
      <Td className="text-right">
        {kind === "unkeyed" ? "—" : head?.key_count != null ? count(head.key_count) : "—"}
      </Td>
      <Td className="text-xs text-fg-muted">
        {feeders.length ? feeders.map((f) => `sensor ${f}`).join(", ") : "commit API"}
      </Td>
      <Td className="max-w-64 truncate text-xs text-fg-muted">
        {consumers(manifest, source.name).join(", ") || "—"}
      </Td>
    </Tr>
  );
}

const route = getRouteApi("/sources/$source");

export function Source() {
  const { source: name } = route.useParams();
  const manifest = useManifest();
  const project = useProject();
  const source = manifest.sources[name];
  const head = useQuery(q.heads(project, name)).data?.[0];
  const sensors = useQuery(q.sensors(project)).data?.sensors.filter((s) => s.commits.includes(name)) ?? [];
  const commitQuery = useInfiniteQuery(q.runs(project, { source: [name] }, 20));
  const commits = commitQuery.data;
  if (!source)
    return (
      <Page>
        <Empty title={`No source named ${name}`} />
      </Page>
    );
  const kind = kindOf(source, manifest);
  const readers = consumers(manifest, name);
  return (
    <Page>
      <PageHeader
        ident
        eyebrow={
          <>
            <Crumb>
              <Link to="/sources" className="hover:text-fg">
                Sources
              </Link>
            </Crumb>
            <Crumb last>{name}</Crumb>
          </>
        }
        title={name}
        description={`A ${kind} source${source.store ? ` on the ${source.store} store` : ", a lineage pointer only"}.`}
      />
      <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(18rem,1fr)]">
        <div className="flex min-w-0 flex-col gap-4">
          <Card>
            <CardHeader title="Head" />
            <Facts className="px-4 pb-4">
              <Fact label="Generation">
                <Generation value={head?.ref.generation ?? source.head.generation} />
              </Fact>
              {kind === "unkeyed" && <Fact label="Version">{head?.version ?? "—"}</Fact>}
              {kind !== "unkeyed" && (
                <Fact label="Keys">{head?.key_count != null ? count(head.key_count) : "—"}</Fact>
              )}
              <Fact label="Committed">{head ? <Time at={head.at} /> : "at registration"}</Fact>
              <Fact label="Fed by">
                {sensors.length
                  ? sensors.map((s) => (
                      <Link
                        key={s.name}
                        to="/sensors/$sensor"
                        params={{ sensor: s.name }}
                        className="mr-2 text-link hover:underline"
                      >
                        {s.name}
                      </Link>
                    ))
                  : "the commit API"}
              </Fact>
              <Fact label="Read by">
                {readers.length
                  ? readers.map((r) => (
                      <Link
                        key={r}
                        to="/assets/$asset"
                        params={{ asset: r }}
                        className="mr-2 text-link hover:underline"
                      >
                        {r}
                      </Link>
                    ))
                  : "—"}
              </Fact>
            </Facts>
          </Card>
          {kind !== "unkeyed" && <SourceKeys name={name} />}
          <Card>
            <CardHeader
              title="Commits"
              description="Each commit that changed something is a run with no tasks"
            />
            {commitQuery.isError ? (
              <div className="px-4 pb-4">
                <ErrorNote error={commitQuery.error} />
              </div>
            ) : !commits ? (
              <Skeleton className="mx-4 mb-4 h-24" />
            ) : (
              <>
                <RunsTable
                  compact
                  runs={commits.pages.flatMap((p) => p.runs)}
                  empty={
                    <Empty compact title="No commits yet">
                      Its head is the one synthesized at registration.
                    </Empty>
                  }
                />
                <LoadMore query={commitQuery} />
              </>
            )}
          </Card>
        </div>
        <CommitForm name={name} kind={kind} />
      </div>
    </Page>
  );
}

function SourceKeys({ name }: { name: string }) {
  const project = useProject();
  const keys = useInfiniteQuery(q.keys(project, name, ""));
  const entries = keys.data?.pages.flatMap((p) => Object.entries(p.keys)) ?? [];
  return (
    <Card>
      <CardHeader
        title="Keys"
        description={keys.data ? plural(keys.data.pages[0]?.total ?? 0, "key") : undefined}
      />
      {keys.isError ? (
        <div className="px-4 pb-4">
          <ErrorNote error={keys.error} title="Couldn't read the key index" />
        </div>
      ) : !keys.data ? (
        <Skeleton className="mx-4 mb-4 h-20" />
      ) : entries.length === 0 ? (
        <Empty compact title="No keys yet" />
      ) : (
        <TableScroll className="max-h-80 overflow-y-auto border-t border-line">
          <Table>
            <thead className="sticky top-0 bg-surface">
              <tr>
                <Th>Key</Th>
                <Th>Generation</Th>
              </tr>
            </thead>
            <tbody>
              {entries.map(([k, generation]) => (
                <Tr key={k}>
                  <Td className="font-mono text-xs">{k}</Td>
                  <Td className="font-mono text-xs text-fg-muted">g{generation}</Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        </TableScroll>
      )}
      <LoadMore query={keys} shown={`${entries.length} shown`} />
    </Card>
  );
}

/** The commit API by hand. Identical content is no change: committing it wakes nothing. */
function CommitForm({ name, kind }: { name: string; kind: Kind }) {
  const commit = useCommitSource();
  const [action, setAction] = useState<"upsert" | "remove">("upsert");
  const [text, setText] = useState("");
  // Keys are one per line, never split on commas (a key may hold one); a pair splits
  // at its first "=", so a version keeps its own. An unkeyed version is taken verbatim.
  const items = text
    .split("\n")
    .map((t) => t.trim())
    .filter(Boolean);
  const pairs: [string, string | undefined][] =
    kind === "keyed"
      ? items.map((i) => {
          const at = i.indexOf("=");
          return at < 0 ? [i, undefined] : [i.slice(0, at).trim(), i.slice(at + 1).trim()];
        })
      : [];
  // A key with no version is written as a change (docs/versions.md §2).
  const invalid = kind === "unkeyed" ? !text.trim() : items.length === 0;
  return (
    <Card className="self-start">
      <CardHeader title="Commit" description="Advance this source through the commit API" />
      <form
        className="flex flex-col gap-3 px-4 pb-4"
        onSubmit={(e) => {
          e.preventDefault();
          if (invalid) return;
          const body =
            kind === "unkeyed"
              ? { source: name, version: text.trim() }
              : action === "remove"
                ? { source: name, remove: items }
                : {
                    source: name,
                    upsert:
                      kind === "keyed" ? Object.fromEntries(pairs.map(([k, v]) => [k, v ?? null])) : items,
                  };
          commit.mutate(body, { onSuccess: () => setText("") });
        }}
      >
        {kind === "unkeyed" ? (
          <Field label="New version" hint="Any string; the version it has is no change">
            <Input value={text} onChange={(e) => setText(e.target.value)} placeholder="2026-10-02T09:00Z" />
          </Field>
        ) : (
          <>
            <Segmented
              label="Commit action"
              value={action}
              onChange={setAction}
              options={[
                {
                  value: "upsert",
                  label: kind === "dynamic partitions" ? "Add keys" : "Upsert",
                },
                { value: "remove", label: "Remove" },
              ]}
            />
            <Field
              label={
                kind === "keyed" && action === "upsert"
                  ? "key, or key=version, one per line"
                  : "Keys, one per line"
              }
            >
              <Textarea
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder={kind === "keyed" && action === "upsert" ? "f1=v3\nf2=v1" : "u-1\nu-2"}
              />
            </Field>
          </>
        )}
        <Button
          type="submit"
          variant="primary"
          icon={<GitCommitHorizontal />}
          disabled={invalid || commit.isPending}
        >
          {commit.isPending
            ? "Committing…"
            : action === "remove" && kind !== "unkeyed"
              ? `Remove ${plural(items.length, "key")}`
              : "Commit"}
        </Button>
      </form>
    </Card>
  );
}
