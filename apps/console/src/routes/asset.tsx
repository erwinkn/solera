import type { ReactNode } from "react";
import { keepPreviousData, useInfiniteQuery, useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { getRouteApi, Link, Outlet } from "@tanstack/react-router";
import { Play, Zap } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import { useAutomationToggle, useRunAutomation } from "@/api/mutations";
import type { AssetDecl, Automation, DimDecl, Head, Json, Manifest } from "@/api/types";
import { assetTone, KIND_ICON, KIND_LABEL, kindOf } from "@/features/graph";
import { RunButton } from "@/features/run-dialog";
import { RunsTable } from "@/features/runs";
import { describeTrigger } from "@/features/triggers";
import { PatternList } from "@/features/patterns";
import { useNow } from "@/lib/clock";
import { cn } from "@/lib/cn";
import { count, duration, plural, until } from "@/lib/format";
import { toneSoft } from "@/lib/status";
import { Button } from "@/ui/button";
import { Empty, Generation, JsonView, Skeleton, Time } from "@/ui/data";
import { Select, Switch } from "@/ui/form";
import { Card, CardHeader, Crumb, Fact, Facts, Page, PageHeader } from "@/ui/layout";
import { Tooltip } from "@/ui/overlay";
import { StatusBadge } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

const route = getRouteApi("/assets/$asset");

/** Which tabs an asset has: keys only where keys exist, inputs only with inputs. */
export function tabsOf(asset: AssetDecl) {
  const each = Object.values(asset.inputs).some((e) => e.each);
  const keyed = asset.outputs.some((o) => o.key);
  return {
    partitions: !!asset.partitions,
    keys: each || keyed,
    inputs: Object.keys(asset.inputs).length > 0 || asset.deps.length > 0,
  };
}

export function AssetLayout() {
  const { asset: name } = route.useParams();
  const { partition } = route.useSearch();
  const navigate = route.useNavigate();
  const manifest = useManifest();
  const project = useProject();
  const asset = manifest.assets[name];
  const status = useQuery(q.assetStatus(project)).data?.[name];
  const rows = useQuery({
    ...q.partitions(project, name),
    enabled: !!asset?.partitions,
  }).data;
  if (!asset) {
    return (
      <Page>
        <Empty title={`No asset named ${name}`}>
          It isn't in the served manifest. It may have been renamed or removed.
        </Empty>
      </Page>
    );
  }
  const kind = kindOf(asset);
  const tabs = tabsOf(asset);
  const tone = assetTone(status);
  const partitions = (rows ?? []).filter((p) => p.status !== "removed").map((p) => p.partition);

  return (
    <Page>
      <PageHeader
        ident
        eyebrow={
          <>
            <Crumb>
              <Link to="/assets" className="hover:text-fg">
                Assets
              </Link>
            </Crumb>
            <Crumb last>{name}</Crumb>
          </>
        }
        title={
          <span className="flex items-center gap-3">
            <span
              className={cn(
                "grid size-8 shrink-0 place-items-center rounded-md [&_svg]:size-4",
                toneSoft[tone],
              )}
            >
              {KIND_ICON[kind]}
            </span>
            {name}
          </span>
        }
        description={asset.doc?.split("\n\n")[0]}
        actions={
          <>
            {asset.partitions && (
              <Select
                aria-label="Partition"
                className="w-auto max-w-64 font-mono text-xs"
                value={partition ?? ""}
                onChange={(e) =>
                  navigate({
                    search: (s) => ({
                      ...s,
                      partition: e.target.value || undefined,
                    }),
                    replace: true,
                  })
                }
              >
                <option value="">All partitions</option>
                {partition && !partitions.includes(partition) && (
                  <option value={partition}>{partition}</option>
                )}
                {partitions.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </Select>
            )}
            <RunButton
              targets={[name]}
              partition={partition}
              icon={<Play />}
              label={partition ? "Run partition" : "Run"}
            />
          </>
        }
        meta={
          <>
            <span>{KIND_LABEL[kind]}</span>
            <span>
              on <span className="text-fg">{asset.placement.executor}</span>
            </span>
            <span>version {asset.version}</span>
            {status && (
              <span>
                {status.partitioned
                  ? `${status.partitions.materialized}/${status.partitions.total} partitions materialized`
                  : status.partitions.materialized
                    ? "materialized"
                    : "not materialized"}
              </span>
            )}
            {Object.entries(asset.tags).map(([k, v]) => (
              <span key={k} className="rounded-full bg-accent-soft px-2 py-0.5 text-fg-muted">
                {k}={v}
              </span>
            ))}
          </>
        }
      />
      <nav aria-label="Asset sections" className="-mt-2 flex gap-1 overflow-x-auto border-b border-line">
        <Tab to="/assets/$asset" name={name} exact>
          Overview
        </Tab>
        {tabs.partitions && (
          <Tab to="/assets/$asset/partitions" name={name}>
            Partitions
            {status && status.partitions.failed > 0 && <Count tone="fail">{status.partitions.failed}</Count>}
          </Tab>
        )}
        {tabs.keys && (
          <Tab to="/assets/$asset/keys" name={name}>
            Keys
            {!!status?.failures && Object.values(status.failures).some(Boolean) && (
              <Count tone="warn">
                {Object.values(status.failures).reduce((a, n) => (a ?? 0) + (n ?? 0), 0)}
              </Count>
            )}
          </Tab>
        )}
        {tabs.inputs && (
          <Tab to="/assets/$asset/inputs" name={name}>
            Inputs
          </Tab>
        )}
        <Tab to="/assets/$asset/history" name={name}>
          History
        </Tab>
        <Tab to="/assets/$asset/runs" name={name}>
          Runs
        </Tab>
      </nav>
      <Outlet />
    </Page>
  );
}

function Tab({
  to,
  name,
  exact,
  children,
}: {
  to:
    | "/assets/$asset"
    | "/assets/$asset/partitions"
    | "/assets/$asset/keys"
    | "/assets/$asset/inputs"
    | "/assets/$asset/history"
    | "/assets/$asset/runs";
  name: string;
  exact?: boolean;
  children: ReactNode;
}) {
  return (
    <Link
      to={to}
      params={{ asset: name }}
      search={(s: { partition?: string }) => ({ partition: s.partition })}
      activeOptions={{ exact: !!exact, includeSearch: false }}
      className={cn(
        "-mb-px flex shrink-0 items-center gap-1.5 border-b-2 border-transparent px-3 py-2 text-sm text-fg-muted motion-1 transition-colors hover:text-fg",
        "data-[status=active]:border-fg data-[status=active]:font-medium data-[status=active]:text-fg",
      )}
    >
      {children}
    </Link>
  );
}

function Count({ tone, children }: { tone: "fail" | "warn"; children: ReactNode }) {
  return (
    <span className={cn("rounded-full px-1.5 text-2xs leading-4 font-semibold tabular", toneSoft[tone])}>
      {children}
    </span>
  );
}

// -- overview tab -------------------------------------------------------------------

export function AssetOverview() {
  const { asset: name } = route.useParams();
  const { partition } = route.useSearch();
  const project = useProject();
  const manifest = useManifest();
  const { data } = useSuspenseQuery(q.asset(project, name));
  const asset = data.asset;
  const stats = useQuery(q.stats(project, undefined, name)).data?.assets.find((a) => a.asset === name);

  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,2fr)_minmax(18rem,1fr)]">
      <div className="flex min-w-0 flex-col gap-4">
        <Heads heads={data.heads} partition={partition} asset={name} />
        <Declaration asset={asset} manifest={manifest} />
      </div>
      <div className="flex min-w-0 flex-col gap-4">
        <AutomationsCard automations={data.automations} />
        <Card>
          <CardHeader title="Performance" description="Finished tasks of this asset, from the run history" />
          {stats ? (
            <Facts className="px-4 pb-4">
              <Fact label="Tasks">{count(stats.tasks)}</Fact>
              <Fact label="Failed">{count(stats.failed)}</Fact>
              <Fact label="Duration p50">{duration(stats.p50)}</Fact>
              <Fact label="Duration p95">{duration(stats.p95)}</Fact>
              <Fact label="Wait p50">{duration(stats.wait_p50)}</Fact>
              <Fact label="Wait p95">{duration(stats.wait_p95)}</Fact>
            </Facts>
          ) : (
            <p className="px-4 pb-4 text-sm text-fg-subtle">No finished tasks yet.</p>
          )}
        </Card>
        {asset.doc && asset.doc.includes("\n\n") && (
          <Card>
            <CardHeader title="About" />
            <p className="px-4 pb-4 text-sm whitespace-pre-line text-fg-muted">{asset.doc}</p>
          </Card>
        )}
        {data.cursor != null && (
          <Card>
            <CardHeader title="Cursor" description="Unpartitioned; per-partition cursors show on each head" />
            <div className="px-4 pb-4">
              <JsonView value={data.cursor as Json} />
            </div>
          </Card>
        )}
      </div>
    </div>
  );
}

function Heads({
  heads,
  partition,
  asset,
}: {
  heads: Record<string, [string, Head][]>;
  partition?: string;
  asset: string;
}) {
  const rows = Object.entries(heads).flatMap(([output, list]) =>
    list
      .filter(([s]) => partition === undefined || s === partition)
      .map(([s, head]) => ({ output, partition: s, head })),
  );
  return (
    <Card>
      <CardHeader title="Heads" description="The committed version of each output, per partition" />
      {rows.length === 0 ? (
        <Empty compact title="Nothing committed yet">
          Run the asset to give its outputs heads.
        </Empty>
      ) : (
        <TableScroll className="max-h-[28rem] overflow-y-auto">
          <Table>
            <thead className="sticky top-0 bg-surface">
              <tr>
                <Th>Output</Th>
                <Th>Partition</Th>
                <Th>Generation</Th>
                <Th className="text-right">Keys</Th>
                <Th className="text-right">Commit</Th>
                <Th>State</Th>
                <Th>Committed</Th>
              </tr>
            </thead>
            <tbody>
              {rows.map(({ output, partition: s, head }) => (
                <Tr key={`${output}/${s}`}>
                  <Td className="font-medium">{output}</Td>
                  <Td className="font-mono text-xs text-fg-muted">{s || "—"}</Td>
                  <Td>
                    <Link
                      to="/assets/$asset/history"
                      params={{ asset }}
                      search={{
                        generation: String(head.ref.generation),
                        vout: output,
                        vpartition: s || undefined,
                      }}
                      className="hover:underline"
                    >
                      <Generation value={head.ref.generation} />
                    </Link>
                  </Td>
                  <Td className="text-right">{head.count != null ? count(head.count) : "—"}</Td>
                  <Td className="text-right text-fg-muted">{head.commit_number ?? "—"}</Td>
                  <Td>
                    {head.materialized ? (
                      <StatusBadge status="materialized" />
                    ) : (
                      <Tooltip content="An incremental pass is still under way: more attempts will complete this head.">
                        <span>
                          <StatusBadge status="delta" text="incomplete" />
                        </span>
                      </Tooltip>
                    )}
                  </Td>
                  <Td className="text-fg-muted">
                    {head.run ? (
                      <Link to="/runs/$run" params={{ run: head.run }} className="hover:text-fg">
                        <Time at={head.at} />
                      </Link>
                    ) : (
                      <Time at={head.at} />
                    )}
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

function dimText(dim: DimDecl): ReactNode {
  if (dim.kind === "dynamic") {
    return (
      <>
        keys of{" "}
        <Link to="/assets/$asset" params={{ asset: dim.output }} className="text-link hover:underline">
          {dim.output}
        </Link>
      </>
    );
  }
  if (dim.kind === "time")
    return `every ${dim.every} from ${dim.start}${dim.end ? ` to ${dim.end}` : ""} (${dim.timezone})`;
  return `${dim.keys.length} static keys`;
}

function Declaration({ asset, manifest }: { asset: AssetDecl; manifest: Manifest }) {
  return (
    <Card>
      <CardHeader title="Declaration" description="As registered in the served manifest" />
      <div className="flex flex-col gap-5 px-4 pb-4">
        {asset.outputs.length > 0 && (
          <Section title="Outputs">
            {asset.outputs.map((o) => {
              const store = manifest.stores[o.store];
              return (
                <Row key={o.name} name={o.name}>
                  <span>
                    {o.store} store
                    {store && <span className="text-fg-subtle"> ({store.writes})</span>}
                  </span>
                  {o.dynamic_partitions && <Tag>dynamic partitions</Tag>}
                  {o.key && !o.dynamic_partitions && <Tag>key {o.key}</Tag>}
                  {o.incremental && !o.dynamic_partitions && <Tag>incremental</Tag>}
                  {o.migrations.length > 0 && <Tag>{plural(o.migrations.length, "migration")}</Tag>}
                </Row>
              );
            })}
          </Section>
        )}
        {(Object.keys(asset.inputs).length > 0 || asset.deps.length > 0) && (
          <Section title="Inputs">
            {Object.entries(asset.inputs).map(([param, input]) => (
              <Row key={param} name={param}>
                <span>
                  <span className="text-fg-subtle">
                    {input.each
                      ? "Each"
                      : input.kind === "in"
                        ? "In"
                        : input.kind === "incremental"
                          ? "Incremental"
                          : "AllPartitions"}
                    (
                  </span>
                  {input.output}
                  <span className="text-fg-subtle">)</span>
                </span>
                {input.batch_size != null && <Tag>{input.batch_size} keys a batch</Tag>}
                {input.each && <Tag>{input.each.concurrency} at a time</Tag>}
                {input.patterns && <PatternList patterns={input.patterns} />}
              </Row>
            ))}
            {asset.deps.map((dep) => (
              <Row key={dep} name={dep}>
                <span className="text-fg-subtle">dep: pinned in lineage, not loaded</span>
              </Row>
            ))}
          </Section>
        )}
        {asset.partitions && (
          <Section title="Partitions">
            {Object.entries(asset.partitions.dims).map(([dim, decl]) => (
              <Row key={dim} name={dim}>
                <span>{dimText(decl)}</span>
              </Row>
            ))}
          </Section>
        )}
        <Facts>
          <Fact label="Placement">
            {asset.placement.executor}
            <span className="text-fg-subtle"> · {asset.placement.kind}</span>
            {Object.entries(asset.placement.placement).map(([k, v]) => (
              <span key={k} className="text-fg-subtle">
                {" "}
                · {k} {String(v)}
              </span>
            ))}
          </Fact>
          <Fact label="Retries">
            {asset.retries.n} · {asset.retries.backoff} from {duration(asset.retries.delay)}
          </Fact>
          <Fact label="Timeout">{duration(asset.timeout)}</Fact>
          <Fact label="On version change">{asset.on_version_change}</Fact>
          {asset.aliases.length > 0 && <Fact label="Formerly">{asset.aliases.join(", ")}</Fact>}
        </Facts>
      </div>
    </Card>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="flex flex-col gap-1.5">
      <h3 className="text-2xs font-medium tracking-wide text-fg-subtle uppercase">{title}</h3>
      <ul className="flex flex-col divide-y divide-line rounded-md border-theme border-line">{children}</ul>
    </div>
  );
}

function Row({ name, children }: { name: string; children: ReactNode }) {
  return (
    <li className="flex flex-wrap items-center gap-x-3 gap-y-1 px-3 py-2 text-sm">
      <span className="min-w-28 font-mono text-xs font-medium text-fg">{name}</span>
      {children}
    </li>
  );
}

function Tag({ children }: { children: ReactNode }) {
  return <span className="rounded-full bg-accent-soft px-2 py-0.5 text-xs text-fg-muted">{children}</span>;
}

export function AutomationsCard({ automations }: { automations: Automation[] }) {
  const toggle = useAutomationToggle();
  const fire = useRunAutomation();
  const now = useNow();
  return (
    <Card>
      <CardHeader
        title="Automations"
        description={automations.length ? undefined : "Nothing runs this asset on its own"}
      />
      {automations.length > 0 && (
        <ul className="flex flex-col divide-y divide-line border-t border-line">
          {automations.map((a) => (
            <li key={a.name} className="flex items-center gap-3 px-4 py-2.5">
              <div className="flex min-w-0 flex-1 flex-col gap-0.5">
                <span className="truncate font-mono text-xs text-fg">{a.name}</span>
                <span className="text-xs text-fg-muted">
                  {describeTrigger(a.trigger, a.watched)}
                  {a.partitions && typeof a.partitions === "string" && ` · ${a.partitions}`}
                </span>
                <span className="text-2xs text-fg-subtle">
                  last {a.last_fired ? <Time at={a.last_fired} /> : "never"}
                  {a.next_at != null && a.enabled && ` · next ${until(a.next_at, now)}`}
                  {a.pending.length > 0 && ` · ${plural(a.pending.length, "change")} pending`}
                </span>
              </div>
              <Button
                size="sm"
                variant="ghost"
                icon={<Zap />}
                onClick={() => fire.mutate(a.name)}
                disabled={fire.isPending}
              >
                Run now
              </Button>
              <Switch
                checked={a.enabled}
                label={`${a.enabled ? "Disable" : "Enable"} ${a.name}`}
                onCheckedChange={(enabled) => toggle.mutate({ name: a.name, enabled })}
              />
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}

// -- runs tab ------------------------------------------------------------------------

export function AssetRuns() {
  const { asset: name } = route.useParams();
  const project = useProject();
  const runs = useInfiniteQuery({
    ...q.runs(project, { asset: [name] }),
    placeholderData: keepPreviousData,
  });
  const rows = runs.data?.pages.flatMap((p) => p.runs) ?? [];
  return (
    <Card>
      {!runs.data ? (
        <Skeleton className="m-4 h-40" />
      ) : (
        <>
          <RunsTable runs={rows} empty={<Empty title="No runs yet">Nothing has run this asset.</Empty>} />
          {runs.hasNextPage && (
            <div className="flex justify-end border-t border-line px-4 py-2.5">
              <Button size="sm" onClick={() => runs.fetchNextPage()} disabled={runs.isFetchingNextPage}>
                Load more
              </Button>
            </div>
          )}
        </>
      )}
    </Card>
  );
}
