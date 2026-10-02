import { keepPreviousData, useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { ArrowDownRight, ArrowUpLeft } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import type { Lineage, Materialization } from "@/api/types";
import { cn } from "@/lib/cn";
import { compact, shortId } from "@/lib/format";
import { Button } from "@/ui/button";
import { Empty, Hash, Skeleton, Time } from "@/ui/data";
import { Select } from "@/ui/form";
import { Card, CardHeader } from "@/ui/layout";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

const route = getRouteApi("/assets/$asset/history");

export function AssetHistory() {
  const { asset: name } = route.useParams();
  const { scope, output, version } = route.useSearch();
  const navigate = route.useNavigate();
  const project = useProject();
  const manifest = useManifest();
  const outputs = manifest.assets[name]?.outputs ?? [];
  const history = useInfiniteQuery({
    ...q.history(project, name, { output, scope }),
    placeholderData: keepPreviousData,
  });
  const rows = history.data?.pages.flatMap((p) => p.materializations) ?? [];
  const selected = version
    ? rows.find((r) => r.version === version && (!output || r.output === output))
    : undefined;

  if (outputs.length === 0) {
    return (
      <Card>
        <Empty title="A job makes no versions">Its runs are its history: see the Runs tab.</Empty>
      </Card>
    );
  }
  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_24rem]">
      <Card>
        <CardHeader
          title="Versions"
          description="Every version a commit installed, newest first, with what changed and its metadata"
          actions={
            outputs.length > 1 && (
              <Select
                aria-label="Output"
                className="w-auto"
                value={output ?? ""}
                onChange={(e) =>
                  navigate({
                    search: (s) => ({
                      ...s,
                      output: e.target.value || undefined,
                    }),
                    replace: true,
                  })
                }
              >
                <option value="">Every output</option>
                {outputs.map((o) => (
                  <option key={o.name} value={o.name}>
                    {o.name}
                  </option>
                ))}
              </Select>
            )
          }
        />
        {!history.data ? (
          <Skeleton className="mx-4 mb-4 h-40" />
        ) : rows.length === 0 ? (
          <Empty compact title="No versions yet">
            Nothing has been committed{scope ? " for this partition" : ""}.
          </Empty>
        ) : (
          <TableScroll className="border-t border-line">
            <Table>
              <thead>
                <tr>
                  <Th>Committed</Th>
                  {outputs.length > 1 && !output && <Th>Output</Th>}
                  {!scope && <Th>Partition</Th>}
                  <Th>Version</Th>
                  <Th className="text-right">Changed</Th>
                  <Th className="text-right">Rows</Th>
                  <Th>Metadata</Th>
                  <Th>Run</Th>
                </tr>
              </thead>
              <tbody>
                {rows.map((m) => (
                  <VersionRow
                    key={`${m.output}/${m.scope}/${m.version}/${m.at}`}
                    m={m}
                    selected={m === selected}
                    showOutput={outputs.length > 1 && !output}
                    showScope={!scope}
                  />
                ))}
              </tbody>
            </Table>
          </TableScroll>
        )}
        {history.hasNextPage && (
          <div className="flex justify-end border-t border-line px-4 py-2.5">
            <Button size="sm" onClick={() => history.fetchNextPage()} disabled={history.isFetchingNextPage}>
              Load more
            </Button>
          </div>
        )}
      </Card>
      <LineagePanel selected={selected} />
    </div>
  );
}

function VersionRow({
  m,
  selected,
  showOutput,
  showScope,
}: {
  m: Materialization;
  selected: boolean;
  showOutput: boolean;
  showScope: boolean;
}) {
  const metadata =
    m.metadata && typeof m.metadata === "object" && !Array.isArray(m.metadata)
      ? Object.entries(m.metadata)
      : [];
  return (
    <Tr className={cn("relative", selected && "bg-select hover:bg-select")}>
      <Td className="text-fg-muted">
        <Link
          from="/assets/$asset/history"
          to="."
          search={(s) => ({
            ...s,
            version: selected ? undefined : m.version,
            output: s.output,
          })}
          replace
          className="after:absolute after:inset-0 after:content-['']"
        >
          <Time at={m.at} />
        </Link>
      </Td>
      {showOutput && <Td>{m.output}</Td>}
      {showScope && <Td className="font-mono text-xs text-fg-muted">{m.scope || "—"}</Td>}
      <Td>
        <Hash value={m.version} />
      </Td>
      <Td className="text-right text-xs whitespace-nowrap">
        {m.added != null || m.removed != null ? (
          <>
            {!!m.added && <span className="text-ok-fg">+{compact(m.added)}</span>}
            {!!m.removed && <span className="ml-1.5 text-fail-fg">−{compact(m.removed)}</span>}
            {!m.added && !m.removed && <span className="text-fg-subtle">same keys</span>}
          </>
        ) : (
          <span className="text-fg-subtle">—</span>
        )}
      </Td>
      <Td className="text-right">{m.rows != null ? compact(m.rows) : "—"}</Td>
      <Td className="max-w-56 truncate text-xs text-fg-muted">
        {metadata.length
          ? metadata
              .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : String(v)}`)
              .join(" · ")
          : "—"}
      </Td>
      <Td>
        <Link
          to="/runs/$run"
          params={{ run: m.run }}
          className="relative z-10 font-mono text-xs text-link hover:underline"
        >
          {shortId(m.run)}
        </Link>
      </Td>
    </Tr>
  );
}

function LineagePanel({ selected }: { selected?: Materialization }) {
  const project = useProject();
  const up = useQuery({
    ...q.lineage(project, selected?.output ?? "", selected?.scope ?? "", selected?.version, "upstream"),
    enabled: !!selected,
  });
  const down = useQuery({
    ...q.lineage(project, selected?.output ?? "", selected?.scope ?? "", selected?.version, "downstream"),
    enabled: !!selected,
  });
  if (!selected) {
    return (
      <Card className="self-start">
        <Empty compact title="Lineage">
          Select a version to see what it was built from, and what was built from it.
        </Empty>
      </Card>
    );
  }
  return (
    <Card className="self-start">
      <CardHeader
        title="Lineage"
        description={
          <>
            {selected.output}
            {selected.scope && ` · ${selected.scope}`} @{" "}
            <span className="font-mono">{selected.version.slice(0, 8)}</span>
          </>
        }
      />
      <div className="flex flex-col gap-4 px-4 pb-4">
        <LineageList title="Built from" icon={<ArrowUpLeft />} lineage={up.data} />
        <LineageList title="Used by" icon={<ArrowDownRight />} lineage={down.data} />
      </div>
    </Card>
  );
}

function LineageList({ title, icon, lineage }: { title: string; icon: React.ReactNode; lineage?: Lineage }) {
  if (!lineage) return <Skeleton className="h-16" />;
  const root = `${lineage.root.output}|${lineage.root.scope}|${lineage.root.version}`;
  const nodes = lineage.nodes.filter((n) => `${n.output}|${n.scope}|${n.version}` !== root);
  return (
    <div className="flex flex-col gap-1.5">
      <h3 className="flex items-center gap-1.5 text-2xs font-medium tracking-wide text-fg-subtle uppercase [&_svg]:size-3">
        {icon}
        {title}
      </h3>
      {nodes.length === 0 ? (
        <p className="text-xs text-fg-subtle">Nothing recorded.</p>
      ) : (
        <ul className="flex flex-col divide-y divide-line rounded-md border-theme border-line">
          {nodes.map((n) => (
            <li
              key={`${n.output}/${n.scope}/${n.version}`}
              className="flex flex-wrap items-center gap-x-2 gap-y-0.5 px-3 py-1.5 text-xs"
            >
              {n.asset ? (
                <Link
                  to="/assets/$asset/history"
                  params={{ asset: n.asset }}
                  search={{
                    output: n.output,
                    scope: n.scope || undefined,
                    version: n.version,
                  }}
                  className="font-medium text-fg hover:underline"
                >
                  {n.output}
                </Link>
              ) : (
                <span className="font-medium">{n.output}</span>
              )}
              {n.scope && <span className="font-mono text-fg-subtle">{n.scope}</span>}
              <Hash value={n.version} />
              {!n.current && (
                <span className="rounded-full bg-idle-soft px-1.5 text-2xs text-idle-fg">superseded</span>
              )}
              <span className="ml-auto text-fg-subtle">{n.at ? <Time at={n.at} /> : null}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
