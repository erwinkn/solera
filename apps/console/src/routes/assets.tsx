import { useQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { List, Network, Play } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import type { AssetStatus, Manifest } from "@/api/types";
import { AssetGraph, assetTone, KIND_ICON, KIND_LABEL, kindOf } from "@/features/graph";
import { RunButton } from "@/features/run-dialog";
import { describeTrigger } from "@/features/triggers";
import { failingKeys } from "@/routes/overview";
import { cn } from "@/lib/cn";
import { firstLine, plural } from "@/lib/format";
import { toneSoft } from "@/lib/status";
import { Empty, SegmentBar, Time } from "@/ui/data";
import { SearchInput, Segmented } from "@/ui/form";
import { Card, Page, PageHeader } from "@/ui/layout";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

const route = getRouteApi("/assets");

export function Assets() {
  const { view = "graph", q: text = "" } = route.useSearch();
  const navigate = route.useNavigate();
  const manifest = useManifest();
  const project = useProject();
  const status = useQuery(q.assetStatus(project)).data;
  const match = (id: string) => !text || id.toLowerCase().includes(text.toLowerCase());

  return (
    <Page className="max-w-none">
      <PageHeader
        title="Assets"
        description={`${plural(Object.keys(manifest.assets).length, "asset")} and ${plural(Object.keys(manifest.sources).length, "source")}. Edges show how each input is consumed.`}
        actions={
          <>
            <SearchInput
              aria-label="Filter assets"
              placeholder="Filter assets"
              className="w-56"
              value={text}
              onChange={(e) =>
                navigate({
                  search: (s) => ({ ...s, q: e.target.value || undefined }),
                  replace: true,
                })
              }
            />
            <Segmented
              label="View"
              value={view}
              onChange={(v) =>
                navigate({
                  search: (s) => ({
                    ...s,
                    view: v === "graph" ? undefined : v,
                  }),
                  replace: true,
                })
              }
              options={[
                {
                  value: "graph",
                  label: (
                    <>
                      <Network aria-hidden /> Graph
                    </>
                  ),
                },
                {
                  value: "list",
                  label: (
                    <>
                      <List aria-hidden /> List
                    </>
                  ),
                },
              ]}
            />
            <RunButton icon={<Play />} />
          </>
        }
      />
      {view === "graph" ? (
        <AssetGraph manifest={manifest} status={status} match={match} />
      ) : (
        <AssetTable manifest={manifest} status={status} match={match} />
      )}
    </Page>
  );
}

function AssetTable({
  manifest,
  status,
  match,
}: {
  manifest: Manifest;
  status: Record<string, AssetStatus> | undefined;
  match: (id: string) => boolean;
}) {
  const names = Object.keys(manifest.assets).filter(match).sort();
  if (names.length === 0)
    return (
      <Card>
        <Empty title="No asset matches" />
      </Card>
    );
  return (
    <Card>
      <TableScroll>
        <Table>
          <thead>
            <tr>
              <Th>Asset</Th>
              <Th>Partitions</Th>
              <Th className="text-right">Failing keys</Th>
              <Th>Updated</Th>
              <Th>Runs on</Th>
              <Th>Automations</Th>
            </tr>
          </thead>
          <tbody>
            {names.map((name) => {
              const asset = manifest.assets[name]!;
              const s = status?.[name];
              const kind = kindOf(asset);
              const p = s?.partitions;
              const keys = failingKeys(s);
              return (
                <Tr key={name} className="relative">
                  <Td className="max-w-[26rem] py-2">
                    <Link
                      to="/assets/$asset"
                      params={{ asset: name }}
                      className="flex items-center gap-2.5 after:absolute after:inset-0 after:content-['']"
                    >
                      <span
                        className={cn(
                          "grid size-6 shrink-0 place-items-center rounded-sm [&_svg]:size-3.5",
                          toneSoft[assetTone(s)],
                        )}
                      >
                        {KIND_ICON[kind]}
                      </span>
                      <span className="flex min-w-0 flex-col">
                        <span className="truncate font-medium text-fg">{name}</span>
                        <span className="truncate text-xs text-fg-subtle">
                          {firstLine(asset.doc) || KIND_LABEL[kind]}
                        </span>
                      </span>
                    </Link>
                  </Td>
                  <Td className="w-56">
                    {p && (
                      <span className="flex items-center gap-2.5">
                        <SegmentBar
                          className="w-24"
                          parts={[
                            {
                              tone: "ok",
                              value: p.complete,
                              label: "complete",
                            },
                            { tone: "run", value: p.running, label: "running" },
                            { tone: "fail", value: p.failed, label: "failed" },
                            {
                              tone: "idle",
                              value: p.missing,
                              label: "missing",
                            },
                          ]}
                        />
                        <span className="text-xs text-fg-muted tabular">
                          {s.partitioned
                            ? `${p.complete}/${p.total}`
                            : p.complete
                              ? "done"
                              : p.running
                                ? "running"
                                : p.failed
                                  ? "failed"
                                  : "—"}
                        </span>
                      </span>
                    )}
                  </Td>
                  <Td className={cn("text-right", keys ? "font-medium text-warn-fg" : "text-fg-subtle")}>
                    {s?.failures ? keys : "—"}
                  </Td>
                  <Td className="text-fg-muted">
                    <Time at={s?.updated_at} />
                  </Td>
                  <Td className="text-fg-muted">{asset.placement.executor}</Td>
                  <Td className="max-w-72 truncate text-xs text-fg-muted">
                    {asset.automations
                      .map((a) =>
                        manifest.automations[a]
                          ? describeTrigger(manifest.automations[a].trigger, manifest.automations[a].watched)
                          : a,
                      )
                      .join(" · ") || "—"}
                  </Td>
                </Tr>
              );
            })}
          </tbody>
        </Table>
      </TableScroll>
    </Card>
  );
}
