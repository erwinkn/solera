import { useState, type ReactNode } from "react";
import { keepPreviousData, useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { ChevronDown, RotateCcw, Search } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import { keyEntry } from "@/api/read";
import { useRetryKeys } from "@/api/mutations";
import type { AssetDecl, Explain, FailureClass, FailureKey, FailedKeys, KeyOutcome } from "@/api/types";
import { PatternList } from "@/features/patterns";
import { StaleKeysCard } from "@/features/stale";
import { join, list } from "@/router";
import { useNow } from "@/lib/clock";
import { cn } from "@/lib/cn";
import { count, plural, shortId, until } from "@/lib/format";
import { label, tone, toneSoft, toneText } from "@/lib/status";
import { Button } from "@/ui/button";
import { Empty, ErrorNote, Generation, Skeleton, Time } from "@/ui/data";
import { Chip, Input, SearchInput, Select } from "@/ui/form";
import { Card, CardHeader } from "@/ui/layout";
import { Menu, MenuItem, Tooltip } from "@/ui/overlay";
import { StatusBadge, StatusIcon } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

const route = getRouteApi("/assets/$asset/keys");
const CLASSES: FailureClass[] = ["failed", "rejected", "retrying", "timed_out", "canceled"];

const isEach = (asset: AssetDecl) => Object.values(asset.inputs).some((e) => e.each);
const keyedEdge = (asset: AssetDecl) => Object.values(asset.inputs).some((e) => e.kind === "incremental");

export function AssetKeys() {
  const { asset: name } = route.useParams();
  const { key, partition } = route.useSearch();
  const manifest = useManifest();
  const asset = manifest.assets[name]!;
  const each = isEach(asset);
  // Stale keys belong to one partition: the chosen one, or the only one an unpartitioned asset has.
  const stalePartition = asset.partitions ? partition : "";
  return (
    <div className="flex flex-col gap-4">
      {stalePartition !== undefined && <StaleKeysCard name={name} partition={stalePartition} />}
      {each && <FailingKeys name={name} />}
      {(each || keyedEdge(asset)) && <ExplainKey key={key} name={name} asset={asset} />}
      {each && <KeyOutcomes name={name} />}
      <LiveKeys name={name} asset={asset} />
    </div>
  );
}

// -- failed keys ---------------------------------------------------------------------

function FailingKeys({ name }: { name: string }) {
  const { partition, outcome } = route.useSearch();
  const navigate = route.useNavigate();
  const project = useProject();
  const now = useNow();
  const classes = list(outcome);
  const failures = useInfiniteQuery({
    ...q.failures(project, name, { partition, outcome: classes }),
    placeholderData: keepPreviousData,
  });
  const retry = useRetryKeys(name);
  const first = failures.data?.pages[0];
  const keys = failures.data?.pages.flatMap((p) => p.keys) ?? [];
  const totals = totalsOf(first);
  const due = first?.partitions.filter((s) => s.has_retries).length ?? 0;

  return (
    <Card>
      <CardHeader
        title="Failed keys"
        description={
          <>
            Keys whose last call didn't succeed, each with its retry record. A failed key keeps its previous
            output, and a run retries it when its class makes it due
            {partition ? (
              <>
                {" "}
                · partition <span className="font-mono">{partition}</span>
              </>
            ) : (
              " · every partition"
            )}
            .
          </>
        }
        actions={
          <Menu
            trigger={
              <Button icon={<RotateCcw />}>
                Retry keys <ChevronDown className="size-3" />
              </Button>
            }
          >
            {(["failed", "rejected", "canceled", "timed_out", "retrying"] as const).map((c) => (
              <MenuItem key={c} onClick={() => retry.mutate({ classes: [c], partition })}>
                Retry {label(c)} keys{totals[c] ? ` (${totals[c]})` : ""}
              </MenuItem>
            ))}
            <MenuItem onClick={() => retry.mutate({ classes: ["all"], partition })}>
              Retry every failed key
            </MenuItem>
          </Menu>
        }
      />
      <div className="flex flex-wrap items-center gap-1.5 px-4 pb-3">
        {CLASSES.map((c) => {
          const active = classes.includes(c);
          if (!active && !totals[c]) return null;
          return (
            <Chip
              key={c}
              active={active}
              count={totals[c] ?? 0}
              onClick={() =>
                navigate({
                  search: (s) => ({
                    ...s,
                    outcome: join(active ? classes.filter((x) => x !== c) : [...classes, c]),
                  }),
                  replace: true,
                })
              }
            >
              <StatusIcon status={c} className={active ? "text-current" : undefined} />
              {label(c)}
            </Chip>
          );
        })}
        {first && (
          <span className="ml-auto text-xs text-fg-subtle">
            {due > 0 ? `${plural(due, "partition")} with keys due now` : "no keys due"}
          </span>
        )}
      </div>
      {failures.isError ? (
        <div className="px-4 pb-4">
          <ErrorNote error={failures.error} />
        </div>
      ) : !failures.data ? (
        <Skeleton className="mx-4 mb-4 h-24" />
      ) : keys.length === 0 ? (
        <Empty compact title="Every key processed">
          No key of this asset is failing
          {classes.length ? " in these classes" : ""}.
        </Empty>
      ) : (
        <TableScroll className="border-t border-line">
          <Table>
            <thead>
              <tr>
                <Th>Key</Th>
                {!partition && <Th>Partition</Th>}
                <Th>Class</Th>
                <Th className="text-right">Tries</Th>
                <Th>Since</Th>
                <Th>Next try</Th>
                <Th>Last error</Th>
                <Th />
              </tr>
            </thead>
            <tbody>
              {keys.map((k) => (
                <Tr key={`${k.partition}/${k.key}`}>
                  <Td className="max-w-72 truncate font-mono text-xs">{k.key}</Td>
                  {!partition && <Td className="font-mono text-xs text-fg-muted">{k.partition || "—"}</Td>}
                  <Td>
                    <StatusBadge status={k.outcome} />
                  </Td>
                  <Td className="text-right">{k.tries}</Td>
                  <Td className="text-fg-muted">
                    <Time at={k.since} />
                  </Td>
                  <Td className="text-fg-muted">{nextTry(k, now)}</Td>
                  <Td className="max-w-96">
                    <Tooltip content={<span className="break-words">{k.message}</span>}>
                      <span className="block truncate text-xs text-fail-fg">{k.message}</span>
                    </Tooltip>
                  </Td>
                  <Td className="text-right">
                    <Link
                      from="/assets/$asset/keys"
                      to="."
                      search={(s) => ({
                        ...s,
                        key: k.key,
                        partition: k.partition || undefined,
                      })}
                      className="text-xs text-link hover:underline"
                    >
                      Explain
                    </Link>
                  </Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        </TableScroll>
      )}
      {failures.hasNextPage && (
        <div className="flex justify-end border-t border-line px-4 py-2.5">
          <Button size="sm" onClick={() => failures.fetchNextPage()} disabled={failures.isFetchingNextPage}>
            Load more
          </Button>
        </div>
      )}
    </Card>
  );
}

function totalsOf(page: FailedKeys | undefined): Partial<Record<FailureClass, number>> {
  const totals: Partial<Record<FailureClass, number>> = {};
  for (const s of page?.partitions ?? []) {
    for (const [c, n] of Object.entries(s.counts) as [FailureClass, number][])
      totals[c] = (totals[c] ?? 0) + n;
  }
  return totals;
}

/** When a key runs again, in its class's terms (per-key-processing.md §8). */
function nextTry(k: FailureKey, now: number): ReactNode {
  if (k.eligible) return <span className="text-wait-fg">due now</span>;
  if (k.next_at) return until(k.next_at, now);
  switch (k.outcome) {
    case "rejected":
      return "when its input changes";
    case "failed":
      return "on the next deploy";
    case "canceled":
      return "only on request";
    default:
      return "—";
  }
}

// -- explain ----------------------------------------------------------------------

/** The inputs `explain` can answer for: keyed incremental ones, an Each input first. */
function explainable(asset: AssetDecl): string[] {
  return Object.entries(asset.inputs)
    .filter(([, e]) => e.kind === "incremental")
    .sort(([, a], [, b]) => Number(!!b.each) - Number(!!a.each))
    .map(([param]) => param);
}

function ExplainKey({ name, asset }: { name: string; asset: AssetDecl }) {
  const { key, partition, input } = route.useSearch();
  const inputs = explainable(asset);
  const chosenInput = input ?? inputs[0];
  const navigate = route.useNavigate();
  const project = useProject();
  const partitions = useQuery({
    ...q.partitions(project, name),
    enabled: !!asset.partitions,
  }).data;
  const effectivePartition =
    partition ?? (asset.partitions ? partitions?.find((p) => p.status !== "removed")?.partition : "");
  const [draft, setDraft] = useState(key ?? "");
  const answer = useQuery({
    ...q.explain(project, name, key ?? "", effectivePartition ?? "", chosenInput),
    enabled: !!key && effectivePartition !== undefined,
  });

  return (
    <Card>
      <CardHeader title="Explain a key" description="Why a key is, or isn't, in this asset's output" />
      <form
        className="flex flex-wrap items-center gap-2 px-4 pb-4"
        onSubmit={(e) => {
          e.preventDefault();
          navigate({
            search: (s) => ({
              ...s,
              key: draft.trim() || undefined,
              partition: s.partition ?? effectivePartition ?? undefined,
            }),
            replace: true,
          });
        }}
      >
        <Input
          aria-label="Key to explain"
          className="w-80 font-mono text-xs"
          placeholder="alpha-file-3"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        {inputs.length > 1 && (
          <Select
            aria-label="Input"
            className="w-auto font-mono text-xs"
            value={chosenInput}
            onChange={(e) => navigate({ search: (s) => ({ ...s, input: e.target.value }), replace: true })}
          >
            {inputs.map((param) => (
              <option key={param} value={param}>
                through {param}
              </option>
            ))}
          </Select>
        )}
        {asset.partitions && (
          <span className="text-xs text-fg-subtle">
            in <span className="font-mono">{effectivePartition ?? "…"}</span>
          </span>
        )}
        <Button type="submit" icon={<Search />}>
          Explain
        </Button>
      </form>
      {key &&
        (answer.isError ? (
          <div className="px-4 pb-4">
            <ErrorNote error={answer.error} title="Couldn't explain this key" />
          </div>
        ) : !answer.data ? (
          <Skeleton className="mx-4 mb-4 h-20" />
        ) : (
          <Answer explain={answer.data} />
        ))}
    </Card>
  );
}

function Answer({ explain: e }: { explain: Explain }) {
  const headline: Record<Explain["verdict"], ReactNode> = {
    ok: (
      <>
        In the output, from {e.upstream} at{" "}
        <Generation value={e.last_ok?.generation ?? e.upstream_generation} />
      </>
    ),
    failing: (
      <>
        {label(e.failure?.outcome)} at <Generation value={e.failure?.generation} />: {e.failure?.message}
      </>
    ),
    excluded: (
      <>
        Excluded by the input's pattern <code className="font-mono">{e.patterns.excluded_by}</code>
      </>
    ),
    not_matched: <>Not matched: outside the input's include patterns</>,
    pending: (
      <>
        Waiting: {e.upstream} wrote it at <Generation value={e.upstream_generation} />, not processed yet
      </>
    ),
    removed: Object.values(e.outputs).some((o) => o.present) ? (
      <>Removed upstream: {e.upstream} no longer has it; its rows go when the removal is delivered</>
    ) : (
      <>Removed upstream: {e.upstream} no longer has it, and its rows are gone</>
    ),
    absent: (
      <>
        Unknown: {e.upstream}
        {e.upstream_partition && ` (${e.upstream_partition})`} has never had this key
      </>
    ),
  };
  const t = e.verdict === "failing" && e.failure ? tone(e.failure.outcome) : tone(e.verdict);
  return (
    <div className="mx-4 mb-4 flex flex-col gap-3 rounded-md border-theme border-line p-4">
      <p className={cn("flex items-start gap-2 text-sm font-medium", toneText[t])}>
        <StatusIcon tone={t} className="mt-0.5 size-4" />
        <span className="min-w-0 break-words">{headline[e.verdict]}</span>
      </p>
      <ul className="flex flex-col gap-1.5 pl-6 text-xs text-fg-muted">
        <li>
          Upstream <span className="text-fg">{e.upstream}</span>
          {e.upstream_partition && <span className="font-mono"> · {e.upstream_partition}</span>}:{" "}
          {e.upstream_generation != null ? (
            <>
              has it at <Generation value={e.upstream_generation} />
            </>
          ) : (
            "doesn't have it"
          )}
        </li>
        {Object.entries(e.outputs).map(([output, o]) => (
          <li key={output}>
            Output <span className="text-fg">{output}</span>:{" "}
            {o.present ? (
              <>
                holds it
                {o.generation != null && (
                  <>
                    {" "}
                    at <Generation value={o.generation} />
                  </>
                )}
              </>
            ) : (
              "doesn't hold it"
            )}
          </li>
        ))}
        {e.failure && (
          <li>
            {plural(e.failure.tries, "try", "tries")} since <Time at={e.failure.since} />
            {e.failure.next_at ? (
              <>
                {" "}
                · next at <Time at={e.failure.next_at} />
              </>
            ) : null}
            {e.failure.until ? (
              <>
                {" "}
                · gives up <Time at={e.failure.until} />
              </>
            ) : null}
          </li>
        )}
        {e.last && (
          <li>
            Last call:{" "}
            <span className={cn(toneSoft[tone(e.last.outcome)], "rounded-xs px-1")}>
              {label(e.last.outcome)}
            </span>{" "}
            at <Generation value={e.last.generation} /> <Time at={e.last.at} /> in run{" "}
            <Link
              to="/runs/$run"
              params={{ run: e.last.run }}
              className="font-mono text-link hover:underline"
            >
              {shortId(e.last.run)}
            </Link>
            {e.last.error && <span className="text-fail-fg"> · {e.last.error}</span>}
          </li>
        )}
        {e.last_ok && e.last_ok !== e.last && (
          <li>
            Last success at <Generation value={e.last_ok.generation} /> <Time at={e.last_ok.at} />
          </li>
        )}
        {e.patterns.spec && (
          <li className="flex flex-wrap items-center gap-1.5">
            Patterns <PatternList patterns={e.patterns.spec} />
            {e.patterns.pending && <span className="text-wait-fg">(a pattern change is being applied)</span>}
          </li>
        )}
      </ul>
    </div>
  );
}

// -- key outcomes ------------------------------------------------------------------

function KeyOutcomes({ name }: { name: string }) {
  const { partition, q: text } = route.useSearch();
  const navigate = route.useNavigate();
  const project = useProject();
  const outcomes = useInfiniteQuery({
    ...q.keyOutcomes(project, name, { partition, q: text }),
    placeholderData: keepPreviousData,
  });
  const rows = outcomes.data?.pages.flatMap((p) => p.outcomes) ?? [];
  // Which try each call was: shown once the engine reports it.
  const tried = rows.some((o) => o.tries != null);
  return (
    <Card>
      <CardHeader
        title="Key outcomes"
        description="Every key an attempt processed, newest first. Kept as long as its run."
        actions={
          <SearchInput
            aria-label="Search keys"
            placeholder="Search keys"
            className="w-56"
            value={text ?? ""}
            onChange={(e) =>
              navigate({
                search: (s) => ({ ...s, q: e.target.value || undefined }),
                replace: true,
              })
            }
          />
        }
      />
      {outcomes.isError ? (
        <div className="px-4 pb-4">
          <ErrorNote error={outcomes.error} />
        </div>
      ) : !outcomes.data ? (
        <Skeleton className="mx-4 mb-4 h-24" />
      ) : rows.length === 0 ? (
        <Empty compact title="No outcomes">
          {text ? "No processed key matches." : "No key has been processed yet."}
        </Empty>
      ) : (
        <TableScroll className="max-h-[26rem] overflow-y-auto border-t border-line">
          <Table>
            <thead className="sticky top-0 bg-surface">
              <tr>
                <Th>Key</Th>
                {!partition && <Th>Partition</Th>}
                <Th>Outcome</Th>
                {tried && <Th className="text-right">Try</Th>}
                <Th>Generation</Th>
                <Th>Error</Th>
                <Th>When</Th>
                <Th>Run</Th>
              </tr>
            </thead>
            <tbody>
              {rows.map((o: KeyOutcome, i) => (
                <Tr key={`${o.attempt}/${o.key}/${i}`}>
                  <Td className="max-w-72 truncate font-mono text-xs">{o.key}</Td>
                  {!partition && <Td className="font-mono text-xs text-fg-muted">{o.partition || "—"}</Td>}
                  <Td>
                    <StatusBadge status={o.outcome} />
                  </Td>
                  {tried && (
                    <Td className="text-right text-fg-muted tabular">
                      {o.tries == null ? "—" : o.tries > 1 ? `retry ${o.tries - 1}` : "first"}
                    </Td>
                  )}
                  <Td>
                    <Generation value={o.generation} />
                  </Td>
                  <Td className="max-w-80 truncate text-xs text-fail-fg" title={o.error ?? undefined}>
                    {o.error}
                  </Td>
                  <Td className="text-fg-muted">
                    <Time at={o.at} />
                  </Td>
                  <Td>
                    <Link
                      to="/runs/$run"
                      params={{ run: o.run }}
                      search={{ attempt: o.attempt }}
                      className="font-mono text-xs text-link hover:underline"
                    >
                      {shortId(o.run)}
                    </Link>
                  </Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        </TableScroll>
      )}
      {outcomes.hasNextPage && (
        <div className="flex justify-end border-t border-line px-4 py-2.5">
          <Button size="sm" onClick={() => outcomes.fetchNextPage()} disabled={outcomes.isFetchingNextPage}>
            Load more
          </Button>
        </div>
      )}
    </Card>
  );
}

// -- live keys ---------------------------------------------------------------------

function LiveKeys({ name, asset }: { name: string; asset: AssetDecl }) {
  const { partition, output } = route.useSearch();
  const navigate = route.useNavigate();
  const project = useProject();
  const keyed = asset.outputs.filter((o) => o.key);
  const chosen = keyed.find((o) => o.name === output) ?? keyed[0];
  const partitions = useQuery({
    ...q.partitions(project, name),
    enabled: !!asset.partitions,
  }).data;
  const effectivePartition =
    partition ?? (asset.partitions ? partitions?.find((p) => p.status === "materialized")?.partition : "");
  const keys = useInfiniteQuery({
    ...q.keys(project, chosen?.name ?? "", effectivePartition ?? ""),
    enabled: !!chosen && effectivePartition !== undefined,
    placeholderData: keepPreviousData,
  });
  if (!chosen) return null;
  const first = keys.data?.pages[0];
  const entries = keys.data?.pages.flatMap((p) => Object.entries(p.keys)) ?? [];
  return (
    <Card>
      <CardHeader
        title="Live keys"
        description={
          <>
            What the key index holds now
            {effectivePartition ? (
              <>
                {" "}
                for <span className="font-mono">{effectivePartition}</span>
              </>
            ) : (
              ""
            )}
            {first && ` · ${plural(first.total, "key")}`}
            {" · a key's version is the generation that last wrote it"}
          </>
        }
        actions={
          keyed.length > 1 && (
            <Select
              aria-label="Output"
              className="w-auto"
              value={chosen.name}
              onChange={(e) =>
                navigate({
                  search: (s) => ({ ...s, output: e.target.value }),
                  replace: true,
                })
              }
            >
              {keyed.map((o) => (
                <option key={o.name} value={o.name}>
                  {o.name}
                </option>
              ))}
            </Select>
          )
        }
      />
      {keys.isError ? (
        <Empty compact title="No index here">
          {keys.error.message.includes("found")
            ? "This output has no head for this partition yet."
            : keys.error.message}
        </Empty>
      ) : effectivePartition === undefined && partitions ? (
        <Empty compact title="No complete partition yet">
          Pick a partition above to read what its index holds.
        </Empty>
      ) : !keys.data ? (
        <Skeleton className="mx-4 mb-4 h-24" />
      ) : entries.length === 0 ? (
        <Empty compact title="No keys">
          The index for this partition is empty.
        </Empty>
      ) : (
        <TableScroll className="max-h-[26rem] overflow-y-auto border-t border-line">
          <Table>
            <thead className="sticky top-0 bg-surface">
              <tr>
                <Th>Key</Th>
                <Th>Generation</Th>
              </tr>
            </thead>
            <tbody>
              {entries.map(([k, entry]) => (
                <Tr key={k}>
                  <Td className="font-mono text-xs">{k}</Td>
                  <Td className="font-mono text-xs text-fg-muted">g{keyEntry(entry).generation}</Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        </TableScroll>
      )}
      {keys.hasNextPage && (
        <div className="flex items-center justify-between border-t border-line px-4 py-2.5 text-xs text-fg-subtle">
          <span>{count(entries.length)} shown</span>
          <Button size="sm" onClick={() => keys.fetchNextPage()} disabled={keys.isFetchingNextPage}>
            Load more
          </Button>
        </div>
      )}
    </Card>
  );
}
