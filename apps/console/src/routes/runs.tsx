import { keepPreviousData, useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { getRouteApi } from "@tanstack/react-router";
import { FilterX, Play } from "lucide-react";
import { q, useProject, type RunFilter } from "@/api/queries";
import type { Facet } from "@/api/types";
import { RunHistogram, RunsTable } from "@/features/runs";
import { MaterializeButton } from "@/features/materialize";
import { count, plural } from "@/lib/format";
import { label } from "@/lib/status";
import { join, list, RANGES, type RunSearch } from "@/router";
import { Button } from "@/ui/button";
import { Empty, ErrorNote, Skeleton } from "@/ui/data";
import { Chip, SearchInput, Segmented, Select } from "@/ui/form";
import { Card, Page, PageHeader } from "@/ui/layout";
import { StatusIcon } from "@/ui/status";

const route = getRouteApi("/runs");

/** The URL's run search as an API filter. Lists stay lists; a range stays a range. */
export function filterOf(search: RunSearch): RunFilter {
  return {
    status: list(search.status),
    trigger: list(search.trigger),
    automation: list(search.automation),
    asset: list(search.asset),
    tag: list(search.tag),
    q: search.q,
    range: search.since ? undefined : search.range,
    since: search.since,
    until: search.until,
  };
}

const STATUSES = ["running", "queued", "paused", "failed", "succeeded", "canceled", "skipped"];

export function Runs() {
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const project = useProject();
  const filter = filterOf(search);
  const set = (patch: Partial<RunSearch>) => navigate({ search: (s) => ({ ...s, ...patch }), replace: true });

  const runs = useInfiniteQuery({
    ...q.runs(project, filter),
    placeholderData: keepPreviousData,
  });
  const facets = useQuery({
    ...q.runFacets(project, filter),
    placeholderData: keepPreviousData,
  }).data;
  const histogram = useQuery({
    ...q.runHistogram(project, filter, 72),
    placeholderData: keepPreviousData,
  }).data;

  const rows = runs.data?.pages.flatMap((page) => page.runs) ?? [];
  const total = runs.data?.pages[0]?.total;
  const filtered = Object.entries(search).some(([k, v]) => k !== "range" && v !== undefined);
  const statuses = list(search.status);
  const facetCount = (facet: Facet[] | undefined, value: string) =>
    facet?.find((f) => f.value === value)?.count ?? 0;

  return (
    <Page>
      <PageHeader
        title="Runs"
        description="Every run, live and finished. Filters live in the address bar, so any view here is a link."
        actions={<MaterializeButton icon={<Play />} />}
      />

      <div className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <SearchInput
            aria-label="Search runs"
            placeholder="Search targets, errors, ids…"
            className="w-full sm:w-72"
            value={search.q ?? ""}
            onChange={(e) => set({ q: e.target.value || undefined })}
          />
          <Segmented
            label="Time range"
            value={search.since ? "custom" : (search.range ?? "all")}
            onChange={(value) =>
              set({
                range: RANGES.find((r) => r === value),
                since: undefined,
                until: undefined,
              })
            }
            options={[
              ...RANGES.map((r) => ({ value: r as string, label: r })),
              { value: "all", label: "All" },
              ...(search.since ? [{ value: "custom", label: "Custom" }] : []),
            ]}
          />
          <FacetSelect
            label="Trigger"
            field="trigger"
            facet={facets?.trigger}
            value={search.trigger}
            onChange={(v) => set({ trigger: v })}
          />
          <FacetSelect
            label="Automation"
            field="automation"
            facet={facets?.automation}
            value={search.automation}
            onChange={(v) => set({ automation: v })}
          />
          <FacetSelect
            label="Asset"
            field="asset"
            facet={facets?.asset}
            value={search.asset}
            onChange={(v) => set({ asset: v })}
          />
          {facets && facets.tag.length > 0 && (
            <FacetSelect
              label="Tag"
              field="tag"
              facet={facets.tag}
              value={search.tag}
              onChange={(v) => set({ tag: v })}
            />
          )}
          {filtered && (
            <Button
              variant="ghost"
              icon={<FilterX />}
              onClick={() => navigate({ search: { range: search.range }, replace: true })}
            >
              Clear
            </Button>
          )}
        </div>
        <div role="group" aria-label="Status" className="flex flex-wrap items-center gap-1.5">
          {STATUSES.map((status) => {
            const active = statuses.includes(status);
            const n = facetCount(facets?.status, status);
            if (!active && n === 0) return null;
            return (
              <Chip
                key={status}
                active={active}
                count={n}
                onClick={() =>
                  set({
                    status: join(active ? statuses.filter((s) => s !== status) : [...statuses, status]),
                  })
                }
              >
                <StatusIcon status={status} className={active ? "text-current" : undefined} />
                {label(status)}
              </Chip>
            );
          })}
        </div>
      </div>

      <Card>
        <div className="border-b border-line px-4 pt-4 pb-3">
          {histogram ? (
            <RunHistogram
              data={histogram}
              onSelect={(since, until) => set({ since, until, range: undefined })}
            />
          ) : (
            <Skeleton className="h-[96px]" />
          )}
        </div>
        {runs.isError ? (
          <div className="p-4">
            <ErrorNote error={runs.error} />
          </div>
        ) : !runs.data ? (
          <div className="flex flex-col gap-2 p-4">
            {Array.from({ length: 8 }, (_, i) => (
              <Skeleton key={i} className="h-7" />
            ))}
          </div>
        ) : (
          <>
            <RunsTable
              runs={rows}
              empty={
                <Empty title={filtered ? "No runs match" : "No runs yet"}>
                  {filtered
                    ? "Nothing in the history matches these filters. Skipped runs only show when you ask for them."
                    : "Materialize an asset, or wait for an automation to fire."}
                </Empty>
              }
            />
            <div className="flex items-center justify-between border-t border-line px-4 py-2.5 text-xs text-fg-subtle">
              <span className="tabular">
                {total !== undefined && `${count(rows.length)} of ${plural(total, "run")}`}
              </span>
              {runs.hasNextPage && (
                <Button size="sm" onClick={() => runs.fetchNextPage()} disabled={runs.isFetchingNextPage}>
                  {runs.isFetchingNextPage ? "Loading…" : "Load more"}
                </Button>
              )}
            </div>
          </>
        )}
      </Card>
    </Page>
  );
}

function FacetSelect({
  label: name,
  field,
  facet,
  value,
  onChange,
}: {
  label: string;
  field: string;
  facet: Facet[] | undefined;
  value: string | undefined;
  onChange: (value: string | undefined) => void;
}) {
  const options = facet ?? [];
  if (options.length === 0 && !value) return null;
  return (
    <Select
      aria-label={name}
      name={field}
      className="w-auto max-w-52"
      value={value ?? ""}
      onChange={(e) => onChange(e.target.value || undefined)}
    >
      <option value="">{name}: any</option>
      {value && !options.some((o) => o.value === value) && <option value={value}>{value}</option>}
      {options.map((o) => (
        <option key={o.value} value={o.value}>
          {o.value} ({o.count})
        </option>
      ))}
    </Select>
  );
}
