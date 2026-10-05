import { cloneElement, type ReactElement, type ReactNode } from "react";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { getRouteApi, Link, useNavigate } from "@tanstack/react-router";
import { Activity, ArrowRight, Hand, KeyRound, Play, XCircle } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import { cleanupStuck } from "@/api/read";
import type { AssetStatus, Automation } from "@/api/types";
import { partitionParts } from "@/features/graph";
import { RunButton } from "@/features/run-dialog";
import { RunHistogram, RunsTable } from "@/features/runs";
import { describeTrigger } from "@/features/triggers";
import { StarvedPools, useStarvedPools } from "@/features/starved";
import { useNow } from "@/lib/clock";
import { cn } from "@/lib/cn";
import { compact, plural, until } from "@/lib/format";
import { toneText, type Tone } from "@/lib/status";
import { Empty, SegmentBar, Skeleton, Time } from "@/ui/data";
import { Card, CardHeader, Page, PageHeader } from "@/ui/layout";
import { Segmented } from "@/ui/form";
import { StatusIcon } from "@/ui/status";

const FAILING = ["failed", "rejected", "retrying", "timed_out"] as const;

export function failingKeys(status: AssetStatus | undefined): number {
  return FAILING.reduce((sum, k) => sum + (status?.failures?.[k] ?? 0), 0);
}

const route = getRouteApi("/");
const BARS = { "6h": 72, "24h": 48, "7d": 56 } as const;
const WORDS = { "6h": "the last 6 hours", "24h": "the last day", "7d": "the last week" } as const;

export function Overview() {
  const { activity } = route.useSearch();
  const project = useProject();
  const manifest = useManifest();
  const diagnostics = useQuery(q.diagnostics()).data;
  const active = useInfiniteQuery(q.runs(project, { status: ["running", "queued"] }, 8)).data;
  // Counted from the listing, as the list below it is: the engine's cleanup tasks aren't the operator's runs.
  const running = active?.pages[0]?.total;
  const failed = useInfiniteQuery(q.runs(project, { status: ["failed"], range: "24h" }, 5)).data;
  const day = useInfiniteQuery(q.runs(project, { range: "24h" }, 1)).data;
  // By default the window fits the project: everything when it is younger than a
  // day (a fresh project isn't one bar), the last day once it is older.
  const whole = useQuery({ ...q.runHistogram(project, {}, 48), enabled: !activity }).data;
  const young = !!whole && (whole.since == null || whole.until - whole.since <= 86400);
  const range = activity ?? (young ? undefined : "24h");
  const windowed = useQuery({
    ...q.runHistogram(project, { range }, BARS[range ?? "24h"]),
    enabled: !!range,
  }).data;
  const histogram = range ? windowed : whole;
  const period = range
    ? WORDS[range]
    : whole?.since != null
      ? "the project's life so far"
      : "the project's life";
  const status = useQuery(q.assetStatus(project)).data;
  const repairs = useQuery(q.repairs(project)).data;
  const cleanups = useQuery(q.cleanups(project)).data;

  const failing = status ? Object.entries(status).filter(([, s]) => failingKeys(s) > 0) : [];
  const keys = failing.reduce((sum, [, s]) => sum + failingKeys(s), 0);
  const starved = useStarvedPools();
  const stuck = cleanups?.filter(cleanupStuck).length ?? 0;
  const operator = repairs && cleanups ? repairs.length + stuck + starved.length : undefined;
  const failedTotal = failed?.pages[0]?.total;
  const navigate = useNavigate();

  return (
    <Page>
      <PageHeader
        title="Overview"
        description={
          <>
            Project <span className="font-medium text-fg">{manifest.name}</span>
            {manifest.build?.commit && (
              <>
                {" "}
                at <span className="font-mono">{manifest.build.commit.slice(0, 7)}</span>
                {manifest.build.dirty && " (dirty)"}
              </>
            )}
            {" · "}
            {plural(Object.keys(manifest.assets).length, "asset")},{" "}
            {plural(Object.keys(manifest.automations).length, "automation")}
          </>
        }
        actions={<RunButton icon={<Play />} />}
      />

      <StarvedPools />

      <div className="grid grid-cols-2 gap-3 sm:gap-4 xl:grid-cols-4">
        <Vital
          link={(c) => <Link to="/runs" search={{ status: "running,queued" }} className={c} />}
          icon={<Activity />}
          label="Running now"
          value={running}
          tone={running ? "run" : "idle"}
          detail={diagnostics ? `${plural(diagnostics.inflight, "attempt")} in flight` : undefined}
        />
        <Vital
          link={(c) => <Link to="/runs" search={{ status: "failed", range: "24h" }} className={c} />}
          icon={<XCircle />}
          label="Failed runs · 24h"
          value={failedTotal}
          tone={failedTotal ? "fail" : "ok"}
          detail={day ? `of ${plural(day.pages[0]?.total ?? 0, "run")}` : undefined}
        />
        <Vital
          link={(c) => <Link to="/assets" className={c} />}
          icon={<KeyRound />}
          label="Failed keys"
          value={status ? keys : undefined}
          tone={keys ? "warn" : "ok"}
          detail={
            status
              ? failing.length
                ? `in ${plural(failing.length, "asset")}`
                : "every key processed"
              : undefined
          }
        />
        <Vital
          link={(c) => <Link to="/health" className={c} />}
          icon={<Hand />}
          label="Needs an operator"
          value={operator}
          tone={operator ? "warn" : "ok"}
          detail={
            repairs && cleanups
              ? operator
                ? [
                    repairs.length && `${repairs.length} owing a repair`,
                    stuck && plural(stuck, "stuck cleanup"),
                    starved.length && plural(starved.length, "idle pool"),
                  ]
                    .filter(Boolean)
                    .join(", ")
                : "nothing to repair, stuck or starved"
              : undefined
          }
        />
      </div>

      <Card>
        <CardHeader
          title="Activity"
          description={`Runs started in ${period}, by outcome. Select a column to see its runs.`}
          actions={
            <Segmented
              size="sm"
              label="Activity range"
              value={activity ?? "fit"}
              onChange={(value) =>
                navigate({
                  to: "/",
                  search: { activity: value === "fit" ? undefined : value },
                  replace: true,
                })
              }
              options={[
                {
                  value: "fit",
                  label: "Fit",
                  title: "Everything while the project is young, then the last day",
                },
                { value: "6h", label: "6h" },
                { value: "24h", label: "24h" },
                { value: "7d", label: "7d" },
              ]}
            />
          }
        />
        <div className="px-4 pb-4">
          {histogram ? (
            <RunHistogram
              data={histogram}
              height={88}
              onSelect={(since, until) => navigate({ to: "/runs", search: { since, until } })}
            />
          ) : (
            <Skeleton className="h-28" />
          )}
        </div>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader
            title="Running now"
            actions={
              <Link to="/runs" search={{ status: "running,queued" }} className={moreClass}>
                <More />
              </Link>
            }
          />
          {!active ? (
            <ListSkeleton />
          ) : (
            <RunsTable
              compact
              runs={active.pages[0]?.runs ?? []}
              empty={
                <Empty compact title="Nothing running">
                  Automations will start runs as their triggers fire.
                </Empty>
              }
            />
          )}
        </Card>
        <Card>
          <CardHeader
            title="Recent failures"
            description="Last 24 hours"
            actions={
              <Link to="/runs" search={{ status: "failed", range: "24h" }} className={moreClass}>
                <More />
              </Link>
            }
          />
          {!failed ? (
            <ListSkeleton />
          ) : (
            <RunsTable
              compact
              runs={failed.pages[0]?.runs ?? []}
              empty={
                <Empty compact title="No failures">
                  Every run in the last day finished well.
                </Empty>
              }
            />
          )}
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
        <AttentionAssets status={status} />
        <UpNext />
      </div>
    </Page>
  );
}

function Vital({
  link,
  icon,
  label,
  value,
  tone,
  detail,
}: {
  link: (className: string) => ReactElement<{ children?: ReactNode }>;
  icon: ReactNode;
  label: string;
  value: number | undefined;
  tone: Tone;
  detail?: string;
}) {
  const anchor = link(
    "group corner-ticks flex min-w-0 flex-col gap-3 rounded-lg border-theme border-line-strong bg-surface p-4 shadow-2 motion-2 transition-transform hover:-translate-y-0.5",
  );
  return cloneElement(
    anchor,
    {},
    <>
      <span className="flex items-center justify-between text-xs font-medium text-fg-muted">
        <span className="flex items-center gap-1.5 [&_svg]:size-3.5">
          <span className={toneText[tone]}>{icon}</span>
          {label}
        </span>
        <ArrowRight aria-hidden className="size-3.5 opacity-0 transition-opacity group-hover:opacity-100" />
      </span>
      {value === undefined ? (
        <Skeleton className="h-9 w-16" />
      ) : (
        <span
          className={cn(
            "figure text-3xl leading-none",
            value > 0 && tone !== "idle" && tone !== "ok" ? toneText[tone] : "text-fg",
          )}
        >
          {compact(value)}
        </span>
      )}
      <span className="truncate text-xs text-fg-subtle">{detail ?? " "}</span>
    </>,
  );
}

const moreClass = "inline-flex items-center gap-1 text-xs text-fg-muted hover:text-fg";
const More = () => (
  <>
    All <ArrowRight aria-hidden className="size-3" />
  </>
);

function ListSkeleton() {
  return (
    <div className="flex flex-col gap-2 px-4 pb-4">
      {[0, 1, 2].map((i) => (
        <Skeleton key={i} className="h-7" />
      ))}
    </div>
  );
}

/** Assets with something wrong or waiting, worst first. */
function AttentionAssets({ status }: { status: Record<string, AssetStatus> | undefined }) {
  const rows = status
    ? Object.entries(status)
        .map(([name, s]) => ({
          name,
          s,
          keys: failingKeys(s),
          score:
            (s.repairs_stuck ?? 0) * 1000 +
            s.partitions.failed * 100 +
            s.partitions.stale * 20 +
            failingKeys(s) * 10 +
            s.partitions.missing,
        }))
        .filter((r) => r.score > 0)
        .sort((a, b) => b.score - a.score)
    : undefined;
  const total = status ? Object.keys(status).length : 0;
  return (
    <Card>
      <CardHeader
        title="Assets needing attention"
        description="Failed, stale or missing partitions, failed keys, stuck repairs"
        actions={
          <Link to="/assets" className={moreClass}>
            <More />
          </Link>
        }
      />
      {!rows ? (
        <ListSkeleton />
      ) : rows.length === 0 ? (
        <Empty compact title={`All ${plural(total, "asset")} current`}>
          No failed, stale or missing partitions, and no failed keys.
        </Empty>
      ) : (
        <ul className="flex flex-col pb-2">
          {rows.slice(0, 8).map(({ name, s, keys }) => (
            <li key={name}>
              <Link
                to={
                  keys
                    ? "/assets/$asset/keys"
                    : s.partitioned
                      ? "/assets/$asset/partitions"
                      : "/assets/$asset"
                }
                params={{ asset: name }}
                className="grid grid-cols-[minmax(0,1fr)_9rem_6.5rem] items-center gap-4 px-4 py-2 hover:bg-surface-2"
              >
                <span className="flex min-w-0 flex-col gap-0.5">
                  <span className="truncate text-sm font-medium text-fg">{name}</span>
                  <span className="flex flex-wrap gap-x-3 text-xs text-fg-muted">
                    {s.partitions.failed > 0 && (
                      <Flag tone="fail">{plural(s.partitions.failed, "failed partition")}</Flag>
                    )}
                    {(s.repairs_stuck ?? 0) > 0 && (
                      <Flag tone="fail">{plural(s.repairs_stuck ?? 0, "stuck repair")}</Flag>
                    )}
                    {s.partitions.stale > 0 && (
                      <Flag tone="warn">{s.partitioned ? `${s.partitions.stale} stale` : "stale"}</Flag>
                    )}
                    {s.partitions.missing > 0 && <Flag tone="idle">{s.partitions.missing} missing</Flag>}
                    {keys > 0 && <Flag tone="warn">{plural(keys, "failed key")}</Flag>}
                  </span>
                </span>
                {s.partitioned ? (
                  <span className="flex flex-col gap-1">
                    <SegmentBar parts={partitionParts(s.partitions)} />
                    <span className="text-2xs text-fg-subtle tabular">
                      {s.partitions.materialized}/{s.partitions.total} partitions materialized
                    </span>
                  </span>
                ) : (
                  <span />
                )}
                <span className="text-xs text-fg-subtle">
                  updated <Time at={s.updated_at} />
                </span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}

function Flag({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-1">
      <StatusIcon tone={tone} className="size-3" />
      {children}
    </span>
  );
}

/** Scheduled automations by their next fire; change-driven ones counted. */
function UpNext() {
  const project = useProject();
  const now = useNow();
  const automations = useQuery(q.automations(project)).data;
  const scheduled = (automations ?? [])
    .filter((a): a is Automation & { next_at: number } => a.enabled && a.next_at != null)
    .sort((a, b) => a.next_at - b.next_at);
  const onchange = (automations ?? []).filter((a) => a.enabled && a.trigger.kind === "onchange");
  const pending = onchange.filter((a) => a.pending.length > 0).length;
  return (
    <Card>
      <CardHeader
        title="Up next"
        description="Scheduled automations by their next fire"
        actions={
          <Link to="/automations" className={moreClass}>
            <More />
          </Link>
        }
      />
      {!automations ? (
        <ListSkeleton />
      ) : (
        <ul className="flex flex-col pb-2">
          {scheduled.slice(0, 6).map((a) => (
            <li key={a.name} className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-4 px-4 py-2">
              <span className="flex min-w-0 flex-col gap-0.5">
                <span className="truncate font-mono text-xs text-fg">{a.name}</span>
                <span className="truncate text-xs text-fg-subtle">
                  {describeTrigger(a.trigger)} → {a.targets.join(", ")}
                </span>
              </span>
              <span className="text-sm text-fg-muted tabular">{until(a.next_at, now)}</span>
            </li>
          ))}
          {scheduled.length === 0 && <Empty compact title="Nothing scheduled" />}
          {onchange.length > 0 && (
            <li className="mx-4 mt-1 border-t border-line pt-2.5 text-xs text-fg-muted">
              {plural(onchange.length, "automation")} fire on change
              {pending > 0 ? `; ${pending} with changes pending` : ", none pending"}.
            </li>
          )}
        </ul>
      )}
    </Card>
  );
}
