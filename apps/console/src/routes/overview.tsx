import { cloneElement, type ReactElement, type ReactNode } from "react";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { Link, useNavigate } from "@tanstack/react-router";
import { Activity, ArrowRight, Hand, KeyRound, Play, XCircle } from "lucide-react";
import { q, useManifest, useProject } from "@/api/queries";
import type { AssetStatus, Automation } from "@/api/types";
import { MaterializeButton } from "@/features/materialize";
import { RunHistogram, RunsTable } from "@/features/runs";
import { describeTrigger } from "@/features/triggers";
import { useNow } from "@/lib/clock";
import { cn } from "@/lib/cn";
import { compact, plural, until } from "@/lib/format";
import { toneText, type Tone } from "@/lib/status";
import { Empty, SegmentBar, Skeleton, Time } from "@/ui/data";
import { Card, CardHeader, Page, PageHeader } from "@/ui/layout";
import { StatusIcon } from "@/ui/status";

const FAILING = ["failed", "rejected", "retrying", "timed_out"] as const;

export function failingKeys(status: AssetStatus | undefined): number {
  return FAILING.reduce((sum, k) => sum + (status?.failures?.[k] ?? 0), 0);
}

export function Overview() {
  const project = useProject();
  const manifest = useManifest();
  const diagnostics = useQuery(q.diagnostics()).data;
  const active = useInfiniteQuery(q.runs(project, { status: ["running", "queued"] }, 8)).data;
  const failed = useInfiniteQuery(q.runs(project, { status: ["failed"], range: "24h" }, 5)).data;
  const day = useInfiniteQuery(q.runs(project, { range: "24h" }, 1)).data;
  const histogram = useQuery(q.runHistogram(project, { range: "24h" }, 48)).data;
  const status = useQuery(q.assetStatus(project)).data;
  const holds = useQuery(q.holds(project)).data;

  const failing = status ? Object.entries(status).filter(([, s]) => failingKeys(s) > 0) : [];
  const keys = failing.reduce((sum, [, s]) => sum + failingKeys(s), 0);
  const operator = holds ? holds.holds.length + holds.unsettled.length + holds.discards.length : undefined;
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
        actions={<MaterializeButton icon={<Play />} />}
      />

      <div className="grid grid-cols-2 gap-3 sm:gap-4 xl:grid-cols-4">
        <Vital
          link={(c) => <Link to="/runs" search={{ status: "running,queued" }} className={c} />}
          icon={<Activity />}
          label="Running now"
          value={diagnostics?.active_runs}
          tone={diagnostics?.active_runs ? "run" : "idle"}
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
          label="Failing keys"
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
            holds
              ? operator
                ? "held scopes, unsettled writes, stuck discards"
                : "nothing held or stuck"
              : undefined
          }
        />
      </div>

      <Card>
        <CardHeader
          title="Activity"
          description="Runs started in the last 24 hours, by outcome. Select a column to see its runs."
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
    "group flex min-w-0 flex-col gap-3 rounded-lg border-theme border-line-strong bg-surface p-4 shadow-2 motion-2 transition-transform hover:-translate-y-0.5",
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
            "text-3xl leading-none font-semibold tabular",
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
          score: s.partitions.failed * 100 + failingKeys(s) * 10 + s.held * 50 + s.partitions.missing,
        }))
        .filter((r) => r.score > 0)
        .sort((a, b) => b.score - a.score)
    : undefined;
  const total = status ? Object.keys(status).length : 0;
  return (
    <Card>
      <CardHeader
        title="Assets needing attention"
        description="Failed or missing partitions, failing keys, held scopes"
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
          No failed or missing partitions, failing keys or held scopes.
        </Empty>
      ) : (
        <ul className="flex flex-col pb-2">
          {rows.slice(0, 8).map(({ name, s, keys }) => (
            <li key={name}>
              <Link
                to={keys ? "/assets/$asset/keys" : "/assets/$asset/partitions"}
                params={{ asset: name }}
                className="grid grid-cols-[minmax(0,1fr)_6rem_auto] items-center gap-4 px-4 py-2 hover:bg-surface-2"
              >
                <span className="flex min-w-0 flex-col gap-0.5">
                  <span className="truncate text-sm font-medium text-fg">{name}</span>
                  <span className="flex flex-wrap gap-x-3 text-xs text-fg-muted">
                    {s.partitions.failed > 0 && (
                      <Flag tone="fail">{plural(s.partitions.failed, "failed partition")}</Flag>
                    )}
                    {s.partitions.missing > 0 && <Flag tone="idle">{s.partitions.missing} missing</Flag>}
                    {keys > 0 && <Flag tone="warn">{plural(keys, "failing key")}</Flag>}
                    {s.held > 0 && <Flag tone="warn">{s.held} held</Flag>}
                  </span>
                </span>
                <SegmentBar
                  parts={[
                    {
                      tone: "ok",
                      value: s.partitions.complete,
                      label: "complete",
                    },
                    {
                      tone: "run",
                      value: s.partitions.running,
                      label: "running",
                    },
                    {
                      tone: "fail",
                      value: s.partitions.failed,
                      label: "failed",
                    },
                    {
                      tone: "idle",
                      value: s.partitions.missing,
                      label: "missing",
                    },
                  ]}
                />
                <span className="text-xs text-fg-subtle">
                  <Time at={s.updated_at} />
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
