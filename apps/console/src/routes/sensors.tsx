import { useQuery, useSuspenseQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { q, useProject } from "@/api/queries";
import type { Json, SensorView, Tick } from "@/api/types";
import { cn } from "@/lib/cn";
import { clock, duration, interval, plural, shortId } from "@/lib/format";
import { label, tone, toneSolid } from "@/lib/status";
import { Empty, JsonView, Skeleton, Time } from "@/ui/data";
import { Card, CardHeader, Crumb, Fact, Facts, Page, PageHeader } from "@/ui/layout";
import { Tooltip } from "@/ui/overlay";
import { StatusBadge, StatusDot } from "@/ui/status";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

/**
 * Sensors (docs/lifecycle.md §11): checks on a clock, run on a long-lived
 * host, that may commit to their sources and request runs. A tick is not a
 * run: ticks that found nothing are rows here, kept a day.
 */

const OUTCOMES = ["committed", "requested", "advanced", "skipped", "refused", "failed"];

export function Sensors() {
  const project = useProject();
  const { data } = useSuspenseQuery(q.sensors(project));
  return (
    <Page>
      <PageHeader
        title="Sensors"
        description="Checks on a clock that commit to sources and request runs. A tick that finds nothing records nothing durable."
        meta={
          <>
            <span>{plural(data.sensors.length, "sensor")}</span>
            <span>{plural(data.hosts.length, "host")} polling</span>
          </>
        }
      />
      {data.sensors.length === 0 ? (
        <Card>
          <Empty title="No sensors">Declare one with `@sensor`, or give a Source an `observe()`.</Empty>
        </Card>
      ) : (
        <div className="grid gap-4 lg:grid-cols-2">
          {data.sensors.map((s) => (
            <SensorCard key={s.name} sensor={s} />
          ))}
        </div>
      )}
      {data.hosts.length > 0 && <Hosts hosts={data.hosts} />}
    </Page>
  );
}

function SensorCard({ sensor }: { sensor: SensorView }) {
  const project = useProject();
  const ticks = useQuery(q.ticks(project, sensor.name)).data;
  return (
    <Card>
      <CardHeader
        ident
        title={
          <Link to="/sensors/$sensor" params={{ sensor: sensor.name }} className="hover:underline">
            {sensor.name}
          </Link>
        }
        description={`every ${interval(sensor.every)} on ${sensor.placement.executor} · commits to ${sensor.commits.join(", ") || "nothing"}`}
        actions={<Due sensor={sensor} />}
      />
      <div className="flex flex-col gap-3 px-4 pb-4">
        {ticks ? <TickStrip ticks={ticks.slice(0, 60)} /> : <Skeleton className="h-6" />}
        {ticks && <OutcomeCounts ticks={ticks} />}
      </div>
    </Card>
  );
}

function Due({ sensor }: { sensor: SensorView }) {
  if (sensor.ticking) {
    return (
      <span className="flex items-center gap-1.5 text-xs text-run-fg">
        <StatusDot tone="run" pulse /> ticking on {sensor.ticking.host}
      </span>
    );
  }
  return (
    <span className="text-xs text-fg-muted tabular">
      {sensor.due_in > 1 ? `next tick in ${Math.round(sensor.due_in)}s` : "due now"}
    </span>
  );
}

/** The latest ticks, oldest left: one mark each, its outcome by tone and name. */
function TickStrip({ ticks }: { ticks: Tick[] }) {
  if (ticks.length === 0) return <p className="text-xs text-fg-subtle">No ticks recorded in the last day.</p>;
  return (
    <div className="flex h-6 items-end gap-[2px]" role="list" aria-label="Recent ticks">
      {[...ticks].reverse().map((t) => {
        const tn = tone(t.outcome);
        const loud = t.outcome !== "skipped";
        return (
          <Tooltip
            key={t.tick}
            content={
              <span className="flex flex-col">
                <span>
                  {clock(t.started_at)} · {label(t.outcome)}
                </span>
                {t.error && <span className="opacity-80">{t.error}</span>}
              </span>
            }
          >
            <span
              role="listitem"
              aria-label={`${clock(t.started_at)} ${t.outcome}`}
              className={cn("w-1.5 rounded-mark", toneSolid[tn], loud ? "h-6" : "h-2.5 opacity-60")}
            />
          </Tooltip>
        );
      })}
    </div>
  );
}

function OutcomeCounts({ ticks }: { ticks: Tick[] }) {
  const counts = OUTCOMES.map((o) => [o, ticks.filter((t) => t.outcome === o).length] as const).filter(
    ([, n]) => n,
  );
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-fg-muted">
      {counts.map(([o, n]) => (
        <span key={o} className="inline-flex items-center gap-1.5">
          <StatusDot tone={tone(o)} />
          {n} {label(o)}
        </span>
      ))}
    </div>
  );
}

function Hosts({ hosts }: { hosts: { id: string; executor: string; deploy: string; seen_at: number }[] }) {
  return (
    <Card>
      <CardHeader
        title="Sensor hosts"
        description="Long-lived processes that poll for due ticks and run them"
      />
      <TableScroll className="border-t border-line">
        <Table>
          <thead>
            <tr>
              <Th>Host</Th>
              <Th>Executor</Th>
              <Th>Deploy</Th>
              <Th>Last seen</Th>
            </tr>
          </thead>
          <tbody>
            {hosts.map((h) => (
              <Tr key={h.id}>
                <Td className="font-mono text-xs">{h.id}</Td>
                <Td>{h.executor}</Td>
                <Td className="font-mono text-xs text-fg-muted">{h.deploy.slice(0, 12)}</Td>
                <Td className="text-fg-muted">
                  <Time at={h.seen_at} />
                </Td>
              </Tr>
            ))}
          </tbody>
        </Table>
      </TableScroll>
    </Card>
  );
}

// -- one sensor -------------------------------------------------------------------

const route = getRouteApi("/sensors/$sensor");

export function Sensor() {
  const { sensor: name } = route.useParams();
  const { all } = route.useSearch();
  const project = useProject();
  const { data } = useSuspenseQuery(q.sensors(project));
  const ticks = useQuery(q.ticks(project, name)).data;
  const sensor = data.sensors.find((s) => s.name === name);
  if (!sensor)
    return (
      <Page>
        <Empty title={`No sensor named ${name}`} />
      </Page>
    );
  return (
    <Page>
      <PageHeader
        ident
        eyebrow={
          <>
            <Crumb>
              <Link to="/sensors" className="hover:text-fg">
                Sensors
              </Link>
            </Crumb>
            <Crumb last>{name}</Crumb>
          </>
        }
        title={name}
        description={sensor.doc?.split("\n\n")[0]}
        actions={<Due sensor={sensor} />}
      />
      <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]">
        <Card>
          <CardHeader
            title="Ticks"
            description="The last day; ticks that changed something stay with the runs they caused"
            actions={
              <Link
                from="/sensors/$sensor"
                to="."
                search={{ all: all ? undefined : true }}
                replace
                className="text-xs text-link hover:underline"
              >
                {all ? "Hide skipped ticks" : "Show skipped ticks"}
              </Link>
            }
          />
          <div className="px-4 pb-3">
            {ticks ? <TickStrip ticks={ticks.slice(0, 120)} /> : <Skeleton className="h-6" />}
          </div>
          {ticks && !all && ticks.every((t) => t.outcome === "skipped") && (
            <Empty compact title="Every tick was skipped">
              Nothing new was found in the last day.
            </Empty>
          )}
          {ticks && ticks.some((t) => all || t.outcome !== "skipped") && (
            <TableScroll className="max-h-[32rem] overflow-y-auto border-t border-line">
              <Table>
                <thead className="sticky top-0 bg-surface">
                  <tr>
                    <Th>Started</Th>
                    <Th>Outcome</Th>
                    <Th className="text-right">Took</Th>
                    <Th>Host</Th>
                    <Th>Runs</Th>
                    <Th>Error</Th>
                  </tr>
                </thead>
                <tbody>
                  {ticks
                    .filter((t) => all || t.outcome !== "skipped")
                    .map((t) => (
                      <Tr key={t.tick}>
                        <Td className="text-fg-muted">
                          <Time at={t.started_at} />
                        </Td>
                        <Td>
                          <StatusBadge status={t.outcome} />
                        </Td>
                        <Td className="text-right text-fg-muted">
                          {t.ended_at ? duration(t.ended_at - t.started_at) : "—"}
                        </Td>
                        <Td className="font-mono text-xs text-fg-muted">{t.host ?? "—"}</Td>
                        <Td className="text-xs">
                          {(t.runs ?? []).map((r) => (
                            <Link
                              key={r}
                              to="/runs/$run"
                              params={{ run: r }}
                              className="mr-2 font-mono text-link hover:underline"
                            >
                              {shortId(r)}
                            </Link>
                          ))}
                        </Td>
                        <Td className="max-w-72 truncate text-xs text-fail-fg">{t.error}</Td>
                      </Tr>
                    ))}
                </tbody>
              </Table>
            </TableScroll>
          )}
        </Card>
        <div className="flex flex-col gap-4">
          <Card>
            <CardHeader title="Definition" />
            <Facts className="grid-cols-2 px-4 pb-4">
              <Fact label="Every">{interval(sensor.every)}</Fact>
              <Fact label="Timeout">{duration(sensor.timeout)}</Fact>
              <Fact label="Runs on">{sensor.placement.executor}</Fact>
              <Fact label="Commits to">
                {sensor.commits.map((c) => (
                  <Link
                    key={c}
                    to="/sources/$source"
                    params={{ source: c }}
                    className="mr-2 text-link hover:underline"
                  >
                    {c}
                  </Link>
                ))}
              </Fact>
            </Facts>
          </Card>
          <Card>
            <CardHeader title="Cursor" description="Durable: where the next tick starts" />
            <div className="px-4 pb-4">
              <JsonView value={sensor.cursor as Json} />
            </div>
          </Card>
          {sensor.accepted && (
            <Card>
              <CardHeader
                title="Last accepted outcome"
                description={`tick ${shortId(sensor.accepted.tick)}`}
              />
              <ul className="flex flex-col gap-1 px-4 pb-4 text-xs text-fg-muted">
                {Object.entries(sensor.accepted.commits).map(([source, head]) => (
                  <li key={source}>
                    committed to <span className="text-fg">{source}</span>{" "}
                    <span className="font-mono">{head}</span>
                  </li>
                ))}
                {sensor.accepted.runs.length > 0 && (
                  <li>requested {plural(sensor.accepted.runs.length, "run")}</li>
                )}
              </ul>
            </Card>
          )}
        </div>
      </div>
    </Page>
  );
}
