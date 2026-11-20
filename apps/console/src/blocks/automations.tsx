import { Play } from "lucide-react"
import { Link } from "wouter"
import { useAutomationCommand, useAutomations, useCanWrite, useSensors, useTicks } from "../api/queries"
import { duration, until } from "../model/format"
import { triggerText } from "../model/status"
import { IconButton } from "../ui/controls"
import { report } from "../ui/overlays"
import { Ago, AssetLink, Empty, Facts, Failure, Loading, RunLink, SourceLink, StatusPill } from "./atoms"
import { AutomationToggle } from "./assets"

/* What starts runs by itself: automations and sensors. */

export function AutomationsTable({ dense }: { dense?: boolean }) {
  const automations = useAutomations()
  const command = useAutomationCommand()
  const canWrite = useCanWrite()
  const now = Date.now() / 1000
  if (automations.error) return <Failure error={automations.error} />
  if (!automations.data) return <Loading what="Loading automations" />
  const rows = [...automations.data.automations].sort((a, b) => (a.next_at ?? Infinity) - (b.next_at ?? Infinity) || a.name.localeCompare(b.name))
  if (!rows.length) return <Empty title="No automation" />
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            {canWrite && <th className="fit">On</th>}
            <th>Asset</th>
            <th>Trigger</th>
            {!dense && <th>Name</th>}
            {!dense && <th>Last started</th>}
            <th className="end">Next start</th>
            {canWrite && <th className="end fit" />}
          </tr>
        </thead>
        <tbody>
          {rows.map((automation) => (
            <tr key={automation.name} data-off={automation.enabled ? undefined : ""}>
              {canWrite && <td className="fit"><AutomationToggle automation={automation} /></td>}
              <td>
                <span className="row wrap">{automation.targets.map((t) => <AssetLink key={t} name={t} />)}</span>
              </td>
              <td>
                {triggerText(automation.trigger).replace(/ \(.*\)$/, "")}
                {automation.trigger.kind === "cron" && <span className="mono muted"> {automation.trigger.expression}</span>}
                {automation.watched.length > 0 && <span className="muted"> {automation.watched.join(", ")}</span>}
              </td>
              {!dense && <td className="mono faint">{automation.name}</td>}
              {!dense && (
                <td className="muted">
                  {automation.last_fired ? <Ago at={automation.last_fired} now={now} /> : <span className="faint">never</span>}
                  {automation.last_run && <> <RunLink id={automation.last_run} /></>}
                </td>
              )}
              <td className="end num">{!automation.enabled ? <span className="faint">off</span> : automation.next_at ? until(automation.next_at, now) : <span className="faint">{automation.trigger.kind === "ondeploy" ? "on deploy" : "on change"}</span>}</td>
              {canWrite && (
                <td className="end fit">
                  <IconButton label={`Run ${automation.name} now`} size="s" onClick={() => report(command.mutateAsync({ name: automation.name, command: "run-now" }), `${automation.name} started a run`)}>
                    <Play size={13} />
                  </IconButton>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function SensorsTable() {
  const sensors = useSensors()
  const now = Date.now() / 1000
  if (sensors.error) return <Failure error={sensors.error} />
  if (!sensors.data) return <Loading />
  if (!sensors.data.sensors.length) return <Empty title="No sensor" />
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>Sensor</th>
            <th>Checks</th>
            <th>Commits to</th>
            <th className="end">Next check</th>
          </tr>
        </thead>
        <tbody>
          {sensors.data.sensors.map((sensor) => (
            <tr key={sensor.name}>
              <td>
                <Link href={`/sensors/${encodeURIComponent(sensor.name)}`} className="name">
                  {sensor.name}
                </Link>
              </td>
              <td>Every {duration(sensor.every)}</td>
              <td>
                <span className="row wrap">
                  {sensor.commits.map((source) => (
                    <SourceLink key={source} name={source} />
                  ))}
                </span>
              </td>
              <td className="end num">{sensor.ticking ? "checking now" : sensor.due_in !== null ? until(now + sensor.due_in, now) : "–"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const TICK = {
  skipped: { tone: "idle", label: "Found nothing new" },
  committed: { tone: "ok", label: "Committed" },
  succeeded: { tone: "ok", label: "Committed" },
  failed: { tone: "failed", label: "Failed" },
  timed_out: { tone: "failed", label: "Timed out" },
} as const

export function SensorFacts({ name }: { name: string }) {
  const sensors = useSensors()
  const sensor = sensors.data?.sensors.find((s) => s.name === name)
  const now = Date.now() / 1000
  if (!sensors.data) return <Loading />
  if (!sensor) return <Empty title="No sensor has that name" />
  return (
    <div className="stack">
      {sensor.doc && <p className="doc">{sensor.doc.replace(/\s+/g, " ").trim()}</p>}
      <Facts
        rows={[
          ["Checks", `Every ${duration(sensor.every)}, with ${duration(sensor.timeout)} to answer`],
          ["Commits to", <span className="row wrap">{sensor.commits.map((s) => <SourceLink key={s} name={s} />)}</span>],
          ["Next check", sensor.ticking ? "Checking now" : sensor.due_in !== null ? until(now + sensor.due_in, now) : null],
          ["Remembers", sensor.cursor !== null && sensor.cursor !== undefined ? <span className="mono">{JSON.stringify(sensor.cursor)}</span> : "Nothing yet"],
        ]}
      />
    </div>
  )
}

export function TicksTable({ name }: { name: string }) {
  const ticks = useTicks(name)
  const now = Date.now() / 1000
  if (ticks.error) return <Failure error={ticks.error} />
  if (!ticks.data) return <Loading what="Loading checks" />
  if (!ticks.data.ticks.length) return <Empty title="It has not checked yet" />
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>When</th>
            <th>Result</th>
            <th>Runs it started</th>
            <th className="end">Took</th>
          </tr>
        </thead>
        <tbody>
          {ticks.data.ticks.map((tick) => (
            <tr key={tick.tick}>
              <td className="muted"><Ago at={tick.started_at} now={now} /></td>
              <td>
                <StatusPill status={TICK[tick.outcome as keyof typeof TICK] ?? { tone: "idle", label: tick.outcome }} quiet={tick.outcome === "skipped"} />
                {tick.error && <div className="error-text">{tick.error}</div>}
              </td>
              <td>{tick.runs.length ? <span className="row wrap">{tick.runs.map((run) => <RunLink key={run} id={run} />)}</span> : <span className="faint">–</span>}</td>
              <td className="end num muted">{tick.ended_at ? duration(tick.ended_at - tick.started_at) : "–"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
