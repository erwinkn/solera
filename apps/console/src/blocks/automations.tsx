import { Play } from "lucide-react"
import { Link } from "wouter"
import { useAutomationCommand, useAutomations, useSensors, useTicks } from "../api/queries"
import { duration, until } from "../model/format"
import { triggerText } from "../model/status"
import { Button } from "../ui/controls"
import { report } from "../ui/overlays"
import { Ago, AssetLink, Empty, Facts, Failure, Loading, RunLink, StatusPill } from "./atoms"
import { AutomationToggle } from "./assets"

/* What starts runs by itself: automations and sensors. */

export function AutomationsTable({ dense }: { dense?: boolean }) {
  const automations = useAutomations()
  const command = useAutomationCommand()
  const now = Date.now() / 1000
  if (automations.error) return <Failure error={automations.error} />
  if (!automations.data) return <Loading what="Loading automations" />
  const rows = [...automations.data.automations].sort((a, b) => (a.next_at ?? Infinity) - (b.next_at ?? Infinity) || a.name.localeCompare(b.name))
  if (!rows.length) return <Empty title="No automation">Declare one on an asset to have it run by itself.</Empty>
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>On</th>
            <th>Brings up to date</th>
            <th>When</th>
            {!dense && <th>Last started</th>}
            <th className="end">Next</th>
            <th className="end" />
          </tr>
        </thead>
        <tbody>
          {rows.map((automation) => (
            <tr key={automation.name} data-off={automation.enabled ? undefined : ""}>
              <td><AutomationToggle automation={automation} /></td>
              <td>
                <span className="row wrap">{automation.targets.map((t) => <AssetLink key={t} name={t} />)}</span>
                {!dense && <div className="mono faint">{automation.name}</div>}
              </td>
              <td>
                {triggerText(automation.trigger)}
                {automation.watched.length > 0 && <span className="muted mono"> {automation.watched.join(", ")}</span>}
              </td>
              {!dense && (
                <td className="muted">
                  {automation.last_fired ? <Ago at={automation.last_fired} now={now} /> : "never"}
                  {automation.last_run && <> · <RunLink id={automation.last_run} /></>}
                </td>
              )}
              <td className="end num">{!automation.enabled ? <span className="faint">off</span> : automation.next_at ? until(automation.next_at, now) : <span className="faint">on change</span>}</td>
              <td className="end">
                <Button size="s" icon={<Play size={13} />} onClick={() => report(command.mutateAsync({ name: automation.name, command: "run-now" }), `${automation.name} started a run`)}>
                  Run now
                </Button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function SensorsTable() {
  const sensors = useSensors()
  if (sensors.error) return <Failure error={sensors.error} />
  if (!sensors.data) return <Loading />
  if (!sensors.data.sensors.length) return <Empty title="No sensor">A sensor checks something outside every few seconds and commits what it finds to a source.</Empty>
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
                <Link href={`/sensors/${encodeURIComponent(sensor.name)}`} className="mono link">
                  <strong>{sensor.name}</strong>
                </Link>
              </td>
              <td>Every {duration(sensor.every)}</td>
              <td>
                <span className="row wrap">
                  {sensor.commits.map((source) => (
                    <Link key={source} href={`/sources/${encodeURIComponent(source)}`} className="mono link">
                      {source}
                    </Link>
                  ))}
                </span>
              </td>
              <td className="end num">{sensor.ticking ? "checking now" : sensor.due_in !== null ? `in ${duration(Math.max(0, sensor.due_in))}` : "–"}</td>
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
  if (!sensors.data) return <Loading />
  if (!sensor) return <Empty title="No sensor has that name" />
  return (
    <div className="stack">
      {sensor.doc && <p className="doc">{sensor.doc.replace(/\s+/g, " ").trim()}</p>}
      <Facts
        rows={[
          ["Checks", `Every ${duration(sensor.every)}, with ${duration(sensor.timeout)} to answer`],
          ["Commits to", <span className="row wrap">{sensor.commits.map((s) => <Link key={s} href={`/sources/${encodeURIComponent(s)}`} className="mono link">{s}</Link>)}</span>],
          ["Next check", sensor.ticking ? "Checking now" : sensor.due_in !== null ? `in ${duration(Math.max(0, sensor.due_in))}` : null],
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
