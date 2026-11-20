import { ChevronRight, Eraser, Trash2 } from "lucide-react"
import { useState, type ReactNode } from "react"
import { Link } from "wouter"
import { useAutomations, useClearCleanups, useCleanups, useDiagnostics, useExecutors, useHealth, useManifest, usePruneRuns, useSensors, useStatuses, useWorkers, type Health } from "../api/queries"
import { partitionLabel, plural, when } from "../model/format"
import { assetStatus, triggerText, type Status, type Tone } from "../model/status"
import { Button } from "../ui/controls"
import { confirm, Pick, report, toast } from "../ui/overlays"
import { Ago, AssetLink, Dot, Empty, Facts, Loading, StatusPill } from "./atoms"
import { failingKeys } from "./assets"

/* The engine itself: is it healthy, where do attempts run, and what needs a
 * person. `useAttention` is the console's answer to "is everything fine?". */

export function healthStatus(health: Health | undefined): Status {
  switch (health?.status) {
    case "ok":
      return { tone: "ok", label: "Healthy" }
    case "degraded":
      return { tone: "stale", label: "Degraded" }
    case "unavailable":
      return { tone: "failed", label: "Unavailable" }
    case "unreachable":
      return { tone: "failed", label: "Cannot reach the server" }
    default:
      return { tone: "idle", label: "Checking" }
  }
}

export type Concern = { id: string; tone: Tone; text: ReactNode; href: string }

/** Everything that needs someone's attention, worst first. Empty means all is well. */
export function useAttention(): { concerns: Concern[]; ready: boolean } {
  const health = useHealth()
  const diagnostics = useDiagnostics()
  const manifest = useManifest()
  const statuses = useStatuses()
  const executors = useExecutors()
  const workers = useWorkers()
  const out: Concern[] = []
  const state = healthStatus(health.data)
  if (state.tone === "failed") out.push({ id: "health", tone: "failed", text: `${state.label}.`, href: "/engine" })
  const loadError = diagnostics.data?.load_error
  if (loadError) out.push({ id: "load", tone: "failed", text: `Code saved or deployed at ${when(loadError.at, Date.now() / 1000)} did not load, so the server still runs the code before it.`, href: "/engine" })
  if (diagnostics.data?.last_error) out.push({ id: "error", tone: "stale", text: `The engine reports a problem in the background: ${diagnostics.data.last_error}`, href: "/engine" })
  for (const [name, status] of Object.entries(statuses.data?.assets ?? {})) {
    const s = assetStatus(status)
    const asset = <span className="name">{name}</span>
    const href = `/assets/${encodeURIComponent(name)}`
    if (s.tone === "failed") out.push({ id: `failed:${name}`, tone: "failed", text: <>{asset} failed. {s.detail}.</>, href })
    else if (status.stale) out.push({ id: `stale:${name}`, tone: "stale", text: <>{asset} is stale. {s.detail}.</>, href })
    const keys = failingKeys(status)
    if (keys) out.push({ id: `keys:${name}`, tone: "stale", text: <>{asset} has {plural(keys, "key")} that did not go through.</>, href: `${href}?tab=keys` })
    if (status.repairs) out.push({ id: `repairs:${name}`, tone: "stale", text: <>{asset} has {plural(status.repairs, "partition")} waiting for a repair.</>, href })
  }
  for (const executor of executors.data?.executors ?? []) {
    if (executor.kind !== "Pool" || !executor.in_flight) continue
    const polling = (workers.data?.workers ?? []).some((w) => w.pools.includes(executor.name))
    if (!polling) out.push({ id: `pool:${executor.name}`, tone: "waiting", text: <>{plural(executor.in_flight, "task")} {executor.in_flight === 1 ? "waits" : "wait"} for a worker on the pool <span className="name">{executor.name}</span>, and none is running.</>, href: "/engine" })
  }
  const stuck = diagnostics.data?.stuck_cleanups.length ?? 0
  if (stuck) out.push({ id: "cleanups", tone: "stale", text: `${plural(stuck, "cleanup")} gave up and ${stuck === 1 ? "needs" : "need"} clearing.`, href: "/engine" })
  for (const partial of diagnostics.data?.incomplete_full_runs ?? [])
    out.push({ id: `partial:${partial.output}:${partial.partition}`, tone: "stale", text: <><span className="name">{partial.asset}</span> {partitionLabel(partial.partition)} was left in the middle of a full run. Its readers wait until it runs again.</>, href: `/assets/${encodeURIComponent(partial.asset)}` })
  if (manifest.data?.errors.length) out.push({ id: "manifest", tone: "failed", text: `The project has ${plural(manifest.data.errors.length, "error")} in its definition.`, href: "/engine" })
  const rank = { failed: 0, stale: 1, waiting: 2, running: 3, idle: 4, ok: 5 }
  out.sort((a, b) => rank[a.tone] - rank[b.tone])
  return { concerns: out, ready: Boolean(statuses.data && diagnostics.data) }
}

/** What needs attention, worst first, each a link to where it can be dealt with. */
export function Attention({ limit }: { limit?: number }) {
  const { concerns, ready } = useAttention()
  if (!ready) return <Loading />
  if (!concerns.length) return <p className="lines-empty muted">Nothing needs attention.</p>
  const shown = limit ? concerns.slice(0, limit) : concerns
  return (
    <ul className="lines">
      {shown.map((concern) => (
        <li key={concern.id}>
          <Link href={concern.href} className="line">
            <Dot tone={concern.tone} />
            <span className="grow">{concern.text}</span>
            <ChevronRight size={14} className="faint" aria-hidden="true" />
          </Link>
        </li>
      ))}
      {shown.length < concerns.length && <li className="line muted">and {concerns.length - shown.length} more</li>}
    </ul>
  )
}

export type Next = { at: number; name: string; what: string; href: string; targets: string[] }

/** What will start by itself, soonest first. */
export function useUpcoming(): Next[] {
  const automations = useAutomations()
  const sensors = useSensors()
  const now = Date.now() / 1000
  const out: Next[] = []
  for (const a of automations.data?.automations ?? [])
    if (a.enabled && a.next_at !== null) out.push({ at: a.next_at, name: a.name, what: triggerText(a.trigger), href: "/automations", targets: a.targets })
  for (const s of sensors.data?.sensors ?? [])
    if (s.due_in !== null) out.push({ at: now + s.due_in, name: s.name, what: `Sensor, every ${s.every}s`, href: `/sensors/${encodeURIComponent(s.name)}`, targets: s.commits })
  return out.sort((a, b) => a.at - b.at)
}

export function EngineFacts() {
  const health = useHealth()
  const diagnostics = useDiagnostics()
  const manifest = useManifest()
  const d = diagnostics.data
  if (!d) return <Loading />
  return (
    <Facts
      rows={[
        ["Health", <StatusPill status={healthStatus(health.data)} />],
        ["Background problem", d.last_error ? <span className="error-text">{d.last_error}</span> : "None"],
        ["Project", <span className="mono">{d.project}</span>],
        ["Deploy", <span><span className="num">number {d.deploy_number}</span> <span className="mono muted">{d.deploy.slice(0, 12)}</span></span>],
        ["Code", manifest.data?.build?.commit ? <span className="mono">{manifest.data.build.commit.slice(0, 10)}</span> : null],
        ["Runs in progress", String(d.active_runs)],
        ["Attempts in flight", String(d.inflight)],
        ["State kept at", <span className="mono">{d.state}</span>],
        ["Namespace", <span className="mono">{d.namespace}</span>],
        ["Database", d.postgres ? "Postgres is configured" : "No Postgres: tables go to files"],
        ["History rows left out", d.skipped_history_rows.length ? String(d.skipped_history_rows.length) : null],
        ["Odd batches held back", d.anomalies.length ? String(d.anomalies.length) : null],
      ]}
    />
  )
}

/** Why the latest code did not load: its traceback, as the server or reload saw it. */
export function LoadError() {
  const error = useDiagnostics().data?.load_error
  if (!error) return null
  return (
    <>
      <p>The code saved or deployed at {when(error.at, Date.now() / 1000)} did not load. The server still runs the code before it, until code that loads replaces it.</p>
      <pre className="code logs" tabIndex={0}>{error.error}</pre>
    </>
  )
}

export function ExecutorsTable() {
  const executors = useExecutors()
  const workers = useWorkers()
  const sensors = useSensors()
  const now = Date.now() / 1000
  if (!executors.data) return <Loading />
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>Where attempts run</th>
            <th>Kind</th>
            <th className="end">In flight</th>
            <th className="end">Limit</th>
            <th>Workers</th>
          </tr>
        </thead>
        <tbody>
          {executors.data.executors.map((executor) => {
            const own = (workers.data?.workers ?? []).filter((w) => w.pools.includes(executor.name))
            const hosts = (sensors.data?.workers ?? []).filter((w) => w.executor === executor.name)
            const pool = executor.kind === "Pool"
            return (
              <tr key={executor.name}>
                <td className="name">{executor.name}</td>
                <td className="muted">{pool ? "Pool: workers come and take work" : executor.kind === "Local" ? "Local: processes on the server" : executor.kind}</td>
                <td className="end num">{executor.in_flight}</td>
                <td className="end num muted">{executor.max_concurrent ?? "None"}</td>
                <td>
                  {pool ? (
                    own.length ? (
                      own.map((w) => (
                        <div key={w.id} className="mono">
                          {w.id} <span className="muted">seen <Ago at={w.seen_at} now={now} /></span>
                        </div>
                      ))
                    ) : (
                      <span className="tone" data-tone={executor.in_flight ? "failed" : "idle"}>No worker is polling{executor.in_flight ? `: ${plural(executor.in_flight, "task")} cannot start` : ""}</span>
                    )
                  ) : hosts.length ? (
                    hosts.map((w) => (
                      <div key={w.id} className="mono">
                        {w.id} <span className="muted">runs sensors</span>
                      </div>
                    ))
                  ) : (
                    <span className="faint">Started as needed</span>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

export function CleanupsPanel() {
  const cleanups = useCleanups()
  const clear = useClearCleanups()
  const now = Date.now() / 1000
  if (!cleanups.data) return <Loading />
  const rows = cleanups.data.cleanups
  if (!rows.length) return <Empty title="No cleanup is stuck or waiting" />
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>Output</th>
            <th>Partition</th>
            <th>State</th>
            <th className="end" />
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const stuck = Array.isArray(row.stuck) ? row.stuck.length : row.stuck ? 1 : 0
            return (
              <tr key={`${row.output}/${row.partition ?? row.id}/${i}`}>
                <td><AssetLink name={row.output} /></td>
                <td className="mono muted">{partitionLabel(row.partition ?? "")}</td>
                <td>
                  {stuck ? <span className="tone" data-tone="failed">{plural(stuck, "cleanup")} gave up</span> : row.due ? `Old data goes ${when(row.due, now)}` : `${row.pending ?? 0} waiting`}
                </td>
                <td className="end">
                  {stuck > 0 && (
                    <Button
                      size="s"
                      icon={<Eraser size={13} />}
                      onClick={async () => {
                        if (await confirm({ title: `Clear the stuck cleanups of ${row.output}?`, description: "The engine forgets them. The files they were meant to remove stay where they are.", confirmLabel: "Clear them" }))
                          await report(clear.mutateAsync({ output: row.output, partition: row.partition ?? "" }), "Cleared")
                      }}
                    >
                      Clear
                    </Button>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const AGES = [
  { value: "3600", label: "an hour" },
  { value: "86400", label: "a day" },
  { value: "604800", label: "a week" },
  { value: "2592000", label: "30 days" },
]

/** Delete finished runs past an age. It counts first, and deletes only once that count is confirmed. */
export function PruneRuns() {
  const prune = usePruneRuns()
  const [age, setAge] = useState("604800")
  const label = AGES.find((a) => a.value === age)!.label
  const run = async () => {
    const before = Date.now() / 1000 - Number(age)
    const found = await report(prune.mutateAsync({ before, dry_run: true }), (r) => (r.deleted.length ? `${plural(r.deleted.length, "run")} found` : `No finished run is older than ${label}`))
    if (!found?.deleted.length) return
    if (!(await confirm({ title: `Delete ${plural(found.deleted.length, "run")} older than ${label}?`, description: "Their history and logs go for good. What they built stays, and so does every run in progress.", confirmLabel: "Delete them", danger: true }))) return
    try {
      const done = await prune.mutateAsync({ before, dry_run: false })
      toast.success(`${plural(done.deleted.length, "run")} deleted`)
    } catch (error) {
      toast.error("That did not work", { description: (error as Error).message })
    }
  }
  return (
    <div className="row wrap">
      <span>Delete finished runs older than</span>
      <Pick label="How old" size="s" value={age} onChange={setAge} options={AGES} />
      <Button size="s" variant="danger" icon={<Trash2 size={13} />} onClick={run}>
        Delete old runs
      </Button>
    </div>
  )
}
