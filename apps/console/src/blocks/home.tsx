import { Link, useLocation } from "wouter"
import { useAutomations, useDiagnostics, useHealth, useManifest, useRuns, useStatuses, useTaskHistogram } from "../api/queries"
import type { Automation } from "../api/types"
import { count, list, until } from "../model/format"
import { projectGraph, topological } from "../model/graph"
import { assetStatus, taskStatus } from "../model/status"
import { Strip, StripAxis, StripTotal } from "./activity"
import { Ago, Dot, Failure, Loading, StatusPill } from "./atoms"
import { healthStatus, useUpcoming } from "./engine"

/* The overview. It answers three questions in the
 * order someone arriving asks them: is everything up to date, what is wrong,
 * and what is running or about to. */

/** The first answer, in one line: how many assets are fine, what is in progress, what starts next. */
export function Summary() {
  const statuses = useStatuses()
  const diagnostics = useDiagnostics()
  const health = useHealth()
  const hourAgo = Math.floor(Date.now() / 3600000) * 3600 - 3600
  const failed = useRuns({ status: ["failed"], since: hourAgo }, 1, 10000)
  const next = useUpcoming()[0]
  const now = Date.now() / 1000
  const tones = Object.values(statuses.data?.assets ?? {}).map((status) => assetStatus(status).tone)
  const n = (tone: string) => tones.filter((t) => t === tone).length
  const rest = [n("failed") && `${n("failed")} failed`, n("stale") && `${n("stale")} stale`, n("running") && `${n("running")} running`, n("idle") && `${n("idle")} never built`].filter(Boolean)
  const active = diagnostics.data?.active_runs ?? 0
  return (
    <dl className="summary">
      <div>
        <dt>Assets</dt>
        <dd>
          <Link href="/assets" className="status">
            <Dot tone={!statuses.data ? "idle" : n("failed") ? "failed" : n("stale") ? "stale" : "ok"} />
            {statuses.data ? `${n("ok")} of ${tones.length} up to date` : "Checking"}
          </Link>
          {rest.length > 0 && <span className="muted">{rest.join(", ")}</span>}
        </dd>
      </div>
      <div>
        <dt>Runs</dt>
        <dd>
          <Link href="/runs?status=running,queued" className="status">
            <Dot tone={active ? "running" : "idle"} />
            {active ? `${count(active)} in progress` : "None in progress"}
          </Link>
          {failed.data && failed.data.total > 0 && (
            <Link href="/runs?status=failed" className="tone" data-tone="failed">
              {count(failed.data.total)} failed in the last hour
            </Link>
          )}
        </dd>
      </div>
      <div>
        <dt>Next start</dt>
        <dd>
          {next ? (
            <>
              <Link href={next.href} className="name">{list(next.targets, 2)}</Link>
              <span className="muted">{until(next.at, now)}</span>
            </>
          ) : (
            <span className="muted">Nothing is scheduled</span>
          )}
        </dd>
      </div>
      <div>
        <dt>Engine</dt>
        <dd>
          <Link href="/engine" className="status">
            <StatusPill status={healthStatus(health.data)} />
          </Link>
          <span className="muted">deploy {diagnostics.data?.deploy_number ?? "–"}</span>
        </dd>
      </div>
    </dl>
  )
}

/** When an asset's automations will next start it, as far as anyone can know. */
function nextStart(automations: Automation[], asset: string, now: number): string {
  const own = automations.filter((automation) => automation.enabled && automation.targets.includes(asset))
  const times = own.flatMap((automation) => (automation.next_at === null ? [] : [automation.next_at]))
  if (times.length) return until(Math.min(...times), now)
  if (own.some((automation) => automation.trigger.kind === "onchange")) return "on change"
  if (own.some((automation) => automation.trigger.kind === "ondeploy")) return "on deploy"
  return "manual"
}

/** Every asset and job in the order data flows: its state, its last and next run, and what it did over the window. */
export function AssetActivity({ window }: { window: number }) {
  const manifest = useManifest()
  const statuses = useStatuses()
  const automations = useAutomations()
  const histogram = useTaskHistogram(window)
  const [, navigate] = useLocation()
  const now = Date.now() / 1000
  if (manifest.error) return <Failure error={manifest.error} />
  if (!manifest.data) return <Loading what="Reading the project" />
  const graph = projectGraph(manifest.data)
  const names = topological(graph.nodes.map((node) => node.id), graph.edges).filter((id) => manifest.data!.assets[id])
  const scale = histogram.data ?? { since: now - window, bucket: window / 60, series: {} }
  return (
    <div className="table-scroll">
      <table className="table activity">
        <thead>
          <tr>
            <th>Asset</th>
            <th className="optional">Kind</th>
            <th className="optional">Up to date</th>
            <th className="optional">Last run</th>
            <th className="optional">Next run</th>
            <th className="strip-col"><StripAxis since={scale.since} bucket={scale.bucket} /></th>
            <th className="end">Runs</th>
          </tr>
        </thead>
        <tbody>
          {names.map((name) => {
            const status = statuses.data?.assets[name]
            const state = assetStatus(status)
            const cells = histogram.data?.series[name] ?? []
            const href = `/assets/${encodeURIComponent(name)}`
            const last = status?.last
            const run = last?.attempt?.split("/")[0]
            return (
              <tr key={name} data-link="" onClick={() => navigate(href)}>
                <td>
                  <span className="row">
                    <Dot tone={state.tone} />
                    <Link href={href} className="name" onClick={(event) => event.stopPropagation()}>
                      {name}
                    </Link>
                    {state.tone !== "ok" && <span className="muted truncate">{state.short || state.label.toLowerCase()}</span>}
                  </span>
                </td>
                <td className="muted optional">{manifest.data!.assets[name]!.outputs.length ? "Asset" : "Job"}</td>
                <td className="num optional">{status ? `${count(status.partitions.materialized)} of ${count(status.partitions.total)}` : ""}</td>
                <td className="optional">
                  {last ? (
                    <Link href={run ? `/runs/${run}` : href} className="status" data-quiet="" onClick={(event) => event.stopPropagation()}>
                      <Dot tone={taskStatus(last.outcome).tone} />
                      <Ago at={last.at} now={now} />
                    </Link>
                  ) : (
                    <span className="faint">never</span>
                  )}
                </td>
                <td className="muted num optional">{nextStart(automations.data?.automations ?? [], name, now)}</td>
                <td className="strip-col"><Strip cells={cells} since={scale.since} bucket={scale.bucket} of={name} /></td>
                <td className="end num"><StripTotal cells={cells} /></td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
