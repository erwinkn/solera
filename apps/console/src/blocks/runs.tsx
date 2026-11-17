import { Search } from "lucide-react"
import { useState } from "react"
import { Link, useLocation } from "wouter"
import { useRunFacets, useRunHistogram, useRuns, type RunsFilter } from "../api/queries"
import type { RunRow } from "../api/types"
import { clock, clockShort, count, duration, list, partitionLabel, plural } from "../model/format"
import { origin, runStatus, type Tone } from "../model/status"
import { Button, TextField } from "../ui/controls"
import { PickMany } from "../ui/overlays"
import { Ago, Dot, Empty, Failure, Loading, StatusPill, useParam } from "./atoms"

/* Lists of runs: a table for a page, a narrow feed for a side pane, and the
 * filters and histogram that go with either. */

export function runWhat(run: RunRow): string {
  if (run.source) return `Commit to ${run.source}`
  return list(run.targets, 2)
}

export function runWhere(run: RunRow): string {
  if (!run.partitions || typeof run.partitions === "string") return run.partitions === "latest" || !run.partitions ? "" : run.partitions
  const names = [...new Set(Object.values(run.partitions).flat().map(partitionLabel).filter(Boolean))]
  return list(names, 2)
}

/** The filter of a runs list, kept in the address bar. */
export function useRunsFilter(): { filter: RunsFilter; set: (name: "status" | "asset" | "q", value: string[] | string) => void } {
  const [status, setStatus] = useParam("status")
  const [asset, setAsset] = useParam("asset")
  const [q, setQ] = useParam("q")
  const split = (text: string) => (text ? text.split(",") : [])
  return {
    filter: { status: split(status), asset: split(asset), q: q || undefined },
    set: (name, value) => {
      const text = Array.isArray(value) ? value.join(",") : value
      ;(name === "status" ? setStatus : name === "asset" ? setAsset : setQ)(text || null)
    },
  }
}

const STATUSES = ["running", "queued", "succeeded", "failed", "canceled", "skipped"]

export function RunsFilters({ filter, set }: ReturnType<typeof useRunsFilter>) {
  const facets = useRunFacets({})
  const assets = facets.data?.asset ?? []
  const counts = new Map((facets.data?.status ?? []).map((f) => [f.value, f.count]))
  return (
    <div className="row wrap filters">
      <div className="search">
        <Search size={14} className="faint" aria-hidden="true" />
        <TextField label="Search runs" hideLabel placeholder="Search errors, assets, tags" value={filter.q ?? ""} onChange={(value) => set("q", value)} />
      </div>
      <PickMany
        label="Status"
        placeholder="Any status"
        value={filter.status ?? []}
        onChange={(value) => set("status", value)}
        options={STATUSES.map((value) => ({ value, label: `${runStatus(value).label}${counts.has(value) ? ` (${count(counts.get(value)!)})` : ""}` }))}
      />
      <PickMany label="Asset" placeholder="Any asset" value={filter.asset ?? []} onChange={(value) => set("asset", value)} options={assets.map((f) => ({ value: f.value, label: f.value, note: plural(f.count, "run") }))} />
      {(filter.q || filter.status?.length || filter.asset?.length) ? (
        <Button variant="quiet" onClick={() => { set("q", ""); set("status", []); set("asset", []) }}>
          Clear
        </Button>
      ) : null}
    </div>
  )
}

const BAR_TONES: [string, Tone][] = [["failed", "failed"], ["canceled", "idle"], ["running", "running"], ["queued", "waiting"], ["succeeded", "ok"], ["skipped", "idle"]]

/** Runs per minute over the last hour, failures on top. */
export function RunHistogram({ filter, height = 56 }: { filter: RunsFilter; height?: number }) {
  const histogram = useRunHistogram(filter)
  const bars = histogram.data?.bars ?? []
  const max = Math.max(1, ...bars.map((bar) => Object.values(bar.counts).reduce((a, b) => a + b, 0)))
  if (!bars.length) return <div className="histogram" style={{ height }} />
  return (
    <figure className="histogram-figure">
      <div className="histogram" style={{ height }} role="img" aria-label={`Runs over time: ${bars.length} bars, up to ${max} runs each`}>
        {bars.map((bar) => {
          const total = Object.values(bar.counts).reduce((a, b) => a + b, 0)
          return (
            <div key={bar.t} className="histogram-bar" title={`${clockShort(bar.t)}: ${Object.entries(bar.counts).map(([k, v]) => `${v} ${runStatus(k).label.toLowerCase()}`).join(", ")}`} style={{ height: `${(total / max) * 100}%` }}>
              {BAR_TONES.map(([status, tone]) => (bar.counts[status] ? <span key={status} data-tone={tone} data-status={status} style={{ flexGrow: bar.counts[status] }} /> : null))}
            </div>
          )
        })}
      </div>
      <figcaption className="histogram-axis faint num">
        <span>{clockShort(bars[0]!.t)}</span>
        <span>runs, one bar for each {duration(histogram.data!.bucket).replace(/ 00s$/, "").replace(/^1m$/, "minute")}</span>
        <span>{clockShort(bars[bars.length - 1]!.t)}</span>
      </figcaption>
    </figure>
  )
}

export function RunsTable({ filter, limit = 50, dense }: { filter: RunsFilter; limit?: number; dense?: boolean }) {
  const [more, setMore] = useState(0)
  const runs = useRuns(filter, limit + more)
  const [, navigate] = useLocation()
  const now = Date.now() / 1000
  if (runs.error) return <Failure error={runs.error} />
  if (!runs.data) return <Loading what="Loading runs" />
  if (!runs.data.runs.length) return <Empty title="No runs match">Change the filters, or start a run.</Empty>
  return (
    <>
      <div className="table-scroll">
        <table className="table">
          <thead>
            <tr>
              <th>Status</th>
              <th>What</th>
              {!dense && <th>Partitions</th>}
              <th>Started by</th>
              <th className="end">Tasks</th>
              <th className="end">Took</th>
              <th className="end">When</th>
            </tr>
          </thead>
          <tbody>
            {runs.data.runs.map((run) => (
              <tr key={run.id} data-link="" onClick={() => navigate(`/runs/${run.id}`)}>
                <td><StatusPill status={runStatus(run.status)} /></td>
                <td>
                  <Link href={`/runs/${run.id}`} className="mono" onClick={(event) => event.stopPropagation()}>
                    <strong>{runWhat(run)}</strong>
                  </Link>
                  {run.error && <div className="error-text truncate run-error">{run.error}</div>}
                </td>
                {!dense && <td className="muted mono">{runWhere(run)}</td>}
                <td className="muted">{origin(run).replace("Automation ", "")}</td>
                <td className="end num">{run.failed_count ? <span className="tone" data-tone="failed">{run.failed_count} failed of {run.task_count}</span> : run.task_count || "–"}</td>
                <td className="end num">{run.finished_at ? duration(run.finished_at - run.created_at) : "–"}</td>
                <td className="end muted"><Ago at={run.created_at} now={now} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="row table-foot">
        <span className="muted grow">
          Showing {count(runs.data.runs.length)} of {count(runs.data.total)}
        </span>
        {runs.data.next && <Button size="s" onClick={() => setMore(more + 100)}>Show more</Button>}
      </div>
    </>
  )
}

/** A narrow list of runs, newest first, for a side pane. */
export function RunsFeed({ filter, selected, limit = 60 }: { filter: RunsFilter; selected?: string; limit?: number }) {
  const runs = useRuns(filter, limit)
  if (runs.error) return <Failure error={runs.error} />
  if (!runs.data) return <Loading />
  if (!runs.data.runs.length) return <Empty title="No runs" />
  return (
    <ul className="feed">
      {runs.data.runs.map((run) => (
        <li key={run.id}>
          <Link href={`/runs/${run.id}`} className="feed-item" aria-current={run.id === selected ? "page" : undefined}>
            <Dot tone={runStatus(run.status).tone} />
            <span className="grow truncate">
              <span className="mono feed-name">{runWhat(run)}</span>
              <span className="muted"> {runWhere(run)}</span>
            </span>
            <span className="faint num">{clock(run.created_at)}</span>
          </Link>
        </li>
      ))}
    </ul>
  )
}
