import { Search } from "lucide-react"
import { useState } from "react"
import { Link, useLocation } from "wouter"
import { useManifest, useRunFacets, useRunHistogram, useRuns, type RunsFilter } from "../api/queries"
import type { Manifest, RunRow } from "../api/types"
import { clockShort, count, duration, list, partitionLabel, per, plural, when } from "../model/format"
import { runStatus, triggerText, type Tone } from "../model/status"
import { Button, TextField, Tip, useChartTip } from "../ui/controls"
import { PickMany } from "../ui/overlays"
import { Ago, Empty, Failure, Loading, StatusPill, useParam } from "./atoms"

/* Lists of runs: a table for a page, a narrow feed for a side pane, and the
 * filters and histogram that go with either. A row says that a run failed;
 * why is on the run's page, and in a hint on the row's status. */

export function runWhat(run: RunRow): string {
  if (run.source) return `Commit to ${run.source}`
  return list(run.targets, 2)
}

/** What started a run, in a few words: the trigger of its automation, or who asked. */
export function runTrigger(run: RunRow, manifest: Manifest | undefined): string {
  if (run.source) return `Commit${run.by ? ` by ${run.by}` : ""}`
  if (run.retry_of) return "Retry"
  const automation = run.automation ? manifest?.automations[run.automation] : undefined
  if (automation) return triggerText(automation.trigger).replace(/ \(.*\)$/, "")
  if (run.automation) return run.automation
  return run.by && run.by !== "api" ? `Manual, from the ${run.by}` : "Manual"
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
const MIN_BARS = 60

/** Runs over time, failures on top. The axis is linear: a bucket where nothing ran keeps its place. */
export function RunHistogram({ filter, height = 40 }: { filter: RunsFilter; height?: number }) {
  const histogram = useRunHistogram(filter)
  const { over, out, tip } = useChartTip()
  const data = histogram.data
  if (!data?.bars.length) return <div className="histogram" style={{ height }} />
  const { bucket } = data
  const end = Math.ceil(data.until / bucket) * bucket
  const n = Math.min(240, Math.max(MIN_BARS, Math.ceil((end - data.since) / bucket)))
  const start = end - n * bucket
  const by = new Map(data.bars.map((bar) => [Math.round((bar.t - start) / bucket), bar.counts]))
  const total = (counts: Record<string, number> | undefined) => Object.values(counts ?? {}).reduce((a, b) => a + b, 0)
  const max = Math.max(1, ...data.bars.map((bar) => total(bar.counts)))
  const time = (at: number) => (end - start > 86400 ? when(at, end) : clockShort(at))
  return (
    <figure className="histogram-figure">
      <div className="histogram" style={{ height }} role="img" aria-label={`Runs over time, up to ${max} per ${per(bucket)}`} onMouseLeave={out}>
        {Array.from({ length: n }, (_, i) => {
          const counts = by.get(i)
          return (
            <div
              key={i}
              className="histogram-slot"
              onMouseEnter={(event) =>
                over(
                  event.currentTarget,
                  <>
                    <span className="chart-tip-time">{time(start + i * bucket)}</span>
                    {counts ? Object.entries(counts).map(([status, k]) => `${count(k)} ${runStatus(status).label.toLowerCase()}`).join(", ") : "No runs"}
                  </>,
                )
              }
            >
              {counts && (
                <div className="histogram-bar" style={{ height: `${(total(counts) / max) * 100}%` }}>
                  {BAR_TONES.map(([status, tone]) => (counts[status] ? <span key={status} data-tone={tone} data-status={status} style={{ flexGrow: counts[status] }} /> : null))}
                </div>
              )}
            </div>
          )
        })}
        {tip}
      </div>
      <figcaption className="histogram-axis faint">
        <span>{time(start)}</span>
        <span>Runs per {per(bucket)}</span>
        <span>now</span>
      </figcaption>
    </figure>
  )
}

export function RunsTable({ filter, limit = 50, dense, brief }: { filter: RunsFilter; limit?: number; dense?: boolean; /** No footer: the newest few, for a page that links to the rest. */ brief?: boolean }) {
  const [more, setMore] = useState(0)
  const runs = useRuns(filter, limit + more)
  const manifest = useManifest()
  const [, navigate] = useLocation()
  const now = Date.now() / 1000
  if (runs.error) return <Failure error={runs.error} />
  if (!runs.data) return <Loading what="Loading runs" />
  if (!runs.data.runs.length) return <Empty title="No runs match" />
  return (
    <>
      <div className="table-scroll">
        <table className="table">
          <thead>
            <tr>
              <th className="fit">Status</th>
              <th>Run</th>
              {!dense && <th className="optional">Partitions</th>}
              <th className="optional">Trigger</th>
              <th className="end optional">Tasks</th>
              <th className="end">Duration</th>
              <th className="end">Started</th>
            </tr>
          </thead>
          <tbody>
            {runs.data.runs.map((run) => (
              <tr key={run.id} data-link="" onClick={() => navigate(`/runs/${run.id}`)}>
                <td className="fit">
                  {run.error ? (
                    <Tip text={<span className="tip-error">{run.error}</span>} side="bottom" align="start">
                      <span><StatusPill status={runStatus(run.status)} /></span>
                    </Tip>
                  ) : (
                    <StatusPill status={runStatus(run.status)} />
                  )}
                </td>
                <td className="wide">
                  <Link href={`/runs/${run.id}`} className="name truncate" onClick={(event) => event.stopPropagation()}>
                    {runWhat(run)}
                  </Link>
                </td>
                {!dense && <td className="muted mono optional">{runWhere(run)}</td>}
                <td className="muted optional">{runTrigger(run, manifest.data)}</td>
                <td className="end num optional">{run.failed_count ? <span className="tone" data-tone="failed">{run.failed_count} of {run.task_count} failed</span> : run.task_count || <span className="faint">–</span>}</td>
                <td className="end num">{run.finished_at ? duration(run.finished_at - run.created_at) : <span className="faint">–</span>}</td>
                <td className="end muted"><Ago at={run.created_at} now={now} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {!brief && (
        <div className="row table-foot">
          <span className="muted grow">
            {count(runs.data.runs.length)} of {count(runs.data.total)} runs
          </span>
          {runs.data.next && <Button size="s" onClick={() => setMore(more + 100)}>Show more</Button>}
        </div>
      )}
    </>
  )
}

