import type { MouseEvent } from "react"
import { Link } from "wouter"
import { useTaskHistogram } from "../api/queries"
import type { HistogramCell } from "../api/types"
import { CELLS, cellTone, totals, WINDOWS } from "../model/activity"
import { clock, clockShort, count, partitionLabel, plural } from "../model/format"
import { taskStatus } from "../model/status"
import { ticks } from "../timeline/scale"
import { Segmented, useChartTip } from "../ui/controls"
import { Empty, Failure, Loading } from "./atoms"

/* Activity as a strip: the last while cut into equal cells of time, each
 * coloured by the worst thing that happened in it. Every strip on a page
 * shares one time axis, drawn once in the column's header. */

export function WindowPicker({ value, onChange }: { value: number; onChange: (seconds: number) => void }) {
  return <Segmented label="How far back activity goes" value={String(value)} onChange={(next) => onChange(Number(next))} options={WINDOWS} />
}

/** What the colours of a strip mean. */
export function StripLegend() {
  return (
    <span className="strip-legend" aria-hidden="true">
      <span><i className="strip-cell" data-tone="ok" /> Ran</span>
      <span><i className="strip-cell" data-tone="failed" /> Failed</span>
      <span><i className="strip-cell" data-tone="running" /> In progress</span>
      <span><i className="strip-cell" /> Nothing ran</span>
    </span>
  )
}

type Scale = { since: number; bucket: number }

const time = (at: number, bucket: number) => (bucket < 60 ? clock(at) : clockShort(at))

/** The time axis of the strips below it: a few round times, and now at the right edge. */
export function StripAxis({ since, bucket }: Scale) {
  const end = since + CELLS * bucket
  const marks = ticks(since, end, 4, null).filter((tick) => (tick.at - since) / (end - since) < 0.86 && tick.at > since)
  return (
    <div className="strip-axis" aria-hidden="true">
      {marks.map((tick) => (
        <span key={tick.at} style={{ left: `${((tick.at - since) / (end - since)) * 100}%` }}>
          {tick.label}
        </span>
      ))}
      <span className="strip-axis-now">now</span>
    </div>
  )
}

function cellText(cell: HistogramCell | undefined): string {
  if (!cell) return "Nothing ran"
  const statuses = Object.entries(cell.counts)
  const tasks = statuses.length === 1 ? `${plural(statuses[0]![1], "task")} ${taskStatus(statuses[0]![0]).label.toLowerCase()}` : statuses.map(([status, n]) => `${count(n)} ${taskStatus(status).label.toLowerCase()}`).join(", ")
  return `${plural(cell.runs, "run")} · ${tasks}`
}

type StripProps = Scale & { cells: HistogramCell[]; /** What the strip is of, for a screen reader: "site_feed". */ of: string }

export function Strip({ cells, since, bucket, of }: StripProps) {
  const { over, out, tip } = useChartTip()
  const by = new Map(cells.map((cell) => [cell.i, cell]))
  const sum = totals(cells)
  return (
    <div className="strip" role="img" aria-label={`${of}: ${sum.runs ? `${plural(sum.runs, "run")}, ${sum.failed} failed` : "nothing ran"}`} onMouseLeave={out}>
      {Array.from({ length: CELLS }, (_, i) => {
        const cell = by.get(i)
        const shared = {
          className: "strip-cell",
          onMouseEnter: (event: MouseEvent<HTMLElement>) =>
            over(
              event.currentTarget,
              <>
                <span className="chart-tip-time">
                  {time(since + i * bucket, bucket)} to {time(since + (i + 1) * bucket, bucket)}
                </span>
                {cellText(cell)}
              </>,
            ),
        }
        return cell ? (
          <Link key={i} href={`/runs/${cell.run}`} tabIndex={-1} aria-hidden="true" data-tone={cellTone(cell.counts)} onClick={(event) => event.stopPropagation()} {...shared} />
        ) : (
          <span key={i} {...shared} />
        )
      })}
      {tip}
    </div>
  )
}

/** The number of runs behind a strip, with the tasks that failed called out. */
export function StripTotal({ cells }: { cells: HistogramCell[] }) {
  const sum = totals(cells)
  if (!sum.runs) return <span className="faint">–</span>
  return (
    <>
      {sum.failed > 0 && <span className="tone strip-failed" data-tone="failed">{plural(sum.failed, "task")} failed</span>}
      {count(sum.runs)}
    </>
  )
}

const natural = new Intl.Collator(undefined, { numeric: true })
const MAX_PARTITIONS = 40

/** One asset's activity, a strip per partition that ran in the window. */
export function PartitionActivity({ asset, window }: { asset: string; window: number }) {
  const histogram = useTaskHistogram(window, asset)
  if (histogram.error) return <Failure error={histogram.error} />
  if (!histogram.data) return <Loading />
  const { since, bucket, series } = histogram.data
  const names = Object.keys(series).sort(natural.compare)
  if (!names.length) return <Empty title="It did not run in this window" />
  return (
    <div className="table-scroll">
      <table className="table activity" data-wide="">
        <thead>
          <tr>
            <th>{names.length === 1 && names[0] === "" ? "Asset" : "Partition"}</th>
            <th className="strip-col"><StripAxis since={since} bucket={bucket} /></th>
            <th className="end">Runs</th>
          </tr>
        </thead>
        <tbody>
          {names.slice(0, MAX_PARTITIONS).map((name) => (
            <tr key={name}>
              <td className={name ? "mono" : "name"}>{partitionLabel(name) || asset}</td>
              <td className="strip-col"><Strip cells={series[name]!} since={since} bucket={bucket} of={partitionLabel(name) || asset} /></td>
              <td className="end num"><StripTotal cells={series[name]!} /></td>
            </tr>
          ))}
        </tbody>
      </table>
      {names.length > MAX_PARTITIONS && <p className="table-foot muted">{count(names.length - MAX_PARTITIONS)} more partitions ran in this window.</p>}
    </div>
  )
}
