import {
  visibleWaterfallRows,
  Waterfall,
  WaterfallBar,
  WaterfallCell,
  WaterfallColumnHeader,
  WaterfallHeaderRow,
  WaterfallIndicator,
  WaterfallRow,
  WaterfallRowHeader,
  type WaterfallData,
} from "@ui-sdk/react/waterfall"
import { ChevronRight } from "lucide-react"
import { memo, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react"
import { Link } from "wouter"
import { duration as formatDuration } from "../model/format"
import { mergeBars, ticks } from "./scale"
import type { Mark, RailSpec, Row } from "./types"

/* The timeline graph: the plan on the left (every step, joined by the rail of
 * what reads what), what happened on the right (bars on one time axis), and a
 * count between them. Rows, expansion, selection and the keyboard are the UI
 * SDK's Waterfall, a treegrid; the rail, marks and bars are drawn here. */

type Props = {
  label: string
  rows: Row[]
  /** Epoch seconds shown by the axis. */
  range: [number, number]
  /** Set while things run: draws the line at now and lets open bars grow. */
  now?: number
  /** The axis reads as time since this moment; without it, as the clock. */
  origin?: number
  rail?: RailSpec
  selected: string | null
  onSelect: (id: string | null) => void
  expanded: string[]
  onExpandedChange: (ids: string[]) => void
  headers: { name: string; progress: string; total: string }
  /** The chart scrolls inside this height once it has more rows than fit. */
  maxHeight?: string
  empty?: ReactNode
}

const LANE = 12
const OVERSCAN = 8

function index(rows: Row[], into = new Map<string, Row>()): Map<string, Row> {
  for (const row of rows) {
    into.set(row.id, row)
    if (row.children) index(row.children, into)
  }
  return into
}

export function Timeline({ label, rows, range, now, origin, rail, selected, onSelect, expanded, onExpandedChange, headers, maxHeight = "70dvh", empty }: Props) {
  const [start, end] = range
  const span = Math.max(end - start, 0.001)
  const byId = useMemo(() => index(rows), [rows])

  // The Waterfall wants one span per row, in milliseconds from the axis start.
  const items = useMemo(() => {
    const convert = (row: Row): WaterfallData[number] => {
      const from = row.bars.length ? Math.min(...row.bars.map((b) => b.start)) : end
      const to = row.bars.length ? Math.max(...row.bars.map((b) => b.end ?? now ?? end)) : end
      return {
        value: row.id,
        label: row.name,
        start: Math.round((from - start) * 1000),
        duration: Math.max(0, Math.round((to - from) * 1000)),
        ...(row.children?.length ? { children: row.children.map(convert) } : {}),
      }
    }
    return rows.map(convert)
  }, [rows, start, end, now])
  const visible = useMemo(() => visibleWaterfallRows(items, expanded), [items, expanded])

  // Rows are one height, set by the theme and the layout: measure it, and
  // again when either changes, to draw the rail and to window long charts.
  const scroller = useRef<HTMLDivElement>(null)
  const probe = useRef<HTMLDivElement>(null)
  const chart = useRef<HTMLDivElement>(null)
  const [rowHeight, setRowHeight] = useState(32)
  const [chartWidth, setChartWidth] = useState(600)
  const [view, setView] = useState({ top: 0, height: 800 })
  useLayoutEffect(() => {
    const measure = () => {
      if (probe.current) setRowHeight(probe.current.getBoundingClientRect().height || 32)
      if (chart.current) setChartWidth(chart.current.getBoundingClientRect().width || 600)
      if (scroller.current) setView({ top: scroller.current.scrollTop, height: scroller.current.clientHeight })
    }
    measure()
    const observer = new ResizeObserver(measure)
    for (const element of [probe.current, chart.current, scroller.current]) if (element) observer.observe(element)
    return () => observer.disconnect()
  }, [])

  const first = Math.max(0, Math.floor(view.top / rowHeight) - OVERSCAN)
  const last = Math.min(visible.length, Math.ceil((view.top + view.height) / rowHeight) + OVERSCAN)
  const windowed = visible.length > 60
  const shown = windowed ? visible.slice(first, last) : visible

  // Which top-level row each visible row sits under, for the rail's lines.
  const tops = useMemo(() => {
    const out: string[] = []
    let top = ""
    for (const row of visible) {
      if (row.level === 1) top = byId.get(row.item.value)?.node ?? row.item.value
      out.push(top)
    }
    return out
  }, [visible, byId])

  const axis = useMemo(() => ticks(start, end, Math.max(2, Math.floor(chartWidth / 84)), origin ?? null), [start, end, chartWidth, origin])
  const at = (time: number) => `${Math.min(100, Math.max(0, ((time - start) / span) * 100))}%`
  const railWidth = rail ? rail.lanes * LANE + 6 : 0
  const fine = (span / Math.max(chartWidth, 1)) * 2 // seconds in two pixels

  const scrollTo = (rowIndex: number) => {
    const element = scroller.current
    if (!element) return
    const top = rowIndex * rowHeight
    if (top < element.scrollTop) element.scrollTop = top
    else if (top + rowHeight * 2 > element.scrollTop + element.clientHeight) element.scrollTop = top + rowHeight * 2 - element.clientHeight
  }

  return (
    <div
      className="tl"
      ref={scroller}
      style={{ maxHeight, ["--tl-rail" as string]: `${railWidth}px`, ...(headers.progress ? {} : { ["--tl-progress" as string]: "0px" }) }}
      onScroll={(event) => setView({ top: event.currentTarget.scrollTop, height: event.currentTarget.clientHeight })}
      data-live={now === undefined ? undefined : ""}
    >
      <Waterfall
        className="tl-grid"
        aria-label={label}
        aria-rowcount={visible.length + 1}
        items={items}
        range={[0, Math.round(span * 1000)]}
        value={selected}
        onValueChange={(value) => onSelect(value)}
        expanded={expanded}
        onExpandedChange={(next) => onExpandedChange(next)}
        onHighlight={(_, details) => windowed && scrollTo(details.index)}
        format={(ms: number) => formatDuration(ms / 1000)}
        getBarLabel={(item) => {
          const row = byId.get(item.value)
          return row ? [row.progress, row.total].filter((part) => typeof part === "string" && part).join(", ") || "Not started" : ""
        }}
      >
        <WaterfallHeaderRow className="tl-row tl-head">
          <WaterfallColumnHeader className="tl-name">{headers.name}</WaterfallColumnHeader>
          <WaterfallColumnHeader className="tl-progress">{headers.progress}</WaterfallColumnHeader>
          <WaterfallColumnHeader className="tl-chart" aria-label="Timeline" ref={chart}>
            {axis
              .filter((tick) => (tick.at - start) / span < 0.93)
              .map((tick) => (
                <span key={tick.at} className="tl-tick" style={{ left: at(tick.at) }}>
                  {tick.label}
                </span>
              ))}
            {now !== undefined && now <= end && <span className="tl-now-cap" style={{ left: at(now) }} />}
          </WaterfallColumnHeader>
          <WaterfallColumnHeader className="tl-total">{headers.total}</WaterfallColumnHeader>
        </WaterfallHeaderRow>

        <div className="tl-body" role="presentation" style={{ height: visible.length * rowHeight }}>
          <div className="tl-overlay" aria-hidden="true">
            {axis.map((tick) => (
              <span key={tick.at} className="tl-gridline" style={{ left: at(tick.at) }} />
            ))}
            {now !== undefined && now <= end && <span className="tl-now" style={{ left: at(now) }} />}
          </div>
          {windowed && first > 0 && <div role="presentation" style={{ height: first * rowHeight }} />}
          {shown.map((entry) => {
            const row = byId.get(entry.item.value)
            if (!row) return null
            return (
              <TimelineRow
                key={row.id}
                row={row}
                level={entry.level}
                rowIndex={entry.index}
                rail={rail}
                top={tops[entry.index] ?? ""}
                rowHeight={rowHeight}
                start={start}
                span={span}
                now={now}
                fine={fine}
              />
            )
          })}
        </div>
        <div className="tl-row tl-probe" ref={probe} role="presentation" aria-hidden="true" />
      </Waterfall>
      {rows.length === 0 && <div className="empty">{empty ?? "Nothing to show."}</div>}
    </div>
  )
}

type RowProps = { row: Row; level: number; rowIndex: number; rail?: RailSpec; top: string; rowHeight: number; start: number; span: number; now?: number; fine: number }

const TimelineRow = memo(function TimelineRow({ row, level, rowIndex, rail, top, rowHeight, start, span, now, fine }: RowProps) {
  const bars = useMemo(() => (row.bars.length > 24 ? mergeBars(row.bars, fine, now ?? 0) : row.bars.map((bar) => ({ ...bar, count: 1 }))), [row.bars, fine, now])
  const left = (time: number) => ((time - start) / span) * 100
  return (
    <WaterfallRow value={row.id} className="tl-row" aria-rowindex={rowIndex + 2} data-tone={row.tone} data-mark={row.mark}>
      <WaterfallRowHeader className="tl-name">
        {rail && <RailCell rail={rail} id={level === 1 ? top : null} top={top} height={rowHeight} mark={row.mark} />}
        <span className="tl-indent" style={{ width: (level - 1) * 18 }} />
        {row.children?.length ? (
          <WaterfallIndicator className="tl-toggle" aria-label={`Show what is inside ${row.name}`}>
            <ChevronRight size={13} />
          </WaterfallIndicator>
        ) : (
          <span className="tl-toggle" />
        )}
        {(!rail || level > 1) && <MarkIcon mark={row.mark} />}
        <span className="tl-label">{row.name}</span>
        {row.tag && <span className="chip" data-tone="stale">{row.tag}</span>}
        {row.note && <span className="tl-note truncate">{row.note}</span>}
      </WaterfallRowHeader>
      <WaterfallCell className="tl-progress num">{row.progress ?? <span className="faint">–</span>}</WaterfallCell>
      <WaterfallBar className="tl-chart">
        {bars.map((bar) => {
          const from = left(bar.start)
          const to = left(bar.end ?? now ?? bar.start)
          if (to < 0 || from > 100) return null
          const style = { left: `${Math.max(0, from)}%`, width: `${Math.max(0, Math.min(100, to) - Math.max(0, from))}%` }
          const title = bar.count > 1 ? `${bar.count} tasks` : bar.title
          const props = { className: "tl-bar", "data-kind": bar.kind, "data-tone": bar.tone, "data-open": bar.end === null ? "" : undefined, "data-loud": bar.loud ? "" : undefined, style, title }
          return bar.href ? <Link key={bar.key} href={bar.href} tabIndex={-1} aria-label={title} {...props} /> : <span key={bar.key} {...props} />
        })}
        {row.planned?.map((plan) => {
          const where = left(plan.at)
          return where >= 0 && where <= 100 ? <span key={plan.at} className="tl-planned" style={{ left: `${where}%` }} title={plan.title} /> : null
        })}
      </WaterfallBar>
      {/* A plain grid cell: the Waterfall keys its cell part by row, so a second one in a row warns. Its arrow keys still reach this one, since they walk the row's gridcells. */}
      <div role="gridcell" tabIndex={-1} className="tl-total num">
        {row.total}
      </div>
    </WaterfallRow>
  )
})

/** One row's slice of the rail. A top-level row draws its node and the lines
 * that reach it; a row under it only lets the lines pass. */
function RailCell({ rail, id, top, height, mark }: { rail: RailSpec; id: string | null; top: string; height: number; mark: Mark }) {
  const own = rail.rows.get(id ?? top)
  if (!own) return <span className="tl-rail" />
  const x = (lane: number) => lane * LANE + LANE / 2
  const mid = height / 2
  const state = (owner: string) => rail.state?.get(owner) ?? "done"
  const lines: ReactNode[] = []
  const line = (key: string, owner: string, d: string) => lines.push(<path key={key} d={d} data-state={state(owner)} data-faint={rail.faint?.has(owner) ? "" : undefined} />)
  for (const t of own.through) line(`t${t.lane}`, t.owner, `M${x(t.lane)} 0V${height}`)
  if (id === null) {
    // under an open row: every line that continues past it
    for (const t of own.taps) if (!t.ends) line(`c${t.lane}`, t.owner, `M${x(t.lane)} 0V${height}`)
    if (own.starts) line("own", top, `M${x(own.lane)} 0V${height}`)
  } else {
    for (const t of own.taps) {
      const from = x(t.lane)
      const to = x(own.lane)
      if (!t.ends) line(`c${t.lane}`, t.owner, `M${from} 0V${height}`)
      if (t.lane === own.lane) line(`i${t.lane}`, t.owner, `M${from} 0V${mid}`)
      else {
        const bend = Math.sign(to - from) * 5
        line(`i${t.lane}`, t.owner, `M${from} 0V${mid - 5}Q${from} ${mid} ${from + bend} ${mid}H${to}`)
      }
    }
    if (own.starts) line("own", id, `M${x(own.lane)} ${mid}V${height}`)
  }
  return (
    <span className="tl-rail">
      <svg width={rail.lanes * LANE + 6} height={height} aria-hidden="true">
        <g className="tl-rail-lines">{lines}</g>
        {id !== null && (
          <g transform={`translate(${x(own.lane) - 7} ${mid - 7})`}>
            <MarkShape mark={mark} />
          </g>
        )}
      </svg>
    </span>
  )
}

export function MarkIcon({ mark }: { mark: Mark }) {
  return (
    <svg className="tl-mark-icon" width={14} height={14} aria-hidden="true">
      <MarkShape mark={mark} />
    </svg>
  )
}

/** A 14×14 marker. Solid: finished. Ring: running. Outline: not started. */
function MarkShape({ mark }: { mark: Mark }) {
  if (mark === "none") return null
  return (
    <g className="tl-mark" data-mark={mark}>
      <circle className="tl-mark-plate" cx={7} cy={7} r={6.5} />
      {mark === "source" ? (
        <rect x={3.4} y={3.4} width={7.2} height={7.2} transform="rotate(45 7 7)" className="tl-mark-outline" />
      ) : mark === "running" ? (
        <circle cx={7} cy={7} r={4.2} className="tl-mark-ring" pathLength={100} />
      ) : mark === "done" || mark === "failed" ? (
        <circle cx={7} cy={7} r={4} className="tl-mark-solid" />
      ) : mark === "stale" ? (
        <>
          <circle cx={7} cy={7} r={3.6} className="tl-mark-outline" />
          <path d="M7 3.4a3.6 3.6 0 0 0 0 7.2z" className="tl-mark-solid" />
        </>
      ) : mark === "waiting" ? (
        <>
          <circle cx={7} cy={7} r={3.6} className="tl-mark-outline" />
          <circle cx={7} cy={7} r={1.3} className="tl-mark-solid" />
        </>
      ) : mark === "skipped" ? (
        <>
          <circle cx={7} cy={7} r={3.6} className="tl-mark-outline" />
          <path d="M4.6 9.4l4.8-4.8" className="tl-mark-outline" />
        </>
      ) : (
        <circle cx={7} cy={7} r={3.6} className="tl-mark-outline" />
      )}
    </g>
  )
}

/** The time now, refreshed while `live`: bars in progress grow with it. */
export function useNow(live: boolean, every = 500): number {
  const [now, setNow] = useState(() => Date.now() / 1000)
  useEffect(() => {
    if (!live) return
    setNow(Date.now() / 1000)
    const timer = setInterval(() => setNow(Date.now() / 1000), every)
    return () => clearInterval(timer)
  }, [live, every])
  return now
}
