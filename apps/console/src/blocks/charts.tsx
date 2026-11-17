import { useState } from "react"
import { useAttemptResults, useManifest } from "../api/queries"
import type { RunDetail } from "../api/types"
import { isLive, TERMINAL_TASK } from "../model/status"
import { runChart, type RunChart } from "../timeline/run"
import { Timeline, useNow } from "../timeline/Timeline"
import { Checkmark } from "../ui/controls"

/* A run on the timeline graph, and the state around it. */

export type RunView = { chart: RunChart; live: boolean; expanded: string[]; setExpanded: (ids: string[]) => void; onlyTrouble: boolean; setOnlyTrouble: (on: boolean) => void; now: number }

/** A run's chart and the state around it, so a page can put the controls where it likes. */
export function useRunView(detail: RunDetail): RunView {
  const manifest = useManifest()
  // a canceled run has ended while the tasks it was running still wind down
  const live = isLive(detail.request) || detail.tasks.some((task) => !TERMINAL_TASK.has(task.status))
  const now = useNow(live)
  const [expanded, setExpanded] = useState<string[] | null>(null)
  const [onlyTrouble, setOnlyTrouble] = useState(false)
  const [wanted, setWanted] = useState<string[]>([])
  // the keys of the per-key attempts whose rows are open, one request each
  const results = useAttemptResults(detail.request.id, wanted)
  const chart = runChart(detail, manifest.data, now, results, { onlyTrouble, expanded })
  if (chart.wanted.join() !== wanted.join()) setWanted(chart.wanted)
  return { chart, live, expanded: expanded ?? chart.open, setExpanded, onlyTrouble, setOnlyTrouble, now }
}

export function RunTimeline({ view, selected, onSelect, maxHeight }: { view: RunView; selected: string | null; onSelect: (id: string | null) => void; maxHeight?: string }) {
  const { chart } = view
  return (
    <Timeline
      label="The run's tasks, and when each ran"
      rows={chart.rows}
      range={chart.range}
      origin={chart.origin}
      now={view.live ? view.now : undefined}
      rail={chart.rail}
      selected={selected}
      onSelect={onSelect}
      expanded={view.expanded}
      onExpandedChange={view.setExpanded}
      headers={{ name: "Step", progress: "Progress", total: "Duration" }}
      maxHeight={maxHeight}
    />
  )
}

export function TroubleFilter({ view }: { view: RunView }) {
  if (view.chart.counts.total < 13) return null
  return (
    <Checkmark checked={view.onlyTrouble} onChange={view.setOnlyTrouble}>
      Only failed or unfinished
    </Checkmark>
  )
}
