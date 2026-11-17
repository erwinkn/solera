import type { ReactNode } from "react"
import type { RailRow } from "../model/graph"
import type { Tone } from "../model/status"
import type { Mark } from "../ui/status"

/* What the timeline draws. The plan is the rows: every step is there before
 * it runs. The outcome is each row's mark, progress and bars. */

export type Bar = {
  key: string
  /** Epoch seconds. `end` null: still going, drawn up to now. */
  start: number
  end: number | null
  tone: Tone
  /** work: time spent running. wait: a thin line, time spent queued or held.
   * span: the light extent of a group whose members are drawn below it. */
  kind: "work" | "wait" | "span"
  /** Stands out: the slowest of its siblings. */
  loud?: boolean
  title?: string
  href?: string
}

export type Row = {
  id: string
  /** The node of the rail this row is, when its id is not the node's name. */
  node?: string
  mark: Mark
  name: string
  /** The name is an id or a key, not a word: set in the monospace face. */
  code?: boolean
  /** Muted text after the name: what it wrote, where it ran, why it waits. */
  note?: ReactNode
  tag?: string
  progress?: ReactNode
  /** Colours the name and progress: a failed row reads as failed at a glance. */
  tone?: Tone
  bars: Bar[]
  total?: string
  children?: Row[]
}

export type RailState = "done" | "running" | "failed" | "todo"

export type RailSpec = {
  rows: Map<string, RailRow>
  lanes: number
  /** How each node's outgoing line is drawn: solid once the node has run. */
  state?: Map<string, RailState>
  /** Nodes whose line is drawn lightly: nothing reads their data, they only list partitions. */
  faint?: Set<string>
}
