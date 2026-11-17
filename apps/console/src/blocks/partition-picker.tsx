import { createListboxController, Listbox, ListboxItem, ListboxItemIndicator } from "@ui-sdk/react/listbox"
import { Check, Search } from "lucide-react"
import { useLayoutEffect, useMemo, useRef, useState } from "react"
import type { PartitionStatus } from "../api/types"
import { count, partitionLabel } from "../model/format"
import { RUNNABLE, type Runnable } from "../model/partitions"
import { partitionStatus } from "../model/status"
import { Button } from "../ui/controls"
import { MarkIcon, TONE_MARK } from "../ui/status"

/* Pick partitions from a list: narrow it by status and by name, then tick
 * rows one at a time or all that are shown. Only the rows in view are drawn,
 * so it stays quick with thousands. */

/** How many rows show at once. */
const ROWS = 6
/** Rows drawn beyond each edge of the view, so a scroll does not show a gap. */
const SPARE = 4

type Props = { /** At least one. */ rows: Runnable[]; selected: ReadonlySet<string>; onChange: (selected: Set<string>) => void }

export function PartitionPicker({ rows, selected, onChange }: Props) {
  const [text, setText] = useState("")
  const [pressed, setPressed] = useState<PartitionStatus[]>([])
  const [scroll, setScroll] = useState(0)
  const [rowHeight, setRowHeight] = useState(0)
  const view = useRef<HTMLDivElement>(null)
  const [list] = useState(() => createListboxController<string, true>())

  const needle = text.trim().toLowerCase()
  const named = useMemo(() => (needle ? rows.filter((row) => row.partition.toLowerCase().includes(needle) || partitionLabel(row.partition).toLowerCase().includes(needle)) : rows), [rows, needle])
  // A status is offered while a partition has it, and counts the ones the name leaves.
  const statuses = RUNNABLE.filter((status) => rows.some((row) => row.status === status))
  const only = pressed.filter((status) => statuses.includes(status))
  const shown = useMemo(() => (only.length ? named.filter((row) => only.includes(row.status)) : named), [named, only.join()])
  const items = useMemo(() => shown.map((row) => ({ value: row.partition, label: partitionLabel(row.partition) })), [shown])
  const ticked = shown.filter((row) => selected.has(row.partition)).map((row) => row.partition)
  const total = rows.filter((row) => selected.has(row.partition)).length
  const visible = Math.min(rows.length, ROWS)

  // The view is `visible` rows tall, whatever a row measures in this theme.
  useLayoutEffect(() => {
    if (view.current) setRowHeight(view.current.clientHeight / visible)
  }, [visible])
  // A filter starts the list from its top again.
  useLayoutEffect(() => {
    if (view.current) view.current.scrollTop = 0
  }, [needle, only.join()])

  const first = rowHeight ? Math.max(0, Math.floor(scroll / rowHeight) - SPARE) : 0
  const last = rowHeight ? Math.min(shown.length, Math.ceil(scroll / rowHeight) + visible + SPARE) : 0
  /** Replace what is ticked among the rows shown, and keep what a filter hides. */
  const tick = (next: string[]) => {
    const hidden = new Set(selected)
    for (const row of shown) hidden.delete(row.partition)
    onChange(new Set([...hidden, ...next]))
  }

  return (
    <div className="picker">
      <label className="picker-search">
        <Search size={14} className="faint" aria-hidden="true" />
        <input
          aria-label="Filter partitions"
          placeholder="Filter"
          spellCheck={false}
          value={text}
          onChange={(event) => setText(event.currentTarget.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") event.preventDefault() // typing a name must not start the run
            if (event.key === "ArrowDown" && shown.length) {
              event.preventDefault()
              list.focus()
            }
          }}
        />
      </label>
      <div className="picker-facets" role="group" aria-label="Filter by status">
        {statuses.map((status) => {
          const { tone, label } = partitionStatus(status)
          const on = only.includes(status)
          return (
            <button key={status} type="button" className="facet" aria-pressed={on} onClick={() => setPressed(on ? only.filter((s) => s !== status) : [...only, status])}>
              <MarkIcon mark={TONE_MARK[tone]} />
              {label}
              <span className="facet-count num">{count(named.filter((row) => row.status === status).length)}</span>
            </button>
          )
        })}
      </div>
      <div ref={view} className="picker-view" style={{ ["--rows" as string]: visible }} onScroll={(event) => setScroll(event.currentTarget.scrollTop)}>
        {shown.length === 0 ? (
          <p className="picker-empty muted">No partition matches</p>
        ) : (
          <Listbox
            multiple
            aria-label="Partitions"
            className="picker-list"
            style={{ height: shown.length * rowHeight }}
            controller={list}
            items={items}
            value={ticked}
            onValueChange={tick}
            keepInView={false}
            onHighlight={(_, { index }) => {
              // bring the row the keys moved to into view: it may not be drawn yet
              const el = view.current
              if (!el || index < 0) return
              const top = index * rowHeight
              if (top < el.scrollTop) el.scrollTop = top
              else if (top + rowHeight > el.scrollTop + el.clientHeight) el.scrollTop = top + rowHeight - el.clientHeight
            }}
          >
            {() =>
              shown.slice(first, last).map((row, offset) => {
                const status = partitionStatus(row.status)
                return (
                  <ListboxItem key={row.partition} value={row.partition} label={partitionLabel(row.partition)} index={first + offset} className="picker-row" style={{ top: (first + offset) * rowHeight }}>
                    <span className="check-box" aria-hidden="true">
                      <ListboxItemIndicator>
                        <Check size={12} strokeWidth={3} />
                      </ListboxItemIndicator>
                    </span>
                    <MarkIcon mark={TONE_MARK[status.tone]} />
                    <span className="mono truncate">{partitionLabel(row.partition)}</span>
                    <span className="sr-only">{status.label}</span>
                  </ListboxItem>
                )
              })
            }
          </Listbox>
        )}
      </div>
      <div className="picker-foot">
        <span className="num grow" role="status">
          {count(total)} of {count(rows.length)} selected
        </span>
        {ticked.length < shown.length && (
          <Button type="button" size="s" variant="quiet" onClick={() => tick(shown.map((row) => row.partition))}>
            {shown.length < rows.length ? "Select shown" : "Select all"}
          </Button>
        )}
        {total > 0 && (
          <Button type="button" size="s" variant="quiet" onClick={() => onChange(new Set())}>
            Clear
          </Button>
        )}
      </div>
    </div>
  )
}
