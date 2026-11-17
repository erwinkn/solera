import type { Tone } from "../model/status"

/* The one mark the console puts beside anything that has a state: a row of a
 * table, a step of the timeline, a line of a list. Its shape says as much as
 * its colour, so it reads without colour too. */

/** Solid: finished. Ring: running. Outline: not started. */
export type Mark = "done" | "running" | "failed" | "todo" | "waiting" | "skipped" | "stale" | "source" | "none"

export const TONE_MARK: Record<Tone, Mark> = { ok: "done", running: "running", failed: "failed", stale: "stale", waiting: "waiting", idle: "todo" }

export function MarkIcon({ mark }: { mark: Mark }) {
  return (
    <svg className="mark-icon" width={14} height={14} aria-hidden="true">
      <MarkShape mark={mark} />
    </svg>
  )
}

/** The mark alone, 14 by 14, for drawing inside another SVG. */
export function MarkShape({ mark }: { mark: Mark }) {
  if (mark === "none") return null
  return (
    <g className="mark" data-mark={mark}>
      {mark === "source" ? (
        <rect x={4} y={4} width={6} height={6} transform="rotate(45 7 7)" className="mark-outline" />
      ) : mark === "running" ? (
        <circle cx={7} cy={7} r={3.6} className="mark-ring" pathLength={100} />
      ) : mark === "done" || mark === "failed" ? (
        <circle cx={7} cy={7} r={3.5} className="mark-solid" />
      ) : mark === "stale" ? (
        <>
          <circle cx={7} cy={7} r={3.1} className="mark-outline" />
          <path d="M7 3.9a3.1 3.1 0 0 0 0 6.2z" className="mark-solid" />
        </>
      ) : mark === "waiting" ? (
        <>
          <circle cx={7} cy={7} r={3.1} className="mark-outline" />
          <circle cx={7} cy={7} r={1.1} className="mark-solid" />
        </>
      ) : mark === "skipped" ? (
        <>
          <circle cx={7} cy={7} r={3.1} className="mark-outline" />
          <path d="M5 9l4-4" className="mark-outline" />
        </>
      ) : (
        <circle cx={7} cy={7} r={3.1} className="mark-outline" />
      )}
    </g>
  )
}
