import type { PartitionRow, PartitionStatus } from "../api/types"

/* The partitions a run can be started on, across the assets picked for it. */

/** The statuses of a partition that can be run, the one that most needs attention first. A removed partition cannot be run. */
export const RUNNABLE: readonly PartitionStatus[] = ["failed", "stale", "missing", "running", "materialized"]

export type Runnable = { partition: string; status: PartitionStatus }

const natural = new Intl.Collator(undefined, { numeric: true })

/** Every partition of `lists` once, in the order of its name. One that several
 * assets share has the status that most needs attention among them. */
export function runnable(lists: PartitionRow[][]): Runnable[] {
  const rank = new Map<string, number>()
  for (const list of lists)
    for (const row of list) {
      const at = RUNNABLE.indexOf(row.status)
      if (at >= 0 && at < (rank.get(row.partition) ?? RUNNABLE.length)) rank.set(row.partition, at)
    }
  return [...rank].map(([partition, at]) => ({ partition, status: RUNNABLE[at]! })).sort((a, b) => natural.compare(a.partition, b.partition))
}
