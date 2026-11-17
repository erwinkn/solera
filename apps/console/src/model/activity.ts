import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query"
import { useContext } from "react"
import { get, query } from "../api/client"
import { ProjectContext } from "../api/queries"
import type { TaskRow } from "../api/types"
import { TERMINAL_TASK } from "./status"

/* The tasks of the last while, for the project and asset timelines. A finished
 * task never changes, so each poll asks only for what is new: the tasks made
 * since the last poll, the ones still in progress, and any that finished in
 * between. The window is a cache of history, rebuilt from scratch on load. */

type Activity = { tasks: TaskRow[]; fetchedAt: number }
type Page = { tasks: TaskRow[]; next: string | null }

const MAX_PAGES = 12 // 12,000 tasks: past that the window is drawn from what was read

export function useActivity(windowSeconds: number, asset?: string, every = 2000) {
  const base = `/api/projects/${encodeURIComponent(useContext(ProjectContext))}`
  const client = useQueryClient()
  const key = [base, "activity", windowSeconds, asset ?? null]
  return useQuery({
    queryKey: key,
    refetchInterval: every,
    placeholderData: keepPreviousData,
    queryFn: async ({ signal }): Promise<Activity> => {
      const before = client.getQueryData<Activity>(key)
      const now = Date.now() / 1000
      const edge = now - windowSeconds
      const tasks = new Map(before?.tasks.map((task) => [task.id, task]))
      const seen = new Set<string>()
      const read = async (fields: Record<string, string | number | string[] | undefined>) => {
        let cursor: string | undefined
        for (let page = 0; page < MAX_PAGES; page++) {
          const found = await get<Page>(`${base}/tasks${query({ ...fields, asset, before: cursor, limit: 1000 })}`, signal)
          for (const task of found.tasks) {
            tasks.set(task.id, task)
            seen.add(task.id)
          }
          if (!found.next) break
          cursor = found.next
        }
      }
      // a task is listed by when its run was made, so reach a little before the edge
      await read({ since: before ? Math.max(edge, before.fetchedAt - 20) : edge - 120 })
      await read({ status: ["queued", "running"] })
      // in progress last time and in neither answer: it finished meanwhile, in a run made earlier
      const settled = new Set<string>()
      for (const task of tasks.values()) if (!TERMINAL_TASK.has(task.status) && !seen.has(task.id)) settled.add(task.run)
      for (const run of [...settled].slice(0, 20)) await read({ run })
      for (const [id, task] of tasks)
        if (TERMINAL_TASK.has(task.status) ? (task.finished_at ?? task.created_at) < edge - 120 : !seen.has(id)) tasks.delete(id)
      return { tasks: [...tasks.values()], fetchedAt: now }
    },
  })
}

/** When a task's work began and ended, as far as the list says: a task in
 * progress has no start recorded yet, so it began once its wait was over. */
export function taskSpan(task: TaskRow, now: number): { start: number; end: number; live: boolean } {
  const live = !TERMINAL_TASK.has(task.status)
  const start = task.started_at ?? task.created_at + (task.wait ?? 0)
  const end = live ? now : (task.finished_at ?? start + (task.duration ?? 0))
  return { start, end: Math.max(end, start), live }
}
