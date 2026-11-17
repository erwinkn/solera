import { Ban, Pause, Play, RotateCcw, Trash2 } from "lucide-react"
import { useEffect, useRef, useState } from "react"
import { Link, useLocation } from "wouter"
import { useAttemptLogs, useAttemptResult, useDeleteRun, useRunCommand, useRunEvents } from "../api/queries"
import { PHASES, type Attempt, type RunDetail, type RunRequest, type Task } from "../api/types"
import { bytes, clock, duration, list, partitionLabel, plural, shortId, when } from "../model/format"
import { attemptStatus, heldReason, isLive, keyStatus, origin, taskStatus, TERMINAL_TASK } from "../model/status"
import { parseRowId, type Picked } from "../timeline/run"
import { Button, Disclosure, Segmented } from "../ui/controls"
import { confirm, report } from "../ui/overlays"
import { AssetLink, Empty, Facts, Loading, Notice, StatusPill } from "./atoms"
import type { RunView } from "./charts"
import { openStartRun } from "./start-run"

/* The pieces of a run's page: what it is, what can be done to it, and the
 * details of whichever step is picked on its chart. */

export function runTitle(run: RunRequest): string {
  if (run.source) return `Commit to ${run.source}`
  return list(run.targets, 2)
}

export function runSentence(run: RunRequest, view: RunView | null): string {
  const c = view?.chart.counts
  const parts = [origin({ origin: "", automation: run.automation ?? null, by: run.by ?? null, retry_of: run.retry_of ?? null, source: run.source ?? null })]
  parts.push(`at ${clock(run.created_at)}`)
  if (!isLive(run)) parts.push(`took ${duration(run.updated_at - run.created_at)}`)
  let text = `${parts.join(", ")}.`
  if (c && c.total) {
    const bits = [`${c.done} of ${plural(c.total, "task")} done`]
    if (c.failed) bits.push(`${c.failed} failed`)
    if (c.canceled) bits.push(`${c.canceled} canceled`)
    if (c.running) bits.push(`${c.running} running`)
    if (c.waiting) bits.push(`${c.waiting} waiting`)
    text += ` ${bits.join(", ")}.`
  }
  return text
}

export function RunActions({ run, size = "m" }: { run: RunRequest; size?: "s" | "m" }) {
  const command = useRunCommand()
  const remove = useDeleteRun()
  const [, navigate] = useLocation()
  const live = isLive(run)
  const act = (name: "cancel" | "retry" | "pause" | "resume", done: string) => report(command.mutateAsync({ id: run.id, command: name }), done)
  if (run.source) return null
  return (
    <div className="row wrap">
      {live && (run.paused ? (
        <Button size={size} icon={<Play size={14} />} onClick={() => act("resume", "The run goes on")}>
          Resume
        </Button>
      ) : (
        <Button size={size} icon={<Pause size={14} />} onClick={() => act("pause", "Paused: tasks in flight finish, no new one starts")}>
          Pause
        </Button>
      ))}
      {live && (
        <Button
          size={size}
          variant="danger"
          icon={<Ban size={14} />}
          onClick={async () => {
            if (await confirm({ title: "Cancel this run?", description: "Workers are asked to stop. What they already committed stays.", confirmLabel: "Cancel the run", danger: true }))
              await act("cancel", "Cancel requested")
          }}
        >
          Cancel
        </Button>
      )}
      {!live && (run.status === "failed" || run.status === "canceled") && (
        <Button
          size={size}
          icon={<RotateCcw size={14} />}
          onClick={async () => {
            const next = await report(command.mutateAsync({ id: run.id, command: "retry" }), "A new run retries what did not finish")
            if (next?.id) navigate(`/runs/${next.id}`)
          }}
        >
          Retry what failed
        </Button>
      )}
      {!live && (
        <Button size={size} icon={<Play size={14} />} onClick={() => openStartRun({ targets: run.targets, mode: run.mode === "full" ? "full" : "incremental" })}>
          Run again
        </Button>
      )}
      {!live && (
        <Button
          size={size}
          variant="danger"
          icon={<Trash2 size={14} />}
          onClick={async () => {
            if (!(await confirm({ title: "Delete this run?", description: "Its history and logs go for good. What it built stays.", confirmLabel: "Delete", danger: true }))) return
            if ((await report(remove.mutateAsync(run.id), "Run deleted")) !== undefined) navigate("/runs")
          }}
        >
          Delete
        </Button>
      )}
    </div>
  )
}

export function RunFacts({ run }: { run: RunRequest }) {
  const partitions = typeof run.partitions === "string" ? run.partitions : run.partitions ? Object.values(run.partitions).flat().map(partitionLabel).filter(Boolean) : []
  const now = Date.now() / 1000
  return (
    <Facts
      rows={[
        ["Started by", origin({ origin: "", automation: run.automation ?? null, by: run.by ?? null, retry_of: null, source: run.source ?? null })],
        ["Retry of", run.retry_of ? <Link href={`/runs/${run.retry_of}`} className="ref">{shortId(run.retry_of)}</Link> : null],
        ["Started", when(run.created_at, now)],
        ["Duration", isLive(run) ? null : duration(run.updated_at - run.created_at)],
        ["Assets", run.source ? null : <span className="row wrap">{run.targets.map((t) => <AssetLink key={t} name={t} />)}</span>],
        ["Partitions", typeof partitions === "string" ? partitions : partitions.length ? list(partitions, 6) : null],
        ["Mode", run.mode === "full" ? "Full: every key again" : run.mode === "commit" ? null : "Incremental: only what changed"],
        ["Added keys", run.upserted?.length ? list(run.upserted, 8) : null],
        ["Removed keys", run.deleted?.length ? list(run.deleted, 8) : null],
        ["Version", run.version ?? null],
        ["Tags", Object.keys(run.tags ?? {}).length ? Object.entries(run.tags!).map(([k, v]) => `${k}=${v}`).join(", ") : null],
        ["Run", <span className="mono">{run.id}</span>],
      ]}
    />
  )
}

/* -- the inspector -------------------------------------------------------------- */

export function RunInspector({ detail, picked }: { detail: RunDetail; picked: string | null }) {
  const at = parseRowId(picked)
  if (!at) return <RunOverview detail={detail} />
  if (at.kind === "asset") return <GroupPanel detail={detail} asset={at.asset} />
  const task = detail.tasks.find((t) => t.id === at.task)
  if (!task) return <Empty title="That step is not part of this run" />
  return <TaskPanel key={picked} detail={detail} task={task} at={at} />
}

/** Errors of a run's tasks, one line each: what failed, then why. */
function Errors({ tasks, named }: { tasks: Task[]; named: (task: Task) => string }) {
  if (!tasks.length) return null
  return (
    <div>
      <h3 className="subhead">Errors</h3>
      <ul className="errors">
        {tasks.map((task) => (
          <li key={task.id}>
            <span className="name">{named(task)}</span>
            <span className="error-text">{task.error}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

function RunOverview({ detail }: { detail: RunDetail }) {
  const failed = detail.tasks.filter((t) => t.error)
  return (
    <div className="stack inspector">
      <RunFacts run={detail.request} />
      <Errors tasks={failed.slice(0, 8)} named={(task) => `${task.asset} ${partitionLabel(task.partition)}`.trim()} />
      {detail.tasks.length > 0 && <p className="faint">Select a step on the timeline to see its attempts and logs.</p>}
    </div>
  )
}

function GroupPanel({ detail, asset }: { detail: RunDetail; asset: string }) {
  const tasks = detail.tasks.filter((t) => t.asset === asset)
  const by = new Map<string, number>()
  for (const task of tasks) by.set(task.status, (by.get(task.status) ?? 0) + 1)
  return (
    <div className="stack inspector">
      <h3 className="inspector-title">{asset}</h3>
      <Facts
        rows={[
          ["Partitions in this run", String(tasks.length)],
          ...[...by.entries()].map(([status, n]): [string, React.ReactNode] => [taskStatus(status).label, String(n)]),
          ["Asset", <AssetLink name={asset} />],
        ]}
      />
      <Errors tasks={tasks.filter((t) => t.error).slice(0, 8)} named={(task) => partitionLabel(task.partition) || asset} />
    </div>
  )
}

function TaskPanel({ detail, task, at }: { detail: RunDetail; task: Task; at: Exclude<Picked, { kind: "asset" }> }) {
  const attempts = detail.attempts[task.id] ?? []
  const preset = at.kind === "task" ? null : at.attempt
  const [chosen, setChosen] = useState<string | null>(preset)
  const attempt = attempts.find((a) => a.id === (chosen ?? preset)) ?? attempts[attempts.length - 1] ?? null
  const status = taskStatus(task.status)
  const live = !TERMINAL_TASK.has(task.status)
  const held = task.status === "queued" ? heldReason(task.held) : task.status === "claimable" ? "No worker has taken it yet. A worker of its pool must be running." : null
  return (
    <div className="stack inspector">
      <div className="row wrap">
        <h3 className="inspector-title">
          {task.asset}
          {task.partition && <span className="mono muted"> {partitionLabel(task.partition)}</span>}
        </h3>
        <StatusPill status={status} />
        <span className="grow" />
        <Link href={`/assets/${encodeURIComponent(task.asset)}`} className="link muted">
          Open the asset
        </Link>
      </div>
      {held && <Notice tone="waiting">{held}</Notice>}
      {task.error && (
        <Notice tone="failed">
          <div className="error-text">{task.error}</div>
        </Notice>
      )}
      {at.kind === "key" && attempt && <KeyCall run={task.run} attempt={attempt.id} name={at.key} asset={task.asset} />}
      {attempts.length > 1 && (
        <Segmented
          label="Which attempt"
          value={attempt?.id ?? ""}
          onChange={setChosen}
          options={attempts.map((a, i) => ({ value: a.id, label: a.batch && a.batch.count > 1 ? `Batch ${a.batch.index + 1}` : `Attempt ${i + 1}` }))}
        />
      )}
      {attempt ? <AttemptPanel run={task.run} task={task} attempt={attempt} count={attempts.length} live={live} /> : <p className="muted">It has not started yet.</p>}
      <Disclosure summary="Everything that happened to this task">
        <Events run={task.run} task={task.id} live={isLive(detail.request)} />
      </Disclosure>
    </div>
  )
}

function AttemptPanel({ run, task, attempt, count, live }: { run: string; task: Task; attempt: Attempt; count: number; live: boolean }) {
  const running = attempt.finished_at === undefined || attempt.finished_at === null
  const now = Date.now() / 1000
  return (
    <div className="stack">
      {attempt.error && attempt.error !== task.error && (
        <Notice tone="failed">
          <div className="error-text">{attempt.error}</div>
        </Notice>
      )}
      <Facts
        rows={[
          ["Outcome", count > 1 ? <StatusPill status={attemptStatus(attempt)} /> : null],
          ["Started", attempt.started_at ? when(attempt.started_at, now) : null],
          ["Duration", attempt.started_at && attempt.finished_at ? duration(attempt.finished_at - attempt.started_at) : null],
          ["Waited", task.wait > 0.05 ? duration(task.wait) : null],
          ["Tries", count > 1 || live ? `${count} of ${task.max_attempts} allowed` : null],
          ["Batch", attempt.batch && attempt.batch.count > 1 ? `${attempt.batch.index + 1} of ${Math.max(attempt.batch.count, attempt.batch.index + 1)}` : null],
          ["Keys in the batch", attempt.batch ? [attempt.batch.added && `${attempt.batch.added} added`, attempt.batch.updated && `${attempt.batch.updated} updated`, attempt.batch.removed && `${attempt.batch.removed} removed`, attempt.batch.unchanged && `${attempt.batch.unchanged} unchanged`].filter(Boolean).join(", ") : null],
          ["Key results", attempt.keys ? Object.entries(attempt.keys).map(([outcome, n]) => `${n} ${keyStatus(outcome).label.toLowerCase()}`).join(", ") : null],
          ["Wrote", attempt.outputs?.length ? attempt.outputs.join(", ") : null],
          ["Ran on", attempt.executor ?? null],
          ["Memory at peak", attempt.peak_memory ? bytes(attempt.peak_memory) : null],
          ["Processor time", attempt.cpu_seconds ? duration(attempt.cpu_seconds) : null],
        ]}
      />
      <PhaseBar attempt={attempt} />
      <Logs run={run} attempt={attempt.id} live={running} />
    </div>
  )
}

const PHASE_WORDS: Record<string, string> = {
  preparing: "Preparing",
  provisioning: "Getting a worker",
  importing: "Importing the project",
  loading: "Loading inputs",
  computing: "Running the asset",
  writing: "Writing outputs",
  settling: "Recording the result",
}

/** Where an attempt's time went, phase by phase. */
export function PhaseBar({ attempt }: { attempt: Attempt }) {
  const phases = PHASES.map((name) => ({ name, seconds: attempt[name] ?? 0 })).filter((p) => p.seconds > 0)
  const total = phases.reduce((sum, p) => sum + p.seconds, 0)
  if (!total) return null
  return (
    <div className="phases">
      <h3 className="subhead">Where the time went</h3>
      <div className="phase-bar" role="img" aria-label={phases.map((p) => `${PHASE_WORDS[p.name]} ${duration(p.seconds)}`).join(", ")}>
        {phases.map((p, i) => (
          <span key={p.name} style={{ width: `${(p.seconds / total) * 100}%`, opacity: 0.35 + (0.65 * (i + 1)) / phases.length }} />
        ))}
      </div>
      <ul className="phase-list">
        {phases.map((p) => (
          <li key={p.name}>
            <span className="muted">
              <i className="phase-key" style={{ opacity: 0.35 + (0.65 * (phases.indexOf(p) + 1)) / phases.length }} />
              {PHASE_WORDS[p.name]}
            </span>
            <span className="num">{duration(p.seconds)}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}

function Logs({ run, attempt, live }: { run: string; attempt: string; live: boolean }) {
  const logs = useAttemptLogs(run, attempt, live)
  const box = useRef<HTMLPreElement>(null)
  const count = logs.data?.length ?? 0
  useEffect(() => {
    if (live && box.current) box.current.scrollTop = box.current.scrollHeight
  }, [count, live])
  return (
    <div>
      <h3 className="subhead">Logs</h3>
      {logs.isLoading ? (
        <Loading />
      ) : !count ? (
        <p className="muted">{logs.isError ? "No logs were kept for this attempt." : "It logged nothing."}</p>
      ) : (
        <pre className="code logs" ref={box} tabIndex={0}>
          {logs.data!.map((line, i) => (
            <div key={i} data-level={line.level}>
              <span className="faint">{line.at ? clock(line.at) : ""} </span>
              {line.message}
              {line.fields && Object.keys(line.fields).length > 0 && <span className="muted"> {Object.entries(line.fields).map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`).join(" ")}</span>}
            </div>
          ))}
        </pre>
      )}
    </div>
  )
}

function KeyCall({ run, attempt, name, asset }: { run: string; attempt: string; name: string; asset: string }) {
  const result = useAttemptResult(run, attempt)
  const call = result.data?.key_outcomes?.find((k) => k.key === name)
  return (
    <div className="stack well">
      <h3 className="subhead">This key in this batch</h3>
      <Facts
        rows={[
          ["Key", <span className="mono">{name}</span>],
          ["Result", call ? <StatusPill status={keyStatus(call.outcome)} /> : null],
          ["Its call took", call ? duration(call.duration) : null],
          ["History", <Link href={`/keys?asset=${encodeURIComponent(asset)}&key=${encodeURIComponent(name)}`} className="link">Everything that happened to this key</Link>],
        ]}
      />
      {call?.error && <div className="error-text">{call.error}</div>}
    </div>
  )
}

const EVENT_WORDS: Record<string, string> = {
  submitted: "The run was asked for",
  ready: "Ready to run",
  held: "Held back",
  claimed: "Got its turn",
  launched: "Sent to a worker",
  booted: "The worker started",
  imported: "The project was imported",
  loaded: "Inputs loaded",
  computing: "The asset started running",
  computed: "The asset returned",
  writing: "Writing outputs",
  stored: "Output stored",
  finished: "The worker reported back",
  committed: "The output was committed",
  succeeded: "Succeeded",
  failed: "Failed",
  canceled: "Canceled",
  skipped: "Nothing to do",
  outage: "The engine was down for a while",
  paused: "Paused",
  resumed: "Resumed",
  retrying: "Will try again",
}

export function Events({ run, task, live }: { run: string; task?: string; live: boolean }) {
  const events = useRunEvents(run, true, live)
  if (events.isLoading) return <Loading />
  const rows = (events.data ?? []).filter((e) => !task || e.task === task)
  if (!rows.length) return <p className="muted">Nothing was recorded.</p>
  const first = rows[0]!.at
  return (
    <ol className="events">
      {rows.map((event) => (
        <li key={event.n}>
          <span className="num faint">+{duration(event.at - first)}</span>
          <span>
            {EVENT_WORDS[event.type] ?? event.type}
            {(event.reason || event.name) && <span className="muted"> {[event.name, event.reason].filter(Boolean).join(", ")}</span>}
          </span>
        </li>
      ))}
    </ol>
  )
}
