import { Play, RotateCcw, Search } from "lucide-react"
import { useState } from "react"
import { Link, useLocation } from "wouter"
import { useAsset, useAutomationCommand, useCommits, useManifest, useOutcomes, usePartitions, useRetryKeys, useStaleKeys, useStatuses } from "../api/queries"
import type { AssetSpec, AssetStatus, Automation, Manifest, PartitionRow, Reason } from "../api/types"
import { count, duration, partitionLabel, plural, until } from "../model/format"
import { projectGraph } from "../model/graph"
import { assetStatus, FAILING_KEYS, keyStatus, partitionStatus, triggerText, type Tone } from "../model/status"
import { Button, TextField, Tip, Toggle } from "../ui/controls"
import { ActionMenu, PickMany, report } from "../ui/overlays"
import { Ago, AssetLink, Dot, Empty, Facts, Failure, Loading, Notice, RunLink, SourceLink, StatusPill } from "./atoms"
import { openStartRun } from "./start-run"

/* Assets: the list, and the parts of one asset's page. */

export function reasonText(reason: Reason): string {
  const input = reason.input ? ` (${reason.input})` : ""
  switch (reason.kind) {
    case "input changed":
      return `An input changed${input}`
    case "upstream stale":
      return `Something it reads is stale${input}`
    case "definition changed":
      return "Its definition changed"
    case "upstream incomplete":
      return `Something it reads is in the middle of a full run${input}`
    default:
      return `${reason.kind}${input}`
  }
}

export function failingKeys(status: AssetStatus | undefined): number {
  return Object.values(status?.stored_counts ?? {}).reduce((a, b) => a + b, 0)
}

export function firstLine(doc: string | null | undefined): string {
  if (!doc) return ""
  const text = doc.replace(/\s+/g, " ").trim()
  const end = text.indexOf(". ")
  return end > 0 ? text.slice(0, end + 1) : text
}

function automationText(spec: AssetSpec, manifest: Manifest): string {
  const own = Object.values(manifest.automations).filter((a) => spec.automations.includes(a.name))
  if (!own.length) return "By hand"
  return own.map((a) => triggerText(a.trigger).replace(/ \(.*\)$/, "")).join(", ")
}

export function AssetsTable({ filter, selected, dense }: { filter?: string; selected?: string; dense?: boolean }) {
  const manifest = useManifest()
  const statuses = useStatuses()
  const [, navigate] = useLocation()
  const now = Date.now() / 1000
  if (manifest.error) return <Failure error={manifest.error} />
  if (!manifest.data) return <Loading what="Reading the project" />
  const names = Object.keys(manifest.data.assets).filter((name) => !filter || name.includes(filter.toLowerCase()))
  if (!names.length) return <Empty title="No asset matches" />
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>Asset</th>
            {!dense && <th>Description</th>}
            <th>Status</th>
            {!dense && <th>Partitions up to date</th>}
            {!dense && <th>Starts</th>}
            <th className="end">Last run</th>
          </tr>
        </thead>
        <tbody>
          {names.map((name) => {
            const spec = manifest.data!.assets[name]!
            const status = statuses.data?.assets[name]
            const state = assetStatus(status)
            const keys = failingKeys(status)
            const note = keys && state.tone === "ok" ? `${plural(keys, "key")} failing` : state.tone !== "ok" ? state.short : ""
            return (
              <tr key={name} data-link="" data-selected={name === selected ? "" : undefined} onClick={() => navigate(`/assets/${encodeURIComponent(name)}`)}>
                <td className="fit">
                  <Link href={`/assets/${encodeURIComponent(name)}`} className="name" onClick={(event) => event.stopPropagation()}>
                    {name}
                  </Link>
                </td>
                {!dense && <td className="wide"><div className="muted truncate">{firstLine(spec.doc)}</div></td>}
                <td className="fit">
                  <span className="row">
                    <StatusPill status={state} />
                    {note && note !== state.label.toLowerCase() && <span className="muted">{note}</span>}
                  </span>
                </td>
                {!dense && <td className="fit">{status?.partitioned ? <PartitionMeter status={status} /> : <span className="faint">Not partitioned</span>}</td>}
                {!dense && <td className="muted fit">{automationText(spec, manifest.data!)}</td>}
                <td className="end muted fit">{status?.last ? <Ago at={status.last.at} now={now} /> : <span className="faint">never</span>}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

/** The sources assets read, and who reads each. */
export function SourcesTable() {
  const manifest = useManifest()
  const [, navigate] = useLocation()
  if (!manifest.data) return <Loading />
  const names = Object.keys(manifest.data.sources)
  if (!names.length) return <Empty title="The project reads no source" />
  const edges = projectGraph(manifest.data).edges
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>Source</th>
            <th>Kind</th>
            <th>Read by</th>
          </tr>
        </thead>
        <tbody>
          {names.map((name) => {
            const readers = edges.filter((edge) => edge.from === name).map((edge) => edge.to)
            return (
              <tr key={name} data-link="" onClick={() => navigate(`/sources/${encodeURIComponent(name)}`)}>
                <td className="fit">
                  <Link href={`/sources/${encodeURIComponent(name)}`} className="name" onClick={(event) => event.stopPropagation()}>
                    {name}
                  </Link>
                </td>
                <td className="muted fit">{manifest.data!.sources[name]!.key ? "A set of keys" : "One version"}</td>
                <td className="muted">{readers.length ? readers.join(", ") : "Nothing"}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const METER: [keyof AssetStatus["partitions"], Tone][] = [["materialized", "ok"], ["running", "running"], ["stale", "stale"], ["failed", "failed"], ["missing", "idle"]]

export function PartitionMeter({ status }: { status: AssetStatus }) {
  const p = status.partitions
  return (
    <div className="row partition-meter">
      <span className="num">
        {count(p.materialized)} of {count(p.total)}
      </span>
      <div className="meter" role="img" aria-label={`${p.materialized} of ${p.total} partitions up to date`}>
        {METER.map(([name, tone]) => (p[name] && name !== "missing" ? <span key={name} data-tone={tone} style={{ flexGrow: p[name] }} /> : null))}
        {p.missing > 0 && <span className="meter-rest" style={{ flexGrow: p.missing }} />}
      </div>
    </div>
  )
}

/* -- one asset ----------------------------------------------------------------- */

export function AssetActions({ name, size = "m" }: { name: string; size?: "s" | "m" }) {
  const statuses = useStatuses()
  const retry = useRetryKeys()
  const failing = statuses.data?.assets[name]?.stored_counts ?? null
  const classes = FAILING_KEYS.filter((c) => failing?.[c])
  const [, navigate] = useLocation()
  const run = async (which: string[]) => {
    const result = await report(retry.mutateAsync({ asset: name, classes: which }), (r) => (r.runs.length ? `${plural(r.runs.length, "run")} started for the failing keys` : "Nothing to retry right now"))
    if (result?.runs[0]) navigate(`/runs/${result.runs[0].id}`)
  }
  return (
    <div className="row wrap">
      {classes.length > 0 && (
        <ActionMenu
          trigger={<Button size={size} icon={<RotateCcw size={14} />}>Retry failing keys</Button>}
          actions={[
            { value: "all", label: "Every failing key", note: String(classes.reduce((n, c) => n + (failing?.[c] ?? 0), 0)), run: () => run([...classes]) },
            null,
            ...classes.map((c) => ({ value: c, label: `Only ${keyStatus(c).label.toLowerCase()}`, note: String(failing?.[c] ?? 0), run: () => run([c]) })),
          ]}
        />
      )}
      <Button size={size} icon={<Play size={14} />} onClick={() => openStartRun({ targets: [name] })}>
        Run
      </Button>
    </div>
  )
}

/** What state the asset is in and why, in words. `bare` leaves out the status itself, for a page whose header already shows it: then only what needs saying is said. */
export function AssetState({ name, bare }: { name: string; bare?: boolean }) {
  const statuses = useStatuses()
  const status = statuses.data?.assets[name]
  const stale = useStaleKeys(name, "", Boolean(status && !status.partitioned && status.stale))
  const state = assetStatus(status)
  const keys = Object.entries(status?.stored_counts ?? {}).filter(([, n]) => n > 0)
  const now = Date.now() / 1000
  const notices = (
    <>
      {stale.data?.reasons.length ? <Notice tone="stale">{stale.data.reasons.map(reasonText).join(". ")}.</Notice> : null}
      {keys.length > 0 && (
        <Notice tone="stale">
          {keys.map(([outcome, n]) => `${plural(n, "key")} ${keyStatus(outcome).label.toLowerCase()}`).join(", ")}. Each keeps its last good output until it succeeds.
        </Notice>
      )}
      {status?.repairs ? <Notice tone="stale">{plural(status.repairs, "partition")} of its outputs wait for a repair after a writer died.</Notice> : null}
    </>
  )
  if (bare) return stale.data?.reasons.length || keys.length || status?.repairs ? <div className="stack notices">{notices}</div> : null
  return (
    <div className="stack">
      <div className="row wrap asset-state">
        <StatusPill status={state} />
        {state.detail && <span className="muted">{state.detail}</span>}
        {status?.last && (
          <span className="muted">
            Last ran <Ago at={status.last.at} now={now} />
          </span>
        )}
      </div>
      {notices}
    </div>
  )
}

/** Every partition as a cell, coloured by its status. Readable with hundreds. */
export function PartitionGrid({ name }: { name: string }) {
  const partitions = usePartitions(name, true)
  const [open, setOpen] = useState<PartitionRow | null>(null)
  if (partitions.error) return <Notice tone="idle">{(partitions.error as Error).message}</Notice>
  if (!partitions.data) return <Loading what="Listing partitions" />
  const rows = partitions.data.partitions
  const counts = new Map<string, number>()
  for (const row of rows) counts.set(row.status, (counts.get(row.status) ?? 0) + 1)
  return (
    <div className="stack">
      <div className="row wrap">
        {[...counts.entries()].map(([status, n]) => (
          <span key={status} className="status" data-quiet="">
            <Dot tone={partitionStatus(status).tone} />
            {count(n)} {partitionStatus(status).label.toLowerCase()}
          </span>
        ))}
      </div>
      <div className="cells" role="list" aria-label="Partitions">
        {rows.map((row) => (
          <Tip key={row.partition} text={`${partitionLabel(row.partition)}: ${partitionStatus(row.status).label.toLowerCase()}`}>
            <button role="listitem" className="cell" data-tone={partitionStatus(row.status).tone} data-status={row.status} aria-pressed={open?.partition === row.partition} aria-label={`${partitionLabel(row.partition)}: ${partitionStatus(row.status).label}`} onClick={() => setOpen(open?.partition === row.partition ? null : row)} />
          </Tip>
        ))}
      </div>
      {open && (
        <div className="well stack">
          <div className="row wrap">
            <span className="mono">{partitionLabel(open.partition)}</span>
            <StatusPill status={partitionStatus(open.status)} />
            <span className="grow" />
            <Button size="s" icon={<Play size={13} />} onClick={() => openStartRun({ targets: [name], partitions: open.partition })}>
              Run this partition
            </Button>
          </div>
          {open.reasons?.length ? <p className="muted">{open.reasons.map(reasonText).join(". ")}.</p> : null}
          {open.last_attempt && (
            <p className="muted">
              Its last run: <RunLink id={open.last_attempt.split("/")[0]!} /> ({open.last_outcome})
            </p>
          )}
        </div>
      )}
    </div>
  )
}

/** A per-key asset's keys: the ones that need attention first, and a way to find any one. */
export function KeysPanel({ name }: { name: string }) {
  const [outcomes, setOutcomes] = useState<string[]>([...FAILING_KEYS])
  const [find, setFind] = useState("")
  const keys = useOutcomes(name, { outcome: outcomes })
  const [, navigate] = useLocation()
  const now = keys.data?.now ?? Date.now() / 1000
  return (
    <>
      <div className="toolbar">
        <form
          className="search"
          onSubmit={(event) => {
            event.preventDefault()
            if (find.trim()) navigate(`/keys?asset=${encodeURIComponent(name)}&key=${encodeURIComponent(find.trim())}`)
          }}
        >
          <Search size={14} className="faint" aria-hidden="true" />
          <TextField label="Find one key" hideLabel placeholder="Find one key, then press Enter" value={find} onChange={setFind} mono />
        </form>
        <PickMany
          label="Outcome"
          placeholder="Every outcome"
          value={outcomes}
          onChange={setOutcomes}
          options={["ok", ...FAILING_KEYS, "unmatched", "removed"].map((value) => ({ value, label: keyStatus(value).label }))}
        />
      </div>
      {keys.error ? (
        <Failure error={keys.error} />
      ) : !keys.data ? (
        <Loading what="Loading keys" />
      ) : !keys.data.keys.length ? (
        <Empty title={outcomes.some((o) => o === "ok") ? "No key matches" : "No key needs attention"} />
      ) : (
        <div className="table-scroll">
          <table className="table">
            <thead>
              <tr>
                <th>Key</th>
                <th>Partition</th>
                <th>Outcome</th>
                <th>Why</th>
                <th className="end">Tries</th>
                <th className="end">Next try</th>
              </tr>
            </thead>
            <tbody>
              {keys.data.keys.map((key) => (
                <tr key={`${key.partition}/${key.key}`} data-link="" onClick={() => navigate(`/keys?asset=${encodeURIComponent(name)}&key=${encodeURIComponent(key.key)}`)}>
                  <td className="mono">
                    <Link href={`/keys?asset=${encodeURIComponent(name)}&key=${encodeURIComponent(key.key)}`} className="ref-plain" onClick={(event) => event.stopPropagation()}>
                      {key.key}
                    </Link>
                  </td>
                  <td className="mono muted">{partitionLabel(key.partition)}</td>
                  <td>
                    <StatusPill status={keyStatus(key.outcome)} />
                    {key.owed && key.outcome === "ok" && <span className="chip" data-tone="stale">changed since</span>}
                  </td>
                  <td className="error-text">{key.message}</td>
                  <td className="end num">{key.tries ?? "–"}</td>
                  <td className="end num muted">{key.next_at ? until(key.next_at, now) : "–"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {keys.data?.next && <p className="table-foot muted">Showing the first {keys.data.keys.length} keys.</p>}
    </>
  )
}

export function CommitsTable({ name }: { name: string }) {
  const commits = useCommits(name)
  const now = Date.now() / 1000
  if (commits.error) return <Failure error={commits.error} />
  if (!commits.data) return <Loading what="Loading history" />
  if (!commits.data.commits.length) return <Empty title="Nothing written yet" />
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th>When</th>
            <th>Output</th>
            <th>Partition</th>
            <th>Batch</th>
            <th className="end">Added</th>
            <th className="end">Removed</th>
            <th className="end">Holds</th>
            <th className="end">Run</th>
          </tr>
        </thead>
        <tbody>
          {commits.data.commits.map((commit) => (
            <tr key={`${commit.output}/${commit.partition}/${commit.at}/${commit.attempt}`}>
              <td className="muted"><Ago at={commit.at} now={now} /></td>
              <td>{commit.output}</td>
              <td className="mono muted">{partitionLabel(commit.partition)}</td>
              <td className="num muted">{commit.batch ? `${commit.batch.index + 1} of ${Math.max(commit.batch.count, commit.batch.index + 1)}` : "–"}</td>
              <td className="end num">{commit.added ?? "–"}</td>
              <td className="end num">{commit.removed ?? "–"}</td>
              <td className="end num muted">{commit.key_count !== null ? plural(commit.key_count, "key") : commit.rows !== null ? plural(commit.rows, "row") : "–"}</td>
              <td className="end"><RunLink id={commit.run} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function AutomationToggle({ automation }: { automation: Automation }) {
  const command = useAutomationCommand()
  return (
    <Toggle
      label={`${automation.name} is ${automation.enabled ? "on" : "off"}`}
      checked={automation.enabled}
      onChange={(on) => report(command.mutateAsync({ name: automation.name, command: on ? "enable" : "disable" }), on ? `${automation.name} is on` : `${automation.name} is off`)}
    />
  )
}

/** How the asset is declared: what it reads and writes, and how it runs. */
export function Definition({ name }: { name: string }) {
  const manifest = useManifest()
  const detail = useAsset(name)
  const spec = manifest.data?.assets[name]
  if (!spec || !manifest.data) return <Loading />
  const owner = (output: string) => manifest.data!.outputs[output]?.asset
  const now = Date.now() / 1000
  const dims = Object.entries(spec.partitions?.dims ?? {})
  return (
    <div className="stack">
      {spec.doc && <p className="doc">{spec.doc.replace(/\s+/g, " ").trim()}</p>}
      <Facts
        rows={[
          [
            "Reads",
            Object.keys(spec.inputs).length || spec.deps.length ? (
              <ul className="plain-list">
                {Object.entries(spec.inputs).map(([param, input]) => (
                  <li key={param}>
                    {owner(input.output) ? <AssetLink name={owner(input.output)!} /> : <SourceLink name={input.output} />}
                    {owner(input.output) && owner(input.output) !== input.output && <span className="muted"> {input.output}</span>}
                    <span className="muted">
                      {input.each ? `, one call per changed key, ${input.each.concurrency} at once` : input.kind === "incremental" ? ", only what changed" : ", whole"}
                      {input.batch_size ? `, ${count(input.batch_size)} keys a batch` : ""}
                    </span>
                  </li>
                ))}
                {spec.deps.map((dep) => (
                  <li key={dep}>
                    <SourceLink name={dep} />
                    <span className="muted">, watched for changes, not loaded</span>
                  </li>
                ))}
              </ul>
            ) : (
              "Nothing"
            ),
          ],
          [
            "Writes",
            spec.outputs.length ? (
              <ul className="plain-list">
                {spec.outputs.map((output) => (
                  <li key={output.name}>
                    <span className="name">{output.name}</span>
                    <span className="muted">
                      {" "}to the {output.store} store{output.key ? `, one row per ${output.key}` : ""}
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              "Nothing: it is a job"
            ),
          ],
          ["Partitions", dims.length ? dims.map(([dim, d]) => `${dim} (${d.kind === "time" ? `every ${(d as { every: string }).every} since ${(d as { start: string }).start}` : d.kind === "dynamic" ? `listed by ${(d as { output: string }).output}` : d.kind})`).join(", ") : "None"],
          ["Runs on", `${spec.placement.executor}${spec.placement.kind !== "Local" ? ` (${spec.placement.kind.toLowerCase()})` : ""}`],
          ["If it fails", spec.retries ? `${plural(spec.retries.n, "retry", "retries")}, ${spec.retries.backoff} backoff from ${duration(spec.retries.delay)}` : "No retry"],
          ["Time limit", spec.timeout ? duration(spec.timeout) : "None"],
          ["At once", spec.concurrency ? `${spec.concurrency} partitions` : null],
          ["Version", spec.version],
        ]}
      />
      <div>
        <h3 className="subhead">Automations</h3>
        {detail.data?.automations.length ? (
          <ul className="plain-list automations-inline">
            {detail.data.automations.map((automation) => (
              <li key={automation.name} className="row">
                <AutomationToggle automation={automation} />
                <span className="grow">
                  {triggerText(automation.trigger)} <span className="mono muted">{automation.name}</span>
                </span>
                <span className="muted num">{automation.enabled ? (automation.next_at ? `next ${until(automation.next_at, now)}` : automation.last_fired ? <>last <Ago at={automation.last_fired} now={now} /></> : "") : "off"}</span>
              </li>
            ))}
          </ul>
        ) : (
          <p className="muted">No automation starts it.</p>
        )}
      </div>
    </div>
  )
}
