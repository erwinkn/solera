import { useState, type ReactNode } from "react"
import { Link, useLocation } from "wouter"
import { useDiagnostics, useHealth, useManifest, useRun, useRunFacets, useRuns, useStatuses } from "../api/queries"
import type { RunDetail } from "../api/types"
import { AssetActions, AssetState, CommitsTable, Definition, failingKeys, firstLine, KeysPanel, PartitionGrid } from "../blocks/assets"
import { Dot, Empty, Failure, Loading, StatusPill, useParam } from "../blocks/atoms"
import { AutomationsTable, SensorFacts, SensorsTable, TicksTable } from "../blocks/automations"
import { PartitionActivity, WindowPicker } from "../blocks/activity"
import { RunTimeline, TroubleFilter, useRunView } from "../blocks/charts"
import { Attention, CleanupsPanel, EngineFacts, ExecutorsTable, healthStatus, PruneRuns, Upcoming, useAttention } from "../blocks/engine"
import { AssetActivity } from "../blocks/home"
import { KeyLookup } from "../blocks/keys"
import { LookMenu, LookSettings } from "../blocks/look"
import { RunActions, RunInspector, runSentence, runTitle } from "../blocks/run"
import { RunHistogram, RunsFilters, runWhat, runWhere, useRunsFilter } from "../blocks/runs"
import { SourceCommitForm, SourceCommits, SourceFacts, SourceKeys } from "../blocks/source"
import { clock, count, duration, longDate, plural } from "../model/format"
import { assetStatus, origin, runStatus } from "../model/status"
import { parseRowId } from "../timeline/run"
import { MarkIcon } from "../ui/status"
import { isCurrent, PLACES, SearchButton, StartRunButton, type Layout } from "./shared"

/* Briefing: one column that reads top to bottom like a status report. A
 * headline says how things are, sentences say why, and the charts sit in the
 * text where they answer the next question. No sidebar, no tabs: you scroll. */

function Shell({ children }: { children: ReactNode }) {
  const [path] = useLocation()
  const diagnostics = useDiagnostics()
  return (
    <div className="br">
      <header className="br-top">
        <Link href="/" className="br-brand">
          <span className="brand">Solera</span> <span className="muted">{diagnostics.data?.project}</span>
        </Link>
        <nav className="br-nav" aria-label="Places">
          {PLACES.map((place) => (
            <Link key={place.href} href={place.href} className="br-nav-item" aria-current={isCurrent(place.href, path) ? "page" : undefined}>
              {place.label}
            </Link>
          ))}
        </nav>
        <span className="grow" />
        <SearchButton compact />
        <LookMenu />
        <StartRunButton size="s" />
      </header>
      <main className="br-page">{children}</main>
    </div>
  )
}

function Article({ kicker, title, lead, actions, children }: { kicker?: ReactNode; title: ReactNode; lead?: ReactNode; actions?: ReactNode; children: ReactNode }) {
  return (
    <article className="br-article">
      <header className="br-head">
        {kicker && <div className="br-kicker label">{kicker}</div>}
        <h1 className="br-title">{title}</h1>
        {lead && <div className="br-lead">{lead}</div>}
        {actions && <div className="br-actions">{actions}</div>}
      </header>
      {children}
    </article>
  )
}

function Section({ title, aside, wide, children }: { title: string; aside?: ReactNode; wide?: boolean; children: ReactNode }) {
  return (
    <section className="br-section" data-wide={wide ? "" : undefined}>
      <div className="br-section-head">
        <h2>{title}</h2>
        <span className="grow" />
        {aside}
      </div>
      {children}
    </section>
  )
}

function Home() {
  const diagnostics = useDiagnostics()
  const health = useHealth()
  const statuses = useStatuses()
  const facets = useRunFacets({})
  const { concerns, ready } = useAttention()
  const [window, setWindow] = useParam("window", "900")
  const now = Date.now() / 1000
  const all = Object.values(statuses.data?.assets ?? {})
  const fine = all.filter((s) => assetStatus(s).tone === "ok").length
  const worst = concerns[0]?.tone
  const headline = !ready ? "Reading the project" : !concerns.length ? "All is well." : worst === "failed" ? `${concerns.length === 1 ? "One thing" : `${concerns.length} things`} need you.` : `${concerns.length === 1 ? "One thing" : `${concerns.length} things`} to keep an eye on.`
  const counts = new Map((facets.data?.status ?? []).map((f) => [f.value, f.count]))
  const total = [...counts.values()].reduce((a, b) => a + b, 0)
  return (
    <Article
      kicker={`${diagnostics.data?.project} · ${longDate(now)}`}
      title={headline}
      lead={
        <p>
          {fine} of {plural(all.length, "asset")} {fine === 1 ? "is" : "are"} up to date. {plural(diagnostics.data?.active_runs ?? 0, "run")} {diagnostics.data?.active_runs === 1 ? "is" : "are"} in progress. The engine is {healthStatus(health.data).label.toLowerCase()} on deploy {diagnostics.data?.deploy_number}.
        </p>
      }
    >
      {concerns.length > 0 && (
        <Section title="What needs attention">
          <Attention />
        </Section>
      )}
      <Section title="Assets" wide aside={<WindowPicker value={Number(window)} onChange={(seconds) => setWindow(String(seconds))} />}>
        <div className="br-chart">
          <AssetActivity window={Number(window)} />
        </div>
      </Section>
      <Section title="Coming next">
        <Upcoming limit={6} />
      </Section>
      <Section title="In history" aside={<Link href="/runs" className="link">All runs</Link>}>
        <p>
          {count(total)} runs are kept{counts.get("failed") ? <>, of which <Link href="/runs?status=failed" className="link">{count(counts.get("failed")!)} failed</Link></> : ", none of which failed"}.
        </p>
        <RunHistogram filter={{}} height={72} />
      </Section>
    </Article>
  )
}

/** Runs as lines of a log: when, what, how it went. */
function RunsLog({ filter }: { filter: ReturnType<typeof useRunsFilter>["filter"] }) {
  const [limit, setLimit] = useState(60)
  const runs = useRuns(filter, limit)
  if (runs.error) return <Failure error={runs.error} />
  if (!runs.data) return <Loading what="Loading runs" />
  if (!runs.data.runs.length) return <Empty title="No runs match" />
  return (
    <>
      <ol className="br-log">
        {runs.data.runs.map((run) => (
          <li key={run.id}>
            <Link href={`/runs/${run.id}`} className="br-log-line">
              <span className="num faint">{clock(run.created_at)}</span>
              <Dot tone={runStatus(run.status).tone} />
              <span className="grow">
                <span className="name">{runWhat(run)}</span> <span className="mono muted">{runWhere(run)}</span>{" "}
                <span className="muted">
                  {runStatus(run.status).label.toLowerCase()}
                  {run.finished_at ? ` in ${duration(run.finished_at - run.created_at)}` : ""}, {origin(run).toLowerCase()}
                </span>
                {run.error && <span className="error-text br-log-error">{run.error}</span>}
              </span>
            </Link>
          </li>
        ))}
      </ol>
      {runs.data.next && (
        <button className="link muted" onClick={() => setLimit(limit + 100)}>
          Show older runs ({count(runs.data.total - runs.data.runs.length)} more)
        </button>
      )}
    </>
  )
}

function Runs() {
  const filters = useRunsFilter()
  return (
    <Article title="Runs" lead={<p>Every request to bring assets up to date, newest first.</p>}>
      <Section title="Over time">
        <RunHistogram filter={filters.filter} height={72} />
      </Section>
      <Section title="The log" aside={null}>
        <RunsFilters {...filters} />
        <RunsLog filter={filters.filter} />
      </Section>
    </Article>
  )
}

function Run({ id }: { id: string }) {
  const run = useRun(id)
  if (run.error) return <Article title="Run"><Failure error={run.error} /></Article>
  if (!run.data) return <Article title="Run"><Loading what="Loading the run" /></Article>
  return <RunArticle detail={run.data} />
}

function RunArticle({ detail }: { detail: RunDetail }) {
  const view = useRunView(detail)
  const [picked, setPicked] = useParam("at")
  const run = detail.request
  const at = parseRowId(picked || null)
  return (
    <Article
      kicker={<Link href="/runs" className="link">Runs</Link>}
      title={runTitle(run)}
      lead={
        <p>
          <StatusPill status={runStatus(run.paused ? "paused" : run.status)} /> {runSentence(run, view)}
        </p>
      }
      actions={<RunActions run={run} />}
    >
      {detail.tasks.length > 0 && (
        <Section title="How it went" wide aside={<TroubleFilter view={view} />}>
          <div className="br-chart">
            <RunTimeline view={view} selected={picked || null} onSelect={setPicked} maxHeight="68dvh" />
          </div>
        </Section>
      )}
      <Section title={at ? "The step you picked" : "About this run"} aside={at ? <button className="link muted" onClick={() => setPicked(null)}>Back to the whole run</button> : null}>
        <RunInspector detail={detail} picked={picked || null} />
      </Section>
    </Article>
  )
}

/** Assets as two short lists: the ones that need a look, then the rest. */
function Assets() {
  const manifest = useManifest()
  const statuses = useStatuses()
  if (!manifest.data) return <Article title="Assets"><Loading /></Article>
  const names = Object.keys(manifest.data.assets)
  const state = (name: string) => assetStatus(statuses.data?.assets[name])
  const trouble = names.filter((n) => state(n).tone !== "ok" || failingKeys(statuses.data?.assets[n]) > 0)
  const fine = names.filter((n) => !trouble.includes(n))
  const line = (name: string) => {
    const s = state(name)
    const keys = failingKeys(statuses.data?.assets[name])
    return (
      <li key={name}>
        <Link href={`/assets/${encodeURIComponent(name)}`} className="br-log-line">
          <Dot tone={s.tone} />
          <span className="grow">
            <span className="name">{name}</span>{" "}
            <span className="muted">
              {s.tone === "ok" ? firstLine(manifest.data!.assets[name]!.doc) : `${s.label}. ${s.detail}.`}
              {keys > 0 && s.tone === "ok" ? ` ${plural(keys, "key")} did not go through.` : ""}
            </span>
          </span>
        </Link>
      </li>
    )
  }
  return (
    <Article title="Assets" lead={<p>{fine.length} of {names.length} are up to date.</p>}>
      {trouble.length > 0 && (
        <Section title="Need a look">
          <ol className="br-log">{trouble.map(line)}</ol>
        </Section>
      )}
      <Section title="Up to date">
        <ol className="br-log">{fine.map(line)}</ol>
      </Section>
      <Section title="Sources">
        <ol className="br-log">
          {Object.keys(manifest.data.sources).map((name) => (
            <li key={name}>
              <Link href={`/sources/${encodeURIComponent(name)}`} className="br-log-line">
                <MarkIcon mark="source" />
                <span className="grow">
                  <span className="name">{name}</span> <span className="muted">Data that lives outside the project.</span>
                </span>
              </Link>
            </li>
          ))}
        </ol>
      </Section>
    </Article>
  )
}

function Asset({ name }: { name: string }) {
  const manifest = useManifest()
  const statuses = useStatuses()
  const [window, setWindow] = useParam("window", "900")
  if (!manifest.data) return <Article title={name}><Loading /></Article>
  const spec = manifest.data.assets[name]
  if (!spec) return <Article title={name}><Empty title="No asset has that name" /></Article>
  const status = statuses.data?.assets[name]
  const perKey = status?.stored_counts !== null && status?.stored_counts !== undefined
  return (
    <Article kicker={<Link href="/assets" className="link">Assets</Link>} title={name} lead={<AssetState name={name} />} actions={<AssetActions name={name} />}>
      <Section title="Lately" wide aside={<WindowPicker value={Number(window)} onChange={(s) => setWindow(String(s))} />}>
        <div className="br-chart">
          <PartitionActivity asset={name} window={Number(window)} />
        </div>
      </Section>
      {status?.partitioned && (
        <Section title="Its partitions">
          <PartitionGrid name={name} />
        </Section>
      )}
      {perKey && (
        <Section title="Its keys">
          <KeysPanel name={name} />
        </Section>
      )}
      <Section title="How it is declared">
        <Definition name={name} />
      </Section>
      <Section title="What it wrote">
        <CommitsTable name={name} />
      </Section>
    </Article>
  )
}

function Source({ name }: { name: string }) {
  return (
    <Article kicker={<Link href="/assets" className="link">Assets</Link>} title={name} lead={<p>A source: data that lives outside the project. The engine knows it at versions.</p>}>
      <Section title="About">
        <SourceFacts name={name} />
      </Section>
      <Section title="Say it changed">
        <SourceCommitForm name={name} />
      </Section>
      <Section title="Keys it holds">
        <SourceKeys name={name} />
      </Section>
      <Section title="Recent commits">
        <SourceCommits name={name} />
      </Section>
    </Article>
  )
}

const Automations = () => (
  <Article title="Automations" lead={<p>What starts runs without anyone asking: schedules, changes to inputs, deploys, and sensors that watch the outside.</p>}>
    <Section title="Coming next">
      <Upcoming limit={5} />
    </Section>
    <Section title="Every automation" wide>
      <AutomationsTable />
    </Section>
    <Section title="Sensors">
      <SensorsTable />
    </Section>
  </Article>
)

const Sensor = ({ name }: { name: string }) => (
  <Article kicker={<Link href="/automations" className="link">Automations</Link>} title={name} lead={<p>A sensor: a check that runs every few seconds and may commit to a source.</p>}>
    <Section title="About">
      <SensorFacts name={name} />
    </Section>
    <Section title="Its checks">
      <TicksTable name={name} />
    </Section>
  </Article>
)

const Engine = () => (
  <Article title="Engine" lead={<p>The server that plans runs, and the places attempts run.</p>}>
    <Section title="State">
      <EngineFacts />
    </Section>
    <Section title="Where attempts run" wide>
      <ExecutorsTable />
    </Section>
    <Section title="Cleanups">
      <CleanupsPanel />
    </Section>
    <Section title="Old runs">
      <PruneRuns />
    </Section>
  </Article>
)

const Keys = () => (
  <Article title="Find a key" lead={<p>What happened to one file, upload or row.</p>}>
    <section className="br-section">
      <KeyLookup />
    </section>
  </Article>
)

const Settings = () => (
  <Article title="Settings" lead={<p>How the console looks. Kept in this browser.</p>}>
    <section className="br-section">
      <LookSettings />
    </section>
  </Article>
)

const NotFound = () => (
  <Article title="Nothing here.">
    <p>
      This address leads nowhere. <Link href="/" className="link">Back to the overview</Link>
    </p>
  </Article>
)

export const briefing: Layout = { Shell, Home, Runs, Run, Assets, Asset, Source, Automations, Sensor, Engine, Keys, Settings, NotFound }
