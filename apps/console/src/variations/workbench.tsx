import { Menu as MenuIcon } from "lucide-react"
import { useState, type ReactNode } from "react"
import { Link, useLocation } from "wouter"
import { useDiagnostics, useHealth, useManifest, useRun, useRuns, useStatuses } from "../api/queries"
import { AssetActions, AssetsTable, AssetState, CommitsTable, Definition, firstLine, KeysPanel, PartitionGrid } from "../blocks/assets"
import { Empty, Failure, Loading, Panel, StatusPill, useParam } from "../blocks/atoms"
import { AutomationsTable, SensorFacts, SensorsTable, TicksTable } from "../blocks/automations"
import { AssetTimeline, ProjectTimeline, RunTimeline, TroubleFilter, useRunView, WindowPicker } from "../blocks/charts"
import { Attention, CleanupsPanel, EngineFacts, ExecutorsTable, healthStatus, PruneRuns, Upcoming, useUpcoming } from "../blocks/engine"
import { KeyLookup } from "../blocks/keys"
import { LookMenu, LookSettings } from "../blocks/look"
import { RunActions, RunInspector, runSentence, runTitle } from "../blocks/run"
import { RunHistogram, RunsFilters, RunsTable, useRunsFilter } from "../blocks/runs"
import { SourceCommitForm, SourceCommits, SourceFacts, SourceKeys } from "../blocks/source"
import type { RunDetail } from "../api/types"
import { count, until } from "../model/format"
import { assetStatus, runStatus } from "../model/status"
import { IconButton, Tabs, TextField } from "../ui/controls"
import { Sheet } from "../ui/overlays"
import { AssetPeek, isCurrent, PLACES, SearchButton, StartRunButton, type Layout } from "./shared"

/* Workbench: a sidebar of places, and a page for each thing. Panels with
 * titles, tabs on the bigger pages, room to read. The familiar take. */

function Nav({ onGo }: { onGo?: () => void }) {
  const [path] = useLocation()
  return (
    <nav className="wb-nav" aria-label="Places">
      {PLACES.map((place) => (
        <Link key={place.href} href={place.href} className="wb-nav-item" aria-current={isCurrent(place.href, path) ? "page" : undefined} onClick={onGo}>
          <place.icon size={16} aria-hidden="true" />
          {place.label}
        </Link>
      ))}
    </nav>
  )
}

function Shell({ children }: { children: ReactNode }) {
  const diagnostics = useDiagnostics()
  const health = useHealth()
  const [menu, setMenu] = useState(false)
  const brand = (
    <Link href="/" className="wb-brand">
      <span className="display">Solera</span>
      <span className="mono muted">{diagnostics.data?.project}</span>
    </Link>
  )
  return (
    <div className="wb">
      <aside className="wb-side">
        {brand}
        <Nav />
        <div className="wb-side-foot">
          <Link href="/engine" className="wb-health">
            <StatusPill status={healthStatus(health.data)} />
          </Link>
        </div>
      </aside>
      <div className="wb-main">
        <header className="wb-top">
          <span className="wb-menu">
            <IconButton label="Open the menu" onClick={() => setMenu(true)}>
              <MenuIcon size={17} />
            </IconButton>
          </span>
          <SearchButton />
          <span className="grow" />
          <LookMenu />
          <StartRunButton />
        </header>
        <main className="wb-page">{children}</main>
      </div>
      <Sheet open={menu} onOpenChange={setMenu} title="Solera" side="left">
        <Nav onGo={() => setMenu(false)} />
      </Sheet>
    </div>
  )
}

function Page({ title, mono, subtitle, actions, children }: { title: ReactNode; mono?: boolean; subtitle?: ReactNode; actions?: ReactNode; children: ReactNode }) {
  return (
    <>
      <header className="wb-head">
        <div className="grow">
          <h1 className={mono ? "wb-title mono" : "wb-title display"}>{title}</h1>
          {subtitle && <p className="muted wb-subtitle">{subtitle}</p>}
        </div>
        {actions}
      </header>
      <div className="stack wb-body">{children}</div>
    </>
  )
}

function Stat({ label, value, note, tone, href }: { label: string; value: ReactNode; note?: ReactNode; tone?: string; href: string }) {
  return (
    <Link href={href} className="panel stat" data-tone={tone}>
      <span className="label">{label}</span>
      <span className="stat-value display">{value}</span>
      <span className="muted truncate">{note}</span>
    </Link>
  )
}

function Home() {
  const diagnostics = useDiagnostics()
  const statuses = useStatuses()
  const health = useHealth()
  const failed = useRuns({ status: ["failed"], since: Math.floor(Date.now() / 3600000) * 3600 - 3600 }, 1, 10000)
  const next = useUpcoming()[0]
  const [window, setWindow] = useParam("window", "900")
  const [picked, setPicked] = useState<string | null>(null)
  const all = Object.values(statuses.data?.assets ?? {})
  const fine = all.filter((s) => assetStatus(s).tone === "ok").length
  const now = Date.now() / 1000
  return (
    <Page title={diagnostics.data?.project ?? "Project"} mono subtitle={`The engine is ${healthStatus(health.data).label.toLowerCase()}, on deploy ${diagnostics.data?.deploy_number ?? "–"}.`}>
      <div className="stats">
        <Stat label="Assets up to date" value={<>{fine}<span className="stat-of"> of {all.length}</span></>} note={fine === all.length ? "Nothing is stale or failing" : `${all.length - fine} need a look`} tone={fine === all.length ? "ok" : "stale"} href="/assets" />
        <Stat label="Running now" value={diagnostics.data?.active_runs ?? "–"} note={`${diagnostics.data?.inflight ?? 0} attempts in flight`} tone="running" href="/runs?status=running,queued" />
        <Stat label="Failed in the last hour" value={failed.data ? count(failed.data.total) : "–"} note={failed.data?.runs[0]?.error ?? "No run failed"} tone={failed.data?.total ? "failed" : "ok"} href="/runs?status=failed" />
        <Stat label="Next to start" value={next ? until(next.at, now).replace("in ", "") : "–"} note={next ? next.targets.join(", ") : "Nothing is scheduled"} href="/automations" />
      </div>
      <div className="wb-split">
        <Panel title="What ran, and what runs next" actions={<WindowPicker value={Number(window)} onChange={(s) => setWindow(String(s))} />} flush>
          <ProjectTimeline window={Number(window)} selected={picked} onSelect={setPicked} maxHeight="62dvh" />
          {picked && (
            <div className="panel-body wb-peek">
              <AssetPeek id={picked} />
            </div>
          )}
        </Panel>
        <div className="stack">
          <Panel title="Needs attention">
            <Attention limit={8} />
          </Panel>
          <Panel title="Coming next">
            <Upcoming />
          </Panel>
        </div>
      </div>
      <Panel title="Latest runs" actions={<Link href="/runs" className="link muted">All runs</Link>} flush>
        <RunsTable filter={{}} limit={8} />
      </Panel>
    </Page>
  )
}

function Runs() {
  const filters = useRunsFilter()
  return (
    <Page title="Runs" subtitle="Every request to bring assets up to date, newest first." actions={<StartRunButton />}>
      <RunsFilters {...filters} />
      <Panel flush>
        <div className="panel-body">
          <RunHistogram filter={filters.filter} />
        </div>
        <RunsTable filter={filters.filter} />
      </Panel>
    </Page>
  )
}

function Run({ id }: { id: string }) {
  const run = useRun(id)
  if (run.error) return <Page title="Run"><Failure error={run.error} /></Page>
  if (!run.data) return <Page title="Run"><Loading what="Loading the run" /></Page>
  return <RunPage detail={run.data} />
}

function RunPage({ detail }: { detail: RunDetail }) {
  const view = useRunView(detail)
  const [picked, setPicked] = useParam("at")
  const run = detail.request
  return (
    <Page
      mono
      title={runTitle(run)}
      subtitle={
        <span className="row wrap">
          <StatusPill status={runStatus(run.paused ? "paused" : run.status)} />
          <span>{runSentence(run, view)}</span>
        </span>
      }
      actions={<RunActions run={run} />}
    >
      <div className="wb-split" data-wide="">
        <Panel title="Timeline" actions={<TroubleFilter view={view} />} flush>
          {detail.tasks.length ? (
            <RunTimeline view={view} selected={picked || null} onSelect={setPicked} maxHeight="70dvh" />
          ) : (
            <Empty title="This run has no tasks">{run.source ? "A commit to a source is recorded as a run so that it shows in history." : "Nothing was owed."}</Empty>
          )}
        </Panel>
        <Panel title={picked ? "Picked step" : "About this run"}>
          <RunInspector detail={detail} picked={picked || null} />
        </Panel>
      </div>
    </Page>
  )
}

function Assets() {
  const [filter, setFilter] = useState("")
  const manifest = useManifest()
  return (
    <Page title="Assets" subtitle="Everything the project builds, and the sources it reads.">
      <div className="filters row">
        <div className="search">
          <TextField label="Filter assets" hideLabel placeholder="Filter by name" value={filter} onChange={setFilter} />
        </div>
      </div>
      <Panel flush>
        <AssetsTable filter={filter} />
      </Panel>
      <Panel title="Sources">
        <ul className="plain-list">
          {Object.keys(manifest.data?.sources ?? {}).map((name) => (
            <li key={name} className="row">
              <Link href={`/sources/${encodeURIComponent(name)}`} className="mono link">
                <strong>{name}</strong>
              </Link>
              <span className="muted">Data that lives outside. Commit to it to say it changed.</span>
            </li>
          ))}
        </ul>
      </Panel>
    </Page>
  )
}

function Asset({ name }: { name: string }) {
  const manifest = useManifest()
  const statuses = useStatuses()
  const [tab, setTab] = useParam("tab", "overview")
  const [window, setWindow] = useParam("window", "900")
  if (!manifest.data) return <Page title={name} mono><Loading /></Page>
  const spec = manifest.data.assets[name]
  if (!spec) return <Page title={name} mono><Empty title="No asset has that name" /></Page>
  const status = statuses.data?.assets[name]
  const perKey = status?.stored_counts !== null && status?.stored_counts !== undefined
  const overview = (
    <div className="stack">
      <AssetState name={name} />
      <Panel title="Lately" actions={<WindowPicker value={Number(window)} onChange={(s) => setWindow(String(s))} />} flush>
        <AssetTimeline asset={name} window={Number(window)} maxHeight="46dvh" />
      </Panel>
      {status?.partitioned && (
        <Panel title="Partitions">
          <PartitionGrid name={name} />
        </Panel>
      )}
    </div>
  )
  return (
    <Page title={name} mono subtitle={firstLine(spec.doc)} actions={<AssetActions name={name} />}>
      <Tabs
        label="Sections of this asset"
        value={tab}
        onChange={setTab}
        tabs={[
          { value: "overview", label: "Overview", content: overview },
          ...(perKey ? [{ value: "keys", label: "Keys", content: <KeysPanel name={name} /> }] : []),
          { value: "history", label: "What it wrote", content: <Panel flush><CommitsTable name={name} /></Panel> },
          { value: "definition", label: "How it is declared", content: <Panel><Definition name={name} /></Panel> },
        ]}
      />
    </Page>
  )
}

function Source({ name }: { name: string }) {
  return (
    <Page title={name} mono subtitle="A source: data that lives outside the project.">
      <div className="wb-split" data-even="">
        <Panel title="About">
          <SourceFacts name={name} />
        </Panel>
        <Panel title="Say it changed">
          <SourceCommitForm name={name} />
        </Panel>
      </div>
      <div className="wb-split" data-even="">
        <Panel title="Keys it holds">
          <SourceKeys name={name} />
        </Panel>
        <Panel title="Recent commits">
          <SourceCommits name={name} />
        </Panel>
      </div>
    </Page>
  )
}

function Automations() {
  return (
    <Page title="Automations" subtitle="What starts runs without anyone asking.">
      <Panel title="Automations" flush>
        <AutomationsTable />
      </Panel>
      <Panel title="Sensors" flush>
        <SensorsTable />
      </Panel>
    </Page>
  )
}

function Sensor({ name }: { name: string }) {
  return (
    <Page title={name} mono subtitle="A sensor: a check that runs every few seconds and may commit to a source.">
      <Panel title="About">
        <SensorFacts name={name} />
      </Panel>
      <Panel title="Its checks" flush>
        <TicksTable name={name} />
      </Panel>
    </Page>
  )
}

function Engine() {
  return (
    <Page title="Engine" subtitle="The server that plans runs, and the places attempts run.">
      <Panel title="State">
        <EngineFacts />
      </Panel>
      <Panel title="Executors and workers" flush>
        <ExecutorsTable />
      </Panel>
      <Panel title="Cleanups" flush>
        <CleanupsPanel />
      </Panel>
      <Panel title="Old runs">
        <PruneRuns />
      </Panel>
    </Page>
  )
}

const Keys = () => (
  <Page title="Find a key" subtitle="What happened to one file, upload or row.">
    <Panel>
      <KeyLookup />
    </Panel>
  </Page>
)

const Settings = () => (
  <Page title="Settings" subtitle="How the console looks. Kept in this browser.">
    <LookSettings />
  </Page>
)

const NotFound = () => (
  <Page title="Nothing here">
    <Empty title="This address leads nowhere">
      <Link href="/" className="link">Back to the overview</Link>
    </Empty>
  </Page>
)

export const workbench: Layout = { Shell, Home, Runs, Run, Assets, Asset, Source, Automations, Sensor, Engine, Keys, Settings, NotFound }
