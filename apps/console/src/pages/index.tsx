import { Menu as MenuIcon, X } from "lucide-react"
import { useState, type ReactNode } from "react"
import { Link, useLocation } from "wouter"
import { useCanWrite, useDiagnostics, useHealth, useManifest, useRun, useStatuses } from "../api/queries"
import type { RunDetail } from "../api/types"
import { PartitionActivity, StripLegend, WindowPicker } from "../blocks/activity"
import { AssetActions, AssetsTable, AssetState, CommitsTable, Definition, firstLine, KeysPanel, PartitionGrid, SourcesTable } from "../blocks/assets"
import { Empty, Failure, Loading, Panel, StatusPill, useParam } from "../blocks/atoms"
import { AutomationsTable, SensorFacts, SensorsTable, TicksTable } from "../blocks/automations"
import { RunTimeline, TroubleFilter, useRunView } from "../blocks/charts"
import { Attention, CleanupsPanel, EngineFacts, ExecutorsTable, healthStatus, LoadError, PruneRuns, useAttention } from "../blocks/engine"
import { AssetActivity, Summary } from "../blocks/home"
import { KeyLookup } from "../blocks/keys"
import { LookMenu, LookSettings } from "../blocks/look"
import { RunActions, RunInspector, runSentence, runTitle } from "../blocks/run"
import { RunHistogram, RunsFilters, RunsTable, useRunsFilter } from "../blocks/runs"
import { SourceCommitForm, SourceCommits, SourceFacts, SourceKeys } from "../blocks/source"
import { shortId } from "../model/format"
import { assetStatus, runStatus } from "../model/status"
import { IconButton, Tabs, TextField } from "../ui/controls"
import { Sheet } from "../ui/overlays"
import { isCurrent, Logo, PLACES, SearchButton, StartRunButton } from "./chrome"

/* The console's frame and pages: a sidebar of places and a page for each thing. Every page has
 * the same bones: where you are, a title with its state and actions, then
 * sections that run edge to edge, parted by hairlines. */

function Nav({ onGo }: { onGo?: () => void }) {
  const [path] = useLocation()
  return (
    <nav className="wb-nav" aria-label="Places">
      {(["project", "system"] as const).map((group) => (
        <div key={group} className="wb-nav-group">
          {PLACES.filter((place) => place.group === group).map((place) => (
            <Link key={place.href} href={place.href} className="wb-nav-item" aria-current={isCurrent(place.href, path) ? "page" : undefined} onClick={onGo}>
              <place.icon size={15} aria-hidden="true" />
              {place.label}
            </Link>
          ))}
        </div>
      ))}
    </nav>
  )
}

/** Where a path is: its place, and under it the one thing the page is about.
 * A place's own page has no other title, so there the place is the heading. */
function Crumbs() {
  const [path] = useLocation()
  const place = PLACES.find((p) => isCurrent(p.href, path))
  const [, section, id] = path.split("/")
  if (!place) return <nav className="wb-crumbs" aria-label="Where you are" />
  if (!id) return <h1 className="wb-crumbs wb-place">{place.label}</h1>
  return (
    <nav className="wb-crumbs truncate" aria-label="Where you are">
      <Link href={place.href}>{place.label}</Link>
      <span className="faint" aria-hidden="true">/</span>
      <span aria-current="page" className={section === "runs" ? "mono truncate" : "truncate"}>{section === "runs" ? shortId(id) : decodeURIComponent(id)}</span>
    </nav>
  )
}

function Shell({ children }: { children: ReactNode }) {
  const diagnostics = useDiagnostics()
  const health = useHealth()
  const [menu, setMenu] = useState(false)
  const side = (onGo?: () => void) => (
    <>
      <Link href="/" className="wb-brand" onClick={onGo}>
        <Logo />
        <span className="wb-brand-name">Solera</span>
        <span className="muted truncate">{diagnostics.data?.project}</span>
      </Link>
      <SearchButton />
      <Nav onGo={onGo} />
      <div className="wb-side-foot">
        <Link href="/engine" className="wb-health" onClick={onGo}>
          <StatusPill status={healthStatus(health.data)} quiet />
        </Link>
        <span className="grow" />
        <LookMenu />
      </div>
    </>
  )
  return (
    <div className="wb">
      <aside className="wb-side">{side()}</aside>
      <div className="wb-main">
        <header className="wb-top">
          <span className="wb-menu">
            <IconButton label="Open the menu" onClick={() => setMenu(true)}>
              <MenuIcon size={16} />
            </IconButton>
          </span>
          <Crumbs />
          <span className="grow" />
          <StartRunButton />
        </header>
        <main className="wb-page">{children}</main>
      </div>
      <Sheet open={menu} onOpenChange={setMenu} title="Menu" side="left">
        <div className="wb-side wb-side-sheet">{side(() => setMenu(false))}</div>
      </Sheet>
    </div>
  )
}

type PageProps = { title?: ReactNode; badge?: ReactNode; meta?: ReactNode; actions?: ReactNode; children: ReactNode }

/** A page under the top bar. With a `title` it is about one thing and opens with a header: the name, its state, what can be done to it. */
function Page({ title, badge, meta, actions, children }: PageProps) {
  return (
    <>
      {title && (
        <header className="wb-head">
          <div className="wb-head-text">
            <div className="row wrap">
              <h1 className="wb-title">{title}</h1>
              {badge}
            </div>
            {meta && <p className="muted wb-meta">{meta}</p>}
          </div>
          {actions}
        </header>
      )}
      <div className="wb-body" data-headed={title ? "" : undefined}>
        {children}
      </div>
    </>
  )
}

function Home() {
  const { concerns } = useAttention()
  const [window, setWindow] = useParam("window", "900")
  return (
    <Page>
      <Summary />
      {concerns.length > 0 && (
        <Panel title="Needs attention" count={concerns.length} flush>
          <Attention limit={6} />
        </Panel>
      )}
      <Panel
        title="Assets"
        actions={
          <>
            <StripLegend />
            <WindowPicker value={Number(window)} onChange={(seconds) => setWindow(String(seconds))} />
          </>
        }
        flush
      >
        <AssetActivity window={Number(window)} />
      </Panel>
      <Panel title="Recent runs" actions={<Link href="/runs" className="link muted">All runs</Link>} flush>
        <RunsTable filter={{}} limit={6} brief />
      </Panel>
    </Page>
  )
}

function Runs() {
  const filters = useRunsFilter()
  return (
    <Page>
      <div className="toolbar">
        <RunsFilters {...filters} />
      </div>
      <div className="wb-chart">
        <RunHistogram filter={filters.filter} />
      </div>
      <RunsTable filter={filters.filter} />
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
    <Page title={runTitle(run)} badge={<StatusPill status={runStatus(run.paused ? "paused" : run.status)} />} meta={runSentence(run, view)} actions={<RunActions run={run} />}>
      <div className="wb-split">
        <Panel title="Timeline" actions={<TroubleFilter view={view} />} flush>
          {detail.tasks.length ? (
            <RunTimeline view={view} selected={picked || null} onSelect={setPicked} maxHeight="var(--tl-max, 70dvh)" />
          ) : (
            <Empty title="This run has no tasks">{run.source ? "A commit to a source is recorded as a run so that it shows in history." : "Nothing was owed."}</Empty>
          )}
        </Panel>
        <aside className="wb-aside" aria-label="Details">
          <Panel
            title={picked ? "Step" : "Run"}
            actions={
              picked && (
                <IconButton label="Back to the run" size="s" onClick={() => setPicked(null)}>
                  <X size={14} />
                </IconButton>
              )
            }
          >
            <RunInspector detail={detail} picked={picked || null} />
          </Panel>
        </aside>
      </div>
    </Page>
  )
}

function Assets() {
  const [filter, setFilter] = useState("")
  const manifest = useManifest()
  return (
    <Page>
      <div className="toolbar">
        <div className="search">
          <TextField label="Filter assets" hideLabel placeholder="Filter by name" value={filter} onChange={setFilter} />
        </div>
      </div>
      <AssetsTable filter={filter} />
      <Panel title="Sources" count={Object.keys(manifest.data?.sources ?? {}).length} flush>
        <SourcesTable />
      </Panel>
    </Page>
  )
}

function Asset({ name }: { name: string }) {
  const manifest = useManifest()
  const statuses = useStatuses()
  const [tab, setTab] = useParam("tab", "overview")
  const [window, setWindow] = useParam("window", "900")
  if (!manifest.data) return <Page title={name}><Loading /></Page>
  const spec = manifest.data.assets[name]
  if (!spec) return <Page title={name}><Empty title="No asset has that name" /></Page>
  const status = statuses.data?.assets[name]
  const perKey = status?.stored_counts !== null && status?.stored_counts !== undefined
  const overview = (
    <>
      <AssetState name={name} bare />
      <Panel title="Activity" actions={<><StripLegend /><WindowPicker value={Number(window)} onChange={(seconds) => setWindow(String(seconds))} /></>} flush>
        <PartitionActivity asset={name} window={Number(window)} />
      </Panel>
      {status?.partitioned && (
        <Panel title="Partitions" count={status.partitions.total}>
          <PartitionGrid name={name} />
        </Panel>
      )}
    </>
  )
  return (
    <Page title={name} badge={<StatusPill status={assetStatus(status)} />} meta={firstLine(spec.doc)} actions={<AssetActions name={name} />}>
      <Tabs
        label="Sections of this asset"
        value={tab}
        onChange={setTab}
        tabs={[
          { value: "overview", label: "Overview", content: overview },
          ...(perKey ? [{ value: "keys", label: "Keys", content: <KeysPanel name={name} /> }] : []),
          { value: "history", label: "History", content: <CommitsTable name={name} /> },
          { value: "definition", label: "Definition", content: <div className="panel-body wb-lead wb-prose"><Definition name={name} /></div> },
        ]}
      />
    </Page>
  )
}

function Source({ name }: { name: string }) {
  const about = (
    <>
      <Panel title="About">
        <SourceFacts name={name} />
      </Panel>
      <Panel title="Keys">
        <SourceKeys name={name} />
      </Panel>
      <Panel title="Recent commits">
        <SourceCommits name={name} />
      </Panel>
    </>
  )
  if (!useCanWrite()) return <Page title={name}>{about}</Page>
  return (
    <Page title={name}>
      <div className="wb-split">
        <div>{about}</div>
        <aside className="wb-aside" aria-label="Commit a change">
          <Panel title="Commit a change">
            <SourceCommitForm name={name} />
          </Panel>
        </aside>
      </div>
    </Page>
  )
}

function Automations() {
  return (
    <Page>
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
    <Page title={name}>
      <Panel title="About">
        <SensorFacts name={name} />
      </Panel>
      <Panel title="Checks" flush>
        <TicksTable name={name} />
      </Panel>
    </Page>
  )
}

function Engine() {
  const failed = Boolean(useDiagnostics().data?.load_error)
  const canWrite = useCanWrite()
  return (
    <Page>
      {failed && (
        <Panel title="Code that did not load">
          <LoadError />
        </Panel>
      )}
      <Panel title="State">
        <EngineFacts />
      </Panel>
      <Panel title="Executors" flush>
        <ExecutorsTable />
      </Panel>
      <Panel title="Cleanups" flush>
        <CleanupsPanel />
      </Panel>
      {canWrite && (
        <Panel title="Old runs">
          <PruneRuns />
        </Panel>
      )}
    </Page>
  )
}

const Keys = () => (
  <Page>
    <KeyLookup />
  </Page>
)

const Settings = () => (
  <Page>
    <LookSettings />
  </Page>
)

const NotFound = () => (
  <Page title="Not found">
    <Empty title="No page at this address">
      <Link href="/" className="link">Back to the overview</Link>
    </Empty>
  </Page>
)

export { Shell, Home, Runs, Run, Assets, Asset, Source, Automations, Sensor, Engine, Keys, Settings, NotFound }
