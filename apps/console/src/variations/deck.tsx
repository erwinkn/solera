import { PanelLeft, PanelRight } from "lucide-react"
import { useState, type ReactNode } from "react"
import { Link, useLocation } from "wouter"
import { useDiagnostics, useHealth, useManifest, useRun, useRunFacets, useStatuses } from "../api/queries"
import type { RunDetail } from "../api/types"
import { AssetActions, AssetsFeed, AssetsTable, AssetState, CommitsTable, Definition, firstLine, KeysPanel, PartitionGrid } from "../blocks/assets"
import { Empty, Failure, Loading, StatusPill, useParam } from "../blocks/atoms"
import { AutomationsTable, SensorFacts, SensorsTable, TicksTable } from "../blocks/automations"
import { AssetTimeline, ProjectTimeline, RunTimeline, TroubleFilter, useRunView, WindowPicker } from "../blocks/charts"
import { Attention, CleanupsPanel, EngineFacts, ExecutorsTable, healthStatus, PruneRuns, Upcoming, useAttention, useUpcoming } from "../blocks/engine"
import { KeyLookup } from "../blocks/keys"
import { LookMenu, LookSettings } from "../blocks/look"
import { RunActions, RunInspector, runSentence, runTitle } from "../blocks/run"
import { RunHistogram, RunsFeed, RunsTable, useRunsFilter } from "../blocks/runs"
import { SourceCommitForm, SourceCommits, SourceFacts, SourceKeys } from "../blocks/source"
import { count, plural, until } from "../model/format"
import { assetStatus, runStatus } from "../model/status"
import { IconButton, Tabs, TextField } from "../ui/controls"
import { Sheet } from "../ui/overlays"
import { AssetPeek, isCurrent, PLACES, SearchButton, StartRunButton, type Layout } from "./shared"

/* Control room: everything on one screen, dense. A strip of places on top, a
 * list on the left, the stage in the middle, and what is picked on the
 * right. Nothing navigates away from the list you came from; a status line
 * at the bottom always says how the project is doing. */

function Shell({ children }: { children: ReactNode }) {
  const [path] = useLocation()
  const diagnostics = useDiagnostics()
  const health = useHealth()
  const statuses = useStatuses()
  const { concerns } = useAttention()
  const next = useUpcoming()[0]
  const now = Date.now() / 1000
  const all = Object.values(statuses.data?.assets ?? {})
  const tones = all.map((s) => assetStatus(s).tone)
  const n = (tone: string) => tones.filter((t) => t === tone).length
  return (
    <div className="dk">
      <header className="dk-top">
        <Link href="/" className="dk-brand">
          <span className="display">Solera</span>
          <span className="mono muted">{diagnostics.data?.project}</span>
        </Link>
        <nav className="dk-nav" aria-label="Places">
          {PLACES.map((place) => (
            <Link key={place.href} href={place.href} className="dk-nav-item" aria-current={isCurrent(place.href, path) ? "page" : undefined}>
              {place.label}
            </Link>
          ))}
        </nav>
        <span className="grow" />
        <SearchButton compact />
        <LookMenu label="" />
        <StartRunButton size="s" />
      </header>
      {children}
      <footer className="dk-status">
        <Link href="/engine" className="row">
          <StatusPill status={healthStatus(health.data)} />
        </Link>
        <span className="num dk-extra">{n("ok")} of {all.length} assets up to date</span>
        {n("failed") > 0 && <span className="tone num" data-tone="failed">{n("failed")} failed</span>}
        {n("stale") > 0 && <span className="tone num" data-tone="stale">{n("stale")} stale</span>}
        <span className="num">{plural(diagnostics.data?.active_runs ?? 0, "run")} in progress</span>
        {concerns.length > 0 && <Link href="/" className="link">{plural(concerns.length, "thing")} to look at</Link>}
        <span className="grow" />
        {next && <span className="num muted truncate">Next: {next.targets.join(", ")} {until(next.at, now)}</span>}
        <span className="muted num">deploy {diagnostics.data?.deploy_number}</span>
      </footer>
    </div>
  )
}

type PanesProps = { list?: ReactNode; listTitle?: string; stage: ReactNode; bar?: ReactNode; side?: ReactNode; sideTitle?: string }

/** The three panes. On a narrow screen the list and the side pane become sheets, opened from the stage's bar. */
function Panes({ list, listTitle = "Browse", stage, bar, side, sideTitle = "Details" }: PanesProps) {
  const [showList, setShowList] = useState(false)
  const [showSide, setShowSide] = useState(false)
  return (
    <div className="dk-panes" data-list={list ? "" : undefined} data-side={side ? "" : undefined}>
      {list && (
        <aside className="dk-list" aria-label={listTitle}>
          <div className="dk-pane-title label">{listTitle}</div>
          <div className="dk-scroll">{list}</div>
        </aside>
      )}
      <main className="dk-stage">
        <div className="dk-bar">
          {list && (
            <span className="dk-only-narrow">
              <IconButton label={`Show ${listTitle.toLowerCase()}`} size="s" onClick={() => setShowList(true)}>
                <PanelLeft size={15} />
              </IconButton>
            </span>
          )}
          {bar}
          {side && (
            <span className="dk-only-medium">
              <IconButton label={`Show ${sideTitle.toLowerCase()}`} size="s" onClick={() => setShowSide(true)}>
                <PanelRight size={15} />
              </IconButton>
            </span>
          )}
        </div>
        <div className="dk-scroll dk-stage-body">{stage}</div>
      </main>
      {side && (
        <aside className="dk-side" aria-label={sideTitle}>
          <div className="dk-pane-title label">{sideTitle}</div>
          <div className="dk-scroll dk-side-body">{side}</div>
        </aside>
      )}
      {list && (
        <Sheet open={showList} onOpenChange={setShowList} title={listTitle} side="left">
          <div onClick={() => setShowList(false)}>{list}</div>
        </Sheet>
      )}
      {side && (
        <Sheet open={showSide} onOpenChange={setShowSide} title={sideTitle} side="right">
          {side}
        </Sheet>
      )}
    </div>
  )
}

const Title = ({ children, mono }: { children: ReactNode; mono?: boolean }) => <h1 className={`dk-title truncate ${mono ? "mono" : ""}`}>{children}</h1>

function Home() {
  const [window, setWindow] = useParam("window", "900")
  const [picked, setPicked] = useState<string | null>(null)
  return (
    <Panes
      listTitle="Assets"
      list={<AssetsFeed />}
      bar={
        <>
          <Title>What ran, and what runs next</Title>
          <span className="grow" />
          <WindowPicker value={Number(window)} onChange={(s) => setWindow(String(s))} />
        </>
      }
      stage={<ProjectTimeline window={Number(window)} selected={picked} onSelect={setPicked} maxHeight="100%" />}
      sideTitle={picked ? "Picked" : "Needs attention"}
      side={
        picked ? (
          <AssetPeek id={picked} />
        ) : (
          <div className="stack">
            <Attention />
            <div>
              <div className="label">Coming next</div>
              <Upcoming limit={8} />
            </div>
          </div>
        )
      }
    />
  )
}

/** Counts by status and by asset: click one to filter the table. */
function Facets({ filters }: { filters: ReturnType<typeof useRunsFilter> }) {
  const facets = useRunFacets({})
  const toggle = (name: "status" | "asset", value: string) => {
    const now = filters.filter[name] ?? []
    filters.set(name, now.includes(value) ? now.filter((v) => v !== value) : [...now, value])
  }
  if (!facets.data) return <Loading />
  return (
    <div className="stack">
      {(["status", "asset"] as const).map((name) => (
        <div key={name}>
          <div className="label dk-facet-title">{name === "status" ? "By status" : "By asset"}</div>
          <ul className="feed">
            {(facets.data![name] ?? []).map((facet) => (
              <li key={facet.value}>
                <button className="feed-item" aria-pressed={(filters.filter[name] ?? []).includes(facet.value)} onClick={() => toggle(name, facet.value)}>
                  {name === "status" ? <span className="dot" data-tone={runStatus(facet.value).tone} /> : null}
                  <span className={`grow truncate ${name === "asset" ? "mono feed-name" : ""}`}>{name === "status" ? runStatus(facet.value).label : facet.value}</span>
                  <span className="faint num">{count(facet.count)}</span>
                </button>
              </li>
            ))}
          </ul>
        </div>
      ))}
    </div>
  )
}

function Runs() {
  const filters = useRunsFilter()
  return (
    <Panes
      listTitle="Filter"
      list={<Facets filters={filters} />}
      bar={
        <>
          <Title>Runs</Title>
          <div className="search dk-search">
            <TextField label="Search runs" hideLabel placeholder="Search errors, assets, tags" value={filters.filter.q ?? ""} onChange={(v) => filters.set("q", v)} />
          </div>
        </>
      }
      stage={
        <>
          <div className="dk-pad">
            <RunHistogram filter={filters.filter} height={44} />
          </div>
          <RunsTable filter={filters.filter} limit={100} dense />
        </>
      }
    />
  )
}

function Run({ id }: { id: string }) {
  const run = useRun(id)
  if (run.error) return <Panes listTitle="Runs" list={<RunsFeed filter={{}} />} bar={<Title>Run</Title>} stage={<div className="dk-pad"><Failure error={run.error} /></div>} />
  if (!run.data) return <Panes listTitle="Runs" list={<RunsFeed filter={{}} selected={id} />} bar={<Title>Run</Title>} stage={<Loading what="Loading the run" />} />
  return <RunPanes detail={run.data} />
}

function RunPanes({ detail }: { detail: RunDetail }) {
  const view = useRunView(detail)
  const [picked, setPicked] = useParam("at")
  const run = detail.request
  return (
    <Panes
      listTitle="Runs"
      list={<RunsFeed filter={{}} selected={run.id} />}
      bar={
        <>
          <Title mono>{runTitle(run)}</Title>
          <StatusPill status={runStatus(run.paused ? "paused" : run.status)} />
          <span className="muted truncate dk-hide-narrow">{runSentence(run, view)}</span>
          <span className="grow" />
          <TroubleFilter view={view} />
          <RunActions run={run} size="s" />
        </>
      }
      stage={
        detail.tasks.length ? (
          <RunTimeline view={view} selected={picked || null} onSelect={setPicked} maxHeight="100%" />
        ) : (
          <Empty title="This run has no tasks">{run.source ? "A commit to a source is recorded as a run so that it shows in history." : "Nothing was owed."}</Empty>
        )
      }
      sideTitle={picked ? "Picked step" : "About this run"}
      side={<RunInspector detail={detail} picked={picked || null} />}
    />
  )
}

function Assets() {
  const [filter, setFilter] = useState("")
  return (
    <Panes
      listTitle="Assets"
      list={<AssetsFeed />}
      bar={
        <>
          <Title>Assets</Title>
          <div className="search dk-search">
            <TextField label="Filter assets" hideLabel placeholder="Filter by name" value={filter} onChange={setFilter} />
          </div>
        </>
      }
      stage={<AssetsTable filter={filter} />}
    />
  )
}

function Asset({ name }: { name: string }) {
  const manifest = useManifest()
  const statuses = useStatuses()
  const [tab, setTab] = useParam("tab", "definition")
  const [window, setWindow] = useParam("window", "900")
  const spec = manifest.data?.assets[name]
  const status = statuses.data?.assets[name]
  const perKey = status?.stored_counts !== null && status?.stored_counts !== undefined
  return (
    <Panes
      listTitle="Assets"
      list={<AssetsFeed selected={name} />}
      bar={
        <>
          <Title mono>{name}</Title>
          <span className="muted truncate dk-hide-narrow">{firstLine(spec?.doc)}</span>
          <span className="grow" />
          <WindowPicker value={Number(window)} onChange={(s) => setWindow(String(s))} />
          <AssetActions name={name} size="s" />
        </>
      }
      stage={
        !manifest.data ? (
          <Loading />
        ) : !spec ? (
          <Empty title="No asset has that name" />
        ) : (
          <>
            <div className="dk-pad">
              <AssetState name={name} />
            </div>
            <AssetTimeline asset={name} window={Number(window)} maxHeight="44dvh" />
            {status?.partitioned && (
              <div className="dk-pad">
                <div className="label">Partitions</div>
                <PartitionGrid name={name} />
              </div>
            )}
          </>
        )
      }
      sideTitle="About this asset"
      side={
        spec ? (
          <Tabs
            label="About this asset"
            value={perKey || tab !== "keys" ? tab : "definition"}
            onChange={setTab}
            tabs={[
              { value: "definition", label: "Declared", content: <Definition name={name} /> },
              ...(perKey ? [{ value: "keys", label: "Keys", content: <KeysPanel name={name} /> }] : []),
              { value: "history", label: "Wrote", content: <CommitsTable name={name} /> },
            ]}
          />
        ) : undefined
      }
    />
  )
}

function Source({ name }: { name: string }) {
  return (
    <Panes
      listTitle="Assets"
      list={<AssetsFeed selected={name} />}
      bar={
        <>
          <Title mono>{name}</Title>
          <span className="muted">Source</span>
        </>
      }
      stage={
        <div className="dk-pad stack">
          <SourceFacts name={name} />
          <div>
            <div className="label">Keys it holds</div>
            <SourceKeys name={name} />
          </div>
          <div>
            <div className="label">Recent commits</div>
            <SourceCommits name={name} />
          </div>
        </div>
      }
      sideTitle="Say it changed"
      side={<SourceCommitForm name={name} />}
    />
  )
}

function Automations() {
  return (
    <Panes
      bar={<Title>Automations and sensors</Title>}
      stage={
        <>
          <AutomationsTable dense />
          <div className="dk-pad label">Sensors</div>
          <SensorsTable />
        </>
      }
      sideTitle="Coming next"
      side={<Upcoming limit={14} />}
    />
  )
}

function Sensor({ name }: { name: string }) {
  return <Panes bar={<><Title mono>{name}</Title><span className="muted">Sensor</span></>} stage={<TicksTable name={name} />} sideTitle="About this sensor" side={<SensorFacts name={name} />} />
}

function Engine() {
  return (
    <Panes
      bar={<Title>Engine</Title>}
      stage={
        <>
          <ExecutorsTable />
          <div className="dk-pad label">Cleanups</div>
          <CleanupsPanel />
          <div className="dk-pad label">Old runs</div>
          <div className="dk-pad">
            <PruneRuns />
          </div>
        </>
      }
      sideTitle="State"
      side={<EngineFacts />}
    />
  )
}

const Keys = () => <Panes bar={<Title>Find a key</Title>} stage={<div className="dk-pad"><KeyLookup /></div>} />
const Settings = () => <Panes bar={<Title>Settings</Title>} stage={<div className="dk-pad dk-narrow"><LookSettings /></div>} />
const NotFound = () => (
  <Panes
    bar={<Title>Nothing here</Title>}
    stage={
      <Empty title="This address leads nowhere">
        <Link href="/" className="link">Back to the overview</Link>
      </Empty>
    }
  />
)

export const deck: Layout = { Shell, Home, Runs, Run, Assets, Asset, Source, Automations, Sensor, Engine, Keys, Settings, NotFound }
