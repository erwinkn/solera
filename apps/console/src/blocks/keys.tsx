import { Search } from "lucide-react"
import { useState } from "react"
import { useManifest, useOutcomeHistory, useOutcomes } from "../api/queries"
import { duration, partitionLabel } from "../model/format"
import { keyStatus } from "../model/status"
import { Button, TextField } from "../ui/controls"
import { Pick } from "../ui/overlays"
import { Ago, AssetLink, Empty, Facts, Failure, Loading, Notice, Panel, RunLink, StatusPill, useParam } from "./atoms"

/* "What happened to this one file?" Pick a per-key asset and a key: its
 * outcome now, and each time a run failed on it. */

export function perKeyAssets(manifest: ReturnType<typeof useManifest>["data"]): string[] {
  return Object.entries(manifest?.assets ?? {})
    .filter(([, spec]) => Object.values(spec.inputs).some((input) => input.each))
    .map(([name]) => name)
}

export function KeyLookup() {
  const manifest = useManifest()
  const [asset, setAsset] = useParam("asset")
  const [key, setKey] = useParam("key")
  const [draft, setDraft] = useState(key)
  const assets = perKeyAssets(manifest.data)
  const chosen = asset || assets[0] || ""
  if (!manifest.data) return <Loading />
  if (!assets.length) return <Empty title="No asset works key by key">An asset called once per key keeps an outcome for each. This project has none.</Empty>
  return (
    <>
      <form
        className="toolbar"
        onSubmit={(event) => {
          event.preventDefault()
          setKey(draft.trim() || null)
        }}
      >
        <Pick label="Asset" value={chosen} onChange={setAsset} options={assets.map((name) => ({ value: name, label: name }))} />
        <div className="search grow">
          <Search size={14} className="faint" aria-hidden="true" />
          <TextField label="Key" hideLabel placeholder="The key, for example alpha-file-3" value={draft} onChange={setDraft} mono autoFocus />
        </div>
        <Button type="submit" variant="primary">
          Look it up
        </Button>
      </form>
      {key ? <KeyStory asset={chosen} name={key} /> : <p className="panel-body muted">A key is one item an asset works on by itself: one file, one upload, one row.</p>}
    </>
  )
}

function KeyStory({ asset, name }: { asset: string; name: string }) {
  const now_ = useOutcomes(asset, { key: name })
  const history = useOutcomeHistory(asset, { key: name })
  const now = Date.now() / 1000
  if (now_.error) return <Failure error={now_.error} />
  if (!now_.data) return <Loading />
  const rows = now_.data.keys
  return (
    <>
      <div className="panel-body stack key-story">
        {!rows.length ? (
          <Notice tone="idle">
            <AssetLink name={asset} /> has no record of <span className="mono">{name}</span>. Either the asset it reads never had this key, or it was removed before it was ever processed.
          </Notice>
        ) : (
          rows.map((row) => {
            const status = keyStatus(row.outcome)
            return (
              <div key={row.partition} className="stack">
                <div className="row wrap">
                  <span className="mono">{row.key}</span>
                  {row.partition && <span className="mono muted">{partitionLabel(row.partition)}</span>}
                  <StatusPill status={status} />
                  {row.owed && <span className="chip" data-tone="stale">{row.outcome === "ok" ? "changed since, due again" : "still due"}</span>}
                </div>
                {row.message && <div className="error-text">{row.message}</div>}
                <Facts
                  rows={[
                    ["What it means", sentence(row.outcome, row.owed)],
                    ["Tries so far", row.tries ? String(row.tries) : null],
                    ["Failing since", row.since ? <Ago at={row.since} now={now} /> : null],
                    ["Next try", row.next_at ? <Ago at={row.next_at} now={now} /> : null],
                    ["Processed at version", row.version !== null && row.version !== undefined ? <span className="mono">{String(row.version)}</span> : null],
                  ]}
                />
              </div>
            )
          })
        )}
      </div>
      <Panel title="Failures in history" flush>
        {history.error ? (
          <Failure error={history.error} />
        ) : !history.data ? (
          <Loading />
        ) : !history.data.outcomes.length ? (
          <p className="panel-body muted">No run in history failed on this key. Calls that went fine are not listed one by one.</p>
        ) : (
          <div className="table-scroll">
            <table className="table">
              <thead>
                <tr>
                  <th>When</th>
                  <th>Partition</th>
                  <th>Result</th>
                  <th>Why</th>
                  <th className="end">Took</th>
                  <th className="end">Run</th>
                </tr>
              </thead>
              <tbody>
                {history.data.outcomes.map((event) => (
                  <tr key={`${event.run}/${event.attempt}/${event.partition}`}>
                    <td className="muted"><Ago at={event.at} now={now} /></td>
                    <td className="mono muted">{partitionLabel(event.partition)}</td>
                    <td><StatusPill status={keyStatus(event.outcome)} /></td>
                    <td className="error-text">{event.error}</td>
                    <td className="end num">{duration(event.duration)}</td>
                    <td className="end"><RunLink id={event.run} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </>
  )
}

function sentence(outcome: string, owed: boolean): string {
  switch (outcome) {
    case "ok":
      return owed ? "It went through, and its input has changed since: the next run takes it again." : "It went through, and nothing changed since."
    case "rejected":
      return "The asset refused it. It keeps its last good output and is tried again when its input changes."
    case "failed":
      return "It raised an error. It keeps its last good output and gets one more try on each new deploy, or when its input changes."
    case "retrying":
      return "It failed in a way that may pass. The engine tries it again by itself, waiting longer each time."
    case "timed_out":
      return "Its call ran out of time."
    case "canceled":
      return "Its run was canceled while it was being processed. The next run of the asset takes it."
    case "unmatched":
      return "A rule on the asset's input leaves this key out, so the asset is never called for it."
    case "removed":
      return "It was removed where the asset reads from, and its rows were removed here."
    default:
      return outcome
  }
}
