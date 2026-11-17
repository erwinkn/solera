import { useState } from "react"
import { useLocation } from "wouter"
import { useCommitSource, useHeads, useManifest, useOutputKeys, useRuns } from "../api/queries"
import { count, plural } from "../model/format"
import { projectGraph } from "../model/graph"
import { Button, TextField } from "../ui/controls"
import { toast } from "../ui/overlays"
import { Ago, AssetLink, Empty, Facts, Loading, RunLink } from "./atoms"

/* A source: data that lives outside, which the engine knows at versions. An
 * operator can tell the engine it changed. */

export function SourceFacts({ name }: { name: string }) {
  const manifest = useManifest()
  const heads = useHeads(name)
  const source = manifest.data?.sources[name]
  const now = Date.now() / 1000
  if (!manifest.data) return <Loading />
  if (!source) return <Empty title="No source has that name" />
  const readers = projectGraph(manifest.data).edges.filter((e) => e.from === name).map((e) => e.to)
  const head = heads.data?.heads[0]
  return (
    <Facts
      rows={[
        ["Kind", source.key ? "Keyed: a set of keys, each at a version" : "One version for the whole source"],
        ["Kept in", `${source.store} store`],
        ["Last changed", head ? <Ago at={head.at} now={now} /> : "Never committed"],
        ["Keys", head?.key_count !== null && head?.key_count !== undefined ? count(head.key_count) : null],
        ["Version", head?.version ?? null],
        ["Read by", readers.length ? <span className="row wrap">{readers.map((r) => <AssetLink key={r} name={r} />)}</span> : "Nothing"],
      ]}
    />
  )
}

const split = (text: string) => text.split(/[\s,]+/).filter(Boolean)

export function SourceCommitForm({ name }: { name: string }) {
  const manifest = useManifest()
  const commit = useCommitSource()
  const [, navigate] = useLocation()
  const [upsert, setUpsert] = useState("")
  const [remove, setRemove] = useState("")
  const [version, setVersion] = useState("")
  const source = manifest.data?.sources[name]
  if (!source) return null
  const keyed = source.key !== null
  const ready = keyed ? split(upsert).length + split(remove).length > 0 : version.trim().length > 0
  const submit = async () => {
    try {
      const result = (await commit.mutateAsync(keyed ? { source: name, upsert: split(upsert), remove: split(remove) } : { source: name, version: version.trim() })) as { changed: boolean; run?: string }
      if (!result.changed) {
        toast("Nothing changed")
        return
      }
      toast.success(`Committed to ${name}`)
      setUpsert("")
      setRemove("")
      setVersion("")
      if (result.run) navigate(`/runs/${result.run}`)
    } catch (error) {
      toast.error("The commit was refused", { description: (error as Error).message })
    }
  }
  return (
    <form
      className="stack"
      onSubmit={(event) => {
        event.preventDefault()
        if (ready) void submit()
      }}
    >
      {keyed ? (
        <>
          <TextField label="Keys added or changed" value={upsert} onChange={setUpsert} rows={3} mono placeholder="one key per line" />
          <TextField label="Keys removed" value={remove} onChange={setRemove} rows={2} mono placeholder="one key per line" />
        </>
      ) : (
        <TextField label="New version" value={version} onChange={setVersion} mono placeholder="for example 2026-11-17" description="The same version again changes nothing." />
      )}
      <div className="row">
        <Button type="submit" variant="primary" disabled={!ready}>
          Commit the change
        </Button>
      </div>
    </form>
  )
}

export function SourceKeys({ name }: { name: string }) {
  const keys = useOutputKeys(name, "")
  if (keys.error) return <p className="muted">This source has no keys to list.</p>
  if (!keys.data) return <Loading />
  const entries = Object.entries(keys.data.keys)
  if (!entries.length) return <Empty title="It holds no key yet" />
  return (
    <div className="stack">
      <ul className="key-list">
        {entries.map(([key]) => (
          <li key={key} className="mono chip">
            {key}
          </li>
        ))}
      </ul>
      <p className="muted">
        {plural(keys.data.total, "key")}
        {keys.data.next ? `, the first ${entries.length} shown` : ""}.
      </p>
    </div>
  )
}

export function SourceCommits({ name }: { name: string }) {
  const runs = useRuns({ origin: ["commit"] }, 100)
  const now = Date.now() / 1000
  const own = (runs.data?.runs ?? []).filter((run) => run.source === name).slice(0, 12)
  if (!runs.data) return <Loading />
  if (!own.length) return <Empty title="No commit in recent history" />
  return (
    <ul className="plain-list">
      {own.map((run) => (
        <li key={run.id} className="row">
          <RunLink id={run.id} />
          <span className="grow muted">{run.by ?? "by hand"}</span>
          <span className="muted"><Ago at={run.created_at} now={now} /></span>
        </li>
      ))}
    </ul>
  )
}
