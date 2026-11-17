import { useState, useSyncExternalStore } from "react"
import { useLocation } from "wouter"
import { useManifest, useStartRun, type RunInput } from "../api/queries"
import { Button, Checkmark, Segmented, TextField } from "../ui/controls"
import { Modal, PickMany, report } from "../ui/overlays"
import { Notice } from "./atoms"

/* Start a run by hand. One dialog for the whole console, opened from anywhere
 * with what the caller already knows filled in. */

type Preset = { targets?: string[]; mode?: "incremental" | "full"; partitions?: string }

let request: Preset | null = null
const listeners = new Set<() => void>()
const emit = () => listeners.forEach((listener) => listener())

export function openStartRun(preset: Preset = {}) {
  request = preset
  emit()
}

export function StartRunDialog() {
  const preset = useSyncExternalStore(
    (listener) => {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    () => request,
  )
  const close = () => {
    request = null
    emit()
  }
  return (
    <Modal open={preset !== null} onOpenChange={(open) => !open && close()} title="Start a run" description="Bring assets up to date now, without waiting for their automations." focus='[aria-label="Assets to run"]'>
      {preset && <Form key={JSON.stringify(preset)} preset={preset} close={close} />}
    </Modal>
  )
}

const WHICH = [
  { value: "latest", label: "Latest" },
  { value: "missing", label: "Never built" },
  { value: "all", label: "All" },
  { value: "list", label: "These" },
] as const
type Which = (typeof WHICH)[number]["value"]

const WHICH_HELP: Record<Which, string> = {
  latest: "The newest partition of each asset. An asset without partitions has just one.",
  missing: "Every partition that has never been built.",
  all: "Every partition. This can be a lot of tasks.",
  list: "The partitions you name, one per line or separated by spaces.",
}

function Form({ preset, close }: { preset: Preset; close: () => void }) {
  const manifest = useManifest()
  const start = useStartRun()
  const [, navigate] = useLocation()
  const [targets, setTargets] = useState<string[]>(preset.targets ?? [])
  const [which, setWhich] = useState<Which>(preset.partitions ? "list" : "latest")
  const [named, setNamed] = useState(preset.partitions ?? "")
  const [mode, setMode] = useState<RunInput["mode"]>(preset.mode ?? "incremental")
  const [upstream, setUpstream] = useState(false)
  const assets = Object.keys(manifest.data?.assets ?? {})
  const names = named.split(/[\s]+/).filter(Boolean)
  const ready = targets.length > 0 && (which !== "list" || names.length > 0)

  const submit = async () => {
    const result = await report(start.mutateAsync({ targets, partitions: which === "list" ? names : which, mode, upstream }), (run) =>
      "id" in run ? "The run is under way" : "Nothing started: that work is already running or queued",
    )
    if (!result) return
    close()
    if ("id" in result) navigate(`/runs/${result.id}`)
  }

  return (
    <form
      className="stack"
      onSubmit={(event) => {
        event.preventDefault()
        if (ready) void submit()
      }}
    >
      <div className="field">
        <span className="field-label">Assets</span>
        <PickMany label="Assets to run" placeholder="Choose assets" value={targets} onChange={setTargets} options={assets.map((name) => ({ value: name, label: name }))} />
      </div>
      <div className="field">
        <span className="field-label">Partitions</span>
        <Segmented label="Which partitions" value={which} onChange={setWhich} options={[...WHICH]} />
        <span className="field-note">{WHICH_HELP[which]}</span>
      </div>
      {which === "list" && <TextField label="Partition names" hideLabel value={named} onChange={setNamed} rows={3} mono placeholder={"alpha\nday=2026-09-01,site=alpha"} />}
      <div className="field">
        <span className="field-label">How much to load</span>
        <Segmented
          label="How much to load"
          value={mode}
          onChange={setMode}
          options={[
            { value: "incremental", label: "Only what changed" },
            { value: "full", label: "Everything again" },
          ]}
        />
      </div>
      {mode === "full" && <Notice tone="stale">A full run loads every key again. An asset that keeps a total starts it over.</Notice>}
      <Checkmark checked={upstream} onChange={setUpstream}>
        Run the assets they read first
      </Checkmark>
      <div className="row" style={{ justifyContent: "flex-end" }}>
        <Button type="button" onClick={close}>
          Not now
        </Button>
        <Button type="submit" variant="primary" disabled={!ready}>
          Start the run
        </Button>
      </div>
    </form>
  )
}
