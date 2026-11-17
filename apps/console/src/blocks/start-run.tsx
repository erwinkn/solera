import { useState, useSyncExternalStore } from "react"
import { useLocation } from "wouter"
import { useManifest, useStalePartitions, useStartRun, useStatuses, type RunInput } from "../api/queries"
import type { Manifest } from "../api/types"
import { plural } from "../model/format"
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
    <Modal open={preset !== null} onOpenChange={(open) => !open && close()} title="Start a run" focus='[aria-label="Assets to run"]'>
      {preset && <Form key={JSON.stringify(preset)} preset={preset} close={close} />}
    </Modal>
  )
}

const WHICH = [
  { value: "stale", label: "Stale" },
  { value: "all", label: "All" },
  { value: "list", label: "Selected" },
] as const
type Which = (typeof WHICH)[number]["value"]

/** `targets` and, with `upstream`, every asset they read, directly or not. */
function withUpstream(manifest: Manifest | undefined, targets: string[], upstream: boolean): string[] {
  const seen = new Set(targets)
  if (manifest && upstream)
    for (const name of seen) // a Set visits what is added while it is walked
      for (const output of [...Object.values(manifest.assets[name]?.inputs ?? {}).map((input) => input.output), ...(manifest.assets[name]?.deps ?? [])]) {
        const owner = manifest.outputs[output]?.asset
        if (owner) seen.add(owner)
      }
  return [...seen]
}

function Form({ preset, close }: { preset: Preset; close: () => void }) {
  const manifest = useManifest()
  const statuses = useStatuses()
  const start = useStartRun()
  const stalePartitions = useStalePartitions()
  const [, navigate] = useLocation()
  const [targets, setTargets] = useState<string[]>(preset.targets ?? [])
  const [which, setWhich] = useState<Which>(preset.partitions ? "list" : "stale")
  const [named, setNamed] = useState(preset.partitions ?? "")
  const [mode, setMode] = useState<RunInput["mode"]>(preset.mode ?? "incremental")
  const [upstream, setUpstream] = useState(false)
  const specs = manifest.data?.assets ?? {}
  const names = named.split(/[\s]+/).filter(Boolean)

  // The partitions choice is about the assets that have partitions; one without has a single state, and always runs.
  const partitioned = targets.filter((name) => specs[name]?.partitions)
  const whole = partitioned.length < targets.length ? [""] : []
  const count = (status: "stale" | "total") => partitioned.reduce((sum, name) => sum + (statuses.data?.assets[name]?.partitions[status] ?? 0), 0)
  // Incremental or full is a choice only where an asset reads an input incrementally.
  const incremental = withUpstream(manifest.data, targets, upstream).some((name) => Object.values(specs[name]?.inputs ?? {}).some((input) => input.kind === "incremental"))
  const nothing = which === "stale" && statuses.data !== undefined && count("stale") === 0 && whole.length === 0
  const ready = targets.length > 0 && (which !== "list" || names.length > 0) && !nothing

  const submit = async () => {
    const partitions = async () => {
      if (partitioned.length === 0 || which === "all") return "all"
      return [...whole, ...(which === "list" ? names : await stalePartitions(partitioned))]
    }
    const result = await report(
      partitions().then((partitions) => start.mutateAsync({ targets, partitions, mode: incremental ? mode : "incremental", upstream })),
      (run) => ("id" in run ? "The run is under way" : "Nothing started: that work is already running or queued"),
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
        <PickMany label="Assets to run" placeholder="Choose assets" value={targets} onChange={setTargets} options={Object.keys(specs).map((name) => ({ value: name, label: name }))} />
      </div>
      {partitioned.length > 0 && (
        <div className="field">
          <span className="field-label">Partitions</span>
          <Segmented label="Which partitions" value={which} onChange={setWhich} options={[...WHICH]} />
          {which !== "list" && statuses.data && <span className="field-note num">{plural(count(which === "stale" ? "stale" : "total"), "partition")}</span>}
        </div>
      )}
      {partitioned.length > 0 && which === "list" && (
        <TextField label="Partition names" hideLabel value={named} onChange={setNamed} rows={3} mono placeholder={"alpha\nday=2026-09-01,site=alpha"} />
      )}
      {incremental && (
        <div className="field">
          <span className="field-label">Mode</span>
          <Segmented
            label="Mode"
            value={mode}
            onChange={setMode}
            options={[
              { value: "incremental", label: "Incremental" },
              { value: "full", label: "Full run" },
            ]}
          />
        </div>
      )}
      {incremental && mode === "full" && <Notice tone="stale">A full run loads every key again. An asset that keeps a total starts it over.</Notice>}
      <Checkmark checked={upstream} onChange={setUpstream}>
        Run dependencies first
      </Checkmark>
      <div className="row" style={{ justifyContent: "flex-end" }}>
        <Button type="button" onClick={close}>
          Cancel
        </Button>
        <Button type="submit" variant="primary" disabled={!ready}>
          Start
        </Button>
      </div>
    </form>
  )
}
