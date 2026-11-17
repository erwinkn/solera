import type { Manifest } from "../api/types"

/* The plan: assets and sources, and which reads which. Laid out as a rail,
 * the way a git log draws branches: every node that something reads owns a
 * vertical line from its row down to its last reader, and each reader taps
 * that line. A chain a → b → c is one straight line. */

export type Node = { id: string; kind: "asset" | "job" | "source" }
export type Edge = { from: string; to: string; kind: "reads" | "partitions" }
export type Graph = { nodes: Node[]; edges: Edge[] }

export function projectGraph(manifest: Manifest): Graph {
  const owner = (output: string) => manifest.outputs[output]?.asset ?? (manifest.sources[output] ? output : null)
  const edges = new Map<string, Edge>()
  const add = (from: string | null, to: string, kind: Edge["kind"]) => {
    if (!from || from === to) return
    const key = `${from}\u0000${to}`
    if (kind === "reads" || !edges.has(key)) edges.set(key, { from, to, kind })
  }
  for (const [name, asset] of Object.entries(manifest.assets)) {
    for (const dimension of Object.values(asset.partitions?.dims ?? {}))
      if (dimension.kind === "dynamic") add(owner((dimension as { output: string }).output), name, "partitions")
    for (const input of Object.values(asset.inputs)) add(owner(input.output), name, "reads")
    for (const dep of asset.deps) add(owner(dep), name, "reads")
  }
  const nodes: Node[] = [
    ...Object.keys(manifest.sources).map((id): Node => ({ id, kind: "source" })),
    ...Object.entries(manifest.assets).map(([id, asset]): Node => ({ id, kind: asset.outputs.length ? "asset" : "job" })),
  ]
  return { nodes, edges: [...edges.values()] }
}

/** Upstream first, and a node's readers right after it where they can be, so
 * chains stay together. Declaration order breaks ties. */
export function topological(ids: string[], edges: Pick<Edge, "from" | "to">[]): string[] {
  const known = new Set(ids)
  const parents = new Map<string, Set<string>>(ids.map((id) => [id, new Set()]))
  const children = new Map<string, string[]>(ids.map((id) => [id, []]))
  for (const { from, to } of edges)
    if (known.has(from) && known.has(to) && !parents.get(to)!.has(from)) {
      parents.get(to)!.add(from)
      children.get(from)!.push(to)
    }
  const out: string[] = []
  const done = new Set<string>()
  const visit = (id: string) => {
    if (done.has(id) || [...parents.get(id)!].some((p) => !done.has(p))) return
    done.add(id)
    out.push(id)
    for (const child of children.get(id)!) visit(child)
  }
  for (const id of ids) visit(id)
  for (const id of ids) if (!done.has(id)) out.push(id) // a cycle: never from a manifest, but never lose a node
  return out
}

/** What one row of the rail draws, in lanes (0 is leftmost). */
export type RailRow = {
  lane: number
  /** Lines passing this row from top to bottom, with the node they belong to. */
  through: { lane: number; owner: string }[]
  /** Lines from above that reach this node: `ends` when this is the line's last reader. */
  taps: { lane: number; owner: string; ends: boolean }[]
  /** This node's own line leaves downward. */
  starts: boolean
}
export type Rail = { rows: Map<string, RailRow>; lanes: number }

export function layoutRail(order: string[], edges: Pick<Edge, "from" | "to">[]): Rail {
  const row = new Map(order.map((id, i) => [id, i]))
  const last = new Map<string, number>()
  const parents = new Map<string, string[]>(order.map((id) => [id, []]))
  for (const { from, to } of edges) {
    const a = row.get(from)
    const b = row.get(to)
    if (a === undefined || b === undefined || b <= a) continue
    parents.get(to)!.push(from)
    last.set(from, Math.max(last.get(from) ?? a, b))
  }
  // lanes[i]: the node whose line occupies lane i, and the row it ends at
  const lanes: ({ owner: string; until: number } | null)[] = []
  const rows = new Map<string, RailRow>()
  let width = 0
  order.forEach((id, i) => {
    const mine = new Set(parents.get(id))
    const taps: RailRow["taps"] = []
    const through: RailRow["through"] = []
    lanes.forEach((line, lane) => {
      if (!line) return
      if (mine.has(line.owner)) taps.push({ lane, owner: line.owner, ends: line.until === i })
      else through.push({ lane, owner: line.owner })
    })
    // continue a parent's line that ends here, else take the leftmost free lane
    let lane = taps.find((tap) => tap.ends)?.lane ?? -1
    for (const tap of taps) if (tap.ends) lanes[tap.lane] = null
    if (lane === -1) {
      lane = lanes.findIndex((line) => line === null)
      if (lane === -1) lane = lanes.length
    }
    const until = last.get(id)
    lanes[lane] = until === undefined ? null : { owner: id, until }
    width = Math.max(width, lane + 1, lanes.length)
    while (lanes.length && lanes[lanes.length - 1] === null) lanes.pop()
    rows.set(id, { lane, through, taps, starts: until !== undefined })
  })
  return { rows, lanes: width }
}
