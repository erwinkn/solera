/**
 * A layered layout for small DAGs (tens to a few hundred nodes): layers by
 * longest path from the roots, order within a layer by barycenter sweeps,
 * then each node placed near the mean height of its neighbours without
 * overlapping. Deterministic, so the graph doesn't shuffle between polls.
 */

export interface LayoutInput {
  nodes: string[];
  edges: { from: string; to: string }[];
  width: number;
  height: number;
  gapX: number;
  gapY: number;
}

export interface Layout {
  positions: Map<string, { x: number; y: number; layer: number }>;
  width: number;
  height: number;
}

export function layout({ nodes, edges, width, height, gapX, gapY }: LayoutInput): Layout {
  const preds = new Map<string, string[]>(nodes.map((n) => [n, []]));
  const succs = new Map<string, string[]>(nodes.map((n) => [n, []]));
  for (const { from, to } of edges) {
    if (!preds.has(from) || !preds.has(to) || from === to) continue;
    preds.get(to)!.push(from);
    succs.get(from)!.push(to);
  }

  // Longest path layering (the graph is a DAG; a cycle would be a manifest bug,
  // so guard with a depth bound rather than loop forever).
  const layerOf = new Map<string, number>();
  const visit = (n: string, depth: number): number => {
    const known = layerOf.get(n);
    if (known !== undefined) return known;
    if (depth > nodes.length) return 0;
    const ps = preds.get(n)!;
    const layer = ps.length ? Math.max(...ps.map((p) => visit(p, depth + 1))) + 1 : 0;
    layerOf.set(n, layer);
    return layer;
  };
  for (const n of nodes) visit(n, 0);

  // Pull roots right, next to their first consumer, so a source sits by its asset.
  for (const n of nodes) {
    const ss = succs.get(n)!;
    if (preds.get(n)!.length === 0 && ss.length) {
      layerOf.set(n, Math.max(0, Math.min(...ss.map((s) => layerOf.get(s)!)) - 1));
    }
  }

  const layers: string[][] = [];
  for (const n of [...nodes].sort()) (layers[layerOf.get(n)!] ??= []).push(n);
  for (let i = 0; i < layers.length; i++) layers[i] ??= [];

  // Barycenter ordering, alternating down and up sweeps.
  const index = new Map<string, number>();
  const reindex = () => layers.forEach((layer) => layer.forEach((n, i) => index.set(n, i)));
  reindex();
  const bary = (n: string, side: Map<string, string[]>) => {
    const ns = side.get(n)!;
    return ns.length ? ns.reduce((s, m) => s + index.get(m)!, 0) / ns.length : index.get(n)!;
  };
  for (let sweep = 0; sweep < 6; sweep++) {
    const down = sweep % 2 === 0;
    const order = down ? layers.keys() : [...layers.keys()].reverse();
    for (const i of order) {
      const side = down ? preds : succs;
      layers[i]!.sort((a, b) => bary(a, side) - bary(b, side) || a.localeCompare(b));
      layers[i]!.forEach((n, j) => index.set(n, j));
    }
  }

  // Heights: each node near the mean of its predecessors, no overlaps, in order.
  const y = new Map<string, number>();
  const step = height + gapY;
  layers.forEach((layer, li) => {
    const wanted = layer.map((n) => {
      const ps = preds.get(n)!.filter((p) => y.has(p));
      return ps.length && li > 0 ? ps.reduce((s, p) => s + y.get(p)!, 0) / ps.length : null;
    });
    let next = 0;
    layer.forEach((n, j) => {
      const target = wanted[j] ?? j * step;
      const placed = Math.max(target, next);
      y.set(n, placed);
      next = placed + step;
    });
  });
  // Roots placed after their consumers: line them up with what they feed.
  for (const layer of layers) {
    let next = -Infinity;
    for (const n of layer) {
      const ss = succs.get(n)!;
      const wanted =
        preds.get(n)!.length === 0 && ss.length
          ? ss.reduce((s, m) => s + y.get(m)!, 0) / ss.length
          : y.get(n)!;
      const placed = Math.max(wanted, next);
      y.set(n, placed);
      next = placed + step;
    }
  }

  const top = Math.min(0, ...y.values());
  const positions = new Map<string, { x: number; y: number; layer: number }>();
  let maxY = 0;
  for (const n of nodes) {
    const layer = layerOf.get(n)!;
    const py = y.get(n)! - top;
    positions.set(n, { x: layer * (width + gapX), y: py, layer });
    maxY = Math.max(maxY, py + height);
  }
  return {
    positions,
    width: layers.length * (width + gapX) - gapX,
    height: maxY,
  };
}
