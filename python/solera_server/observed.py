"""The observation record (docs/observed-set.md): what a consumer partition
has processed of one keyed incremental input — its observed set `S`, key ->
observation — stored as three layers, the first that holds a key deciding:

- **points**: `key -> {present, version, patterns, context, life}`, explicit;
- **ranges**: disjoint half-open key ranges `(lo, hi] -> layer`: every key in
  it as upstream had it after commit `endpoint`, if the layer's patterns
  take it, else absent (`lo`/`hi` None: unbounded);
- the **base**: one layer for every other key (`endpoint` None: the empty
  base, every key absent; `held`: a per-key consumer's full run, every key
  its outputs or stored outcomes hold present at no upstream version).

A layer is `{endpoint, patterns, context, life}`: the commit it was observed
at, the input's patterns then (normalised: None takes every key), the id of
the whole and dep versions it was processed under (`contexts`), and the
upstream index's life (its prefix). Pure functions over JSON-able dicts:
the engine decides with them and records the resulting operations, which
the model applies (an event is a decision)."""

from __future__ import annotations

OLDER = object()  # `at`'s answer for a key present at an endpoint, at a version since replaced


def gaps(rec: dict) -> list[tuple]:
    """The key ranges `(lo, hi]` where the empty base decides: none if the
    base is a commit or a per-key consumer's held keys, else what the
    ranges leave uncovered between the first key and the last. An unkeyed
    input's record — the last commit it read — has none."""

    if "commit" in rec or rec["base"]["endpoint"] is not None or rec["base"].get("held"):
        return []
    out, edge = [], None
    for r in rec["ranges"]:
        if r["lo"] != edge:
            out.append((edge, r["lo"]))
        edge = r["hi"]
        if edge is None:
            return out
    return [*out, (edge, None)]
