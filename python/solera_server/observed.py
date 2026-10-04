"""The observation record (docs/observed-set.md): what a consumer partition
has processed of one keyed incremental input — its observed set `S`, key ->
observation — stored as three layers, the first that holds a key deciding:

- **points**: `key -> {present, version, patterns, context, life}`, explicit;
- **ranges**: disjoint half-open key ranges `(lo, hi] -> layer`: every key in
  it as upstream had it after commit `endpoint`, if the layer's patterns
  take it, else absent (`lo`/`hi` None: unbounded);
- the **base**: one layer for every other key (`endpoint` None: the empty
  base, every key absent; `held`: a per-key consumer's full run, every key
  its outputs or failure records hold present at no upstream version).

A layer is `{endpoint, patterns, context, life}`: the commit it was observed
at, the input's patterns then (normalised: None takes every key), the id of
the whole and dep versions it was processed under (`contexts`), and the
upstream index's life (its prefix). Pure functions over JSON-able dicts:
the engine decides with them and records the resulting operations, which
the model applies (an event is a decision)."""

from __future__ import annotations

import json
from collections.abc import Callable
from hashlib import blake2b

from solera.patterns import Matcher

OLDER = object()  # `at`'s answer for a key present at an endpoint, at a version since replaced


def normalised(patterns: dict | None) -> dict | None:
    """An input's patterns as a layer keeps them: None for every key (no
    include is the universal include, never an empty list, A27 R3)."""

    if not patterns:
        return None
    out = {k: v for k, v in patterns.items() if v}
    return out or None


def context_id(versions: dict) -> str:
    """A stable id for the whole and dep versions a layer was processed under."""

    return blake2b(json.dumps(versions, sort_keys=True).encode(), digest_size=8).hexdigest()


def record(life: str, patterns: dict | None = None, context: dict | None = None, *, held=False) -> dict:
    """An empty observation record: the empty base, in `life` — or, `held`,
    the base of a per-key consumer's full run: what it holds."""

    out = {"base": {"endpoint": None, "patterns": normalised(patterns), "context": None, "life": life}}
    if held:
        out["base"]["held"] = True
    out.update(ranges=[], points={}, contexts={})
    if context is not None:
        out["base"]["context"] = _context(out, context)
    return out


def _context(rec: dict, versions: dict) -> str:
    cid = context_id(versions)
    rec["contexts"][cid] = dict(versions)
    return cid


def layer(rec: dict, endpoint: int | None, patterns: dict | None, context: dict, life: str) -> dict:
    """A layer's label, its context entered in the record's table."""

    return {
        "endpoint": endpoint,
        "patterns": normalised(patterns),
        "context": _context(rec, context),
        "life": life,
    }


def _in(key: str, lo: str | None, hi: str | None) -> bool:
    return (lo is None or key > lo) and (hi is None or key <= hi)


def holder(rec: dict, key: str) -> dict:
    """The layer that decides `key`: its point, the range holding it, or the base."""

    if key in rec["points"]:
        return rec["points"][key]
    return next((r for r in rec["ranges"] if _in(key, r["lo"], r["hi"])), rec["base"])


def decode(
    rec: dict, key: str, at: Callable[[int, str], object], held: Callable[[str], bool] = lambda key: False
) -> tuple | None:
    """The observation of `key`: `(version, context versions)`, or None for
    not held (observed absent and never observed are one). `at(endpoint,
    key)` is upstream's version of `key` after commit `endpoint`, None if
    absent — or `OLDER`, present then at a version the index no longer
    keeps, decoded as version None; `held(key)`, whether a per-key consumer
    holds it (a held base, its keys at no upstream version either)."""

    found = holder(rec, key)
    if "present" in found:  # a point
        if not found["present"]:
            return None
        return found["version"], rec["contexts"].get(found["context"], {})
    if found.get("held"):  # present at no upstream version
        return (None, {}) if held(key) else None
    if found["endpoint"] is None or not Matcher(found["patterns"])(key):
        return None
    version = at(found["endpoint"], key)
    if version is None:
        return None
    return (None if version is OLDER else version), rec["contexts"].get(found["context"], {})


def overwrite(rec: dict, lo: str | None, hi: str | None, label: dict) -> None:
    """A batch observed every key in `(lo, hi]` at `label`: the range goes in,
    splitting every range it overlaps — their parts outside stay — and the
    points inside go, which it supersedes."""

    kept = []
    for r in rec["ranges"]:
        if not _overlaps(r["lo"], r["hi"], lo, hi):
            kept.append(r)
            continue
        if r["lo"] is None and lo is not None or r["lo"] is not None and lo is not None and r["lo"] < lo:
            kept.append({**r, "hi": lo})  # its part below
        if hi is not None and (r["hi"] is None or r["hi"] > hi):
            kept.append({**r, "lo": hi})  # its part above
    kept.append({**label, "lo": lo, "hi": hi})
    rec["ranges"] = sorted(kept, key=lambda r: (r["lo"] is not None, r["lo"] or ""))
    rec["points"] = {k: p for k, p in rec["points"].items() if not _in(k, lo, hi)}


def _overlaps(a_lo, a_hi, b_lo, b_hi) -> bool:
    below = a_hi is not None and b_lo is not None and a_hi <= b_lo
    above = b_hi is not None and a_lo is not None and b_hi <= a_lo
    return not (below or above)


def point(rec: dict, key: str, present: bool, version, label: dict) -> None:
    """A batch observed `key` as it says: present at `version`, or absent."""

    rec["points"][key] = {
        "present": present,
        "version": version if present else None,
        **{k: label[k] for k in ("patterns", "context", "life")},
    }


def relabel(rec: dict, lo: str | None, hi: str | None, endpoint: int) -> None:
    """The fold: the range `(lo, hi]` decodes the same at `endpoint` (its
    changed keys were made points first)."""

    for r in rec["ranges"]:
        if r["lo"] == lo and r["hi"] == hi:
            r["endpoint"] = endpoint


def drop(rec: dict, key: str) -> None:
    """An override that decodes the same without it."""

    rec["points"].pop(key, None)


def tidy(rec: dict) -> None:
    """Adjacent ranges with one label merge; one spanning every key becomes
    the base; contexts no layer names go."""

    merged: list[dict] = []
    for r in rec["ranges"]:
        last = merged[-1] if merged else None
        if last is not None and last["hi"] == r["lo"] and _label(last) == _label(r):
            last["hi"] = r["hi"]
        else:
            merged.append(dict(r))
    if len(merged) == 1 and merged[0]["lo"] is None and merged[0]["hi"] is None:
        rec["base"], merged = _label(merged[0]), []
    rec["ranges"] = merged
    named = {
        rec["base"]["context"],
        *(r["context"] for r in merged),
        *(p["context"] for p in rec["points"].values()),
    }
    rec["contexts"] = {cid: v for cid, v in rec["contexts"].items() if cid in named}


def _label(r: dict) -> dict:
    return {k: r[k] for k in ("endpoint", "patterns", "context", "life")}


def apply(rec: dict, ops: list[dict]) -> None:
    """A commit's decided operations, in order, then `tidy`: what the model
    applies to a partition's record (the engine computed them, folds
    included)."""

    for op in ops:
        kind = op["op"]
        if kind == "context":
            rec["contexts"][op["id"]] = dict(op["versions"])
        elif kind == "range":
            overwrite(rec, op["lo"], op["hi"], op["label"])
        elif kind == "point":
            point(rec, op["key"], op["present"], op.get("version"), op["label"])
        elif kind == "relabel":
            relabel(rec, op["lo"], op["hi"], op["endpoint"])
        elif kind == "drop":
            drop(rec, op["key"])
        elif kind == "reset":
            rec.clear()
            rec.update(op["record"])
        else:
            raise ValueError(f"an observation record has no operation {kind!r}")
    tidy(rec)
