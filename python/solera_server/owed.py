"""What a consumer partition owes of a keyed input (docs/observed-set.md):
candidates from its observation record's layers, read through the index's
Δ(P, H, keys); each classed once, from its decoded old state to its new
one at the head H; a task's batches walking them in key order from its
progress; and what a batch's commit records, the fold included.

Classes: `added` (not held, present now), `removed` (held, absent or no
longer taken), `updated` (held at another version, or under another
context), and — for a key a run asked for that is held as it is —
`unchanged`. A key's version is the source's own word where its index
entry carries one (its payload), else the generation that wrote it."""

from __future__ import annotations

import copy
from collections.abc import AsyncIterator
from dataclasses import dataclass

from solera.keys.delta import delta
from solera.keys.index import KeyIndex
from solera.patterns import Matcher

from . import observed

CHUNK = 1000  # differences one Δ read asks for


@dataclass(frozen=True)
class Now:
    """The comparison's new side: the head H (a commit), the input's patterns
    and the whole and dep versions now, and the upstream index's life."""

    head: int
    patterns: dict | None
    context: dict
    life: str


@dataclass(frozen=True)
class Owe:
    """One key of a batch: its class, its decoded old observation `(version,
    context)` or None, and its version and generation at H (None: absent)."""

    key: str
    cls: str
    old: tuple | None
    new: object | None
    generation: int | None


@dataclass(frozen=True)
class Batch:
    """A task's next batch: its keys, the key range it covers, `(after, end]`
    — `end` None to the last key — and whether it is the walk's last."""

    keys: list[Owe]
    after: str | None
    end: str | None
    final: bool


def version_of(generation, payload):
    if isinstance(payload, bytes):
        return payload.decode()
    return payload if payload is not None else generation


def segments(rec: dict):
    """The key space in order, as `(lo, hi, layer)`: the ranges, the base filling the gaps."""

    lo = None
    for r in rec["ranges"]:
        if r["lo"] != lo:
            yield lo, r["lo"], rec["base"]
        yield r["lo"], r["hi"], r
        lo = r["hi"]
        if lo is None:
            return
    yield lo, None, rec["base"]


def _same(layer: dict, now: Now, cid: str) -> bool:
    return (
        layer["patterns"] == observed.normalised(now.patterns)
        and layer["context"] == cid
        and layer["life"] == now.life
    )


async def _live(index: KeyIndex, at: int | None, after: str | None, hi: str | None) -> AsyncIterator:
    """Every key live after commit `at` (None: none are) in `(after, hi]`, as Δ(−∞, at)."""

    if at is None:
        return
    async for d in _diffs(index, None, at, after, hi):
        yield d


async def _diffs(index: KeyIndex, p, h, after, hi) -> AsyncIterator:
    while True:
        page = await delta(index, p, h, after=after, first=CHUNK)
        for d in page.diffs:
            if hi is not None and d.key > hi:
                return
            yield d
        if page.cursor is None or (hi is not None and page.cursor >= hi):
            return
        after = page.cursor


async def _merged(a: AsyncIterator, b: AsyncIterator) -> AsyncIterator:
    """Two key-ordered streams as `(key, from a, from b)`."""

    sentinel = object()
    x, y = await anext(a, sentinel), await anext(b, sentinel)
    while x is not sentinel or y is not sentinel:
        if y is sentinel or (x is not sentinel and x.key < y.key):
            yield x.key, x, None
            x = await anext(a, sentinel)
        elif x is sentinel or y.key < x.key:
            yield y.key, None, y
            y = await anext(b, sentinel)
        else:
            yield x.key, x, y
            x, y = await anext(a, sentinel), await anext(b, sentinel)


async def candidates(index: KeyIndex, rec: dict, now: Now, after: str | None = None) -> AsyncIterator[Owe]:
    """Every key the partition may owe past `after`, in key order, classed
    (`cls` None: owed nothing). A segment observed under the labels of now
    yields its changes since its head; one under other patterns or context
    is compared whole, both ends read; a point is decided by itself."""

    cid = observed.context_id(now.context)
    take = Matcher(now.patterns)
    points = sorted(k for k in rec["points"] if after is None or k > after)
    for lo, hi, layer in segments(rec):
        if hi is not None and after is not None and hi <= after:
            continue
        start = after if after is not None and (lo is None or after > lo) else lo
        mine = [k for k in points if (start is None or k > start) and (hi is None or k <= hi)]
        at_h = {d.key: d for d in (await delta(index, None, now.head, keys=mine)).diffs} if mine else {}
        then = Matcher(layer["patterns"])
        context = rec["contexts"].get(layer["context"], {})
        if layer["endpoint"] is not None and _same(layer, now, cid):
            stream = _merged(_diffs(index, layer["endpoint"], now.head, start, hi), _none())
            whole = False
        else:
            stream = _merged(_live(index, layer["endpoint"], start, hi), _live(index, now.head, start, hi))
            whole = True
        found = []
        async for key, before, current in stream:
            if key in rec["points"]:
                continue
            if whole:
                old = (
                    (version_of(before.generation, before.payload), context) if before and then(key) else None
                )
                new = current if current and take(key) else None
            else:  # a difference between the layer's head and H, under one label
                d = before
                old = (None, context) if d.before and then(key) else None  # its version: read when needed
                new = d if d.after and take(key) else None
            found.append(_owe(key, old, new, now.context, versioned=whole))
        for key in mine:
            p = rec["points"][key]
            old = (p["version"], rec["contexts"].get(p["context"], {})) if p["present"] else None
            d = at_h.get(key)
            found.append(
                _owe(key, old, d if d is not None and take(key) else None, now.context, versioned=True)
            )
        for owe in sorted(found, key=lambda o: o.key):
            yield owe


async def _none() -> AsyncIterator:
    return
    yield


def _owe(key, old, new, context, *, versioned: bool) -> Owe:
    version = version_of(new.generation, new.payload) if new is not None else None
    generation = new.generation if new is not None else None
    if old is None and new is None:
        cls = None
    elif old is None:
        cls = "added"
    elif new is None:
        cls = "removed"
    elif not versioned:  # it differs between the layer's head and H: changed
        cls = "updated"
    else:
        cls = "updated" if old[0] != version or old[1] != context else None
    return Owe(key, cls, old, version, generation)


async def owed(index: KeyIndex, rec: dict, now: Now, after: str | None = None) -> list[Owe]:
    """Every key owed past `after`: the staleness, and what a default run loads."""

    return [o async for o in candidates(index, rec, now, after) if o.cls is not None]


async def batch(
    index: KeyIndex, rec: dict, now: Now, size: int, after: str | None = None, keys=None
) -> Batch:
    """A task's next batch past its progress `after`, at H. By default the
    first `size` owed keys, covering up to the last; with `keys` a list,
    the next `size` of those named (the input's patterns filtering them),
    each owed or `unchanged`; with `keys="all"`, every key present at H,
    owed or `unchanged`, and every owed removal."""

    take = Matcher(now.patterns)
    if isinstance(keys, list):
        named = sorted(k for k in set(keys) if take(k) and (after is None or k > after))
        page, rest = named[:size], named[size:]
        found = {d.key: d for d in (await delta(index, None, now.head, keys=page)).diffs} if page else {}
        out = []
        for key in page:
            old = await _decoded(index, rec, key)
            d = found.get(key)
            owe = _owe(key, old, d, now.context, versioned=True)
            if owe.cls is None and d is not None:
                owe = Owe(key, "unchanged", old, owe.new, owe.generation)
            if owe.cls is not None:
                out.append(owe)
        return Batch(out, after, page[-1] if page else after, not rest)
    picked: list[Owe] = []
    source = candidates(index, rec, now, after)
    if keys == "all":
        source = _with_unchanged(index, rec, now, after, source)
    async for owe in source:
        if owe.cls is None:
            continue
        picked.append(owe)
        if len(picked) == size:
            more = await anext(_owed_only(source), None)
            if more is None:
                break
            return Batch(await _filled(index, rec, picked), after, picked[-1].key, False)
    return Batch(await _filled(index, rec, picked), after, None, True)


async def _owed_only(stream):
    async for owe in stream:
        if owe.cls is not None:
            yield owe


async def _with_unchanged(index, rec, now, after, owed_stream) -> AsyncIterator[Owe]:
    """`keys="all"`: every key present at H taken by the patterns, owed or
    `unchanged`, and the owed removals among them, all in key order."""

    take = Matcher(now.patterns)
    owed = {owe.key: owe async for owe in owed_stream if owe.cls is not None}
    left = sorted(owed)
    async for d in _live(index, now.head, after, None):
        while left and left[0] < d.key:  # owed, and absent at H: a removal
            yield owed[left.pop(0)]
        if left and left[0] == d.key:
            yield owed[left.pop(0)]
        elif take(d.key):
            old = await _decoded(index, rec, d.key)
            yield Owe(d.key, "unchanged", old, version_of(d.generation, d.payload), d.generation)
    for key in left:
        yield owed[key]


async def _decoded(index: KeyIndex, rec: dict, key: str) -> tuple | None:
    found = {}

    async def at(endpoint, k):
        if endpoint not in found:
            found[endpoint] = {d.key: d for d in (await delta(index, None, endpoint, keys=[k])).diffs}
        d = found[endpoint].get(k)
        return None if d is None else version_of(d.generation, d.payload)

    holder = observed.holder(rec, key)
    if "present" not in holder and holder["endpoint"] is not None and Matcher(holder["patterns"])(key):
        version = await at(holder["endpoint"], key)
        return None if version is None else (version, rec["contexts"].get(holder["context"], {}))
    return observed.decode(rec, key, lambda e, k: None)


async def _filled(index: KeyIndex, rec: dict, owes: list[Owe]) -> list[Owe]:
    """The batch's old observations whose versions a difference did not carry,
    read at their layers' heads."""

    out = []
    for owe in owes:
        if owe.old is not None and owe.old[0] is None:
            owe = Owe(owe.key, owe.cls, await _decoded(index, rec, owe.key), owe.new, owe.generation)
        out.append(owe)
    return out


async def commit_ops(
    index: KeyIndex, rec: dict, now: Now, b: Batch, served: dict | None = None, *, named=False
) -> list[dict]:
    """What a batch's commit records: its range `(after, end] @ H` — or, for
    keys named outright, a point each — the points of what a source served
    otherwise (`served`: key -> version, None for no row), then the fold:
    every older range relabels to H, its keys changed since its head kept as
    points at their observed version, and a point that decodes the same
    without it goes."""

    work = copy.deepcopy(rec)
    label = observed.layer(work, now.head, now.patterns, now.context, now.life)
    ops: list[dict] = [{"op": "context", "id": label["context"], "versions": dict(now.context)}]
    if named:
        for owe in b.keys:
            ops.append(
                {
                    "op": "point",
                    "key": owe.key,
                    "present": owe.new is not None,
                    "version": owe.new,
                    "label": label,
                }
            )
    else:
        ops.append({"op": "range", "lo": b.after, "hi": b.end, "label": label})
    for key, version in (served or {}).items():
        planned = next((o.new for o in b.keys if o.key == key), None)
        if version != planned:
            ops.append(
                {
                    "op": "point",
                    "key": key,
                    "present": version is not None,
                    "version": version,
                    "label": label,
                }
            )
    observed.apply(work, ops)
    fold = []
    for r in list(work["ranges"]):
        if r["endpoint"] >= now.head:
            continue
        span = {"patterns": r["patterns"], "context": r["context"], "life": r["life"]}
        async for d in _diffs(index, r["endpoint"], now.head, r["lo"], r["hi"]):
            if d.key in work["points"]:
                continue
            was = await _decoded(index, work, d.key)
            if was is None:
                fold.append({"op": "point", "key": d.key, "present": False, "label": span})
            else:
                fold.append({"op": "point", "key": d.key, "present": True, "version": was[0], "label": span})
        fold.append({"op": "relabel", "lo": r["lo"], "hi": r["hi"], "endpoint": now.head})
    observed.apply(work, fold)
    for key, p in list(work["points"].items()):
        holder = next((r for r in work["ranges"] if observed._in(key, r["lo"], r["hi"])), work["base"])
        if (
            holder["endpoint"] is None
            or holder["context"] != p["context"]
            or holder["patterns"] != p["patterns"]
        ):
            continue
        without = await _decoded(index, {**work, "points": {}}, key)
        mine = (p["version"], work["contexts"].get(p["context"], {})) if p["present"] else None
        if without == mine:
            fold.append({"op": "drop", "key": key})
    return ops + fold
