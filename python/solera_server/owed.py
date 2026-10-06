"""What a consumer partition owes of a keyed input (docs/observed-set.md):
candidates from its observation record's layers, read through the index's
Δ(P, H, keys) and scans at H; each classed once, from its decoded old
state to its new one at the head H; a task's batches walking them in key
order from its progress; and what a batch's commit records, the fold
included.

The index serves no key view at an older commit, nor versions there:
only Δ, with presence at both ends, and scans at H. So a layer observed at
P decodes a key it did not change since at H's version, and one changed
since — present at P, by Δ's flips — at a version since replaced (None),
which a present key is owed an update for: the redundant update a revert
to the old version costs, in exchange for no old versions kept.

Classes: `added` (not held, present now), `removed` (held, absent or no
longer taken), `updated` (held at another version, or under another
context), and — for a key a run asked for that is held as it is —
`unchanged`. A key's version is the source's own word where its index
entry carries one (its payload), else the generation that wrote it.

`held`: a per-key consumer's own indexes (its outputs and stored
outcomes), what a held base decodes from — a key there is present at no
upstream version, so owed an update if upstream has it, else a removal."""

from __future__ import annotations

import copy
from collections.abc import AsyncIterator
from dataclasses import dataclass

from solera.keys.delta import delta, version_of
from solera.keys.layers import LayerIndex
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


async def _live(index: LayerIndex, at: int, after: str | None, hi: str | None) -> AsyncIterator:
    """Every key live at H, `at`, in `(after, hi]`: a scan at H, as Δ(−∞, H)."""

    async for d in _diffs(index, None, at, after, hi):
        yield d


async def _diffs(index: LayerIndex, p, h, after, hi) -> AsyncIterator:
    while True:
        page = await delta(index, p, h, after=after, first=CHUNK)
        for d in page.diffs:
            if hi is not None and d.key > hi:
                return
            yield d
        if page.cursor is None or (hi is not None and page.cursor >= hi):
            return
        after = page.cursor


async def _holding(held, after: str | None, hi: str | None) -> AsyncIterator:
    """Every key a per-key consumer holds in `(after, hi]`: the union of its indexes."""

    stream = _none()
    for index in held or ():
        stream = _either(_merged(stream, _diffs(index, None, None, after, hi)))
    async for d in stream:
        yield d


async def _either(pairs) -> AsyncIterator:
    async for _, x, y in pairs:
        yield x or y


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


async def candidates(
    index: LayerIndex, rec: dict, now: Now, after: str | None = None, held=None
) -> AsyncIterator[Owe]:
    """Every key the partition may owe past `after`, in key order, classed
    (`cls` None: owed nothing). A segment observed under the labels of now
    yields its changes since its head; one under other patterns or context
    is compared whole — a scan at H, its keys' presence at the segment's
    head from Δ's flips; a held base against what the consumer holds; a
    point is decided by itself."""

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
        if layer.get("held"):
            stream = _merged(_holding(held, start, hi), _live(index, now.head, start, hi))
            whole = True
        elif layer["endpoint"] is not None and _same(layer, now, cid):
            stream = _merged(_diffs(index, layer["endpoint"], now.head, start, hi), _none())
            whole = False
        else:
            changed = (
                _none()
                if layer["endpoint"] is None
                else _diffs(index, layer["endpoint"], now.head, start, hi)
            )
            stream = _merged(changed, _live(index, now.head, start, hi))
            whole = True

        def point(key, at_h=at_h):
            p = rec["points"][key]
            old = (p["version"], rec["contexts"].get(p["context"], {})) if p["present"] else None
            d = at_h.get(key)
            return _owe(key, old, d if d is not None and take(key) else None, now.context, versioned=True)

        left = iter(mine)  # the segment's points, merged in as the stream passes them: nothing collected
        nxt = next(left, None)
        async for key, before, current in stream:
            while nxt is not None and nxt < key:
                yield point(nxt)
                nxt = next(left, None)
            if key in rec["points"]:
                continue
            if layer.get("held"):
                old = (None, {}) if before else None
                new = current if current and take(key) else None
            elif whole:
                if before is not None:  # changed since the segment's head: present then by Δ's flips
                    was, version = before.before, None
                else:  # unchanged since: as at H
                    was = current is not None and layer["endpoint"] is not None
                    version = version_of(current.generation, current.payload) if was else None
                old = (version, context) if was and then(key) else None
                new = current if current and take(key) else None
            else:  # a difference between the layer's head and H, under one label
                d = before
                old = (None, context) if d.before and then(key) else None  # at a version since replaced
                new = d if d.after and take(key) else None
            yield _owe(key, old, new, now.context, versioned=whole)
        while nxt is not None:
            yield point(nxt)
            nxt = next(left, None)


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


async def uncovered(index: LayerIndex, rec: dict, take) -> bool:
    """Whether a key present upstream now, that `take` takes, decodes from
    the empty base: in a gap of the record (`observed.gaps`) and no point.
    Each gap a scan at the head, stopping at the first such key."""

    for lo, hi in observed.gaps(rec):
        async for d in _diffs(index, None, None, lo, hi):
            if take(d.key) and d.key not in rec["points"]:
                return True
    return False


async def owed(index: LayerIndex, rec: dict, now: Now, after: str | None = None, held=None) -> list[Owe]:
    """Every key owed past `after`: the staleness, and what a default run loads."""

    return [o async for o in candidates(index, rec, now, after, held) if o.cls is not None]


async def batch(
    index: LayerIndex, rec: dict, now: Now, size: int, after: str | None = None, keys=None, held=None
) -> Batch:
    """A task's next batch past its progress `after`, at H. By default the
    first `size` owed keys, covering up to the last; with `keys` a list,
    the next `size` of those named, each owed — one the input's patterns
    leave out, a removal if held — or `unchanged`; with `keys="all"`, every
    key present at H, owed or `unchanged`, and every owed removal."""

    take = Matcher(now.patterns)
    if isinstance(keys, list):
        named = sorted(k for k in set(keys) if after is None or k > after)
        page, rest = named[:size], named[size:]
        mine = [k for k in page if take(k)]
        found = {d.key: d for d in (await delta(index, None, now.head, keys=mine)).diffs} if mine else {}
        out = []
        for key in page:
            old = await _decoded(index, rec, key, held)
            d = found.get(key)
            owe = _owe(key, old, d, now.context, versioned=True)
            if owe.cls is None and d is not None:
                owe = Owe(key, "unchanged", old, owe.new, owe.generation)
            if owe.cls is not None:
                out.append(owe)
        return Batch(out, after, page[-1] if page else after, not rest)
    picked: list[Owe] = []
    source = candidates(index, rec, now, after, held)
    if keys == "all":
        source = _with_unchanged(index, rec, now, after, source, held)
    async for owe in source:
        if owe.cls is None:
            continue
        picked.append(owe)
        if len(picked) == size:
            more = await anext(_owed_only(source), None)
            if more is None:
                break
            return Batch(picked, after, picked[-1].key, False)
    return Batch(picked, after, None, True)


async def _owed_only(stream):
    async for owe in stream:
        if owe.cls is not None:
            yield owe


async def _with_unchanged(index, rec, now, after, owed_stream, held=None) -> AsyncIterator[Owe]:
    """`keys="all"`: every key present at H taken by the patterns, owed or
    `unchanged`, and the owed removals among them, all in key order."""

    take = Matcher(now.patterns)
    async for key, owe, d in _merged(owed_stream, _live(index, now.head, after, None)):
        if owe is not None and owe.cls is not None:  # owed (a removal: absent at H)
            yield owe
        elif d is None or not take(key):
            continue
        elif owe is not None:  # compared, owed nothing: its old observation is known
            yield Owe(key, "unchanged", owe.old, owe.new, owe.generation)
        else:  # not a candidate: its layer saw it as it is now, unchanged since
            holder = observed.holder(rec, key)
            context = rec["contexts"].get(holder.get("context"), {})
            new = version_of(d.generation, d.payload)
            yield Owe(key, "unchanged", (new, context), new, d.generation)


async def _decoded(index: LayerIndex, rec: dict, key: str, held=None) -> tuple | None:
    """`key`'s observation as its record decodes it, read at the head: its
    layer's — observed at P — at the head's version if Δ(P, head) does not
    name it, else, present at P by its flips, at a version since replaced."""

    holder = observed.holder(rec, key)
    if holder.get("held"):
        for index in held or ():
            if (await delta(index, None, None, keys=[key])).diffs:
                return None, {}
        return None
    if "present" in holder or holder["endpoint"] is None or not Matcher(holder["patterns"])(key):
        return observed.decode(rec, key, lambda e, k: None)
    context = rec["contexts"].get(holder["context"], {})
    changed = (await delta(index, holder["endpoint"], None, keys=[key])).diffs
    if changed:
        return (None, context) if changed[0].before else None
    now = (await delta(index, None, None, keys=[key])).diffs
    return (version_of(now[0].generation, now[0].payload), context) if now else None


async def commit_ops(
    index: LayerIndex, rec: dict, now: Now, b: Batch, served: dict | None = None, *, named=False, held=None
) -> list[dict]:
    """What a batch's commit records: its range `(after, end] @ H` — or, for
    keys named outright, a point each — the points of what a source served
    otherwise (`served`: key -> version, None for no row), then the fold:
    every older range relabels to H, its keys changed since its head kept as
    points — present ones at a version since replaced — and a point that
    decodes the same without it goes."""

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
            was = await _decoded(index, work, d.key, held)
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
        without = await _decoded(index, {**work, "points": {}}, key, held)
        mine = (p["version"], work["contexts"].get(p["context"], {})) if p["present"] else None
        if without == mine:
            fold.append({"op": "drop", "key": key})
    return ops + fold
