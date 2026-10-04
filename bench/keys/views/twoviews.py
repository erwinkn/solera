"""The two views (docs/key-index-two-views-design.md), prototyped on real
`.kx` files through the shipped `KeyIndex` readers and native jobs.

- The time view (T): an aligned tree over commit numbers, fanout `b`. Level
  0 is a commit's delta; level 1 a pack (the b deltas copied into one
  object, one section each); level j >= 2 a node, the net change over
  `[s, s + b^j - 1]`, built from its b children by the net merge
  (`Merge.spans` with no endpoints and `net=True`: after from the newest,
  before from the oldest, absent-to-absent dropped).
- The key view (K): the base (every live key at `w`, no filter) plus T's
  cover of `[w + 1, head]`, newest first.
- Retention: K's chain and each pinned K's chain, by name; and for readers,
  by `retention`:
  - "floor": every level from the oldest reader start (nodes starting at or
    after it, packs ending at or after it). Bounded by a commit horizon the
    caller applies: readers more than X behind are dropped (Erwin's call).
  - "cover": the units of the canonical cover of every active reader
    interval `[start, end]` (end None: the head), nothing else. Nobody is
    dropped; every start, end and landing point must be an interval.
  K's base watermark holds only its chain, never every level since (A25 R8).

A reader rebuilds a `TwoViews` from `to_json` and reads through `PackIO`,
which serves pack sections as files: a small pack in one GET, a large one
by ranges.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace

from solera import _native
from solera.keys.index import Changes, FileInfo, IndexState, KeyIndex, Options, Span
from solera.keys.io import ObjectIO
from solera.keys.threads import in_thread

SMALL = 2 * 2**20  # a pack this small is read whole, once


class PackIO(ObjectIO):
    """`ObjectIO` that reads `{pack}@{offset}+{pack size}.kx` as the section
    of the pack at that offset: from one whole read of a small pack, or by
    ranges of a large one."""

    def __init__(self, store, **kw):
        super().__init__(store, **kw)
        self._packs: dict[str, asyncio.Future] = {}

    async def _pack(self, obj: str, total: int) -> bytes:
        fut = self._packs.get(obj)
        if fut is None:
            if len(self._packs) > 256:
                self._packs.clear()
            fut = asyncio.ensure_future(ObjectIO.read(self, obj, 0, total, total))
            self._packs[obj] = fut
        return await fut

    async def read(self, path: str, start: int, end: int, size: int) -> bytes:
        if "@" not in path:
            return await super().read(path, start, end, size)
        base, rest = path[: -len(".kx")].rsplit("@", 1)
        off, total = (int(x) for x in rest.split("+"))
        obj = base + ".kx"
        if total <= SMALL:
            return (await self._pack(obj, total))[off + start : off + end]
        return await super().read(obj, off + start, off + end, total)

    async def read_whole(self, path: str, size: int) -> bytes:
        return await self.read(path, 0, size, size)


@dataclass
class Written:
    entries: int = 0
    bytes: int = 0
    puts: int = 0

    def add(self, files: list[FileInfo]):
        self.entries += sum(f.entries for f in files)
        self.bytes += sum(f.size for f in files)
        self.puts += len(files)


def _fi(d: list) -> list[FileInfo]:
    return [FileInfo.from_json(x) for x in d]


def _js(files: list[FileInfo]) -> list:
    return [f.to_json() for f in files]


class TwoViews:
    def __init__(
        self, io: ObjectIO, prefix: str, opts: Options, *, b: int = 4, top: int = 10, retention: str = "floor"
    ):
        self.io, self.prefix, self.o, self.b, self.top = io, prefix, opts, b, top
        self.retention = retention
        self.base_o = replace(opts, bits_per_item=0)  # the base keeps no filter
        self.head = -1
        self.w = -1  # the base holds the state after commit w
        self.base: list[FileInfo] = []
        self.deltas: dict[int, list[FileInfo]] = {}  # unpacked commits
        self.packs: dict[int, list[tuple[int, list[FileInfo]]]] = {}  # start -> [(commit, sections)]
        self.pack_objs: dict[int, tuple[str, int]] = {}  # start -> (object name, size)
        self.nodes: dict[tuple[int, int], list[FileInfo]] = {}  # (level >= 2, start) -> files
        self.pins: dict[str, tuple[int, list[FileInfo], int]] = {}  # name -> (w, base files, S)
        self.next_pack = 0
        self.next_node: dict[int, int] = {}
        self.intervals: list[tuple[int, int | None]] = []  # active readers' [start, end], set by the caller
        self.written = {"pack": Written(), "node": Written(), "base": Written()}
        self.deleted_bytes = 0

    # -- state ----------------------------------------------------------------------------

    def to_json(self) -> dict:
        return {
            "prefix": self.prefix,
            "b": self.b,
            "top": self.top,
            "head": self.head,
            "w": self.w,
            "base": _js(self.base),
            "deltas": {str(c): _js(f) for c, f in self.deltas.items()},
            "packs": {str(s): [[c, _js(f)] for c, f in secs] for s, secs in self.packs.items()},
            "nodes": [[j, s, _js(f)] for (j, s), f in self.nodes.items()],
            "pins": {n: [w, _js(f), at] for n, (w, f, at) in self.pins.items()},
        }

    @classmethod
    def from_json(cls, io: ObjectIO, opts: Options, d: dict) -> TwoViews:
        v = cls(io, d["prefix"], opts, b=d["b"], top=d["top"])
        v.head, v.w, v.base = d["head"], d["w"], _fi(d["base"])
        v.deltas = {int(c): _fi(f) for c, f in d["deltas"].items()}
        v.packs = {int(s): [(c, _fi(f)) for c, f in secs] for s, secs in d["packs"].items()}
        v.nodes = {(j, s): _fi(f) for j, s, f in d["nodes"]}
        v.pins = {n: (w, _fi(f), at) for n, (w, f, at) in d["pins"].items()}
        return v

    def index(self, runs: list[list[FileInfo]], opts: Options | None = None) -> KeyIndex:
        """A `KeyIndex` over `runs` (newest first), each a span of its own."""

        runs = [r for r in runs if r]
        spans = tuple(Span(i, i, ((i, 0),), tuple(r)) for i, r in enumerate(reversed(runs)))
        return KeyIndex(self.io, self.prefix, IndexState(spans=spans, prefix=self.prefix), opts or self.o)

    # -- T: units, covers ----------------------------------------------------------------------

    def built(self, j: int, s: int) -> bool:
        if j == 0:
            return s in self.deltas or (s - s % self.b) in self.packs
        if j == 1:
            return s in self.packs
        return (j, s) in self.nodes

    def unit_runs(self, unit) -> list[list[FileInfo]]:
        kind, s = unit
        if kind == 0:
            if s in self.deltas:
                return [self.deltas[s]]
            return [f for c, f in self.packs[s - s % self.b] if c == s]
        if kind == 1:
            return [f for _, f in reversed(self.packs[s])]
        return [self.nodes[(kind, s)]]

    def cover(self, first: int, last: int) -> list:
        """The units tiling commits [first, last], oldest first."""

        out, c, b = [], first, self.b
        while c <= last:
            # The largest built node starting at c that ends by `last` (a level
            # in between may be gone: a chain keeps its nodes, not their children).
            j = next(
                (j for j in range(self.top, 0, -1) if c % b**j == 0 and c + b**j - 1 <= last and self.built(j, c)),
                0,
            )
            if not self.built(j, c):
                raise LookupError(f"commit {c} is not held (floor passed it, or never written)")
            out.append((j, c))
            c += b**j
        return out

    def runs(self, first: int, last: int) -> list[list[FileInfo]]:
        """The cover's runs, newest first."""

        out = []
        for unit in reversed(self.cover(first, last)):
            out.extend(self.unit_runs(unit))
        return [r for r in out if r]

    def k_runs(self, pin: str | None = None) -> list[list[FileInfo]]:
        """K's runs, newest first: the chain, then the base (or a pin's)."""

        if pin is None:
            return self.runs(self.w + 1, self.head) + [self.base]
        w, base, at = self.pins[pin]
        return self.runs(w + 1, at) + [base]

    # -- writing ----------------------------------------------------------------------------

    def k(self) -> KeyIndex:
        return self.index(self.k_runs())

    def commit(self, c: int, files: list[FileInfo]) -> None:
        if c != self.head + 1:
            raise ValueError(f"commit {c} after head {self.head}")
        self.head = c
        if c == 0:  # the base load: the base itself
            self.base, self.w = list(files), 0
            self.next_pack = 0
            return
        self.deltas[c] = list(files)

    def pin(self, name: str) -> None:
        """Pin K as of the head (a full pass's snapshot)."""

        self.pins[name] = (self.w, list(self.base), self.head)

    async def unpin(self, name: str) -> None:
        w, base, _ = self.pins.pop(name)
        if base != self.base and not any(b == base for _, b, _ in self.pins.values()):
            await self._delete(base)

    def floor(self) -> int:
        """The oldest reader start: T keeps every level from it."""

        return min((a for a, _ in self.intervals), default=self.head + 1)

    def chains(self) -> set:
        """The units K's chain and every pinned chain use: kept by name."""

        out = set(self.cover(self.w + 1, self.head))
        for w, _, at in self.pins.values():
            out |= set(self.cover(w + 1, at))
        if self.retention == "cover":
            for a, e in self.intervals:
                e = self.head if e is None else min(e, self.head)
                if a <= e:
                    out |= set(self.cover(a, e))
        return out

    async def _delete(self, files: list[FileInfo]) -> None:
        self.deleted_bytes += sum(f.size for f in files)
        await self.io.delete([f"{self.prefix}{f.name}.kx" for f in files if "@" not in f.name])

    async def upkeep(self) -> None:
        """Packs, nodes and base merges due at the head, then what no chain
        and no reader needs any more."""

        await self._packs()
        await self._nodes()
        await self._base()
        await self._collect()

    async def _packs(self) -> None:
        b = self.b
        while self.next_pack + b - 1 <= self.head:
            s = self.next_pack
            self.next_pack += b
            commits = [c for c in range(s, s + b) if c in self.deltas]
            blobs, metas = [], []
            for c in commits:
                for f in self.deltas[c]:
                    blobs.append(await self.io.read_whole(f"{self.prefix}{f.name}.kx", f.size))
                    metas.append(c)
            data = b"".join(blobs)
            name = f"p{s:012d}"
            await self.io.write(f"{self.prefix}{name}.kx", data)
            off, secs = 0, {}
            for c, blob in zip(metas, blobs, strict=True):
                secs.setdefault(c, []).append(FileInfo.describe(f"{name}@{off}+{len(data)}", blob))
                off += len(blob)
            self.packs[s] = [(c, secs.get(c, [])) for c in range(s, s + b)]
            self.pack_objs[s] = (name, len(data))
            w = self.written["pack"]
            w.entries += sum(f.entries for fs in secs.values() for f in fs)
            w.bytes += len(data)
            w.puts += 1
            for c in commits:
                await self._delete(self.deltas.pop(c))

    def _children(self, j: int, s: int) -> list | None:
        step = self.b ** (j - 1)
        kids = [(j - 1, s + i * step) for i in range(self.b)]
        return kids if all(self.built(kj, ks) for kj, ks in kids) else None

    async def _merge(self, runs, name, *, base=False, opts=None) -> list[FileInfo]:
        o = opts or self.o
        idx = self.index(runs, o)
        job = _native.Merge.spans(len(runs), endpoints=[], base=base, net=not base, **idx._writer())
        return await idx._run(job, runs, lambda n: f"{name}.{n:04d}")

    async def _nodes(self) -> None:
        b = self.b
        for j in range(2, self.top + 1):
            size = b**j
            s = self.next_node.get(j, 0)
            # The child level has been processed (built or skipped) below this.
            done = self.next_pack if j == 2 else self.next_node.get(j - 1, 0)
            while s + size <= done:
                kids = self._children(j, s)
                if kids is not None and s >= min(self.floor(), self.w + 1):
                    runs = []
                    for kid in reversed(kids):
                        runs.extend(self.unit_runs(kid))
                    files = await self._merge([r for r in runs if r], f"n{j}-{s:012d}")
                    self.nodes[(j, s)] = files
                    self.written["node"].add(files)
                s += size
            self.next_node[j] = s

    async def _base(self) -> None:
        """The base absorbs the chain once its built part holds a quarter of the base."""

        ends = [s + self.b**j - 1 for (j, s) in self.nodes if s + self.b**j - 1 > self.w]
        if not ends:
            return
        e = max(ends)
        try:
            chain = self.runs(self.w + 1, e)
        except LookupError:
            return
        base_n = sum(f.entries for f in self.base)
        if sum(f.entries for r in chain for f in r) * 4 < base_n:
            return
        files = await self._merge(chain + [self.base], f"b{e:012d}", base=True, opts=self.base_o)
        self.written["base"].add(files)
        old = self.base
        self.base, self.w = files, e
        if not any(old == p for _, p, _ in self.pins.values()):
            await self._delete(old)

    async def _delete_pack(self, s: int) -> None:
        self.packs.pop(s)
        name, size = self.pack_objs.pop(s)
        self.deleted_bytes += size
        await self.io.delete([f"{self.prefix}{name}.kx"])

    async def _collect(self) -> None:
        f, keep = self.floor(), self.chains()
        packs = {s - s % self.b for j, s in keep if j in (0, 1)}
        cover = self.retention == "cover"
        for (j, s) in list(self.nodes):
            if (cover or s < f) and (j, s) not in keep:
                await self._delete(self.nodes.pop((j, s)))
        for s in list(self.packs):
            if (cover or s + self.b - 1 < f) and s not in packs:
                await self._delete_pack(s)

    def stored(self) -> dict:
        """Bytes held, by kind (pinned bases counted once each)."""

        seen, pinned = {f.name for f in self.base}, 0
        for _, files, _ in self.pins.values():
            new = [x for x in files if x.name not in seen]
            pinned += sum(x.size for x in new)
            seen |= {x.name for x in new}
        return {
            "base": sum(x.size for x in self.base),
            "pinned": pinned,
            "packs": sum(size for _, size in self.pack_objs.values()),
            "nodes": sum(x.size for fs in self.nodes.values() for x in fs),
            "deltas": sum(x.size for fs in self.deltas.values() for x in fs),
        }

    # -- reading -------------------------------------------------------------------------------

    async def changes_page(self, first: int, last: int, after, limit: int) -> Changes:
        runs = self.runs(first, last)
        if not runs:
            return Changes([], b"", [], b"", [], None)
        idx = self.index(runs)
        ks, cs, gs, ds, ps, cursor = await idx._read_page(runs, after, limit, changes=(0, None))
        return _net(Changes(list(ks), bytes(cs), list(gs), bytes(ds), list(ps), cursor))

    async def changes_of(self, first: int, last: int, keys: list[bytes]) -> Changes:
        runs = self.runs(first, last)
        idx = self.index(runs)
        blocks, codecs = await idx._key_blocks(runs, keys)
        ks, cs, gs, ds, ps, _, _ = await in_thread(_native.span_changes, blocks, codecs, None, None, 2**62, 0, None)
        want = set(keys)
        pick = [i for i, k in enumerate(ks) if k in want and not (cs[i] == NEITHER and ds[i])]
        return Changes(
            [ks[i] for i in pick],
            bytes(cs[i] for i in pick),
            [gs[i] for i in pick],
            bytes(ds[i] for i in pick),
            [ps[i] for i in pick],
            None,
        )

    async def lookup(self, keys: list[bytes], *, pin: str | None = None):
        return await self.index(self.k_runs(pin)).lookup(keys)

    async def page(self, after, limit: int, *, pin: str | None = None):
        return await self.index(self.k_runs(pin)).page(after, limit)


def _net(page: Changes) -> Changes:
    """A page without the keys absent at both ends: a cover read from single
    commits reports them as "neither"; T never delivers them (read-ahead keys
    are classed from K)."""

    keep = [i for i in range(len(page.keys)) if not (page.classes[i] == NEITHER and page.deleted[i])]
    if len(keep) == len(page.keys):
        return page
    return Changes(
        [page.keys[i] for i in keep],
        bytes(page.classes[i] for i in keep),
        [page.generations[i] for i in keep],
        bytes(page.deleted[i] for i in keep),
        [page.payloads[i] for i in keep],
        page.cursor,
    )


# The read-ahead rule (docs/key-index-two-views-design.md § Read-ahead), with
# the review's guard: an entry read at or after the pass's end N is the
# delivered state of a later read; this pass leaves the key alone.
ADDED, UPDATED, REMOVED, NEITHER = 0, 1, 2, 3


def read_ahead(delivered: dict, at_n: dict, last: int) -> dict:
    """`delivered`: key -> (r, generation read, delivered live); `at_n`: key ->
    (generation, payload) for the keys live at N. Returns key -> class (or
    None: skip) for every read-ahead key."""

    out = {}
    for k, (r, g_read, was_live) in delivered.items():
        if r >= last:
            out[k] = None  # read at or after N: newer than anything this pass delivers
            continue
        now = at_n.get(k)
        if was_live and now is not None:
            out[k] = None if now[0] <= g_read else UPDATED
        elif was_live:
            out[k] = REMOVED
        elif now is not None:
            out[k] = ADDED
        else:
            out[k] = None
    return out
