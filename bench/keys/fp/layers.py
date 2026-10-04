"""Stamped layers (docs/key-index-from-first-principles.md), the phase 2
prototype: bench code, over `ObjectIO` and the native kernels in
`native/src/layers.rs`.

- A commit writes its **delta**: sorted keys, change kinds (added, updated,
  removed), a source's payload, in self-delimiting zstd blocks; no index,
  filter or footer. Its block boundaries go to the state (small deltas are
  read whole and need none; a larger delta's become its layer index object).
  A delta is the layer [c, c], read with the commit's generation as stamp.
- A **layer** covers commits [a, b]: per key changed in it, presence at b,
  stamp (generation of its last change), flips (generations of its adds and
  removes newer than the cut), payload, and whether it was present at the
  layer's start. Its **main** part holds what head reads and readers after
  the layer need; its **side** part what only a reader whose P falls inside
  the layer needs: in the base (the oldest layer) every absent key (its
  graveyard), elsewhere keys absent at both ends (added and removed inside:
  under churn, most temporary keys).
- **Upkeep** merges adjacent layers: four of a tier, or the base absorbing
  the layers above it once they hold a quarter of it; every merge's output,
  if it reaches past the cut, holds at most k x (bytes of every newer layer)
  + Z. The **cut** is the oldest live P (the newer of it and the window's
  edge, if a window is set); merges drop flips at or below it.
- **Δ(P, H, keys)** reads the layers that end after P: per key with an entry
  stamped after g(P), presence at H from the newest entry, presence at P
  flipped once per flip after g(P). P = None is −∞.

Lifecycle (A17's checklist, where it applies): object names carry the
index's life, the writing engine's epoch and a unique id; a merge's attempt
is counted in the state before its upload, three per input set at most;
publication checks the life and that the inputs are still current; replaced
files become garbage at publication and are deleted once no pin taken before
it remains, by an engine whose state write after listing succeeded (the
barrier); the orphan collector deletes only unreferenced files of its own
epoch or older, judged after its listing.
"""

from __future__ import annotations

import asyncio
import bisect
import heapq
import math
import uuid
from dataclasses import asdict, dataclass, field

from solera import _native

DELTA, LAYER = 0, 1
ADDED, UPDATED, REMOVED = 0, 1, 2
BLOCK = 16 * 1024
FILE_LIMIT = 64 * 2**20
SMALL = 256 * 1024  # a part this small is read whole and has no index object
F, KREAD, Z, B0, R = 4, 4.0, 1 * 2**20, 8 * 1024, 4.0
ATTEMPTS = 3
DELTA_TO_LAYER = 1.45  # a stamped entry is ~1.4x a delta entry (measured: 8.1 against 5.8 B)

# The cold reader's plan model (docs: query form 1): requests 30 ms each, 64
# in flight, 80 MB/s per connection (the harness's link), decoding 120M
# entries/s; ranges of 16 MB.
RTT, PARALLEL, LINK, DECODE, RANGE = 0.030, 64, 80e6, 120e6, 16 * 2**20


class CutError(ValueError):
    """A read with P below the cut: its flips may be gone. The caller falls
    back to a full compare."""


@dataclass
class Part:
    files: list[list]  # [path, size, entries]
    index: str | None = None  # the layer index object, None when read whole
    index_size: int = 0

    @property
    def size(self) -> int:
        return sum(f[1] for f in self.files)

    @property
    def entries(self) -> int:
        return sum(f[2] for f in self.files)

    def paths(self) -> list[str]:
        return [f[0] for f in self.files] + ([self.index] if self.index else [])


@dataclass
class Layer:
    id: str
    a: int
    b: int
    main: Part
    side: Part | None = None
    stamp: int | None = None  # a delta's generation; None for a stamped layer

    @property
    def size(self) -> int:
        return self.main.size + (self.side.size if self.side else 0)

    @property
    def entries(self) -> int:
        return self.main.entries + (self.side.entries if self.side else 0)

    def parts(self) -> list[Part]:
        return [self.main] + ([self.side] if self.side else [])

    def paths(self) -> list[str]:
        return [p for part in self.parts() for p in part.paths()]

    def to_json(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_json(d: dict) -> Layer:
        g = d.get("side")
        return Layer(d["id"], d["a"], d["b"], Part(**d["main"]), Part(**g) if g else None, d.get("stamp"))


@dataclass
class State:
    """What the journal holds for one index: its life, layers (oldest first),
    the cut, generations of commits at or after the cut, the publication
    sequence, garbage awaiting deletion, pins and merge attempts."""

    prefix: str
    life: str = "l0"
    layers: list[Layer] = field(default_factory=list)
    cut: int = -1
    gens: dict[int, int] = field(default_factory=dict)
    seq: int = 0
    garbage: list[list] = field(default_factory=list)  # [path, seq at which it was replaced]
    pins: dict[str, int] = field(default_factory=dict)  # name -> seq when taken
    attempts: dict[str, int] = field(default_factory=dict)
    stopped: list[str] = field(default_factory=list)

    @property
    def head(self) -> int:
        return self.layers[-1].b if self.layers else -1

    def to_json(self) -> dict:
        d = asdict(self)
        d["layers"] = [x.to_json() for x in self.layers]
        d["gens"] = {str(k): v for k, v in self.gens.items()}
        return d

    @staticmethod
    def from_json(d: dict) -> State:
        s = State(**{k: v for k, v in d.items() if k not in ("layers", "gens")})
        s.layers = [Layer.from_json(x) for x in d["layers"]]
        s.gens = {int(k): v for k, v in d["gens"].items()}
        return s

    def g(self, p: int) -> int:
        """The generation of commit p (P must be at or after the cut)."""
        if p < self.cut:
            raise CutError(f"P = {p} is below the cut {self.cut}")
        return self.gens[p]


def tier(size: int) -> int:
    return max(0, int(math.log(max(size, B0) / B0, F)))


def estimate(layers: list[Layer]) -> float:
    """An upper bound on a merge's output bytes: its inputs', deltas' grown
    to stamped entries."""
    return sum(x.size * (DELTA_TO_LAYER if x.stamp is not None else 1.0) for x in layers)


# -- reading ---------------------------------------------------------------------------------


class Index:
    """A part's blocks: first keys, files, offsets, lengths, newest stamps,
    entry counts."""

    def __init__(self, raw: bytes):
        self.first, self.file, self.off, self.len, self.max, self.count = _native.index_decode(raw)

    def blocks(self, after: bytes | None, upto: bytes | None) -> range:
        """The blocks that may hold keys in (after, upto]."""
        lo = 0 if after is None else max(0, bisect.bisect_right(self.first, after) - 1)
        hi = len(self.first) if upto is None else bisect.bisect_right(self.first, upto)
        return range(lo, max(lo, hi))

    def block_of(self, key: bytes) -> int:
        return bisect.bisect_right(self.first, key) - 1


class Reader:
    """Reads one manifest (the state a batch pinned) through `io`. Caches
    what a reader fetched (indexes, small parts, block windows), as the
    engine does across a run's batches; nothing else."""

    def __init__(self, io, state: State, window: int = 1 << 20, seek_only: bool = False, side_always: bool = False):
        self.io, self.s = io, state
        self.side_always = side_always  # read every side part (measures what the split saves)
        self.window = window
        self.seek_only = seek_only  # the engine on its own files: no request costs to weigh
        self._ix: dict[str, Index] = {}
        self._whole: dict[str, bytes] = {}
        self._blocks: dict[tuple[str, int], bytes] = {}

    # -- fetching ---------------------------------------------------------------------

    async def indexes(self, parts: list[Part]) -> None:
        """One round trip: every part's index object, or the whole part when
        it is small."""

        async def one(p: Part):
            if p.index is not None:
                if p.index not in self._ix:
                    self._ix[p.index] = Index(await self.io.read_whole(p.index, p.index_size))
            else:
                for path, size, _ in p.files:
                    if path not in self._whole:
                        self._whole[path] = await self.io.read_whole(path, size)

        await asyncio.gather(*(one(p) for p in parts))

    async def chunks(self, p: Part, blocks: list[int], extend: bool = True) -> list[bytes]:
        """Whole blocks `blocks` (sorted) of part `p`, as chunks in key order;
        missing blocks are fetched in one range GET per contiguous group,
        extended to `window` bytes when `extend`."""
        if p.index is None:
            return [self._whole[f[0]] for f in p.files]
        ix = self._ix[p.index]
        need = [i for i in blocks if (p.index, i) not in self._blocks]
        groups: list[list[int]] = []
        for i in need:
            if groups and i == groups[-1][-1] + 1 and ix.file[i] == ix.file[groups[-1][-1]]:
                groups[-1].append(i)
            else:
                groups.append([i])

        async def fetch(g: list[int]):
            first, last = g[0], g[-1]
            if extend:
                total = sum(ix.len[i] for i in g)
                while total < self.window and last + 1 < len(ix.first) and ix.file[last + 1] == ix.file[first]:
                    last += 1
                    total += ix.len[last]
            f = p.files[ix.file[first]]
            start, end = ix.off[first], ix.off[last] + ix.len[last]
            data = await self.io.read(f[0], start, end, f[1])
            for i in range(first, last + 1):
                o = ix.off[i] - start
                self._blocks[(p.index, i)] = data[o : o + ix.len[i]]

        await asyncio.gather(*(fetch(g) for g in groups))
        return [self._blocks[(p.index, i)] for i in blocks]

    # -- Δ(P, H, after c, first N) -----------------------------------------------------

    def _over(self, p: int | None) -> list[tuple[Layer, list[Part]]]:
        """Newest first: the layers that end after P and the parts to read
        (a layer's side part only when P falls inside it)."""
        out = []
        for x in reversed(self.s.layers):
            if p is not None and x.b <= p:
                break
            inside = p is not None and (x.a <= p or self.side_always)
            parts = [x.main] + ([x.side] if x.side and inside else [])
            out.append((x, parts))
        return out

    def _upto(self, over, g_p: int | None, p: int | None, after: bytes | None, limit: int) -> bytes | None:
        """A key bound such that the layers' entries in (after, bound] are
        about `limit` x 1.5 (a straddled layer counted by its share of commits
        after P; stale entries make the distinct keys fewer); None means the
        end."""
        target = limit * 1.5
        streams = []
        for x, parts in over:
            share = 1.0
            if p is not None and x.a <= p:
                share = (x.b - p) / (x.b - x.a + 1)
            for part in parts:
                if part.index is None:
                    continue  # small: in hand whole, it costs nothing to read further
                ix = self._ix[part.index]
                rng = ix.blocks(after, None)
                streams.append([(ix.first[i], ix.count[i] * share, ix.max[i]) for i in rng])
        if not streams:
            return None
        acc = 0.0
        for first, n, mx in heapq.merge(*streams, key=lambda t: t[0]):
            if g_p is not None and mx <= g_p:
                continue
            acc += n
            if acc >= target and (after is None or first > after):
                return first
        return None

    async def page(self, p: int | None, after: bytes | None, limit: int, pattern=None) -> tuple:
        """Δ(P, H, after `after`, first `limit`): (keys, at_p, at_h, stamps,
        payloads, cursor); cursor None at the end. `pattern` (a function on a
        block's (first, next first) interval, `globs.intersects`-like) skips
        blocks that cannot hold a match; keys are still filtered by the
        caller."""
        g_p = None if p is None else self.s.g(p)
        over = self._over(p)
        await self.indexes([part for _, parts in over for part in parts])
        keys, at_p, at_h, stamps, payloads = [], bytearray(), bytearray(), [], []
        while True:
            upto = self._upto(over, g_p, p, after, limit - len(keys))
            inputs = []
            for x, parts in over:  # each part an input of its own (a base's two parts hold disjoint keys)
                for part in parts:
                    if part.index is None:
                        inputs.append(([self._whole[f[0]] for f in part.files], x.stamp or 0))
                        continue
                    ix = self._ix[part.index]
                    blocks = [
                        i
                        for i in ix.blocks(after, upto)
                        if (g_p is None or ix.max[i] > g_p)
                        and (pattern is None or pattern(ix.first[i], ix.first[i + 1] if i + 1 < len(ix.first) else None))
                    ]
                    inputs.append((await self.chunks(part, blocks), x.stamp or 0))
            k, ap, ah, st, pl, last = _native.layers_scan(inputs, g_p, after, upto, limit - len(keys))
            keys += k
            at_p += ap
            at_h += ah
            stamps += st
            payloads += pl
            if last is not None:
                return keys, bytes(at_p), bytes(at_h), stamps, payloads, last
            if upto is None:
                return keys, bytes(at_p), bytes(at_h), stamps, payloads, None
            after = upto  # short of `limit`: read on (another round trip)

    # -- full scans --------------------------------------------------------------------

    async def scan_all(self, pattern: bytes | None = None, prefix: bytes | None = None, slice_bytes: int = 64 << 20, parallel: int = 4) -> dict:
        """Δ(−∞, H, everything, pattern): every present key matching, read in
        key-range slices of about `slice_bytes` of the largest layer, every
        layer's blocks of a slice fetched in parallel range GETs (16 MB each,
        64 in flight), `parallel` slices decoded at once (the native scan
        releases the GIL). A literal `prefix` bounds the key range (a seek).
        Returns counts; nothing is cached."""
        over = self._over(None)
        parts = [x.main for x, _ in over]
        await self.indexes(parts)
        lo = prefix
        hi = None if prefix is None else prefix + b"\xff" * 8
        big = max((p for p in parts if p.index is not None), key=lambda p: p.size, default=None)
        bounds: list[bytes | None] = [lo]
        if big is not None:
            ix = self._ix[big.index]
            acc = 0
            for i in ix.blocks(lo, hi):
                acc += ix.len[i]
                if acc >= slice_bytes:
                    bounds.append(ix.first[i])
                    acc = 0
        bounds.append(hi)
        sem = asyncio.Semaphore(parallel)
        found = 0

        async def one(after, upto):
            nonlocal found
            native_after = None if after is lo else after  # a key equal to the prefix counts
            async with sem:
                inputs = []
                for x, _ in over:
                    part = x.main
                    if part.index is None:
                        inputs.append(([self._whole[f[0]] for f in part.files], x.stamp or 0))
                        continue
                    ix = self._ix[part.index]
                    blocks = list(ix.blocks(after, upto))
                    chunks = []
                    for f in sorted({ix.file[i] for i in blocks}):
                        bs = [i for i in blocks if ix.file[i] == f]
                        if bs:
                            start, end = ix.off[bs[0]], ix.off[bs[-1]] + ix.len[bs[-1]]
                            chunks.append(await self.io.read(part.files[f][0], start, end, part.files[f][1]))
                    inputs.append((chunks, x.stamp or 0))
                keys, *_ = await asyncio.to_thread(_native.layers_scan, inputs, None, native_after, upto, 2**62, pattern)
                found += len(keys)

        await asyncio.gather(*(one(a, b) for a, b in zip(bounds, bounds[1:], strict=False)))
        return {"matches": found, "slices": len(bounds) - 1}

    # -- sorted key lists --------------------------------------------------------------

    def _plan(self, p: Part, keys: list[bytes]) -> list[int] | None:
        """The blocks of `p` the keys fall in, or None to stream the part:
        whichever is faster (request waves, bytes over a connection,
        decoding), fewer requests on a tie within 10%."""
        ix = self._ix[p.index]
        blocks = sorted({b for b in (ix.block_of(k) for k in keys) if b >= 0})
        if self.seek_only:
            return blocks
        groups = sum(1 for j, b in enumerate(blocks) if j == 0 or b != blocks[j - 1] + 1)
        per = max(ix.len) if ix.len else 0
        seek = (math.ceil(groups / PARALLEL) * (RTT + per / LINK) + len(blocks) * (sum(ix.count) / max(1, len(ix.count))) / DECODE, groups)
        n16 = math.ceil(p.size / RANGE)
        stream = (math.ceil(n16 / PARALLEL) * (RTT + min(p.size, RANGE) / LINK) + p.entries / DECODE, n16)
        best = min(seek, stream)
        other = stream if best is seek else seek
        if other[0] <= 1.1 * best[0] and other[1] < best[1]:
            best = other
        return None if best is stream else blocks

    async def lookup(self, keys: list[bytes], p: int | None = None) -> list:
        """Per key (sorted, unique): the newest entry's (present, stamp,
        payload), or None; with `p`, only layers ending after P."""
        over = self._over(p)
        parts = [x.main for x, _ in over]
        await self.indexes(parts)
        inputs = []
        for x, _ in over:
            part = x.main
            if part.index is None:
                inputs.append(([self._whole[f[0]] for f in part.files], x.stamp or 0))
                continue
            blocks = self._plan(part, keys)
            if blocks is None:
                blocks = list(range(len(self._ix[part.index].first)))
            inputs.append((await self.chunks(part, blocks, extend=False), x.stamp or 0))
        found = _native.layers_lookup(inputs, keys)
        if p is not None:
            g = self.s.g(p)
            found = [f if f is not None and f[1] > g else None for f in found]
        return found


# -- the index ---------------------------------------------------------------------------------


@dataclass
class Written:
    entries: int = 0
    bytes: int = 0
    puts: int = 0
    merges: int = 0


class Layers:
    """One engine's view of one index: the writer, upkeep and collection."""

    def __init__(self, io, prefix: str, *, epoch: int = 1, state: State | None = None, window: int = 0):
        self.io = io
        self.s = state or State(prefix=prefix)
        self.epoch = epoch
        self.window = window
        self.written = {"delta": Written(), "tier": Written(), "base": Written()}
        self.running: set[str] = set()  # outputs of merges in progress
        self._reader: Reader | None = None

    # -- names -------------------------------------------------------------------------

    def _name(self, what: str, ext: str) -> str:
        return f"{self.s.prefix}{self.s.life}/{what}-e{self.epoch}-{uuid.uuid4().hex[:12]}.{ext}"

    async def _write_part(self, files: list, index: bytes, what: str, kind: str) -> Part:
        out = []
        for i, (data, n) in enumerate(files):
            path = self._name(f"{what}-{i}", "lay")
            self.running.add(path)
            await self.io.write(path, data)
            out.append([path, len(data), n])
            self.written[kind].bytes += len(data)
            self.written[kind].puts += 1
            self.written[kind].entries += n
        part = Part(out)
        if part.size > SMALL:
            part.index = self._name(what, "lix")
            part.index_size = len(index)
            self.running.add(part.index)
            await self.io.write(part.index, index)
            self.written[kind].puts += 1
            self.written[kind].bytes += len(index)
        return part

    # -- the writer ----------------------------------------------------------------------

    def reader(self) -> Reader:
        """The engine's own reader of its current state: indexes and small
        parts stay in hand across commits (warm metadata); blocks do not."""
        if self._reader is None or self._reader.s is not self.s:
            old = self._reader
            self._reader = Reader(self.io, self.s, window=0, seek_only=True)
            if old is not None:
                self._reader._ix, self._reader._whole = old._ix, old._whole
        self._reader._blocks.clear()
        return self._reader

    async def load_base(self, chunks, generation: int) -> None:
        """Commit 0: every key present, from sorted arrow binary chunks."""
        import numpy as np

        w = _native.LayerWriter(LAYER, block_size=BLOCK, file_limit=FILE_LIMIT)
        for arr in chunks:
            arr = arr.cast("large_binary") if str(arr.type) != "large_binary" else arr
            bufs = arr.buffers()
            offs = np.frombuffer(bufs[1], dtype=np.int64)[arr.offset : arr.offset + len(arr) + 1]
            data = bytes(bufs[2])[offs[0] : offs[-1]]
            w.add_arena(data, (offs - offs[0]).astype("<u8").tobytes(), generation)
        files, index = w.finish()
        part = await self._write_part(files, index, "base-0", "delta")
        self.s.layers = [Layer("L0-0", 0, 0, part)]
        self.s.gens[0] = generation
        self.s.cut = max(self.s.cut, -1)
        self.running.clear()

    async def resolve(self, ups: list[bytes], rms: list[bytes]) -> tuple[list[bytes], bytes]:
        """Δ(−∞, head, keys): each written key's kind (an update of an absent
        key is an add; a removal of an absent key is nothing)."""
        keys = sorted(set(ups) | set(rms))
        found = dict(zip(keys, await self.reader().lookup(keys), strict=True)) if keys else {}
        removing = set(rms)
        out_keys, kinds = [], bytearray()
        for k in keys:
            hit = found[k]
            live = hit is not None and hit[0]
            if k in removing:
                if live:
                    out_keys.append(k)
                    kinds.append(REMOVED)
            else:
                out_keys.append(k)
                kinds.append(UPDATED if live else ADDED)
        return out_keys, bytes(kinds)

    async def commit(self, c: int, generation: int, ups: list[bytes], rms: list[bytes], payloads=None) -> dict:
        """Resolve the written keys at the head, write the delta, commit it."""
        keys, kinds = await self.resolve(ups, rms)
        w = _native.LayerWriter(DELTA, block_size=BLOCK, file_limit=FILE_LIMIT)
        w.add_delta(keys, kinds, payloads)
        files, index = w.finish()
        part = await self._write_part(files, index, f"d{c}", "delta")
        self.s.layers.append(Layer(f"D{c}", c, c, part, stamp=generation))
        self.s.gens[c] = generation
        self.running.clear()
        return {"added": kinds.count(ADDED), "updated": kinds.count(UPDATED), "removed": kinds.count(REMOVED)}

    # -- upkeep --------------------------------------------------------------------------

    def set_cut(self, oldest_p: int | None, head: int) -> None:
        """The cut: the oldest live P, and with a window, at least head − W.
        It never moves back."""
        cut = self.s.cut
        if oldest_p is not None:
            cut = max(cut, oldest_p)
        if self.window:
            cut = max(cut, head - self.window)
        self.s.cut = cut
        self.s.gens = {k: v for k, v in self.s.gens.items() if k >= cut or k == self.s.head}

    def _allowed(self, ins: list[Layer], newer: list[Layer]) -> bool:
        if ins[-1].b <= self.s.cut:
            return True
        return estimate(ins) <= KREAD * sum(x.size for x in newer) + Z

    def plan(self) -> tuple[int, int] | None:
        ls = self.s.layers
        if len(ls) < 2:
            return None
        # The base absorbs the layers just above it once they hold a quarter of it.
        best, acc = None, 0
        for j in range(1, len(ls)):
            acc += ls[j].size
            if acc * R < ls[0].size:
                continue
            if self._allowed(ls[: j + 1], ls[j + 1 :]):
                best = (0, j + 1)
        if best and self._key(ls[best[0] : best[0] + best[1]]) not in self.s.stopped:
            return best
        # Tiers: the newest group of F adjacent layers of one tier, its oldest F.
        i = len(ls) - 1
        while i >= 1:
            t = tier(ls[i].size)
            j = i
            while j - 1 >= 1 and tier(ls[j - 1].size) == t:
                j -= 1
            if i - j + 1 >= F:
                ins = ls[j : j + F]
                if self._allowed(ins, ls[j + F :]) and self._key(ins) not in self.s.stopped:
                    return (j, F)
            i = j - 1
        return None

    @staticmethod
    def _key(ins: list[Layer]) -> str:
        return ",".join(x.id for x in ins)

    async def merge(self, lo: int, count: int) -> Layer | None:
        """One merge: count the attempt, read, merge, upload, publish."""
        ins = self.s.layers[lo : lo + count]
        key = self._key(ins)
        self.s.attempts[key] = self.s.attempts.get(key, 0) + 1  # durable before the upload (D109)
        bottom = lo == 0
        kind = "base" if bottom else "tier"
        reads = []
        for x in ins:  # oldest first; a side part is an input of its own
            for part in x.parts():
                datas = await asyncio.gather(*(self.io.read_whole(f[0], f[1]) for f in part.files))
                reads.append((list(datas), x.stamp or 0))
        cut_g = self.s.gens[self.s.cut] if self.s.cut >= 0 else 0  # flips are generations
        main, main_ix, side, side_ix, _, _ = await asyncio.to_thread(
            _native.layers_merge, reads, cut_g, bottom, block_size=BLOCK, file_limit=FILE_LIMIT
        )
        a, b = ins[0].a, ins[-1].b
        lid = f"L{a}-{b}"
        out = Layer(lid, a, b, await self._write_part(main, main_ix, f"l{a}-{b}", kind))
        if side:
            out.side = await self._write_part(side, side_ix, f"s{a}-{b}", kind)
        self.written[kind].merges += 1
        return out if self.publish(ins, out) else None

    def publish(self, ins: list[Layer], out: Layer, life: str | None = None) -> bool:
        """`IndexMerged`: applies if the life matches and the inputs are still
        exactly adjacent layers of the current state."""
        ids = [x.id for x in self.s.layers]
        want = [x.id for x in ins]
        if (life or self.s.life) != self.s.life:
            return False
        for lo in range(len(ids) - len(want) + 1):
            if ids[lo : lo + len(want)] == want:
                self.s.seq += 1
                self.s.layers[lo : lo + len(want)] = [out]
                self.s.garbage += [[p, self.s.seq] for x in ins for p in x.paths()]
                self.s.attempts.pop(self._key(ins), None)
                for p in out.paths():
                    self.running.discard(p)
                return True
        return False

    async def upkeep(self, oldest_p: int | None = None) -> None:
        self.set_cut(oldest_p, self.s.head)
        while True:
            step = self.plan()
            if step is None:
                break
            ins = self.s.layers[step[0] : step[0] + step[1]]
            if self.s.attempts.get(self._key(ins), 0) >= ATTEMPTS:
                self.s.stopped.append(self._key(ins))  # alarm: this input set fails
                continue
            await self.merge(*step)
        await self.collect()

    # -- pins and garbage ----------------------------------------------------------------

    def pin(self, name: str) -> State:
        """A reader's pin: the manifest it reads stays until it unpins."""
        self.s.pins[name] = self.s.seq
        return State.from_json(self.s.to_json())

    def unpin(self, name: str) -> None:
        self.s.pins.pop(name, None)

    def deletable(self) -> list[str]:
        """Garbage replaced at a publication no pin predates."""
        floor = min(self.s.pins.values(), default=math.inf)
        return [p for p, seq in self.s.garbage if seq <= floor]

    async def collect(self, barrier=None) -> list[str]:
        """Delete garbage no pin needs. `barrier` (a journal write, raising if
        this engine is fenced) runs first: a fenced engine deletes nothing."""
        dead = self.deletable()
        if not dead:
            return []
        if barrier is not None:
            await barrier()
        await self.io.delete(dead)
        gone = set(dead)
        self.s.garbage = [g for g in self.s.garbage if g[0] not in gone]
        return dead

    def referenced(self) -> set[str]:
        return {p for x in self.s.layers for p in x.paths()} | {g[0] for g in self.s.garbage} | self.running

    async def collect_orphans(self, listing: list[str], barrier=None) -> list[str]:
        """Files under this life's prefix that nothing names, judged after the
        listing and against this engine's state, deleted only if written by
        this engine's epoch or an older one."""
        named = self.referenced()
        mine = f"{self.s.prefix}{self.s.life}/"
        dead = []
        for p in listing:
            if not p.startswith(mine) or p in named:
                continue
            ep = int(p.rsplit("-e", 1)[1].split("-", 1)[0])
            if ep <= self.epoch:
                dead.append(p)
        if dead:
            if barrier is not None:
                await barrier()
            await self.io.delete(dead)
        return dead

    # -- accounting ----------------------------------------------------------------------

    def stored(self) -> int:
        return sum(x.size + x.main.index_size + (x.side.index_size if x.side else 0) for x in self.s.layers)
