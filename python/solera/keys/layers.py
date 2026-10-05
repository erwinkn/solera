"""Stamped layers: the key index behind Δ(P, H, keys) (docs/key-index-design.md).

An index is a list of **layers** tiling its commits from 0 to the head,
oldest first. A commit's delta is the layer `[c, c]`; upkeep merges adjacent
layers. Per key a layer holds its presence at its end and at its start, the
commit and generation of its last change, its **flips** (the commits that
added or removed it, newest first, those at or below the cut dropped) and a
source's payload. Presence at a commit P at or after the cut is presence at
H flipped once per flip after P.

`LayerState` is the engine-held record, plain data changed only through its
pure transitions, so the engine can journal it. `LayerIndex` does the I/O:
a commit's delta (resolved exactly at the head), Δ(P, H, keys), lookups at
the head, and merges. Files are self-delimiting blocks with no index or
footer; a part's block boundaries are its index object, written beside it
when the part is larger than `SMALL` (smaller parts are read whole).
"""

from __future__ import annotations

import asyncio
import bisect
import heapq
import math
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, replace

from .. import _native
from ..ids import ulid
from .io import ObjectIO
from .threads import in_thread

BLOCK = 16 * 1024  # raw bytes per block
FILE_LIMIT = 64 * 2**20  # bytes per file
SMALL = 256 * 1024  # a part this small has no index object: it is read whole
SEGMENT = 8 * 2**20  # bytes a streaming job reads of an input at once
WINDOW = 1 << 20  # bytes a page fetches of a part at least (consecutive pages share it)
PAGE_BUDGET = 64 * 2**20  # bytes a page reads at most before it returns short
SPARSE_BYTES = 64 * 2**20  # a sparse resolve holding more than this streams instead
CACHE = 64 * 2**20  # bytes of blocks a reader keeps

# The merge rule (docs/key-index-design.md § Compaction).
FANOUT = 4  # layers of one tier that merge
TIER_BASE = 8 * 1024  # bytes: tier 0 holds up to FANOUT x this
READER_BOUND = 4.0  # a layer reaching past the cut holds at most this x the bytes newer than it, ...
SLACK = 1 * 2**20  # ... plus this
BASE_SHARE = 4.0  # the base absorbs the layers above it once they hold 1/this of it
ATTEMPTS = 3  # attempts of one merge (life and inputs) before upkeep stops trying it
MAX_LAYERS = 64  # past this, commits wait for upkeep (backpressure)
DELTA_GROWTH = 1.45  # a stamped entry's size over a delta entry's, to bound a merge's output

# The cold reader's plan (§ Query forms): request round trips, requests in
# flight, a connection's bytes per second, entries decoded per second.
RTT, PARALLEL, LINK, DECODE = 0.030, 64, 80e6, 120e6


class CutError(ValueError):
    """Δ from a P below the cut: flips at or below it may be gone. The caller
    falls back to a full compare."""


class NotHeld(ValueError):
    """A read at an H this state cannot answer: a merged layer reaches past
    it. A batch reads the state it pinned at its H."""


def key_str(key: bytes) -> str:
    return key.decode("utf-8", "surrogateescape")


def key_bytes(key: str) -> bytes:
    return key.encode("utf-8", "surrogateescape")


def index_prefix(output: str, partition: str) -> str:
    """Where a new index's files go: `keys/{output}/{partition}/` (`_` for the
    unpartitioned partition). An index keeps its prefix when its output is renamed."""

    from urllib.parse import quote

    return f"keys/{output}/{quote(partition or '_', safe='')}/"


def delta_names(d: dict | None) -> list[str]:
    """The object names of a commit's delta, from its JSON (`DeltaFiles`):
    its files, and its index if it has one."""

    part = (d or {}).get("part") or {}
    return [f["name"] for f in part.get("files") or ()] + ([part["index"]] if part.get("index") else [])


# -- the engine-held record -------------------------------------------------------------------


@dataclass(frozen=True)
class FileRef:
    name: str
    size: int
    entries: int
    first: bytes = b""
    last: bytes = b""

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "size": self.size,
            "entries": self.entries,
            "first": key_str(self.first),
            "last": key_str(self.last),
        }

    @classmethod
    def from_json(cls, d: dict) -> FileRef:
        return cls(d["name"], d["size"], d["entries"], key_bytes(d["first"]), key_bytes(d["last"]))


@dataclass(frozen=True)
class Part:
    """Files in key order, and the name of their index object (None: the
    part is small and read whole)."""

    files: tuple[FileRef, ...] = ()
    index: str | None = None
    index_size: int = 0

    @property
    def size(self) -> int:
        return sum(f.size for f in self.files)

    @property
    def entries(self) -> int:
        return sum(f.entries for f in self.files)

    def names(self) -> list[str]:
        return [f.name for f in self.files] + ([self.index] if self.index else [])

    def to_json(self) -> dict:
        return {
            "files": [f.to_json() for f in self.files],
            "index": self.index,
            "index_size": self.index_size,
        }

    @classmethod
    def from_json(cls, d: dict) -> Part:
        return cls(tuple(FileRef.from_json(f) for f in d["files"]), d.get("index"), d.get("index_size", 0))


@dataclass(frozen=True)
class Layer:
    """Commits `[a, b]`. `main`: what head reads and readers after the layer
    need; `side`: what only a reader whose P lies inside it needs (in the
    base, every absent key; elsewhere keys absent at both ends). A commit's
    delta has `generation`: its entries' (a merged layer's carry their own).
    `complete`: no flip was ever dropped from it (written with the cut below
    `a`), so a merge may check every key's presence against its flips."""

    a: int
    b: int
    main: Part
    side: Part | None = None
    generation: int | None = None
    complete: bool = True

    @property
    def delta(self) -> bool:
        return self.generation is not None

    @property
    def size(self) -> int:
        return self.main.size + (self.side.size if self.side else 0)

    @property
    def entries(self) -> int:
        return self.main.entries + (self.side.entries if self.side else 0)

    @property
    def id(self) -> str:
        """What a merge planned against this layer names it by: its commits
        and its first file (unique: names are)."""

        first = (
            self.main.files[0].name
            if self.main.files
            else (self.side.files[0].name if self.side and self.side.files else "")
        )
        return f"{self.a}-{self.b}:{first}"

    def parts(self) -> list[Part]:
        return [self.main] + ([self.side] if self.side else [])

    def names(self) -> list[str]:
        return [n for p in self.parts() for n in p.names()]

    def to_json(self) -> dict:
        return {
            "a": self.a,
            "b": self.b,
            "main": self.main.to_json(),
            "side": self.side.to_json() if self.side else None,
            "generation": self.generation,
            "complete": self.complete,
        }

    @classmethod
    def from_json(cls, d: dict) -> Layer:
        side = d.get("side")
        return cls(
            d["a"],
            d["b"],
            Part.from_json(d["main"]),
            Part.from_json(side) if side else None,
            d.get("generation"),
            d.get("complete", True),
        )


@dataclass(frozen=True)
class DeltaFiles:
    """A commit's delta as written: its part, how the live count changes,
    and the commit's generation (every entry's)."""

    part: Part
    added: int
    removed: int
    generation: int

    def to_json(self) -> dict:
        return {
            "part": self.part.to_json(),
            "added": self.added,
            "removed": self.removed,
            "generation": self.generation,
        }

    @classmethod
    def from_json(cls, d: dict) -> DeltaFiles:
        return cls(Part.from_json(d["part"]), d["added"], d["removed"], d["generation"])


@dataclass(frozen=True)
class LayerState:
    """What the engine holds per index: its live key count, its layers
    (oldest first, tiling commits 0 to the head), the cut (the oldest P it
    answers: the engine's oldest live P), and the newest generation. `life`
    names this incarnation: a reset, a move or a removal starts another, and
    a merge planned against another life is refused."""

    prefix: str = ""
    life: str = ""
    count: int = 0
    cut: int = -1
    layers: tuple[Layer, ...] = ()
    generation: int = 0  # the newest commit's

    def to_json(self) -> dict:
        return {
            "prefix": self.prefix,
            "life": self.life,
            "count": self.count,
            "cut": self.cut,
            "generation": self.generation,
            "layers": [x.to_json() for x in self.layers],
        }

    @classmethod
    def from_json(cls, d: dict | None) -> LayerState:
        if not d:
            return cls()
        return cls(
            d["prefix"],
            d.get("life", ""),
            d["count"],
            d.get("cut", -1),
            tuple(Layer.from_json(x) for x in d["layers"]),
            d.get("generation", 0),
        )

    def path(self, name: str) -> str:
        return f"{self.prefix}{name}"

    @property
    def head(self) -> int:
        """The last commit the layers hold (-1: none yet)."""

        return self.layers[-1].b if self.layers else -1

    def referenced(self) -> set[str]:
        """Every object name the index still needs."""

        return {n for x in self.layers for n in x.names()}

    def at(self, h: int | None) -> LayerState:
        """The state as of commit `h` (None: the head): the layers up to one
        ending at `h`. Raises `NotHeld` if a layer reaches past `h`."""

        if h is None or h == self.head:
            return self
        if h > self.head:
            raise NotHeld(f"commit {h} is past the head {self.head}")
        for i, x in enumerate(self.layers):
            if x.b == h:
                return replace(self, layers=self.layers[: i + 1])
        raise NotHeld(f"no layer ends at commit {h}: read the state a batch pinned at it")

    # -- transitions (pure) ------------------------------------------------------------------

    def committed(self, commit: int, delta: DeltaFiles) -> LayerState:
        """Install a commit's delta as the layer `[head + 1, c]`: the commits
        between changed nothing in it (an index that skips commits, as a
        failure index does). Generations rise with commits: one that does
        not is a writer's error, refused."""

        if commit <= self.head:
            if not delta.part.files:
                return self  # an empty delta not advancing the head: nothing
            raise ValueError(f"commit {commit} does not follow the head {self.head}")
        if self.layers and delta.generation <= self.generation:
            raise ValueError(f"commit {commit}'s generation {delta.generation} is not past {self.generation}")
        layer = Layer(self.head + 1, commit, delta.part, None, delta.generation)
        return replace(
            self,
            count=self.count + delta.added - delta.removed,
            layers=self.layers + (layer,),
            generation=delta.generation,
        )

    def holds(self, ids: list[str]) -> bool:
        """Whether these are still adjacent layers of this state: a merge
        planned against them may publish."""

        mine = [x.id for x in self.layers]
        return any(mine[i : i + len(ids)] == ids for i in range(len(mine) - len(ids) + 1))

    def merged(self, ids: list[str], out: Layer) -> LayerState:
        """Swap adjacent input layers (by id) for their merge."""

        mine = [x.id for x in self.layers]
        for i in range(len(mine) - len(ids) + 1):
            if mine[i : i + len(ids)] == ids:
                ins = self.layers[i : i + len(ids)]
                if (out.a, out.b) != (ins[0].a, ins[-1].b):
                    raise ValueError(f"a merge of {ids} covers [{out.a}, {out.b}]")
                return replace(self, layers=self.layers[:i] + (out,) + self.layers[i + len(ids) :])
        raise ValueError(f"layers {ids} are not this index's, or not adjacent")

    def with_cut(self, cut: int) -> LayerState:
        """The cut never moves back: a stale (lower) one changes nothing."""

        return self if cut <= self.cut else replace(self, cut=cut)

    def backlogged(self) -> bool:
        """Upkeep is behind (failing, or slower than commits): the layers pile
        up. Commits to the partition then wait, from every writer of the
        index (outputs, sources, failure indexes alike)."""

        return len(self.layers) > MAX_LAYERS

    # -- the merge rule ---------------------------------------------------------------------

    def plan(
        self, busy: set[str] = frozenset(), stopped: set[str] = frozenset(), lane: str | None = None
    ) -> tuple[str, int, int] | None:
        """The next merge, as `(lane, first, count)`, or None. Two lanes —
        the base absorbing the layers above it, and tiers — each pick inputs
        no running merge holds (`busy`, ids), so the lanes never share one;
        `lane` asks for one lane's only. `stopped`: input sets
        (`attempt_key`) upkeep gave up on."""

        ls = self.layers
        free = [x.id not in busy for x in ls]

        def allowed(lo: int, count: int) -> bool:
            ins = ls[lo : lo + count]
            if not all(free[lo : lo + count]) or self.attempt_key(ins) in stopped:
                return False
            if ins[-1].b <= self.cut:
                return True  # no reader's P lies inside it
            out = sum(x.size * (DELTA_GROWTH if x.delta else 1.0) for x in ins)
            return out <= READER_BOUND * sum(x.size for x in ls[lo + count :]) + SLACK

        if len(ls) >= 2 and lane in (None, "base"):
            best, acc = None, 0
            for j in range(1, len(ls)):
                acc += ls[j].size
                if acc * BASE_SHARE >= ls[0].size and allowed(0, j + 1):
                    best = ("base", 0, j + 1)
            if best:
                return best
        if lane == "base":
            return None
        i = len(ls) - 1
        while i >= 1:
            t = tier(ls[i].size)
            j = i
            while j - 1 >= 1 and tier(ls[j - 1].size) == t:
                j -= 1
            if i - j + 1 >= FANOUT and allowed(j, FANOUT):
                return ("tier", j, FANOUT)
            i = j - 1
        return None

    def attempt_key(self, ins) -> str:
        """A merge's identity across engines: the life and its inputs."""

        return f"{self.life}|" + ",".join(x.id for x in ins)


def epoch_of(name: str) -> int | None:
    """The epoch a merge output's name carries (`{life}/l…-e{epoch}-{id}…`),
    for the orphan collector: an engine judges only its own epoch's outputs
    and older ones. None: not a merge output (a commit's delta)."""

    base = name.rsplit("/", 1)[-1]
    if not base.startswith("l"):
        return None
    for part in base.split("-"):
        if part.startswith("e") and part[1:].isdigit():
            return int(part[1:])
    return None


def tier(size: int) -> int:
    return max(0, int(math.log(max(size, TIER_BASE) / TIER_BASE, FANOUT)))


# -- results ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Diff:
    """One key's difference between P and H: present at P, present at H,
    and its generation and payload at H (None where absent there)."""

    key: str
    before: bool
    after: bool
    generation: int | None = None
    payload: bytes | None = None


@dataclass(frozen=True)
class DeltaPage:
    """Differences in key order, and where a range read resumes (None: done)."""

    diffs: list[Diff]
    cursor: str | None = None


@dataclass(frozen=True)
class Delta:
    """A commit's delta, encoded but not written: its files `(data, entries,
    first, last)`, its index, and how the count changes."""

    files: list[tuple]
    index: bytes
    added: int
    changed: int
    removed: int
    listed: tuple | None = None  # up to `collect` changed keys: (written, removed)

    @property
    def entries(self) -> int:
        return sum(f[1] for f in self.files)


# -- reading ------------------------------------------------------------------------------------


class _Index:
    """A part's blocks: first keys, files, offsets, lengths, entries, newest commits."""

    def __init__(self, raw: bytes):
        self.first, self.file, self.off, self.len, self.count, self.newest = _native.layers_index(raw)

    def span(self, after: bytes | None, upto: bytes | None) -> range:
        """The blocks that may hold keys in (after, upto]."""

        lo = 0 if after is None else max(0, bisect.bisect_right(self.first, after) - 1)
        hi = len(self.first) if upto is None else bisect.bisect_right(self.first, upto)
        return range(lo, max(lo, hi))

    def of(self, key: bytes) -> int:
        return bisect.bisect_right(self.first, key) - 1


class _Blocks:
    """What a reader fetched, kept up to `limit` bytes, least recently used out."""

    def __init__(self, limit: int):
        self.limit, self.size = limit, 0
        self.items: OrderedDict = OrderedDict()

    def get(self, k):
        v = self.items.get(k)
        if v is not None:
            self.items.move_to_end(k)
        return v

    def put(self, k, v: bytes) -> None:
        if k in self.items:
            return
        self.items[k] = v
        self.size += len(v)
        while self.size > self.limit and len(self.items) > 1:
            _, old = self.items.popitem(last=False)
            self.size -= len(old)


class LayerIndex:
    """One index's I/O over a state (for a batch: the state it pinned at its
    H). Reads go through `io`; `cache`, when given, is the engine's
    (`LayerCache`): indexes and small parts kept in memory across reads, and
    files on its disk read in place of the store."""

    def __init__(self, io: ObjectIO, state: LayerState, *, cache=None, blocks: int = CACHE):
        self.io, self.state = io, state
        self._small = cache if cache is not None else {}
        self._disk = cache if hasattr(cache, "read") else None
        self._blocks = _Blocks(blocks)
        self.read_bytes = 0

    async def _read(self, name: str, start: int, end: int, size: int) -> bytes:
        """Bytes of an object: from the engine's disk when it holds it, else the store."""

        path = self.state.path(name)
        if self._disk is not None:
            data = await in_thread(self._disk.read, path, start, end)
            if data is not None:
                return data
        data = await self.io.read(path, start, end, size)
        self.read_bytes += len(data)
        return data

    async def _write(self, name: str, data: bytes) -> None:
        """Upload an object; the engine's disk keeps a copy of what it wrote."""

        path = self.state.path(name)
        await self.io.write(path, data)
        if self._disk is not None and name.endswith(".lay"):
            await in_thread(self._disk.install, path, data, self.state.prefix)

    # -- fetching -------------------------------------------------------------------------

    async def _prepare(self, parts: list[Part]) -> None:
        """One round trip: every part's index object, or the whole part when
        it is small — what this reader does not hold yet."""

        async def one(p: Part):
            if p.index is not None:
                if ("ix", p.index) not in self._small:
                    raw = await self._read(p.index, 0, p.index_size, p.index_size)
                    self._small[("ix", p.index)] = _Index(raw)
            else:
                for f in p.files:
                    if ("whole", f.name) not in self._small:
                        self._small[("whole", f.name)] = await self._read(f.name, 0, f.size, f.size)

        await asyncio.gather(*(one(p) for p in parts))

    def _ix(self, p: Part) -> _Index:
        return self._small[("ix", p.index)]

    async def _chunks(self, p: Part, blocks: list[int], window: int = 0) -> list[bytes]:
        """Whole blocks `blocks` (sorted) of part `p`, as chunks in key order;
        missing ones fetched in one range GET per run of consecutive blocks,
        extended to `window` bytes."""

        if p.index is None:
            return [self._small[("whole", f.name)] for f in p.files]
        ix = self._ix(p)
        need = [i for i in blocks if self._blocks.get((p.index, i)) is None]
        groups: list[list[int]] = []
        for i in need:
            if groups and i == groups[-1][-1] + 1 and ix.file[i] == ix.file[groups[-1][-1]]:
                groups[-1].append(i)
            else:
                groups.append([i])
        got: dict[int, bytes] = {}

        async def fetch(g: list[int]):
            first, last = g[0], g[-1]
            total = sum(ix.len[i] for i in g)
            while total < window and last + 1 < len(ix.first) and ix.file[last + 1] == ix.file[first]:
                last += 1
                total += ix.len[last]
            f = p.files[ix.file[first]]
            start, end = ix.off[first], ix.off[last] + ix.len[last]
            data = await self._read(f.name, start, end, f.size)
            for i in range(first, last + 1):
                o = ix.off[i] - start
                b = data[o : o + ix.len[i]]
                got[i] = b
                self._blocks.put((p.index, i), b)

        await asyncio.gather(*(fetch(g) for g in groups))
        return [got.get(i) or self._blocks.get((p.index, i)) for i in blocks]

    def _over(self, p: int | None) -> list[tuple[Layer, list[Part]]]:
        """Newest first: the layers that end after P, and the parts to read
        (the side part of the one P lies inside; none for P = −∞)."""

        out = []
        for x in reversed(self.state.layers):
            if p is not None and x.b <= p:
                break
            parts = [x.main] + ([x.side] if x.side is not None and p is not None and x.a <= p else [])
            out.append((x, parts))
        return out

    @staticmethod
    def _stamp(x: Layer) -> tuple[int, int]:
        """The commit and generation a delta's entries take."""

        return (x.b, x.generation) if x.delta else (0, 0)

    # -- Δ(P, H, keys) ------------------------------------------------------------------------

    def _check(self, p: int | None) -> None:
        if p is not None and p < self.state.cut:
            raise CutError(f"P = {p} is below the cut {self.state.cut}")

    async def delta(
        self,
        p: int | None,
        *,
        keys: list[bytes] | None = None,
        after: bytes | None = None,
        upto: bytes | None = None,
        first: int = 10_000,
        glob: bytes | None = None,
        take: Callable[[bytes], bool] | None = None,
        budget: int = PAGE_BUDGET,
    ):
        """Δ(P, H) at this state's head: `(diffs, cursor)` — diffs as native
        tuples `(key, before, after, generation, payload)` — for the sorted
        `keys` (cursor None), or the first `first` keys after `after` (and at
        most `upto`) that differ, matching `glob` and `take`. A page stops
        early, with its cursor, once it has read `budget` bytes."""

        self._check(p)
        if p is not None and p >= self.state.head:
            return [], None
        over = self._over(p)
        await self._prepare([part for _, parts in over for part in parts])
        if keys is not None:
            keys = sorted(set(keys))
            if take is not None:
                keys = [k for k in keys if take(k)]
            inputs = []
            for x, parts in over:
                for part in parts:
                    if part.index is None:
                        blocks = None
                    else:
                        ix = self._ix(part)
                        blocks = sorted({b for b in (ix.of(k) for k in keys) if b >= 0})
                    chunks = await self._chunks(part, blocks or [])
                    inputs.append((chunks, *self._stamp(x)))
            out = await in_thread(_native.layers_scan, inputs, p, glob=glob, keys=keys) if keys else ([],) * 6
            return _rows(out), None
        rows: list[tuple] = []
        read0 = self.read_bytes
        cursor = after
        while True:
            want = first - len(rows)
            bound = self._bound(over, p, cursor, want, upto)
            inputs = []
            for x, parts in over:
                for part in parts:
                    if part.index is None:
                        inputs.append(([self._small[("whole", f.name)] for f in part.files], *self._stamp(x)))
                        continue
                    ix = self._ix(part)
                    span = list(ix.span(cursor, bound))
                    if p is not None and not x.delta:
                        span = [i for i in span if ix.newest[i] > p]
                    if glob is not None and span:
                        firsts = [ix.first[i] for i in span]
                        nxt = ix.first[span[-1] + 1] if span[-1] + 1 < len(ix.first) else None
                        keep = _native.glob_blocks(glob, firsts, nxt)
                        span = [i for i, k in zip(span, keep, strict=True) if k]
                    inputs.append((await self._chunks(part, span, WINDOW), *self._stamp(x)))
            out = await in_thread(
                _native.layers_scan, inputs, p, after=cursor, upto=bound, limit=want, glob=glob
            )
            got = _rows(out)
            if take is not None:
                got = [r for r in got if take(r[0])]
            rows += got
            last = out[5]
            if last is not None:  # `want` rows reached at `last`
                return rows, last
            if bound is None or (upto is not None and bound >= upto):
                return rows, None
            cursor = bound
            if len(rows) >= first or self.read_bytes - read0 >= budget:
                return rows, cursor

    def _bound(self, over, p, after, want: int, upto: bytes | None) -> bytes | None:
        """A key bound such that the layers' entries in (after, bound] are
        about 1.5 x `want` (a straddled layer counted by its share of commits
        after P; stale entries make distinct keys fewer); None: the end."""

        target = max(1, want) * 1.5
        streams = []
        for x, parts in over:
            share = 1.0
            if p is not None and x.a <= p:
                share = (x.b - p) / max(1, x.b - x.a + 1)
            for part in parts:
                if part.index is None:
                    continue  # held whole: reading further costs nothing
                ix = self._ix(part)
                streams.append([(ix.first[i], ix.count[i] * share) for i in ix.span(after, None)])
        acc = 0.0
        for k, n in heapq.merge(*streams, key=lambda t: t[0]):
            acc += n
            if acc >= target and (after is None or k > after):
                return k if upto is None or k < upto else upto
        return upto

    # -- lookups at the head --------------------------------------------------------------

    def _plan(self, part: Part, keys: list[bytes]) -> list[int]:
        """The blocks of `part` to read for `keys`: those the keys fall in,
        or every block when streaming the part is faster (request waves, a
        connection's bytes, decoding), fewer requests on a tie within 10%.
        Decided in bytes and requests, never in blocks."""

        ix = self._ix(part)
        blocks = sorted({b for b in (ix.of(k) for k in keys) if b >= 0})
        groups = sum(1 for j, b in enumerate(blocks) if j == 0 or b != blocks[j - 1] + 1)
        per = max(ix.len) if ix.len else 0
        avg = part.entries / max(1, len(ix.first))
        seek = (math.ceil(groups / PARALLEL) * (RTT + per / LINK) + len(blocks) * avg / DECODE, groups)
        ranges = math.ceil(part.size / (16 * 2**20))
        stream = (
            math.ceil(ranges / PARALLEL) * (RTT + min(part.size, 16 * 2**20) / LINK) + part.entries / DECODE,
            ranges,
        )
        best, other = (seek, stream) if seek <= stream else (stream, seek)
        if other[0] <= 1.1 * best[0] and other[1] < best[1]:
            best = other
        return blocks if best is seek else list(range(len(ix.first)))

    async def _inputs(self, keys: list[bytes]) -> list:
        over = self._over(None)
        await self._prepare([x.main for x, _ in over])
        inputs = []
        for x, _ in over:
            blocks = None if x.main.index is None else self._plan(x.main, keys)
            inputs.append((await self._chunks(x.main, blocks or []), *self._stamp(x)))
        return inputs

    async def lookup(self, keys: list[bytes]) -> dict[bytes, tuple[int, bytes | None]]:
        """The keys (any order) present at the head: key → (generation, payload)."""

        keys = sorted(set(keys))
        if not keys:
            return {}
        found = await in_thread(_native.layers_lookup, await self._inputs(keys), keys)
        return {k: (f[1], f[2]) for k, f in zip(keys, found, strict=True) if f is not None and f[0]}

    # -- the writer ---------------------------------------------------------------------------

    async def resolve(
        self, written, *, generation: int = 0, replaced: bool = False, collect: int = 0
    ) -> Delta:
        """A commit's delta from its written entries (a `SortedEntries`),
        each resolved exactly at the head, the sparse way: the blocks the
        keys fall in (or whole parts, where streaming them is faster), held
        while it resolves. With `replaced`, each update and remove records
        the generation it replaced, back from the commit's `generation` (an
        immutable store's cleanup); `collect` lists up to that many changed
        keys."""

        keys = list(written.keys())
        inputs = await self._inputs(keys) if keys else []
        files, index, added, changed, removed, listed = await in_thread(
            _native.layers_resolve,
            inputs,
            written,
            replaced=generation if replaced else None,
            collect=collect,
            block_size=BLOCK,
            file_limit=FILE_LIMIT,
        )
        return Delta(list(files), bytes(index), added, changed, removed, listed)

    async def write(self, name: str, delta: Delta, generation: int) -> DeltaFiles:
        """Upload a delta under `name` (unique: its commit and attempt), and
        its index when the part is not small."""

        part = await self._upload(name, delta.files, delta.index)
        return DeltaFiles(part, delta.added, delta.removed, generation)

    async def sparse_bytes(self, keys: list[bytes]) -> int:
        """What a sparse resolve of `keys` would hold: the blocks it reads."""

        over = self._over(None)
        await self._prepare([x.main for x, _ in over])
        total = 0
        for x, _ in over:
            if x.main.index is None:
                total += x.main.size
            else:
                ix = self._ix(x.main)
                total += sum(ix.len[i] for i in self._plan(x.main, keys))
        return total

    async def write_patch(
        self, run, *, name: str, generation: int, replaced: bool = False, collect: int = 0
    ) -> tuple[DeltaFiles, tuple | None]:
        """A patch's delta (`run`, sorted entries, removes among them),
        written: resolved sparsely, or streamed against the whole index when
        the sparse way would hold more than `SPARSE_BYTES` (the rule is in
        bytes, never in blocks). Returns the delta's files and up to
        `collect` changed keys, `(written, removed)`, or None past that."""

        keys = list(run.keys())
        if keys and await self.sparse_bytes(keys) > SPARSE_BYTES:
            return await self._join(name, generation, replaced, collect, sorted=run)
        delta = await self.resolve(run, generation=generation, replaced=replaced, collect=collect)
        return await self.write(name, delta, generation), delta.listed

    async def write_replace(
        self,
        rows=None,
        *,
        chunks=None,
        key: str | None = None,
        overlay=None,
        name: str,
        generation: int,
        replaced: bool = False,
        collect: int = 0,
    ) -> tuple[DeltaFiles, tuple | None]:
        """A full replacement: `rows` (a `Rows`), or `chunks` of sorted keys
        streamed (rows keyed by `key`, when chunks are tables), is every key
        now — with `overlay`'s upserts in place of the stream's and its
        removes gone — and present keys it omits are removed. Streams the
        index once."""

        return await self._join(
            name,
            generation,
            replaced,
            collect,
            rows=rows,
            chunks=chunks,
            key=key,
            overlay=overlay,
            replace=True,
        )

    async def _join(
        self,
        name,
        generation,
        replaced,
        collect,
        *,
        rows=None,
        sorted=None,
        chunks=None,
        key=None,
        overlay=None,
        replace=False,
    ) -> tuple[DeltaFiles, tuple | None]:
        over = self._over(None)
        job = _native.LayerJob.join(
            [self._stamp(x) for x, _ in over],
            rows=rows,
            sorted=sorted,
            replace=replace,
            replaced=generation if replaced else None,
            collect=collect,
            key=key,
            overlay=overlay,
            block_size=BLOCK,
            file_limit=FILE_LIMIT,
        )
        refs: list[FileRef] = []

        async def on_file(part, data, entries, first, last):
            fname = f"{name}-{len(refs)}.lay"
            await self._write(fname, data)
            refs.append(FileRef(fname, len(data), entries, first, last))

        out = await self._drive(job, [x.main for x, _ in over], on_file, chunks=chunks)
        part = await self._index_part(name, "d", refs, out["main"])
        return DeltaFiles(part, out["added"], out["removed"], generation), out["collected"]

    async def compute(
        self, run, *, generation: int = 0, replace: bool = False, replaced: bool = False, collect: int = 0
    ) -> Delta:
        """A delta for `run` (sorted entries), computed but not uploaded: the
        engine's resolver answers a worker with it, and the worker uploads it
        under its own name. A patch resolves sparsely; a replacement streams
        the index, its files kept in memory."""

        if not replace:
            return await self.resolve(run, generation=generation, replaced=replaced, collect=collect)
        over = self._over(None)
        job = _native.LayerJob.join(
            [self._stamp(x) for x, _ in over],
            sorted=run,
            replace=True,
            replaced=generation if replaced else None,
            collect=collect,
            block_size=BLOCK,
            file_limit=FILE_LIMIT,
        )
        files: list[tuple] = []

        async def on_file(part, data, entries, first, last):
            files.append((data, entries, first, last))

        out = await self._drive(job, [x.main for x, _ in over], on_file)
        return Delta(files, out["main"], out["added"], out["changed"], out["removed"], out["collected"])

    async def _upload(self, name: str, files: list[tuple], index: bytes) -> Part:
        refs = []
        for i, (data, entries, first, last) in enumerate(files):
            fname = f"{name}-{i}.lay"
            await self._write(fname, data)
            refs.append(FileRef(fname, len(data), entries, first, last))
        part = Part(tuple(refs))
        if part.size > SMALL:
            iname = f"{name}.lix"
            await self._write(iname, index)
            part = replace(part, index=iname, index_size=len(index))
        return part

    # -- merges ----------------------------------------------------------------------------------

    async def merge(
        self, lo: int, count: int, *, epoch: int, name: str | None = None
    ) -> tuple[list[str], Layer]:
        """Merge layers `[lo, lo + count)` into one, uploaded under names
        carrying the life, `epoch` and a unique id. Returns the inputs' ids
        and the output, to publish through the journal (`LayerState.merged`
        after `holds`)."""

        st = self.state
        ins = list(st.layers[lo : lo + count])
        bottom = lo == 0
        stem = name or f"{st.life}/l{ins[0].a:012d}-{ins[-1].b:012d}-e{epoch}-{ulid()}"
        cut = st.cut if st.cut >= 0 else None  # no cut yet: every flip stays
        check = all(x.complete for x in ins) and all(x.a > st.cut for x in ins)
        streams = []  # oldest first; a side part is an input of its own
        for x in ins:
            for part in x.parts():
                streams.append((x, part))
        job = _native.LayerJob.merge(
            [self._stamp(x) for x, _ in streams],
            cut=cut,
            bottom=bottom,
            check=check,
            block_size=BLOCK,
            file_limit=FILE_LIMIT,
        )
        files: dict[str, list] = {"main": [], "side": []}

        async def on_file(part: str, data: bytes, entries: int, first: bytes, last: bytes):
            n = len(files[part])
            fname = f"{stem}-{part[0]}{n}.lay"
            await self._write(fname, data)
            files[part].append(FileRef(fname, len(data), entries, first, last))

        out = await self._drive(job, [p for _, p in streams], on_file)
        main = await self._index_part(stem, "m", files["main"], out["main"])
        side = await self._index_part(stem, "s", files["side"], out["side"]) if files["side"] else None
        layer = Layer(
            ins[0].a, ins[-1].b, main, side, None, complete=all(x.complete for x in ins) and st.cut < ins[0].a
        )
        return [x.id for x in ins], layer

    async def _index_part(self, stem: str, tag: str, refs: list[FileRef], index: bytes) -> Part:
        part = Part(tuple(refs))
        if part.size > SMALL:
            iname = f"{stem}-{tag}.lix"
            await self._write(iname, index)
            part = replace(part, index=iname, index_size=len(index))
        return part

    async def replace_all(self, rows, *, name: str, generation: int, replaced: bool = False) -> DeltaFiles:
        """A full replacement by `rows`: `write_replace`, its delta files alone."""

        files, _ = await self.write_replace(rows, name=name, generation=generation, replaced=replaced)
        return files

    async def _segments(self, part: Part):
        """A part's bytes in segments of whole blocks, at most `SEGMENT`
        each, read from the engine's disk where it holds them."""

        if part.index is None:
            for f in part.files:
                yield await self._read(f.name, 0, f.size, f.size)
            return
        await self._prepare([part])
        ix = self._ix(part)
        i = 0
        while i < len(ix.first):
            j, start = i + 1, ix.off[i]
            while j < len(ix.first) and ix.file[j] == ix.file[i] and ix.off[j] + ix.len[j] - start <= SEGMENT:
                j += 1
            f = part.files[ix.file[i]]
            yield await self._read(f.name, start, ix.off[j - 1] + ix.len[j - 1], f.size)
            i = j

    async def _drive(self, job, parts: list[Part], on_file, chunks=None) -> dict:
        """Run a streaming `LayerJob` over `parts` (its inputs, in its order):
        feed segments and streamed chunks as it asks, hand its files to
        `on_file`, return `finish()`."""

        readers = [self._segments(p) for p in parts]
        it = iter(chunks) if chunks is not None else None
        try:
            while (step := await in_thread(job.step)) is not None:
                kind, x = step
                if kind == "run":
                    seg = await anext(readers[x], None)
                    if seg is None:
                        job.end(x)
                    else:
                        job.feed(x, seg)
                elif kind == "rows":
                    chunk = None if it is None else await in_thread(next, it, None)
                    if chunk is None:
                        job.end_rows()
                    else:
                        job.feed_rows(chunk)
                else:
                    await on_file(*x)
        finally:
            for r in readers:
                await r.aclose()
        return job.finish()


def _rows(out) -> list[tuple]:
    keys, before, after, gens, payloads, _ = out
    return [
        (
            keys[i],
            bool(before[i]),
            bool(after[i]),
            gens[i] if after[i] else None,
            payloads[i] if after[i] else None,
        )
        for i in range(len(keys))
    ]


# -- a commit's delta, read by others ---------------------------------------------------------


def delta_keys(data: bytes, generation: int) -> tuple[list[bytes], list[bytes]]:
    """A delta file's written keys, and its removed keys (`generation`: its commit's)."""

    entries = _native.layers_decode(data, 0, generation)
    return [e[0] for e in entries if e[1]], [e[0] for e in entries if not e[1]]


def replaced_entries(data: bytes, generation: int) -> list[tuple[bytes, int]]:
    """What a delta file's updates and removes replaced, where the writer
    recorded it (an immutable store's outputs): `(key, generation)`, for its
    cleanup only — the index never reads it. `generation`: the commit's, which
    each is written back from."""

    return [(e[0], e[7]) for e in _native.layers_decode(data, 0, generation) if e[7] is not None]


@dataclass(frozen=True)
class DeltaKeys:
    """The keys a commit's delta writes — not those it removes — read a page
    at a time in key order: a store's selection when there are too many to
    list (`solera.stores.Partition`)."""

    io: ObjectIO
    prefix: str
    part: Part
    generation: int  # the commit's

    async def chunks(self, size: int = 100_000):
        """Chunks of the written keys, as `str`."""

        state = LayerState(prefix=self.prefix, layers=(Layer(0, 0, self.part, None, self.generation),))
        index, after = LayerIndex(self.io, state), None
        while self.part.files:
            rows, after = await index.delta(None, after=after, first=size)
            chunk = [key_str(r[0]) for r in rows]
            if chunk:
                yield chunk
            if after is None:
                return
