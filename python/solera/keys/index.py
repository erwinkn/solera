"""Key index operations (docs/key-index-design.md, docs/object-store-state.md §6).

An index is a list of spans per keyed output and partition: key-sorted
`.kx` file sets, each covering a stretch of commits, tiling them from 0 to
the head. `IndexState` is the engine-held record of them; it is plain
data, changed only through its pure transition methods, so the engine can
journal it. `KeyIndex` does the I/O: computing a commit's delta (always
exact), writing it as the commit's span, lookups and scans at the head or
at a reserved endpoint, `changes(P -> N)`, and merging adjacent spans. A
full replacement and a merge stream over their inputs (`jobs`): memory is
a few segments per span and one output file.

Spans are read newest first: for any key, its first entry in the newest
span holding it is its current state.
"""

from __future__ import annotations

import asyncio
import bisect
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from urllib.parse import quote

from .. import _native
from ..ids import ulid
from . import (
    FOOTER_SIZE,
    LimitError,
    Merge,
    Rows,
    SortedEntries,
    check_block,
    jobs,
    parse_footer,
    parse_index,
    parse_tail,
)
from .io import RANGE, ObjectIO
from .reads import Cold, Full
from .threads import in_thread

# -- engine-held state ---------------------------------------------------------------


def key_str(key: bytes) -> str:
    return key.decode("utf-8", "surrogateescape")


def key_bytes(key: str) -> bytes:
    return key.encode("utf-8", "surrogateescape")


_s, _b = key_str, key_bytes


def digest(data: bytes) -> str:
    """A file's content digest: XXH3-128, hex."""

    return _native.content_digest(data)


def delta_keys(data: bytes) -> tuple[list[bytes], list[bytes]]:
    """A delta file's written keys, and its deleted keys."""

    keys, _, deleted, _ = SortedEntries.decode(data).entries()
    return [k for k, d in zip(keys, deleted, strict=True) if not d], [
        k for k, d in zip(keys, deleted, strict=True) if d
    ]


def index_prefix(output: str, partition: str) -> str:
    """Where a new index's files go: `keys/{output}/{partition}/` (`_` for the
    unpartitioned partition). An index keeps its prefix when its output is renamed."""

    return f"keys/{output}/{quote(partition or '_', safe='')}/"


@dataclass(frozen=True)
class FileInfo:
    name: str
    min: bytes
    max: bytes
    entries: int
    size: int
    tail: int  # bytes from the start of the filters to the end of the file
    index: int  # bytes from the start of the block index to the end of the file
    digest: str = ""  # XXH3-128 of the file's bytes, hex: what a cached copy is checked against

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "min": _s(self.min),
            "max": _s(self.max),
            "entries": self.entries,
            "size": self.size,
            "tail": self.tail,
            "index": self.index,
            "digest": self.digest,
        }

    @classmethod
    def from_json(cls, d: dict) -> FileInfo:
        return cls(
            d["name"], _b(d["min"]), _b(d["max"]), d["entries"], d["size"], d["tail"], d["index"], d["digest"]
        )

    @classmethod
    def describe(cls, name: str, data: bytes) -> FileInfo:
        footer = parse_footer(data[-FOOTER_SIZE:])
        tail = parse_tail(data[footer["filters_offset"] :], len(data))
        return cls(
            name,
            tail["min_key"],
            tail["max_key"],
            footer["entries"],
            len(data),
            len(data) - footer["filters_offset"],
            len(data) - footer["index_offset"],
            digest(data),
        )


@dataclass(frozen=True)
class Span:
    """The keys commits `[a, b]` changed (docs/key-index-design.md § The
    structure): per key, one version per segment it changed in (the newest,
    newest first), with its predecessor before `a` on the oldest. `starts`
    holds `(commit, generation)` for the span's start and for each segment
    start: the endpoints that were live when the span was written. `files`
    are in key order, never overlapping; a key's versions may cross from one
    into the next. A commit's delta is the span `[c, c]`; the span from commit
    0 is the base, which keeps live keys only in its initial segment."""

    a: int
    b: int
    starts: tuple[tuple[int, int], ...]
    files: tuple[FileInfo, ...]
    counts: tuple[int, ...] = ()  # entries per segment, beside `starts`

    @property
    def entries(self) -> int:
        return sum(f.entries for f in self.files)

    @property
    def size(self) -> int:
        return sum(f.size for f in self.files)

    def to_json(self) -> dict:
        return {
            "a": self.a,
            "b": self.b,
            "starts": [list(x) for x in self.starts],
            "counts": list(self.counts),
            "files": [f.to_json() for f in self.files],
        }

    @classmethod
    def from_json(cls, d: dict) -> Span:
        return cls(
            d["a"],
            d["b"],
            tuple(tuple(x) for x in d["starts"]),
            tuple(FileInfo.from_json(f) for f in d["files"]),
            tuple(d.get("counts") or ()),
        )


@dataclass(frozen=True)
class IndexState:
    """What the engine holds per index: `count` live keys, and its spans,
    oldest first, tiling its commits from 0 to the head with no gap
    (docs/key-index-design.md). Writes are exact, so `count` always is.
    `life` names this incarnation of the index: a reset, a move or a removal
    starts a new one, and a merge planned against another life is refused."""

    count: int = 0
    spans: tuple[Span, ...] = ()
    prefix: str = ""  # where the files live (`index_prefix` when the index was created)
    life: str = ""

    def to_json(self) -> dict:
        return {
            "prefix": self.prefix,
            "life": self.life,
            "count": self.count,
            "spans": [s.to_json() for s in self.spans],
        }

    @classmethod
    def from_json(cls, d: dict | None) -> IndexState:
        if not d:
            return cls()
        return cls(d["count"], tuple(Span.from_json(s) for s in d["spans"]), d["prefix"], d.get("life", ""))

    def path(self, name: str) -> str:
        return f"{self.prefix}{name}.kx"

    # -- views ------------------------------------------------------------------------

    @property
    def files(self) -> tuple[FileInfo, ...]:
        return tuple(f for s in self.spans for f in s.files)

    @property
    def head(self) -> int:
        """The last commit the spans hold (-1: none yet)."""

        return self.spans[-1].b if self.spans else -1

    def newest_first(self) -> list[list[FileInfo]]:
        """The spans as sorted runs, newest first: what a reader merges."""

        return [list(s.files) for s in reversed(self.spans)]

    def referenced(self) -> set[str]:
        """Every file name the index still needs."""

        return {f.name for f in self.files}

    def generation(self, commit: int) -> int | None:
        """The generation of commit `commit`, where it starts a span or a
        segment: the bound a read at that endpoint takes."""

        for s in self.spans:
            if s.a <= commit <= s.b:
                return dict(s.starts).get(commit)
        return None

    def covers(self, first: int, last: int) -> bool:
        """Whether the spans can answer for commits `[first, last]`: `first`
        starts a span or a segment (a reserved endpoint), or lies past the
        head, and `last` is at most the head."""

        if last > self.head:
            return False
        return first > last or first == 0 or self.generation(first) is not None

    def slice(self, first: int | None = None, last: int | None = None) -> IndexState:
        """The part of the index one reader needs: every span, or with `first`
        the spans overlapping `[first, last]` (a catch-up's)."""

        if first is None:
            return self
        hi = last if last is not None else math.inf
        return replace(self, spans=tuple(s for s in self.spans if s.b >= first and s.a <= hi))

    # -- transitions (pure) ----------------------------------------------------------------

    def committed(self, commit_number: int, delta: DeltaFiles) -> IndexState:
        """Install a commit's delta as the span `[c, c]`; an empty delta is a
        span too, so the spans keep no holes. An index that skips commits (a
        failure index, which only failing commits write) gets the span
        `[head + 1, c]`: the commits between changed nothing in it. An empty
        delta that does not advance the head (a commit that wrote no keys)
        changes nothing."""

        if commit_number <= self.head and not delta.files:
            return self
        if commit_number <= self.head:
            raise ValueError(f"commit {commit_number} does not follow the head {self.head}")
        span = Span(
            self.head + 1,
            commit_number,
            ((self.head + 1, delta.generation),),
            tuple(delta.files),
            (sum(f.entries for f in delta.files),),
        )
        return replace(self, count=self.count + delta.added - delta.removed, spans=self.spans + (span,))

    def merged(self, inputs: list[tuple[int, int]], out: Span) -> IndexState:
        """Swap adjacent input spans `(a, b)` for their merge."""

        ranges = [(s.a, s.b) for s in self.spans]
        lo = ranges.index(tuple(inputs[0]))
        if ranges[lo : lo + len(inputs)] != [tuple(x) for x in inputs] or (out.a, out.b) != (
            inputs[0][0],
            inputs[-1][1],
        ):
            raise ValueError(f"spans {inputs} are not this index's, or not adjacent")
        return replace(self, spans=self.spans[:lo] + (out,) + self.spans[lo + len(inputs) :])

    def holds(self, inputs: list[tuple[int, int]], names: list[list[str]]) -> bool:
        """Whether the index still holds exactly these spans, with these files:
        a merge planned against them may publish."""

        current = {(s.a, s.b): [f.name for f in s.files] for s in self.spans}
        return all(current.get(tuple(r)) == n for r, n in zip(inputs, names, strict=True))


@dataclass(frozen=True)
class Delta:
    """A patch's delta, encoded but not yet written: its `.kx` files, and how
    the live key count changes. `listed`: up to the `collect` asked for, the
    written keys and the deleted keys."""

    files: list[bytes]
    added: int
    removed: int
    listed: tuple[list[bytes], list[bytes]] | None = None

    def __len__(self) -> int:
        return sum(parse_footer(d[-FOOTER_SIZE:])["entries"] for d in self.files)


@dataclass(frozen=True)
class DeltaKeys:
    """The keys a commit's delta files write — not those they delete — read a
    page at a time in key order: a store's selection when there are too many
    to list (`solera.stores.Partition`)."""

    io: ObjectIO
    prefix: str
    files: tuple[FileInfo, ...]

    async def chunks(self, size: int = 100_000):
        """Chunks of the written keys, as `str`."""

        span = Span(0, 0, ((0, 0),), tuple(self.files))
        index = KeyIndex(self.io, self.prefix, IndexState(spans=(span,), prefix=self.prefix))
        after = None
        while self.files:
            keys, _, _, after = await index.page(after, size)
            chunk = [key_str(k) for k in keys]
            if chunk:
                yield chunk
            if after is None:
                return


@dataclass(frozen=True)
class DeltaFiles:
    """A commit's delta as written: its files, the count's change, and the
    generation it was written at (its span starts there)."""

    files: list[FileInfo]
    added: int
    removed: int
    generation: int = 0

    def to_json(self) -> dict:
        return {
            "files": [f.to_json() for f in self.files],
            "added": self.added,
            "removed": self.removed,
            "generation": self.generation,
        }

    @classmethod
    def from_json(cls, d: dict) -> DeltaFiles:
        return cls(
            [FileInfo.from_json(f) for f in d["files"]], d["added"], d["removed"], d.get("generation", 0)
        )


@dataclass
class Options:
    block_size: int = 64 * 1024
    level: int = 1
    bits_per_item: int = 14
    k: int = 10
    max_file_bytes: int = 64 * 2**20
    whole_threshold: int = 2 * RANGE  # spans this small are read whole: no more requests than tail + block
    small_file: int = (
        2 * 2**20
    )  # files this small are read whole: cheaper to transfer than a second round trip
    # When a patch streams the whole index instead of reading blocks (docs/resolved-commits.md §6,
    # from bench/keys/bench.py's crossover grid, where each breaks even in wall time at about 2% and
    # 24 reads per segment; streaming's far fewer requests tip close calls its way): its run is over
    # this share of the entries...
    stream_density: float = 0.02
    # ...or, after the filters, its exact reads need more blocks than this many per streamed segment.
    stream_reads: float = 16.0
    # The merge policy (docs/key-index-design.md § The merge policy).
    guard: float = 4.0  # a merge's largest input holds at most this many times the others
    window: int = 4  # spans of one size merged at once
    base_ratio: float = 4.0  # the base absorbs what follows once that is a quarter of it
    read_ratio: float = 1.0  # the read rule's λ: a reader at an endpoint reads at most that much more...
    read_slack: int = 10 * 2**20  # ...or this many bytes
    fan_in: int = 32  # spans past which merges are forced
    stale: float = 0.25  # a span rewritten alone must drop at least this share of its entries


# -- reading ------------------------------------------------------------------------


@dataclass
class _Parsed:
    info: FileInfo
    tail: dict  # parsed index, plus the filters when `filters`
    filters: bool = True
    data: bytes | None = None  # the file from its start through at least its last block
    window: dict[int, bytes] = field(default_factory=dict)  # the blocks a scan's last page fetched
    firsts: list[bytes] = field(init=False)

    def __post_init__(self):
        self.firsts = [b[0] for b in self.tail["blocks"]]

    def runs(self, wanted) -> list[list[int]]:
        """Sorted block indexes grouped into range reads: consecutive blocks, up to `RANGE` each."""

        blocks = self.tail["blocks"]
        out: list[list[int]] = []
        for i in sorted(wanted):
            if out and out[-1][-1] == i - 1 and blocks[i][1] + blocks[i][2] - blocks[out[-1][0]][1] <= RANGE:
                out[-1].append(i)
            else:
                out.append([i])
        return out

    def span(self, run: list[int]) -> tuple[int, int]:
        blocks = self.tail["blocks"]
        return blocks[run[0]][1], blocks[run[-1]][1] + blocks[run[-1]][2]


class _Lazy:
    """One span read for a page (`Merge.read`): its files from the block
    holding `after`, a segment of consecutive blocks at a time, fetched only
    when the job asks — the first just enough for `want` entries, each next
    twice the last, up to `jobs.SEGMENT`. Files no key past `after` can be in
    are skipped. Through the index's caches (`_open`, `_blocks`): a scan's
    next page reads neither a block index nor a block twice."""

    def __init__(self, index: KeyIndex, files: list[FileInfo], after: bytes | None, want: int):
        self.index, self.after, self.want = index, after, want
        self.files = [f for f in files if after is None or f.max > after]
        self.fi, self.p, self.bi, self.last = 0, None, 0, 0
        self.start: int | None = None  # where this reader's last segment began

    async def next(self):
        while self.fi < len(self.files):
            if self.p is None:
                self.p = await self.index._open(self.files[self.fi], filters=False)
                start = bisect.bisect_right(self.p.firsts, self.after) - 1 if self.after is not None else 0
                self.bi = max(0, start)
            p = self.p
            blocks = p.tail["blocks"]
            if self.bi >= len(blocks):
                p.window = {}  # read past: the next page starts in a later file
                self.fi, self.p, self.start, self.last = self.fi + 1, None, None, 0
                continue
            i = j = self.bi
            got, target = 0, min(jobs.SEGMENT, 2 * self.last) if self.last else jobs.SEGMENT
            while j < len(blocks):
                size = blocks[j][1] + blocks[j][2] - blocks[i][1]
                if j > i and (size > target or (not self.last and got >= self.want)):
                    break
                got += blocks[j][3]
                j += 1
            fetched = await self.index._blocks(p, range(i, j))
            if p.data is None:
                # The next page starts in the block holding this one's last key: in
                # this reader's last segment or the one before, and it reads on into
                # what earlier pages fetched past it. A block is never paid for twice.
                keep = self.start if self.start is not None else i
                p.window = {k: v for k, v in {**p.window, **fetched}.items() if k >= keep}
                self.start = i
            self.bi, self.last = j, blocks[j - 1][1] + blocks[j - 1][2] - blocks[i][1]
            data = b"".join(fetched[k] for k in range(i, j))
            metas, off = [], 0
            for k in range(i, j):
                metas.append((off, len(fetched[k]), blocks[k][4]))
                off += len(fetched[k])
            return data, metas, p.tail["codec"]
        return None


async def _scan_local(snap, after, limit: int, keep_deleted: bool, ceiling: int):
    """A page of local copies as a native run, and its cursor; past `ceiling`
    bytes of keys and payloads, `Full` — the page could not be kept."""

    try:
        return await in_thread(snap.scan, after, limit, drop_deleted=not keep_deleted, max_bytes=ceiling)
    except LimitError as e:
        raise Full(str(e)) from e


class KeyIndex:
    """I/O over one index. `state` is the pinned `IndexState` to read.

    The `io` may carry `local` — the engine cache's copies by path — read in
    place of the store whenever they hold every file a read needs, and
    `served` — a `Reads` record (docs/resolved-commits.md §7): answering,
    its calls are taken from it; recording, every `page`, page of `changes`
    and `lookup` is kept in it, and one that would need the store raises `Cold`."""

    def __init__(self, io: ObjectIO, prefix: str | None, state: IndexState, options: Options | None = None):
        self.io = io
        self.prefix = (prefix if prefix is not None else state.prefix).rstrip("/") + "/"
        self.state = state
        self.o = options or Options()
        self._parsed: dict[str, _Parsed] = {}
        self.route = ""  # how the last resolve read the index: "sparse" or "stream"
        self.local_reads = False  # whether the last streaming job read local copies
        self._identity: str | None = None
        self.on_write = None  # called with (path, FileInfo, bytes) for every file written

    def path(self, name: str) -> str:
        return f"{self.prefix}{name}.kx"

    # -- file access ---------------------------------------------------------------------

    def _small(self, f: FileInfo) -> bool:
        """Read whole at once rather than tail, then blocks: mostly tail anyway, or
        cheaper to transfer than a second round trip."""

        return f.size <= 2 * f.tail or f.size <= self.o.small_file

    async def _open(self, f: FileInfo, *, data: bool = False, filters: bool = True) -> _Parsed:
        """A parsed file: its block index, plus its filters when `filters`, plus
        its data blocks when `data`. Reads only what was not read before."""

        p = self._parsed.get(f.name)
        if p is None or (filters and not p.filters):
            if data or self._small(f):
                raw = await self.io.read_whole(self.path(f.name), f.size)
                p = _Parsed(f, parse_tail(raw[f.size - f.tail :], f.size), data=raw)
            elif filters:
                raw = await self.io.read(self.path(f.name), f.size - f.tail, f.size, f.size)
                p = _Parsed(f, parse_tail(raw, f.size))
            else:
                raw = await self.io.read(self.path(f.name), f.size - f.index, f.size, f.size)
                p = _Parsed(f, parse_index(raw, f.size), filters=False)
            self._parsed[f.name] = p
        if data and p.data is None:
            p.data = await self.io.read(self.path(f.name), 0, f.size - f.tail, f.size)
        return p

    async def _blocks(self, p: _Parsed, wanted) -> dict[int, bytes]:
        """Fetch blocks by index, consecutive ones as a single range read."""

        blocks = p.tail["blocks"]
        out: dict[int, bytes] = {}
        wanted = set(wanted)
        for i in wanted & p.window.keys():
            out[i] = p.window[i]
        wanted -= out.keys()
        if p.data is not None:
            for i in wanted:
                _, off, size, _, crc = blocks[i]
                out[i] = p.data[off : off + size]
                check_block(out[i], crc)
            return out

        async def fetch(run: list[int]):
            start, end = p.span(run)
            data = await self.io.read(self.path(p.info.name), start, end, p.info.size)
            for i in run:
                _, off, size, _, crc = blocks[i]
                out[i] = data[off - start : off - start + size]
                check_block(out[i], crc)

        await asyncio.gather(*(fetch(r) for r in p.runs(wanted)))
        return out

    # -- a commit's delta ----------------------------------------------------------------

    async def resolve(
        self,
        run: SortedEntries,
        *,
        commit_number: int,
        attempt: str,
        generation: int = 0,
        collect: int = 0,
    ) -> tuple[DeltaFiles, tuple[list[bytes], list[bytes]] | None]:
        """A patch's delta — `run`'s upserts and removes, at `generation` —
        written as the commit's files (docs/resolved-commits.md §6). A small
        patch reads only what it must — the sparse reader; a dense one, or
        one whose exact reads would touch too many blocks, streams the whole
        index instead; an empty index reads nothing. Every key a filter holds
        has its entry read, so the counts are exact and every written key
        names its predecessor (docs/key-index-design.md). Returns the files
        and, up to `collect` keys, the written keys and the deleted keys
        (None past it). A full replacement is `replace`."""

        entries = sum(f.entries for f in self.state.files)
        if entries and len(run) > self.o.stream_density * entries:
            return await self._stream(run, commit_number, attempt, generation, collect)
        delta = await self._sparse(run, generation, collect=collect, switch=True)
        if delta is None:
            return await self._stream(run, commit_number, attempt, generation, collect)
        self.route = "sparse"
        return await self.write(commit_number, attempt, delta, generation), delta.listed

    async def delta(self, run: SortedEntries, *, generation: int = 0) -> Delta:
        """A patch's delta through the sparse reader whatever its size, not written."""

        return await self._sparse(run, generation, collect=0, switch=False)

    async def _sparse(self, run: SortedEntries, generation: int, *, collect: int, switch: bool):
        """The sparse reader's delta; None when `switch` and streaming would read less."""

        sparse = await self._find(run, switch=switch)
        if sparse is None:
            return None
        (files, added, removed, _), listed = await in_thread(
            sparse.delta, generation=generation, collect=collect, **self._writer()
        )
        return Delta(files, added, removed, listed)

    async def lookup(self, keys: list[bytes], at: int | None = None) -> dict[bytes, tuple[int, bytes | None]]:
        """Exactly, the live `(generation, payload)` of each of `keys` the
        index holds — the newest entry wins, and a deleted key is absent:
        for selections named outright (a run's `keys=`), immutable stores'
        reads (docs/lifecycle.md §9.8) and failed keys' prior records.
        Every span at once; the filters only skip files that cannot hold a
        key. With `at`, as of that reserved endpoint: the state after commit
        `at - 1`."""

        keys = sorted(set(keys))
        if at is not None and at <= self.state.head:
            below = self._endpoint(at)
            spans = [list(s.files) for s in reversed(self.state.spans) if s.a < at]
            runs, codecs = await self._key_blocks(spans, keys)
            found, generations, deleted, payloads = await in_thread(
                _native.span_lookup, runs, codecs, keys, below
            )
            return {
                k: (generations[i], payloads[i]) for i, k in enumerate(keys) if found[i] and not deleted[i]
            }

        async def store():
            run = SortedEntries.of(keys)
            return (await self._find(run, switch=False)).live()

        async def local(snap, _ceiling):
            hits = await in_thread(snap.get, keys)
            return {
                k: (h[0], h[2]) for k, h in zip(keys, hits, strict=True) if h is not None and not h[1]
            }, None

        return await self._read("lookup", (keys,), self.state.newest_first(), local, store)

    # -- reads: local copies, a record, or the store ----------------------------------------

    @property
    def identity(self) -> str:
        """The pinned state's digest: what a recorded read is bound to."""

        if self._identity is None:
            self._identity = digest(json.dumps(self.state.to_json(), sort_keys=True).encode())
        return self._identity

    def _snapshot(self, spans: list[list[FileInfo]]):
        """The local copies of `spans` as a snapshot, if the `io` holds them all."""

        local = getattr(self.io, "local", None)
        if not local or not all(self.path(f.name) in local for span in spans for f in span):
            return None
        return _native.Snapshot([[local[self.path(f.name)] for f in span] for span in spans])

    async def _read(self, call: str, args: tuple, spans, local, store):
        served = getattr(self.io, "served", None)
        if served is not None and not served.recording:
            hit = served.answer(self.identity, call, args)
            if hit is not None:
                return hit
        recording = served is not None and served.recording
        snap = self._snapshot(spans)
        if recording and snap is None:
            raise Cold(call)  # first: a cold index is fetched for the next start
        if recording:
            served.admit(call, args)  # `Full` before any reading: the record could not keep it
        page = None
        if snap is not None:
            out, page = await local(snap, served.decoded_left() if recording else 2**64 - 1)
        elif recording:
            raise Cold(call)
        else:
            out = await store()
        if recording:  # encoded off the loop, from the native page where there is one
            await in_thread(served.record, self.identity, call, args, out, page)
        return out

    async def _stream(self, run: SortedEntries, commit_number, attempt, generation, collect):
        """The streaming merge-join of a patch with every span."""

        self.route = "stream"
        runs = self.state.newest_first()
        job = Merge.patch(run, len(runs), **self._writer(), collect=collect, generation=generation)
        files = await self._run(job, runs, lambda n: f"{commit_number:012d}-{attempt}.{n:04d}")
        return DeltaFiles(files, job.added, job.removed, generation), job.collected()

    async def replace(
        self,
        rows: Rows | Iterable,
        commit_number: int,
        attempt: str,
        *,
        collect: int = 0,
        key: str | None = None,
        generation: int = 0,
        overlay: SortedEntries | None = None,
    ) -> tuple[DeltaFiles, tuple[list[bytes], list[bytes]] | None]:
        """A full replacement: `rows` is the whole new content — a `Rows`, or
        chunks of keys sorted, pulled as needed (with `key`, rows keyed by
        that column). Every key is written at `generation` — unless it
        carries a payload equal to its live entry's — carrying the key's
        predecessor, and live keys not in `rows` are deleted. The delta goes
        out as the commit's files as they fill. Returns them and, up to
        `collect` keys, the written keys and the deleted keys (None past
        it). Streamed chunks may have a run laid over them (`overlay`): its
        upserts in place of their entries of its keys, its removes gone — a
        patch over the keys a store holds, read back."""

        runs = self.state.newest_first()
        job = Merge.replace(
            rows if isinstance(rows, Rows) else None,
            len(runs),
            **self._writer(),
            collect=collect,
            key=key,
            generation=generation,
            overlay=overlay,
        )
        files = await self._run(job, runs, lambda n: f"{commit_number:012d}-{attempt}.{n:04d}", rows)
        return DeltaFiles(files, job.added, job.removed, generation), job.collected()

    def _writer(self) -> dict:
        o = self.o
        return {
            "block_size": o.block_size,
            "level": o.level,
            "bits_per_item": o.bits_per_item,
            "k": o.k,
            "max_file_bytes": o.max_file_bytes,
        }

    async def _run(self, job: Merge, runs, name=None, rows=None) -> list[FileInfo]:
        """Drive a streaming job over `runs`; its files are written as `name(n)`
        (with no `name`, counted and discarded). When the `io`'s local copies
        hold every file of `runs`, the job reads those, not the store."""

        local = getattr(self.io, "local", None)

        files: dict[int, FileInfo] = {}

        async def put(n: int, data: bytes):
            if name is None:
                return
            await self.io.write(self.path(name(n)), data)
            files[n] = FileInfo.describe(name(n), data)
            if self.on_write is not None:
                self.on_write(self.path(name(n)), files[n], data)

        held = None
        if local is not None and all(self.path(f.name) in local for run in runs for f in run):
            held = [[local[self.path(f.name)] for f in run] for run in runs]
        self.local_reads = held is not None
        await jobs.run(job, self.io, self.path, runs, put, None if isinstance(rows, Rows) else rows, held)
        return [files[n] for n in sorted(files)]

    async def _find(self, run: SortedEntries, *, switch: bool):
        """What the index holds for each entry of `run`, as a native `Sparse`
        state — read live or deleted, or absent by the key filters (writes
        are exact: a key a filter holds has its entry read). Newest first, spans small enough are read whole, all at once; from
        the first larger one on, every span goes through its filters, since
        "absent" must hold across every span that could hold the key. Only
        entries the filters cannot decide get block reads, in the files whose
        key filter matched, all spans at once. With `switch`, None once those
        reads would touch more blocks than streaming the index costs segments
        × `stream_reads`. Python chooses files and fetches; no key becomes a
        Python object."""

        sparse = _native.Sparse(run)
        spans = self.state.newest_first()
        n = next((i for i, span in enumerate(spans) if not self._read_whole(span)), len(spans))
        whole, filtered = spans[:n], spans[n:]

        # 1. The whole spans, fetched at once and then consulted newest first.
        ranges = {f.name: sparse.span(f.min, f.max) for span in spans for f in span}
        await asyncio.gather(
            *(
                self._open(f, data=True)
                for span in whole
                for f in span
                if ranges[f.name][0] < ranges[f.name][1]
            )
        )
        for span in whole:
            for f in span:
                lo, hi = ranges[f.name]
                if lo == hi or not sparse.unknown:
                    continue
                p = self._parsed[f.name]
                got = await self._blocks(p, sparse.blocks(p.firsts, lo=lo, hi=hi))
                await in_thread(sparse.read, list(got.items()), p.tail["codec"], p.firsts, lo=lo, hi=hi)
        if not sparse.unknown or not filtered:
            return sparse

        # 2. The rest through their filters.
        files = [f for span in filtered for f in span if ranges[f.name][0] < ranges[f.name][1]]
        parsed = await asyncio.gather(*(self._open(f) for f in files))
        for i, p in enumerate(parsed):
            tail = p.tail
            sparse.filter(i, *ranges[p.info.name], tail["key_filter"])
        sparse.classify()
        if not sparse.maybe:
            return sparse

        # 3. Exact reads of the rest, in every file whose key filter matched them.
        needs = [(i, p, sparse.blocks(p.firsts, file=i)) for i, p in enumerate(parsed)]
        needs = [(i, p, blocks) for i, p, blocks in needs if blocks]
        if switch:
            reads = sum(len(blocks) for _, _, blocks in needs)
            size = sum(f.size for f in self.state.files)
            if reads > self.o.stream_reads * math.ceil(size / jobs.SEGMENT):
                return None
        fetched = await asyncio.gather(*(self._blocks(p, blocks) for _, p, blocks in needs))
        for (i, p, _), got in zip(needs, fetched, strict=True):  # newest first: its entry wins
            await in_thread(sparse.read, list(got.items()), p.tail["codec"], p.firsts, file=i)
        return sparse

    def _read_whole(self, span: list[FileInfo]) -> bool:
        """Whether a span is small enough to read whole without looking at its filters."""

        return sum(f.size for f in span) <= self.o.whole_threshold

    # -- writing ------------------------------------------------------------------------

    async def write(self, commit_number: int, attempt: str, delta: Delta, generation: int = 0) -> DeltaFiles:
        """Write a patch's delta as the commit's files, `{commit}-{attempt}.{n}`:
        the attempt id keeps a retried commit from colliding with its own upload."""

        files = [
            FileInfo.describe(f"{commit_number:012d}-{attempt}.{n:04d}", d) for n, d in enumerate(delta.files)
        ]
        await asyncio.gather(
            *(self.io.write(self.path(f.name), d) for f, d in zip(files, delta.files, strict=True))
        )
        return DeltaFiles(files, delta.added, delta.removed, generation)

    # -- scans: the full pass ---------------------------------------------------------------

    async def _read_page(self, spans: list[list[FileInfo]], after: bytes | None, limit: int, **read):
        """One page of a span read over `spans` (newest first) past `after`
        (`Merge.read`'s `changes`, or `bound` and `drop_deleted`): keys,
        classes, generations, deleted flags, payloads, and the cursor (None
        when done). Each span is read lazily from the block holding `after`,
        as far as the page needs: a key whose versions run across blocks and
        files is followed to its end, so every page but the last advances,
        holding a few segments and one key's fold (A17 R2, R10)."""

        job = Merge.read(len(spans), after=after, limit=limit, **read)
        readers = [_Lazy(self, files, after, limit + 1) for files in spans]
        page = None
        while (step := await in_thread(job.step)) is not None:
            kind, x = step
            if kind == "run":
                seg = await readers[x].next()
                if seg is None:
                    job.end(x)
                else:
                    job.feed(x, *seg)
            else:
                page = x
        ks, cs, gs, ds, ps, last, more = page
        return ks, cs, gs, ds, ps, (last if more else None)

    async def _scan(
        self, spans: list[list[FileInfo]], after: bytes | None, limit: int, drop_deleted: bool, below=None
    ):
        """Up to `limit` entries of the merged view with keys > `after` —
        each key's newest version, or its newest older than generation
        `below` (the view at a reserved endpoint) — and the cursor to
        continue from (`None` when the view is exhausted)."""

        ks, _, gs, ds, ps, cursor = await self._read_page(
            spans, after, limit, bound=below, drop_deleted=drop_deleted
        )
        return ks, gs, ds, ps, cursor

    def _endpoint(self, at: int) -> int | None:
        """The generation a read at reserved endpoint `at` (the state after
        commit `at - 1`) takes as its bound; None past the head: no bound."""

        if at > self.state.head:
            return None
        g = self.state.generation(at)
        if g is None:
            raise LookupError(f"commit {at} is not an endpoint of this index")
        return g

    async def page(self, after: bytes | None, limit: int, at: int | None = None):
        """One page of the full pass: live keys > `after`, their generations
        and payloads, and the next cursor (`None` when done). With `at`, as
        of that reserved endpoint: the state after commit `at - 1`."""

        if at is not None and at <= self.state.head:
            below = self._endpoint(at)
            spans = [list(s.files) for s in reversed(self.state.spans) if s.a < at]
            keys, generations, _, payloads, nxt = await self._scan(spans, after, limit, True, below)
            return keys, generations, payloads, nxt
        spans = self.state.newest_first()

        async def store():
            keys, generations, _, payloads, nxt = await self._scan(spans, after, limit, drop_deleted=True)
            return keys, generations, payloads, nxt

        async def local(snap, ceiling):
            page, nxt = await _scan_local(snap, after, limit, False, ceiling)
            keys, generations, _, payloads = page.entries()
            return (keys, generations, payloads, nxt), page

        return await self._read("page", (after, limit), spans, local, store)

    # -- changes(P -> N) --------------------------------------------------------------------

    def _range(self, first: int, last: int) -> tuple[list[list[FileInfo]], int, int]:
        """The runs of the spans overlapping commits `[first, last]`, newest
        first, and the generations `[g(first), g(last + 1))` they are clipped to."""

        if not self.state.covers(first, last):
            raise LookupError(f"commits {first}..{last}: {first} is not an endpoint of this index")
        g_p = 0 if first == 0 else self._endpoint(first)
        g_n1 = self._endpoint(last + 1)
        runs = [list(s.files) for s in reversed(self.state.spans) if s.b >= first and s.a <= last]
        return runs, g_p, g_n1

    async def changes(
        self,
        first: int,
        last: int,
        *,
        after: bytes | None = None,
        limit: int = 100_000,
        keys: list[bytes] | None = None,
        until: bytes | None = None,
        lower: dict[bytes, tuple[int, bool]] | None = None,
    ):
        """`changes(first -> last)`, a page at a time: yields `Changes` pages —
        every key changed in commits `[first, last]`, its class (0 added, 1
        updated, 2 removed, 3 neither: a read-ahead may need it) and its
        state at `last`. `first` and `last + 1` are reserved endpoints, or
        `last` is the head (docs/key-index-design.md § changes). `keys`: only
        those keys, one page. `after`/`until`: keys in `(after, until)`, a
        prefix's range. `lower`: read-ahead bounds, `{key: (generation read,
        delivered live)}` — such a key is skipped unless it changed after it
        was read, and then classed from what was delivered; applied to each
        page as read, so it stays out of the recorded call. Resume a scan by
        passing a page's `cursor` as `after`. Each page is one `_read`: served
        from the engine's record, or recorded, like `page` and `lookup`."""

        if keys is not None:
            keys = sorted(set(keys))
        while True:
            page = await self.changes_page(first, last, after, limit, keys=keys, until=until)
            yield self._lowered(page, lower)
            if page.cursor is None:
                return
            after = page.cursor

    async def changes_page(
        self,
        first: int,
        last: int,
        after: bytes | None,
        limit: int,
        *,
        keys: list[bytes] | None = None,
        until: bytes | None = None,
    ) -> Changes:
        """One page of `changes` (its arguments but `lower`; `keys` sorted
        and distinct): the one recorded call, `changes`."""

        spans, g_p, g_n1 = self._range(first, last)

        def clipped(ks, cs, gs, ds, ps, cursor) -> Changes:
            page = Changes(list(ks), bytes(cs), list(gs), bytes(ds), list(ps), cursor)
            if until is not None:
                page = page.below(until)
                if page.cursor is not None and page.cursor >= until:
                    page = replace(page, cursor=None)
            return page

        async def store():
            if not spans:
                return Changes([], b"", [], b"", [], None)
            if keys is not None:
                runs, codecs = await self._key_blocks(spans, keys)
                ks, cs, gs, ds, ps, _, _ = await in_thread(
                    _native.span_changes, runs, codecs, None, None, 2**62, g_p, g_n1
                )
                want = set(keys)
                picked = [i for i, k in enumerate(ks) if k in want]
                return clipped(
                    [ks[i] for i in picked],
                    bytes(cs[i] for i in picked),
                    [gs[i] for i in picked],
                    bytes(ds[i] for i in picked),
                    [ps[i] for i in picked],
                    None,
                )
            return clipped(*await self._read_page(spans, after, limit, changes=(g_p, g_n1)))

        async def local(snap, ceiling):
            try:
                if keys is not None:
                    ks, cs, gs, ds, ps, _, _ = await in_thread(
                        snap.changes_of, keys, g_p, g_n1, max_bytes=ceiling
                    )
                    return clipped(ks, cs, gs, ds, ps, None), None
                ks, cs, gs, ds, ps, last_key, more = await in_thread(
                    snap.changes, after, limit, g_p, g_n1, max_bytes=ceiling
                )
            except LimitError as e:
                raise Full(str(e)) from e
            return clipped(ks, cs, gs, ds, ps, last_key if more else None), None

        return await self._read("changes", (first, last, after, limit, keys, until), spans, local, store)

    async def _key_blocks(self, spans: list[list[FileInfo]], keys: list[bytes]):
        """The blocks of `spans` (spans, newest first) that may hold the
        versions of `keys`, as runs and their codecs: each file a run of its
        own, in key order, so a key's versions still come newest first when
        they cross from one file into the next."""

        runs, codecs = [], []
        for files in spans:
            parsed = await asyncio.gather(*(self._open(f, filters=False) for f in files))
            for p in parsed:
                wanted = set()
                for k in keys:
                    if k < p.info.min or k > p.info.max:
                        continue
                    i = bisect.bisect_left(p.firsts, k)  # blocks starting below k, and those starting at it
                    wanted.add(max(0, i - 1))
                    while i < len(p.firsts) and p.firsts[i] == k:
                        wanted.add(i)
                        i += 1
                got = await self._blocks(p, wanted)
                runs.append([got[i] for i in sorted(wanted)])
                codecs.append(p.tail["codec"])
        return runs, codecs

    @staticmethod
    def _lowered(page: Changes, lower: dict[bytes, tuple[int, bool]] | None) -> Changes:
        """The read-ahead rule (docs/key-index-design.md § changes): a key a
        `keys=` selection delivered at generation `g` is skipped unless its
        state at `last` is newer; then its class is relative to what was
        delivered."""

        if not lower:
            return page
        keep = []
        classes = bytearray(page.classes)
        for i, k in enumerate(page.keys):
            read = lower.get(k)
            if read is None:
                keep.append(i)
                continue
            generation, was_live = read
            if page.generations[i] <= generation:
                continue  # unchanged since it was read
            now_live = not page.deleted[i]
            classes[i] = CLASSES[(was_live, now_live)]
            keep.append(i)
        return Changes(
            [page.keys[i] for i in keep],
            bytes(classes[i] for i in keep),
            [page.generations[i] for i in keep],
            bytes(page.deleted[i] for i in keep),
            [page.payloads[i] for i in keep],
            page.cursor,
        )

    # -- merges (docs/key-index-design.md § The merge policy) -----------------------------------

    def plan_merge(
        self,
        endpoints: set[int],
        *,
        lane: str = "any",
        busy: frozenset = frozenset(),
        rejected: frozenset = frozenset(),
    ):
        """The next merge, `(start, count)` of adjacent spans, or None. `lane`:
        "base" plans only merges into the base, "tail" only the others; `busy`
        names the spans `(a, b)` a merge under way holds; `rejected` the span
        rewrites (`rewrite_key`) found to drop too little. Every merge obeys
        the guard; below the span cap, the read rule; past it, the cheapest
        guarded window merges whatever the read rule says."""

        sp = list(self.state.spans)
        if lane == "base" and busy:
            sp = sp[: next((i for i, s in enumerate(sp) if (s.a, s.b) in busy), len(sp))]
        policy = _Policy(sp, endpoints, self.o, rejected)
        if lane != "tail":
            plan = policy.into_base()
            if plan is not None or lane == "base":
                return plan
        free = [(s.a, s.b) not in busy for s in sp]
        plan = policy.tail(free)
        if plan is None and len(self.state.spans) > self.o.fan_in:
            plan = policy.forced(free)
        return plan

    def _merge_job(self, plan: tuple[int, int], endpoints: set[int]):
        lo, count = plan
        ins = self.state.spans[lo : lo + count]
        a, b = ins[0].a, ins[-1].b
        ends = sorted(e for e in endpoints if a < e <= b)
        gens = [self.state.generation(e) for e in ends]
        if any(g is None for g in gens):
            raise ValueError(f"endpoints {ends} do not all start a span or a segment of {a}..{b}")
        job = Merge.spans(count, endpoints=gens, base=a == 0, **self._writer())
        return job, ins, ends, gens, [list(s.files) for s in reversed(ins)]

    async def drops_enough(self, plan: tuple[int, int], endpoints: set[int]) -> bool:
        """Whether a span rewritten alone would drop a quarter of its entries
        (the guard's other half), counted by its merge writing nothing: a
        rewrite that would not is never uploaded (A17 R7)."""

        job, ins, _, _, runs = self._merge_job(plan, endpoints)
        await self._run(job, runs, None)
        return sum(job.segments) * 4 <= sum(s.entries for s in ins) * 3

    async def merge(
        self, plan: tuple[int, int], endpoints: set[int], *, epoch: int = 0, checked: bool = False
    ) -> SpanMerged | None:
        """Run merge `plan`: the spans it names merged into one, keeping the
        versions the live `endpoints` see. Returns what publishing it takes,
        or None for a span rewritten alone that would not drop a quarter of
        its entries — found by `drops_enough` first, unless `checked` says
        that was done: nothing is uploaded for it."""

        lo, count = plan
        if count == 1 and not checked and not await self.drops_enough(plan, endpoints):
            return None
        job, ins, ends, gens, runs = self._merge_job(plan, endpoints)
        a, b = ins[0].a, ins[-1].b
        stamp = ulid()
        # Named by the merging engine's epoch: the orphan collector of an engine
        # since fenced, whose epoch is lower, never takes it for its own.
        files = await self._run(job, runs, lambda n: f"m{a:012d}-{b:012d}-{epoch:06d}-{stamp}.{n:04d}")
        counts = job.segments
        out = Span(
            a, b, (ins[0].starts[0],) + tuple(zip(ends, gens, strict=True)), tuple(files), tuple(counts)
        )
        return SpanMerged(
            [(s.a, s.b) for s in ins],
            [[f.name for f in s.files] for s in ins],
            out,
            sum(s.entries for s in ins),
            sum(counts),
        )

    @staticmethod
    def rewrite_key(span: Span, endpoints: set[int]) -> str:
        """What names a span's rewrite: its files and the live endpoints
        inside it. A rewrite found to drop too little is not tried again
        until one of them changes."""

        live = sorted(c for c, _ in span.starts[1:] if c in endpoints)
        return ",".join(f.name for f in span.files) + "|" + ",".join(map(str, live))


CLASSES = {(False, True): 0, (True, True): 1, (True, False): 2, (False, False): 3}


@dataclass(frozen=True)
class Changes:
    """A page of `changes(P -> N)`: keys, their classes (0 added, 1 updated,
    2 removed, 3 neither), and the generation, deleted flag and payload of
    each one's state at N; `cursor`: where the next page starts, None at the end."""

    keys: list[bytes]
    classes: bytes
    generations: list[int]
    deleted: bytes
    payloads: list
    cursor: bytes | None

    def below(self, until: bytes) -> Changes:
        n = bisect.bisect_left(self.keys, until)
        return Changes(
            self.keys[:n],
            self.classes[:n],
            self.generations[:n],
            self.deleted[:n],
            self.payloads[:n],
            self.cursor,
        )


@dataclass(frozen=True)
class SpanMerged:
    """A merge to publish: its input spans `(a, b)` and their files' names
    (what the index must still hold), and its output; the entries it read and wrote."""

    inputs: list[tuple[int, int]]
    names: list[list[str]]
    span: Span
    read: int
    written: int

    def to_json(self) -> dict:
        return {
            "inputs": [list(r) for r in self.inputs],
            "names": self.names,
            "span": self.span.to_json(),
            "read": self.read,
            "written": self.written,
        }


class _Policy:
    """The merge policy over spans (docs/key-index-design.md § The merge
    policy): the guard (a merge's largest input holds at most `guard` times
    the others, in entries), the two-ended read rule at every live endpoint a
    merge would put inside its output (in bytes), the triggers — into the
    base, four alike, stragglers — and, past the span cap, forced merges."""

    def __init__(self, spans: list[Span], endpoints: set[int], o: Options, rejected: frozenset = frozenset()):
        self.sp, self.live, self.o, self.rejected = spans, endpoints, o, rejected
        self.entries = [max(s.entries, 1) for s in spans]

    def guarded(self, lo: int, count: int) -> bool:
        w = self.entries[lo : lo + count]
        return count == 1 or max(w) <= self.o.guard * (sum(w) - max(w))

    def segments(self, lo: int, count: int) -> list[tuple[int, float]]:
        """The output's segments, `(start, bytes)`: the inputs' segments, those
        whose dividing endpoint is gone joined (sizes summed: an upper bound)."""

        out: list[list] = []
        for s in self.sp[lo : lo + count]:
            per = s.size / max(s.entries, 1)
            counts = s.counts or (s.entries,)
            for (c, _), n in zip(s.starts, counts, strict=False):
                if out and c not in self.live:
                    out[-1][1] += n * per
                else:
                    out.append([c, n * per])
        return [(c, b) for c, b in out]

    def readable(self, lo: int, count: int) -> bool:
        segs = self.segments(lo, count)
        total = sum(b for _, b in segs)
        before = 0.0
        for c, b in segs:
            if before > 0 and c in self.live:
                after = total - before
                if before > max(self.o.read_ratio * after, self.o.read_slack):
                    return False
                if after > max(self.o.read_ratio * before, self.o.read_slack):
                    return False
            before += b
        return True

    def allowed(self, lo: int, count: int) -> bool:
        return self.guarded(lo, count) and self.readable(lo, count)

    def into_base(self):
        if len(self.sp) < 2 or self.sp[0].a != 0:
            return None
        base = self.entries[0]
        need = base / self.o.base_ratio
        acc, j0 = 0, None
        for j in range(1, len(self.sp)):
            acc += self.entries[j]
            if acc >= need:
                j0 = j
                break
        if j0 is None:
            return None
        ends = [len(self.sp) - 1] + [
            j - 1
            for j in range(len(self.sp) - 1, 0, -1)
            if any(self.sp[j].a <= e <= self.sp[j].b for e in self.live)
        ]
        for j in ends:
            if j >= j0 and self.allowed(0, j + 1):
                return 0, j + 1
        return None

    def tail(self, free: list[bool]):
        sp, w = self.sp, self.o.window

        def ok(lo, count):
            return all(free[lo : lo + count]) and self.allowed(lo, count)

        for j in range(len(sp) - w, 0, -1):  # never the base
            win = self.entries[j : j + w]
            if max(win) <= sum(win) - max(win) and ok(j, w):
                return j, w
        for j in range(1, len(sp) - 1):  # a straggler: smaller than its newer neighbour
            if self.entries[j] < self.entries[j + 1]:
                for count in range(2, w + 1):
                    for lo in range(max(1, j + 2 - count), min(j, len(sp) - count) + 1):
                        if ok(lo, count):
                            return lo, count
        for j in range(1, len(sp)):  # versions no live endpoint sees any more
            s = sp[j]
            if KeyIndex.rewrite_key(s, self.live) in self.rejected:
                continue  # found to drop too little, and nothing changed since
            if free[j] and any(c not in self.live for c, _ in s.starts[1:]):
                dead = sum(
                    n
                    for (c, _), n in zip(s.starts[1:], (s.counts or ())[1:], strict=False)
                    if c not in self.live
                )
                if dead * 4 >= s.entries * self.o.stale * 4:
                    return j, 1
        return None

    def forced(self, free: list[bool]):
        best = None
        for count in (2, 3, 4):
            for lo in range(1, len(self.sp) - count + 1):
                if all(free[lo : lo + count]) and self.guarded(lo, count):
                    cost = sum(self.entries[lo : lo + count])
                    if best is None or cost < best[0]:
                        best = (cost, lo, count)
        return None if best is None else (best[1], best[2])
