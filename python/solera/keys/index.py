"""Key index operations (docs/object-store-state.md §6).

An index is one log-structured merge tree of `.kx` files per keyed output
and partition. `IndexState` is the engine-held record of which files exist;
it is plain data, changed only through its pure transition methods, so the
engine can journal it. `KeyIndex` does the I/O: computing a commit's delta,
writing delta files, paging, reading pending deltas, and compaction. A full
replacement and a compaction stream over the whole index
(`jobs`): memory is a few segments per level and one output file.

Levels: level 0 holds delta files, one per commit, with overlapping key
ranges, newest first. Levels 1+ hold compacted files with non-overlapping
ranges; deeper levels are older. For any key, the newest file holding it
has its current entry.
"""

from __future__ import annotations

import asyncio
import bisect
import itertools
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
    merge_page,
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
    level: int
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
            "level": self.level,
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
            d["name"],
            d["level"],
            _b(d["min"]),
            _b(d["max"]),
            d["entries"],
            d["size"],
            d["tail"],
            d["index"],
            d["digest"],
        )

    @classmethod
    def describe(cls, name: str, level: int, data: bytes) -> FileInfo:
        footer = parse_footer(data[-FOOTER_SIZE:])
        tail = parse_tail(data[footer["filters_offset"] :], len(data))
        return cls(
            name,
            level,
            tail["min_key"],
            tail["max_key"],
            footer["entries"],
            len(data),
            len(data) - footer["filters_offset"],
            len(data) - footer["index_offset"],
            digest(data),
        )


@dataclass(frozen=True)
class IndexState:
    """What the engine holds per index: `count` live keys, the files by level,
    and the delta log consumers read — `(commit, files)`, oldest first. A delta
    file stays in the log after compaction merges it out of the levels, until
    no consumer needs it. Writes are exact, so `count` always is."""

    count: int = 0
    files: tuple[FileInfo, ...] = ()
    log: tuple[tuple[int, tuple[FileInfo, ...]], ...] = ()
    prefix: str = ""  # where the files live (`index_prefix` when the index was created)

    def to_json(self) -> dict:
        return {
            "prefix": self.prefix,
            "count": self.count,
            "files": [f.to_json() for f in self.files],
            "log": [[b, [f.to_json() for f in fs]] for b, fs in self.log],
        }

    @classmethod
    def from_json(cls, d: dict | None) -> IndexState:
        if not d:
            return cls()
        return cls(
            d["count"],
            tuple(FileInfo.from_json(f) for f in d["files"]),
            tuple((b, tuple(FileInfo.from_json(f) for f in fs)) for b, fs in d["log"]),
            d["prefix"],
        )

    def path(self, name: str) -> str:
        return f"{self.prefix}{name}.kx"

    def slice(self, first: int | None = None, last: int | None = None) -> IndexState:
        """The part of the index one reader needs: the levels, and the log
        entries in `[first, last]` (none when `first` is None)."""

        if first is None:
            return replace(self, log=())
        hi = last if last is not None else math.inf
        return replace(self, log=tuple(e for e in self.log if first <= e[0] <= hi))

    def covers(self, first: int, last: int) -> bool:
        """Whether the log still holds every commit in `[first, last]`."""

        logged = {b for b, _ in self.log}
        return all(b in logged for b in range(first, last + 1))

    # -- views ------------------------------------------------------------------------

    def level(self, n: int) -> list[FileInfo]:
        files = [f for f in self.files if f.level == n]
        if n == 0:
            return sorted(files, key=lambda f: f.name, reverse=True)  # newest first
        return sorted(files, key=lambda f: f.min)

    @property
    def depth(self) -> int:
        return max((f.level for f in self.files), default=0)

    def newest_first(self) -> list[list[FileInfo]]:
        """Levels in recency order: each level-0 file alone, then levels 1..depth."""

        out = [[f] for f in self.level(0)]
        for n in range(1, self.depth + 1):
            files = self.level(n)
            if files:
                out.append(files)
        return out

    def referenced(self) -> set[str]:
        """Every file name the index still needs: its levels and its log."""

        return {f.name for f in self.files} | {f.name for _, fs in self.log for f in fs}

    # -- transitions (pure) ----------------------------------------------------------------

    def committed(self, commit_number: int, delta: DeltaFiles, *, keep_log: bool) -> IndexState:
        """Install a commit's delta files. An empty index takes them straight into level 1."""

        level = 1 if not self.files else 0
        placed = tuple(replace(f, level=level) for f in delta.files)
        log = self.log + ((commit_number, placed),) if keep_log and placed else self.log
        return IndexState(
            count=self.count + delta.added - delta.removed,
            files=self.files + placed,
            log=log,
            prefix=self.prefix,
        )

    def compacted(self, added: list[FileInfo], removed: list[str]) -> IndexState:
        """Swap compaction inputs for outputs (a moved file is both, under one name)."""

        gone = set(removed)
        return replace(self, files=tuple(f for f in self.files if f.name not in gone) + tuple(added))

    def truncated(self, lowest_needed_commit: int | None) -> IndexState:
        """Drop log entries no consumer still needs (`None`: no consumers at all)."""

        if lowest_needed_commit is None:
            return replace(self, log=())
        return replace(self, log=tuple(e for e in self.log if e[0] >= lowest_needed_commit))


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

        index = KeyIndex(self.io, self.prefix, IndexState(log=((0, self.files),), prefix=self.prefix))
        after = None
        while self.files:
            keys, _, deleted, _, after = await index.pending(0, 0, after, size)
            chunk = [key_str(k) for k, d in zip(keys, deleted, strict=True) if not d]
            if chunk:
                yield chunk
            if after is None:
                return


@dataclass(frozen=True)
class DeltaFiles:
    files: list[FileInfo]
    added: int
    removed: int

    def to_json(self) -> dict:
        return {"files": [f.to_json() for f in self.files], "added": self.added, "removed": self.removed}

    @classmethod
    def from_json(cls, d: dict) -> DeltaFiles:
        return cls([FileInfo.from_json(f) for f in d["files"]], d["added"], d["removed"])


@dataclass
class Options:
    block_size: int = 64 * 1024
    level: int = 1
    bits_per_item: int = 14
    k: int = 10
    max_file_bytes: int = 64 * 2**20
    whole_threshold: int = 2 * RANGE  # levels this small are read whole: no more requests than tail + block
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
    l0_max_files: int = 8
    l0_max_bytes: int = 64 * 2**20
    level_base: int = 64 * 2**20
    fanout: int = 10


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
    its calls are taken from it; recording, every `page`, `pending` and
    `lookup` is kept in it, and one that would need the store raises `Cold`."""

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
        return await self.write(commit_number, attempt, delta), delta.listed

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

    async def lookup(self, keys: list[bytes]) -> dict[bytes, tuple[int, bytes | None]]:
        """Exactly, the live `(generation, payload)` of each of `keys` the
        index holds — the newest entry wins, and a deleted key is absent:
        for selections named outright (a run's `keys=`), immutable stores'
        reads (docs/lifecycle.md §9.8) and failed keys' prior records.
        Every level at once; the filters only skip files that cannot hold a
        key."""

        keys = sorted(set(keys))

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

    def _snapshot(self, levels: list[list[FileInfo]]):
        """The local copies of `levels` as a snapshot, if the `io` holds them all."""

        local = getattr(self.io, "local", None)
        if not local or not all(self.path(f.name) in local for level in levels for f in level):
            return None
        return _native.Snapshot([[local[self.path(f.name)] for f in level] for level in levels])

    async def _read(self, call: str, args: tuple, levels, local, store):
        served = getattr(self.io, "served", None)
        if served is not None and not served.recording:
            hit = served.answer(self.identity, call, args)
            if hit is not None:
                return hit
        recording = served is not None and served.recording
        snap = self._snapshot(levels)
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
        """The streaming merge-join of a patch with every level."""

        self.route = "stream"
        runs = self.state.newest_first()
        job = Merge.patch(run, len(runs), **self._writer(), collect=collect, generation=generation)
        files = await self._run(job, runs, lambda n: f"{commit_number:012d}-{attempt}.{n:04d}", 0)
        return DeltaFiles(files, job.added, job.removed), job.collected()

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
        files = await self._run(job, runs, lambda n: f"{commit_number:012d}-{attempt}.{n:04d}", 0, rows)
        return DeltaFiles(files, job.added, job.removed), job.collected()

    def _writer(self) -> dict:
        o = self.o
        return {
            "block_size": o.block_size,
            "level": o.level,
            "bits_per_item": o.bits_per_item,
            "k": o.k,
            "max_file_bytes": o.max_file_bytes,
        }

    async def _run(self, job: Merge, runs, name=None, level: int = 0, rows=None) -> list[FileInfo]:
        """Drive a streaming job over `runs`; its files are written as `name(n)`, at
        `level`. When the `io`'s local copies hold every file of `runs`, the
        job reads those, not the store."""

        local = getattr(self.io, "local", None)

        files: dict[int, FileInfo] = {}

        async def put(n: int, data: bytes):
            await self.io.write(self.path(name(n)), data)
            files[n] = FileInfo.describe(name(n), level, data)
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
        state — read live or deleted, absent by the key filters, or, for an
        upsert carrying no payload, live by the key and tombstone filters.
        Newest first, levels small enough are read whole, all at once; from
        the first larger one on, every level goes through its filters, since
        "absent" must hold across every level that could hold the key. Only
        entries the filters cannot decide get block reads, in the files whose
        key filter matched, all levels at once. With `switch`, None once those
        reads would touch more blocks than streaming the index costs segments
        × `stream_reads`. Python chooses files and fetches; no key becomes a
        Python object."""

        sparse = _native.Sparse(run)
        levels = self.state.newest_first()
        n = next((i for i, level in enumerate(levels) if not self._read_whole(level)), len(levels))
        whole, filtered = levels[:n], levels[n:]

        # 1. The whole levels, fetched at once and then consulted newest first.
        spans = {f.name: sparse.span(f.min, f.max) for level in levels for f in level}
        await asyncio.gather(
            *(
                self._open(f, data=True)
                for level in whole
                for f in level
                if spans[f.name][0] < spans[f.name][1]
            )
        )
        for level in whole:
            for f in level:
                lo, hi = spans[f.name]
                if lo == hi or not sparse.unknown:
                    continue
                p = self._parsed[f.name]
                got = await self._blocks(p, sparse.blocks(p.firsts, lo=lo, hi=hi))
                await in_thread(sparse.read, list(got.items()), p.tail["codec"], p.firsts, lo=lo, hi=hi)
        if not sparse.unknown or not filtered:
            return sparse

        # 2. The rest through their filters.
        files = [f for level in filtered for f in level if spans[f.name][0] < spans[f.name][1]]
        parsed = await asyncio.gather(*(self._open(f) for f in files))
        for i, p in enumerate(parsed):
            tail = p.tail
            sparse.filter(i, *spans[p.info.name], tail["key_filter"])
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

    def _read_whole(self, level: list[FileInfo]) -> bool:
        """Whether a level is small enough to read whole without looking at its filters."""

        return sum(f.size for f in level) <= self.o.whole_threshold

    # -- writing ------------------------------------------------------------------------

    async def write(self, commit_number: int, attempt: str, delta: Delta) -> DeltaFiles:
        """Write a patch's delta as the commit's files, `{commit}-{attempt}.{n}`:
        the attempt id keeps a retried commit from colliding with its own upload."""

        files = [
            FileInfo.describe(f"{commit_number:012d}-{attempt}.{n:04d}", 0, d)
            for n, d in enumerate(delta.files)
        ]
        await asyncio.gather(
            *(self.io.write(self.path(f.name), d) for f, d in zip(files, delta.files, strict=True))
        )
        return DeltaFiles(files, delta.added, delta.removed)

    # -- scans: full pass and pending deltas ----------------------------------------------

    async def _scan(self, levels: list[list[FileInfo]], after: bytes | None, limit: int, drop_deleted: bool):
        """Up to `limit` entries of the merged view with keys > `after`, and the
        cursor to continue from (`None` when the view is exhausted).

        `levels` are newest first; the files within one level never overlap.
        Files are chosen from their metadata before anything is read — per
        level, only those covering the next `limit` keys — and only their
        block indexes are read, never their filters."""

        chosen, bound = [], None
        for level in levels:
            got = 0
            for f in sorted(level, key=lambda f: f.min):
                if after is not None and f.max <= after:
                    continue
                if got > limit:
                    # Everything below this file's first key is complete in the chosen ones.
                    bound = f.min if bound is None else min(bound, f.min)
                    break
                chosen.append(f)
                # A file the cursor falls inside may have nothing left past it: count only whole files.
                got += f.entries if after is None or f.min > after else 0
        parsed = await asyncio.gather(*(self._open(f, filters=False) for f in chosen))
        spans = []
        for p in parsed:
            blocks = p.tail["blocks"]
            start = max(0, bisect.bisect_right(p.firsts, after) - 1) if after is not None and blocks else 0
            end, got = start, 0
            # Enough blocks for `limit` entries, and at least one block past the
            # one holding `after`, so every call makes progress.
            while end < len(blocks) and (got < limit + 1 or end - start < 2):
                got += blocks[end][3]
                end += 1
            spans.append(range(start, end))
            if end < len(blocks):
                nxt = blocks[end][0]  # everything below the next unfetched block is complete
                bound = nxt if bound is None else min(bound, nxt)
        fetched = await asyncio.gather(
            *(self._blocks(p, span) for p, span in zip(parsed, spans, strict=True))
        )
        runs = []
        for p, span, got in zip(parsed, spans, fetched, strict=True):
            runs.append([got[i] for i in span])
            if p.data is None:
                p.window = got  # the next page starts in it: a file never pays for the same block twice
        codecs = [p.tail["codec"] for p in parsed]  # each file's own
        # Natively, off the loop: the merge stops at the page, never building the rest.
        keys, generations, deleted, payloads, last, more = await in_thread(
            merge_page, runs, codecs, after, bound, limit, drop_deleted
        )
        cursor = last if last is not None else after
        if len(keys) == limit:  # full: more past it, or past the fetched blocks
            return keys, generations, deleted, payloads, cursor if more or bound is not None else None
        return keys, generations, deleted, payloads, cursor if bound is not None else None

    async def page(self, after: bytes | None, limit: int):
        """One page of the full pass: live keys > `after`, their
        generations and payloads, and the next cursor (`None` when done)."""

        levels = self.state.newest_first()

        async def store():
            keys, generations, _, payloads, nxt = await self._scan(levels, after, limit, drop_deleted=True)
            return keys, generations, payloads, nxt

        async def local(snap, ceiling):
            page, nxt = await _scan_local(snap, after, limit, False, ceiling)
            keys, generations, _, payloads = page.entries()
            return (keys, generations, payloads, nxt), page

        return await self._read("page", (after, limit), levels, local, store)

    async def pending(self, first_commit: int, last_commit: int, after: bytes | None, limit: int):
        """Changes in commits `[first_commit, last_commit]`, newest winning, keys > `after`:
        keys, generations, deleted flags, payloads, and the next cursor (`None` when done)."""

        logged = dict(self.state.log)
        missing = [b for b in range(first_commit, last_commit + 1) if b not in logged]
        if missing:
            raise LookupError(f"delta log no longer holds commits {missing[:5]}")
        # Each commit is a level of its own: its files (a split delta) never overlap.
        levels = [list(logged[b]) for b in range(last_commit, first_commit - 1, -1)]

        async def store():
            return await self._scan(levels, after, limit, drop_deleted=False)

        async def local(snap, ceiling):
            page, nxt = await _scan_local(snap, after, limit, True, ceiling)
            return (*page.entries(), nxt), page

        args = (first_commit, last_commit, after, limit)
        return await self._read("pending", args, levels, local, store)

    async def pending_pages(
        self, first_commit: int, last_commit: int, after: bytes | None = None, limit=100_000
    ):
        """Every page of `pending` from `after`, through one merge of the
        commits' files, each read once: paging with `pending` starts that
        merge over for every page, which far behind a long log costs the
        whole merge per page. Yields `(keys, generations, deleted flags,
        payloads)`. Where reads are served or the files are local copies,
        `pending` itself pages, as recorded reads are page by page."""

        logged = dict(self.state.log)
        missing = [b for b in range(first_commit, last_commit + 1) if b not in logged]
        if missing:
            raise LookupError(f"delta log no longer holds commits {missing[:5]}")
        levels = [sorted(logged[b], key=lambda f: f.min) for b in range(last_commit, first_commit - 1, -1)]
        if getattr(self.io, "served", None) is not None or self._snapshot(levels) is not None:
            while True:
                keys, generations, deleted, payloads, after = await self.pending(
                    first_commit, last_commit, after, limit
                )
                yield keys, generations, deleted, payloads
                if after is None:
                    return
        from .jobs import _Run

        job = Merge.scan(len(levels), after=after, limit=limit)
        readers = [_Run(self.io, self.path, files) for files in levels]
        try:
            while (step := await in_thread(job.step)) is not None:
                kind, x = step
                if kind == "run":
                    segment = await readers[x].next()
                    if segment is None:
                        job.end(x)
                    else:
                        job.feed(x, *segment)
                else:
                    yield x
        finally:
            await asyncio.gather(*(r.close() for r in readers))

    # -- compaction ------------------------------------------------------------------------

    def plan_compaction(self) -> tuple[list[FileInfo], int] | None:
        """The next compaction: input files (newest first) and the output level.

        Level 0 acts once it holds `l0_max_files` files or `l0_max_bytes`. It
        merges into level 1 — with every level-1 file it overlaps, which for
        random keys is all of them — only once it holds a `fanout`-th of level
        1's bytes, so each merge rewrites level 1 for at least that many new
        bytes; until then its files merge among themselves, into one level-0
        file. A level over its target (`level_base · fanout^(n-1)`) pushes one
        file — the one overlapping the next level least — down, merging it with
        the files it overlaps; the deepest level moves down whole, which
        rewrites nothing."""

        s, o = self.state, self.o
        l0 = s.level(0)
        size = sum(f.size for f in l0)
        if l0 and (len(l0) >= o.l0_max_files or size >= o.l0_max_bytes):
            l1 = s.level(1)
            if len(l0) > 1 and size * o.fanout < sum(f.size for f in l1):
                return l0, 0
            lo, hi = min(f.min for f in l0), max(f.max for f in l0)
            return l0 + [f for f in l1 if f.max >= lo and f.min <= hi], 1
        for n in range(1, s.depth + 1):
            files = s.level(n)
            if not files or sum(f.size for f in files) <= o.level_base * o.fanout ** (n - 1):
                continue
            if n == s.depth:
                return files, n + 1
            below = s.level(n + 1)
            pick = min(
                files,
                key=lambda f, below=below: sum(g.size for g in below if g.max >= f.min and g.min <= f.max),
            )
            return [pick] + [g for g in below if g.max >= pick.min and g.min <= pick.max], n + 1
        return None

    async def compact(self, plan=None) -> tuple[list[FileInfo], list[str]] | None:
        """Run one compaction; returns the added files and removed names for
        `IndexState.compacted`. The objects its merge drops are listed by the
        deltas' predecessors already (exact writes). Its inputs are read from
        the `io`'s local copies when they hold them."""

        plan = plan or self.plan_compaction()
        if plan is None:
            return None
        inputs, out_level = plan
        if out_level > self.state.depth and out_level > 1:
            # The deepest level moves down whole: nothing below it to merge with, and
            # its files never overlap. Level-0 files do, so level 0 is always merged.
            return [replace(f, level=out_level) for f in inputs], [f.name for f in inputs]
        drop = out_level >= self.state.depth  # nothing older below: tombstones can go
        # Runs, newest first: each level-0 file alone, a deeper level's files together.
        runs = []
        for lv, group in itertools.groupby(inputs, lambda f: f.level):
            group = list(group)
            runs += [[f] for f in group] if lv == 0 else [group]
        job = Merge.compact(len(runs), drop_deleted=drop, **self._writer())
        # A level-0 file is as recent as its newest input: level 0 orders by name, and delta
        # names start with their commit.
        stamp = ulid() if out_level else f"{inputs[0].name.split('-', 1)[0]}-c{ulid()}"
        added = await self._run(
            job, runs, lambda n: f"c{stamp}-{n:04d}" if out_level else f"{stamp}.{n:04d}", out_level
        )
        return added, [f.name for f in inputs]
