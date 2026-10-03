"""The engine's cache of index files (docs/resolved-commits.md §5).

Index files never change once written, so a cached copy is never stale,
only evicted. Each file is kept on local disk in its local form
(`solera._native.build_local`: blocks decompressed, with restart points, a
checksummed directory), opened with its directory in memory. An index is
**warm** when every file of a snapshot is present; resolves only ever read
warm snapshots, and pin the files they read.

Budgets:
- disk: local files, candidates, temporary files and reservations. Every
  operation that adds bytes reserves them first; a build writes into the
  room it holds and stops at its edge, then reserves more and builds again
  or gives up. A fill or an install that cannot reserve, even after
  eviction, is not done, and an index whose snapshot no longer fits is
  demoted (its files become evictable). At most `builds` whole files are in memory at
  once, fetched or being built.
- candidates: deltas the resolver returned, kept until their attempt
  commits them (then installed without a GET) or ends; their own budget,
  evicted oldest first.

Admission is by index, with hysteresis: an index is admitted when its
snapshot, plus a compaction's overlap, fits beside the indexes active in
the last `window` seconds; eviction takes retired files (inputs of a
published compaction), then files of inactive or demoted indexes, never
pinned files or an active index's. A file's local size is estimated from
its source until one is built; from then on the size it built to counts,
so an index shown not to fit is not fetched again until room changes.

A cached file is the object it claims to be: what is installed or kept
after a restart has the size and digest of the `FileInfo` that names it,
and anything else is dropped and fetched again.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field

from .. import _native
from .index import FileInfo, IndexState, digest
from .io import ObjectIO
from .threads import in_thread

GROWTH = 2.0  # local bytes per compressed data byte, reserved before a build (extended if it needs more)
RETIRED = 10_000  # paths of retired files remembered, so a fill finishing late does not keep one
OBSERVED = 100_000  # local sizes remembered, per source file


@dataclass
class _File:
    path: str  # the object's path: the cache key
    local: str
    size: int  # bytes on disk
    prefix: str  # the index it belongs to
    handle: object  # solera._native.LocalFile
    used: float
    source_size: int
    digest: str  # of the source, hex
    pins: int = 0
    retired: bool = False  # a published compaction let go of it


@dataclass
class _Candidate:
    path: str
    size: int  # source bytes
    digest: str
    data: bytes
    at: float


@dataclass
class _Index:
    used: float = 0.0
    admitted: bool = False
    files: set[str] = field(default_factory=set)
    bytes: int = 0  # of its local files


class _Candidates(OrderedDict):
    """Candidates, oldest first, with their bytes kept as they come and go."""

    def __init__(self):
        super().__init__()
        self.bytes = 0

    def __setitem__(self, key, c):
        old = super().get(key)
        self.bytes += c.size - (old.size if old is not None else 0)
        super().__setitem__(key, c)

    def __delitem__(self, key):
        self.bytes -= super().__getitem__(key).size
        super().__delitem__(key)

    def pop(self, key, *default):
        if key in self:
            self.bytes -= super().__getitem__(key).size
        return super().pop(key, *default)

    def popitem(self, last=True):
        key, c = super().popitem(last=last)
        self.bytes -= c.size
        return key, c

    def clear(self):
        super().clear()
        self.bytes = 0


def _logged(state: IndexState) -> list[FileInfo]:
    return [f for _, files in state.log for f in files]


class Corrupt(Exception):
    """A file's bytes are not the object its `FileInfo` names."""


def verify(f: FileInfo, path: str, data: bytes) -> None:
    """`data` is the file `f` names: its size and its digest."""

    if len(data) != f.size or digest(data) != f.digest:
        raise Corrupt(f"{path}: {len(data)} bytes that do not match its size and digest")


class Pin:
    """Files held for one reader; released on exit."""

    def __init__(self, cache: EngineCache, files: list[_File], runs: list[list[object]]):
        self.cache, self.files, self.runs = cache, files, runs
        self.handles = {f.path: f.handle for f in files}  # path -> LocalFile

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        for f in self.files:
            f.pins -= 1
            if f.retired and f.pins == 0:
                self.cache._drop(f.path)
        self.files = []


class EngineCache:
    def __init__(
        self,
        root: str,
        *,
        disk: int = 16 * 2**30,
        candidates: int = 2**30,
        window: float = 900.0,
        builds: int = 2,
        clock=time.monotonic,
    ):
        self.root = root
        self.disk, self.candidate_budget, self.window, self.clock = disk, candidates, window, clock
        os.makedirs(root, exist_ok=True)
        self.files: dict[str, _File] = {}
        self.indexes: dict[str, _Index] = {}
        self.candidates: _Candidates = _Candidates()  # (path, size, digest) -> _Candidate
        self._file_bytes = 0  # of every local file
        self.reserved = 0
        self._fills: dict[str, asyncio.Future] = {}
        self._builds = asyncio.Semaphore(builds)
        self._retired: OrderedDict[str, None] = OrderedDict()
        self._observed: OrderedDict[tuple[str, str], int] = OrderedDict()  # (path, digest) -> built size
        # After a restart: local files whose directory checks out are kept, their
        # indexes not admitted until a reader asks again; anything else goes.
        for name in os.listdir(root):
            local = os.path.join(root, name)
            try:
                if not name.endswith(".kxl"):
                    raise ValueError("a temporary file")
                handle = _native.LocalFile(local)
            except (ValueError, OSError):
                with contextlib.suppress(OSError):
                    os.unlink(local)
                continue
            path = handle.source
            prefix = path[: path.rindex("/") + 1] if "/" in path else ""
            self._add(
                _File(path, local, handle.size, prefix, handle, 0.0, handle.source_size, handle.digest.hex())
            )

    # -- accounting -----------------------------------------------------------------------

    @property
    def used(self) -> int:
        """Bytes taken: kept as files and candidates come and go, never summed."""

        return self._file_bytes + self.candidates.bytes + self.reserved

    def _local(self, path: str) -> str:
        """A local file's name: a fixed-length hash of its object's path, whatever
        the path's length (a partition is domain data); the file names its source."""

        return os.path.join(self.root, hashlib.sha256(path.encode()).hexdigest()[:32] + ".kxl")

    def _active(self, now: float) -> set[str]:
        return {p for p, ix in self.indexes.items() if ix.admitted and now - ix.used <= self.window}

    def _add(self, f: _File) -> None:
        self.files[f.path] = f
        ix = self.indexes.setdefault(f.prefix, _Index())
        ix.files.add(f.path)
        ix.bytes += f.size
        self._file_bytes += f.size

    def _evict(self, need: int, keep: str | None = None) -> bool:
        """Make `need` bytes free: candidates past their budget, then retired
        files, then files of inactive or demoted indexes, least recently used.
        Never pinned files or an active index's."""

        now = self.clock()
        while self.candidates and self.candidates.bytes > self.candidate_budget:
            self.candidates.popitem(last=False)
        if self.used + need <= self.disk:
            return True
        active = self._active(now) - ({keep} if keep else set())
        order = sorted(
            (
                f
                for f in self.files.values()
                if f.pins == 0 and (f.retired or (f.prefix not in active and f.prefix != keep))
            ),
            key=lambda f: (not f.retired, f.used),
        )
        for f in order:
            self._drop(f.path)
            if self.used + need <= self.disk:
                return True
        while self.candidates and self.used + need > self.disk:
            self.candidates.popitem(last=False)
        return self.used + need <= self.disk

    def _drop(self, path: str) -> None:
        f = self.files.pop(path, None)
        if f is None:
            return
        self._file_bytes -= f.size
        ix = self.indexes.get(f.prefix)
        if ix is not None:
            ix.files.discard(path)
            ix.bytes -= f.size
        with contextlib.suppress(OSError):
            os.unlink(f.local)

    def _estimate(self, path: str, f: FileInfo) -> int:
        """The local size of `f`: as it built before, or from its compressed data."""

        built = self._observed.get((path, f.digest))
        return built if built is not None else int(GROWTH * max(0, f.size - f.tail)) + 4096

    def _built(self, path: str, f: FileInfo, size: int) -> None:
        self._observed[(path, f.digest)] = size
        self._observed.move_to_end((path, f.digest))
        while len(self._observed) > OBSERVED:
            self._observed.popitem(last=False)

    def _present(self, path: str, f: FileInfo) -> _File | None:
        """The cached copy of `f`, if it is that object; a copy that is not goes."""

        local = self.files.get(path)
        if local is None:
            return None
        if local.source_size == f.size and local.digest == f.digest:
            return local
        if local.pins == 0:
            self._drop(path)
        return None

    # -- admission --------------------------------------------------------------------------

    def admit(self, state: IndexState) -> bool:
        """Whether this index may be cached: admitted before, or its snapshot plus
        one compaction's overlap (its largest level) fits beside the active indexes."""

        now = self.clock()
        ix = self.indexes.setdefault(state.prefix, _Index())
        ix.used = now
        if ix.admitted:
            return True
        # The active indexes' bytes, kept per index: one pass over indexes, none over files.
        others = sum(
            o.bytes
            for p, o in self.indexes.items()
            if o.admitted and now - o.used <= self.window and p != state.prefix
        )
        if self.need(state) + others + self.candidate_budget <= self.disk:
            ix.admitted = True
        return ix.admitted

    def need(self, state: IndexState) -> int:
        """Room an index needs: its snapshot and its logged deltas, and one
        compaction's overlap (its largest level)."""

        levels: dict[int, int] = {}
        for f in state.files:
            levels[f.level] = levels.get(f.level, 0) + self._estimate(state.path(f.name), f)
        held = {f.name for f in state.files}
        logged = sum(self._estimate(state.path(f.name), f) for f in _logged(state) if f.name not in held)
        return sum(levels.values()) + logged + max(levels.values(), default=0)

    def demote(self, prefix: str) -> None:
        ix = self.indexes.get(prefix)
        if ix is not None:
            ix.admitted = False

    def warm(self, state: IndexState) -> bool:
        return all(self._present(state.path(f.name), f) is not None for f in state.files)

    def retire(self, paths: list[str]) -> None:
        """A published compaction let go of these files: no new snapshot reads
        them. They go as soon as no reader holds them."""

        for path in paths:
            self._retired[path] = None
            while len(self._retired) > RETIRED:
                self._retired.popitem(last=False)
            f = self.files.get(path)
            if f is not None:
                f.retired = True
                if f.pins == 0:
                    self._drop(path)

    def held(self, state: IndexState) -> Pin:
        """Whatever of the snapshot's files and logged deltas the cache holds,
        pinned: a reader that takes what is local and does without the rest."""

        self.admit(state)  # a read: the index is active, and a recovered one is admitted again
        now, files = self.clock(), []
        for f in {**{f.name: f for f in _logged(state)}, **{f.name: f for f in state.files}}.values():
            local = self._present(state.path(f.name), f)
            if local is not None:
                local.pins += 1
                local.used = now
                files.append(local)
        return Pin(self, files, [])

    def pin(self, state: IndexState) -> Pin | None:
        """The snapshot's local files, newest run first, pinned; None unless warm."""

        if not self.warm(state):
            return None
        self.admit(state)  # a read: the index is active, and a recovered one is admitted again
        now = self.clock()
        held, runs = [], []
        for level in state.newest_first():
            run = []
            for f in level:
                local = self.files[state.path(f.name)]
                local.pins += 1
                local.used = now
                held.append(local)
                run.append(local.handle)
            runs.append(run)
        return Pin(self, held, runs)

    # -- filling ------------------------------------------------------------------------

    async def fill(self, io: ObjectIO, state: IndexState) -> bool:
        """Fetch every missing file of an admitted index — its snapshot, and
        the deltas its log holds — once each however many readers ask; True
        when the snapshot is warm."""

        if not self.admit(state):
            return False
        wanted = {**{f.name: f for f in _logged(state)}, **{f.name: f for f in state.files}}
        missing = [f for f in wanted.values() if self._present(state.path(f.name), f) is None]
        await asyncio.gather(*(self._fill_one(io, state.prefix, state.path(f.name), f) for f in missing))
        return self.warm(state)

    def _fill_one(self, io: ObjectIO, prefix: str, path: str, f: FileInfo):
        fut = self._fills.get(path)
        if fut is None:
            fut = self._fills[path] = asyncio.ensure_future(self._do_fill(io, prefix, path, f))
            fut.add_done_callback(lambda _f: self._fills.pop(path, None))
        return fut

    async def _do_fill(self, io: ObjectIO, prefix: str, path: str, f: FileInfo) -> bool:
        async with self._builds:  # the fetched bytes and the build: bounded together
            need = self._estimate(path, f)
            if not self._evict(need, keep=prefix):
                self.demote(prefix)
                return False
            self.reserved += need
            try:
                data = await io.read_whole(path, f.size)
                verify(f, path, data)
                return await self._put(prefix, path, f, data, need)
            finally:
                self.reserved -= need

    async def _put(self, prefix: str, path: str, f: FileInfo, data: bytes, reserved: int) -> bool:
        """Build and open the local form of `data`, a verified copy of `f`,
        written as it is built into the `reserved` bytes held: a build that
        reaches them stops, and runs again in more room if room can be had —
        else nothing is kept and the index is demoted. Memory is a block, not
        the file."""

        local = self._local(path)
        tmp = f"{local}.{uuid.uuid4().hex}.tmp"
        digest_ = bytes.fromhex(f.digest)
        extra = 0
        try:
            while True:  # written as built, into the room held: past it, more room or none
                ceiling = reserved + extra
                try:
                    size = await in_thread(lambda c=ceiling: _native.build_local(data, path, digest_, tmp, c))
                    break
                except _native.LimitError:
                    self._built(path, f, ceiling + 1)  # at least this: what the next admission counts
                    if not self._evict(ceiling, keep=prefix):
                        self.demote(prefix)
                        return False
                    self.reserved += ceiling
                    extra += ceiling
                except ValueError as e:
                    raise Corrupt(f"{path}: {e}") from e
            self._built(path, f, size)
            handle = await in_thread(lambda: _native.LocalFile(tmp))
            # Published only if no other copy got there first and the file still matters.
            if path in self.files or path in self._retired:
                return path in self.files
            os.replace(tmp, local)  # the handle's open file is the same one, renamed
        finally:
            self.reserved -= extra
            with contextlib.suppress(OSError):
                os.unlink(tmp)
        self._add(_File(path, local, size, prefix, handle, self.clock(), f.size, f.digest))
        return True

    async def install(self, prefix: str, f: FileInfo, path: str, data: bytes) -> bool:
        """Write-through: a file the engine wrote or holds — a compaction output,
        a committed delta — into the cache of an admitted index, once verified
        against `f`. False when it could not reserve the room: the index is
        then demoted."""

        ix = self.indexes.get(prefix)
        if ix is None or not ix.admitted or self._present(path, f) is not None:
            return self._present(path, f) is not None
        verify(f, path, data)
        async with self._builds:
            need = self._estimate(path, f)
            if not self._evict(need, keep=prefix):
                self.demote(prefix)
                return False
            self.reserved += need
            try:
                return await self._put(prefix, path, f, data, need)
            finally:
                self.reserved -= need

    # -- candidates ---------------------------------------------------------------------------

    def offer(self, path: str, data: bytes) -> str:
        """A delta the resolver returned, kept until its attempt commits it; returns its digest."""

        dg = digest(data)
        self.candidates[(path, len(data), dg)] = _Candidate(path, len(data), dg, data, self.clock())
        self._evict(0)
        return dg

    async def committed(self, prefix: str, f: FileInfo, path: str) -> bool:
        """A committed delta: installed from its candidate when the bytes match
        (no GET), else left for a fill. True when it is now cached."""

        c = self.candidates.pop((path, f.size, f.digest), None)
        if c is None:
            return self._present(path, f) is not None
        return await self.install(prefix, f, path, c.data)

    def forget(self, path: str) -> None:
        """Drop candidates for `path` (an attempt that ended without committing)."""

        for key in [k for k in self.candidates if k[0] == path]:
            del self.candidates[key]

    def corrupt(self, path: str) -> None:
        """A local file failed validation: drop it; the next fill refetches it."""

        self._drop(path)
