"""The engine's cache of index files (docs/resolved-commits.md §5).

Index files never change once written, so a cached copy is never stale,
only evicted. Each file is kept on local disk in its local form
(`solera._native.build_local`: blocks decompressed, with restart points, a
checksummed directory), opened with its directory in memory. An index is
**warm** when every file of a snapshot is present; resolves only ever read
warm snapshots, and pin the files they read.

Budgets:
- disk: local files, candidates, temporary files and reservations. Every
  operation that adds bytes reserves them first; a fill or an install that
  cannot reserve, even after eviction, is not done, and an index whose
  snapshot no longer fits is demoted (its files become evictable).
- candidates: deltas the resolver returned, kept until their attempt
  commits them (then installed without a GET) or ends; their own budget,
  evicted oldest first.

Admission is by index, with hysteresis: an index is admitted when its
snapshot, plus a compaction's overlap, fits beside the indexes active in
the last `window` seconds; eviction takes files of inactive or demoted
indexes, never pinned files or an active index's.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from urllib.parse import quote

from .. import _native
from .index import FileInfo, IndexState, digest
from .io import ObjectIO

GROWTH = 2.0  # local bytes per compressed data byte, reserved before a fill (released to the real size after)


@dataclass
class _File:
    path: str  # the object's path: the cache key
    local: str
    size: int  # bytes on disk
    prefix: str  # the index it belongs to
    handle: object  # solera._native.LocalFile
    used: float
    pins: int = 0


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


class Corrupt(Exception):
    """A local file and its source both failed validation."""


class Pin:
    """Files held for one reader; released on exit."""

    def __init__(self, cache: EngineCache, files: list[_File], runs: list[list[object]]):
        self.cache, self.files, self.runs = cache, files, runs

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        for f in self.files:
            f.pins -= 1
        self.files = []


class EngineCache:
    def __init__(
        self,
        root: str,
        *,
        disk: int = 16 * 2**30,
        candidates: int = 2**30,
        window: float = 900.0,
        clock=time.monotonic,
    ):
        self.root = root
        self.disk, self.candidate_budget, self.window, self.clock = disk, candidates, window, clock
        os.makedirs(root, exist_ok=True)
        self.files: dict[str, _File] = {}
        self.indexes: dict[str, _Index] = {}
        self.candidates: OrderedDict[tuple, _Candidate] = OrderedDict()
        self.reserved = 0
        self._fills: dict[str, asyncio.Future] = {}
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
            self.files[path] = _File(path, local, handle.size, prefix, handle, 0.0)
            self.indexes.setdefault(prefix, _Index()).files.add(path)

    # -- accounting -----------------------------------------------------------------------

    @property
    def used(self) -> int:
        return (
            sum(f.size for f in self.files.values())
            + sum(c.size for c in self.candidates.values())
            + self.reserved
        )

    def _local(self, path: str) -> str:
        return os.path.join(self.root, quote(path, safe="") + ".kxl")

    def _active(self, now: float) -> set[str]:
        return {p for p, ix in self.indexes.items() if ix.admitted and now - ix.used <= self.window}

    def _evict(self, need: int, keep: str | None = None) -> bool:
        """Make `need` bytes free: candidates past their budget, then files of
        inactive or demoted indexes, least recently used, then unpinned files
        no longer in their index. Never pinned files or an active index's."""

        now = self.clock()
        while self.candidates and sum(c.size for c in self.candidates.values()) > self.candidate_budget:
            self.candidates.popitem(last=False)
        if self.used + need <= self.disk:
            return True
        active = self._active(now) - ({keep} if keep else set())
        order = sorted(
            (f for f in self.files.values() if f.pins == 0 and f.prefix not in active and f.prefix != keep),
            key=lambda f: f.used,
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
        ix = self.indexes.get(f.prefix)
        if ix is not None:
            ix.files.discard(path)
        with contextlib.suppress(OSError):
            os.unlink(f.local)

    @staticmethod
    def _estimate(f: FileInfo) -> int:
        return int(GROWTH * max(0, f.size - f.tail)) + 4096

    # -- admission --------------------------------------------------------------------------

    def admit(self, state: IndexState) -> bool:
        """Whether this index may be cached: admitted before, or its snapshot plus
        one compaction's overlap (its largest level) fits beside the active indexes."""

        now = self.clock()
        ix = self.indexes.setdefault(state.prefix, _Index())
        ix.used = now
        if ix.admitted:
            return True
        others = sum(
            f.size for f in self.files.values() if f.prefix in self._active(now) and f.prefix != state.prefix
        )
        if self.need(state) + others + self.candidate_budget <= self.disk:
            ix.admitted = True
        return ix.admitted

    def need(self, state: IndexState) -> int:
        """Room an index needs: its snapshot, and one compaction's overlap (its largest level)."""

        levels: dict[int, int] = {}
        for f in state.files:
            levels[f.level] = levels.get(f.level, 0) + self._estimate(f)
        return sum(levels.values()) + max(levels.values(), default=0)

    def demote(self, prefix: str) -> None:
        ix = self.indexes.get(prefix)
        if ix is not None:
            ix.admitted = False

    def warm(self, state: IndexState) -> bool:
        return all(state.path(f.name) in self.files for f in state.files)

    def pin(self, state: IndexState) -> Pin | None:
        """The snapshot's local files, newest run first, pinned; None unless warm."""

        if not self.warm(state):
            return None
        now = self.clock()
        ix = self.indexes.setdefault(state.prefix, _Index())
        ix.used = now
        current = {state.path(f.name) for f in state.files}
        for path in [p for p in ix.files if p not in current and self.files[p].pins == 0]:
            self._drop(path)  # superseded by a compaction: no snapshot reads it again
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
        """Fetch every missing file of an admitted index, once each however many
        readers ask; True when the snapshot is warm."""

        if not self.admit(state):
            return False
        missing = [f for f in state.files if state.path(f.name) not in self.files]
        await asyncio.gather(*(self._fill_one(io, state.prefix, state.path(f.name), f) for f in missing))
        return self.warm(state)

    def _fill_one(self, io: ObjectIO, prefix: str, path: str, f: FileInfo):
        fut = self._fills.get(path)
        if fut is None:
            fut = self._fills[path] = asyncio.ensure_future(self._do_fill(io, prefix, path, f))
            fut.add_done_callback(lambda _f: self._fills.pop(path, None))
        return fut

    async def _do_fill(self, io: ObjectIO, prefix: str, path: str, f: FileInfo) -> bool:
        need = self._estimate(f)
        if not self._evict(need, keep=prefix):
            self.demote(prefix)
            return False
        self.reserved += need
        try:
            data = await io.read_whole(path, f.size)
            if f.digest and digest(data) != f.digest:
                raise Corrupt(f"{path}: content does not match its digest")
            return await self._put(prefix, path, data, f.digest or digest(data))
        finally:
            self.reserved -= need

    async def _put(self, prefix: str, path: str, data: bytes, dg: str) -> bool:
        """Build and open the local form of `data` (a reservation is held)."""

        local = self._local(path)
        tmp = f"{local}.{os.getpid()}.tmp"

        def build():
            try:
                body = _native.build_local(data, path, bytes.fromhex(dg))
            except ValueError as e:
                raise Corrupt(f"{path}: {e}") from e
            with open(tmp, "wb") as out:
                out.write(body)
            os.replace(tmp, local)
            return _native.LocalFile(local), len(body)

        try:
            handle, size = await asyncio.to_thread(build)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
        now = self.clock()
        self.files[path] = _File(path, local, size, prefix, handle, now)
        self.indexes.setdefault(prefix, _Index()).files.add(path)
        return True

    async def install(self, prefix: str, f: FileInfo, path: str, data: bytes) -> bool:
        """Write-through: a file the engine just wrote (a compaction output), into
        the cache of an admitted index. False when it could not reserve the room:
        the index is then demoted."""

        ix = self.indexes.get(prefix)
        if ix is None or not ix.admitted or path in self.files:
            return path in self.files
        need = self._estimate(f)
        if not self._evict(need, keep=prefix):
            self.demote(prefix)
            return False
        self.reserved += need
        try:
            return await self._put(prefix, path, data, f.digest or digest(data))
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
            return path in self.files
        return await self.install(prefix, f, path, c.data)

    def forget(self, path: str) -> None:
        """Drop candidates for `path` (an attempt that ended without committing)."""

        for key in [k for k in self.candidates if k[0] == path]:
            del self.candidates[key]

    def corrupt(self, path: str) -> None:
        """A local file failed validation: drop it; the next fill refetches it."""

        self._drop(path)
