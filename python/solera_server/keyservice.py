"""The engine's key cache and resolver (docs/resolved-commits.md §4–§5), on a
thread of their own: nothing here runs on the engine's event loop.

- `resolve` answers a worker's request from the cache, or declines.
- `direct` resolves a source commit in process.
- `committed` keeps the cache warm with what a commit installed — a delta
  the resolver returned is a candidate already, installed without a GET.
- `retired` evicts files a published merge let go of; merges install their
  own outputs (`LayerIndex` writes through the cache).

Whatever here reads index files from the object store holds a reader pin
(`pin`) at the event counter it read the index at, until its reads are
done: collection (`floor`) deletes nothing a pinned reader may still read.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import itertools
import logging
import math
import os
import tempfile
import threading
from urllib.parse import unquote, urlsplit

from solera.keys import SortedEntries
from solera.keys.io import ObjectIO
from solera.keys.layer_cache import LayerCache
from solera.keys.layers import FileRef, LayerState
from solera.keys.resolver import Limits, Prepared, Resolver
from solera.tasks import Tasks

log = logging.getLogger(__name__)


def cache_root(objects_url: str) -> str:
    """Where the engine keeps its key cache by default: beside `file://`
    state (`{its directory}/.key-cache/{its name}`), else in a temporary
    directory named after the store."""

    u = urlsplit(objects_url)
    if u.scheme == "file":
        path = unquote(u.path).rstrip("/")
        return os.path.join(os.path.dirname(path), ".key-cache", os.path.basename(path))
    name = hashlib.sha256(objects_url.encode()).hexdigest()[:16]
    return os.path.join(tempfile.gettempdir(), "solera-key-cache", name)


class KeyService:
    def __init__(
        self,
        objects,
        root: str,
        *,
        disk: int = 16 * 2**30,
        memory: int = 2**30,
        limits: Limits | None = None,
    ):
        self.objects, self.root = objects, root
        self.disk, self.memory = disk, memory
        self.limits = limits or Limits()
        self.loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stopped = False
        self._pins: dict[int, float] = {}  # token -> event counter: readers of index files
        self._lock = threading.Lock()
        self._tokens = itertools.count()
        self.tasks = Tasks("key service")  # operations running on the loop

    # -- reader pins ----------------------------------------------------------------------

    def pin(self, at: float) -> int:
        with self._lock:
            token = next(self._tokens)
            self._pins[token] = at
            return token

    def unpin(self, token: int) -> None:
        with self._lock:
            self._pins.pop(token, None)

    def floor(self) -> float:
        """The oldest event counter a reader here holds: for collection's `pin_floor`."""

        with self._lock:
            return min(self._pins.values(), default=math.inf)

    # -- the thread -----------------------------------------------------------------------

    def start(self) -> None:
        """Start the thread; raises what its setup raised (a cache directory
        that cannot be made), and is not tried again: every resolve then
        declines, and workers resolve themselves."""

        if self.loop is not None or self._stopped:
            return
        started: concurrent.futures.Future = concurrent.futures.Future()

        def run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                self.io = ObjectIO(self.objects)
                self.cache = LayerCache(self.root, disk=self.disk, memory=self.memory)
                self.resolver = Resolver(self.cache, self.io, self.limits, pins=self)
            except BaseException as e:
                loop.close()
                started.set_exception(e)
                return
            started.set_result(loop)
            loop.run_forever()

        thread = threading.Thread(target=run, name="solera-keys", daemon=True)
        thread.start()
        try:
            loop = started.result()
        except BaseException:
            self._stopped = True
            raise
        self._thread, self.loop = thread, loop

    async def stop(self) -> None:
        """Cancel what this service runs — requests, fills, computations — and
        wait for each to end: one waiting on a thread (a build, a resolve)
        ends once its thread does, its room and temporary files released
        then, not before. Then the loop's threads, then the loop."""

        self._stopped = True
        if self.loop is None:
            return
        loop, self.loop = self.loop, None

        async def drain():
            await self._close_tasks()
            await loop.shutdown_default_executor()

        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(drain(), loop))
        loop.call_soon_threadsafe(loop.stop)
        await asyncio.to_thread(self._thread.join)
        loop.close()

    async def _close_tasks(self) -> None:
        """Cancel and wait for everything running on the loop: operations,
        then the resolver's fills and computes."""

        for tasks in (self.tasks, self.resolver._fills, self.resolver._inflight):
            await tasks.close()

    def _running(self) -> bool:
        """Started on first use; not again once stopped, or once it failed to start."""

        if self.loop is None and not self._stopped:
            try:
                self.start()
            except Exception as e:
                log.warning("key cache: cannot start: %s", e)
        return self.loop is not None

    def _submit(self, coro) -> concurrent.futures.Future:
        return asyncio.run_coroutine_threadsafe(self._own(coro), self.loop)

    async def _own(self, coro):
        """Runs `coro` as one of this service's operations: `stop` cancels and waits for it.
        Its failure reaches the submitter (`_logged` for one fired and forgotten)."""

        return await self.tasks.spawn(coro, awaited=True)

    def _fire(self, make) -> None:
        if self._running():
            self._submit(make()).add_done_callback(_logged)

    # -- requests ---------------------------------------------------------------------------

    async def resolve(self, attempt: str, body: bytes, prepared, live, at: float) -> bytes | None:
        """A worker's request, its indexes read at the event counter `at` (held meanwhile)."""

        if not self._running():
            return None
        token = self.pin(at)
        try:
            return await asyncio.wrap_future(
                self._submit(self.resolver.resolve(attempt, body, prepared, live))
            )
        finally:
            self.unpin(token)

    async def direct(
        self,
        index: LayerState,
        kind: str,
        run: SortedEntries,
        generation: int,
        commit_number: int,
        path: str,
        at: float,
    ):
        """A resolve of a sorted run against `index` as the engine holds it at
        `at` — a source commit's, in process (docs/resolved-commits.md
        §4): `(answer, delta)`, under the resolver's limits."""

        if not self._running():
            return {"result": "declined", "reason": "busy"}, None
        p = Prepared("", commit_number, generation, index, commit_number - 1, True, at, replaced=False)
        token = self.pin(at)
        try:
            return await asyncio.wrap_future(self._submit(self.resolver.compute(p, kind, run, path)))
        finally:
            self.unpin(token)

    def committed(self, prefix: str, files: list[FileRef], at: float) -> None:
        """A commit at the event counter `at` installed `files` (a commit's
        delta) into the index at `prefix`: cached when the cache holds that
        index already — from the resolver's candidate, else fetched once."""

        if not self._running():
            return
        token = self.pin(at)
        fut = self._submit(self._committed(prefix, files))
        fut.add_done_callback(_logged)
        fut.add_done_callback(lambda _f: self.unpin(token))

    async def _committed(self, prefix: str, files: list[FileRef]) -> None:
        held = self.cache.holds(prefix)
        for f in files:
            path = f"{prefix}{f.name}"
            if self.cache.committed(path, f.size, prefix) or not held:
                continue  # installed from its candidate, or an index not worth a GET
            data = await self.io.read_whole(path, f.size)
            if len(data) == f.size:
                self.cache.install(path, data, prefix)

    def retired(self, paths: list[str]) -> None:
        """A published merge, or collection, let go of these files."""

        self._fire(lambda: self._retire(paths))

    async def _retire(self, paths: list[str]) -> None:
        self.cache.retire(paths)

    def ended(self, attempt: str) -> None:
        """An attempt ended: candidates it did not commit go."""

        self._fire(lambda: self._forget(attempt))

    async def _forget(self, attempt: str) -> None:
        self.cache.forget(attempt)


def _logged(fut: concurrent.futures.Future) -> None:
    if not fut.cancelled() and fut.exception() is not None:
        log.warning("key cache: %s", fut.exception())
