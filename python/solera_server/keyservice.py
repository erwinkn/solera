"""The engine's key cache and resolver (docs/resolved-commits.md §4–§7), on a
thread of their own: nothing here runs on the engine's event loop.

- `resolve` answers a worker's request from the cache, or declines.
- `committed` keeps the cache warm with what a commit installed — a delta
  the resolver returned is a candidate already, installed without a GET.
- `installed` takes a file the engine wrote (a compaction output).
- `reads` answers an attempt's input reads at its `start` (§7): the
  worker's own read code over local copies, recorded.

Whatever here reads index files from the object store holds a reader pin
(`hold`) at the event counter it read the index at, until its reads are
done: collection (`floor`) deletes nothing a pinned reader may still read.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import itertools
import json
import logging
import math
import os
import tempfile
import threading
from urllib.parse import unquote, urlsplit

from solera.keys import LocalError, SortedEntries
from solera.keys.cache import Corrupt, EngineCache
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO
from solera.keys.reads import Cold, Full, Reads
from solera.keys.resolver import Limits, Prepared, Resolver

log = logging.getLogger(__name__)

INSTALL_QUEUE = 128 * 2**20  # bytes of written files waiting to be installed: two full outputs
READS_MAX_ENTRIES = 1_000_000  # entries one start reply's reads may carry
READS_MAX_BYTES = 16 * 2**20  # ...and bytes, encoded
READS_TIMEOUT = 2.0  # seconds the engine spends on them before answering without


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
        options: Options | None = None,
        disk: int = 16 * 2**30,
        candidates: int = 2**30,
        window: float = 900.0,
        limits: Limits | None = None,
    ):
        self.objects, self.root, self.options = objects, root, options or Options()
        self.disk, self.candidates, self.window = disk, candidates, window
        self.limits = limits or Limits()
        self.loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stopped = False
        self._holds: dict[int, float] = {}  # token -> event counter: readers of index files
        self._hold_lock = threading.Lock()
        self._tokens = itertools.count()
        self._owners: set[asyncio.Task] = set()  # operations running on the loop
        self._installing = 0  # bytes of `installed` files waiting

    # -- reader pins ----------------------------------------------------------------------

    def hold(self, position: float) -> int:
        with self._hold_lock:
            token = next(self._tokens)
            self._holds[token] = position
            return token

    def release(self, token: int) -> None:
        with self._hold_lock:
            self._holds.pop(token, None)

    def floor(self) -> float:
        """The oldest position a reader here holds: for collection's `pin_floor`."""

        with self._hold_lock:
            return min(self._holds.values(), default=math.inf)

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
                self.cache = EngineCache(
                    self.root, disk=self.disk, candidates=self.candidates, window=self.window
                )
                self.resolver = Resolver(self.cache, self.io, self.options, self.limits, holds=self)
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
            owners = (
                set(self._owners)
                | set(self.resolver._fills)
                | set(self.resolver._inflight.values())
                | set(self.cache._fills.values())
            )
            for t in owners:
                t.cancel()
            await asyncio.gather(*owners, return_exceptions=True)
            await loop.shutdown_default_executor()

        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(drain(), loop))
        loop.call_soon_threadsafe(loop.stop)
        await asyncio.to_thread(self._thread.join)
        loop.close()

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
        """Runs `coro` as one of this service's operations: `stop` cancels and waits for it."""

        task = asyncio.current_task()
        self._owners.add(task)
        try:
            return await coro
        finally:
            self._owners.discard(task)

    def _fire(self, make) -> None:
        if self._running():
            self._submit(make()).add_done_callback(_logged)

    # -- requests ---------------------------------------------------------------------------

    async def resolve(self, attempt: str, body: bytes, prepared, live, position: float) -> bytes | None:
        """A worker's request, its indexes read at `position` (held meanwhile)."""

        if not self._running():
            return None
        token = self.hold(position)
        try:
            return await asyncio.wrap_future(
                self._submit(self.resolver.resolve(attempt, body, prepared, live))
            )
        finally:
            self.release(token)

    async def direct(
        self,
        index,
        kind: str,
        run: SortedEntries,
        generation: int,
        commit_number: int,
        path: str,
        position: float,
    ):
        """A resolve of a sorted run against `index` as the engine holds it at
        `position` — a source commit's, in process (docs/resolved-commits.md
        §4): `(answer, delta)`, under the resolver's limits."""

        if not self._running():
            return {"result": "declined", "reason": "busy"}, None
        p = Prepared("", commit_number, generation, index, commit_number - 1, True, position)
        token = self.hold(position)
        try:
            return await asyncio.wrap_future(self._submit(self.resolver.compute(p, kind, run, path)))
        finally:
            self.release(token)

    def committed(self, prefix: str, path, files: list[FileInfo], position: float) -> None:
        """A commit at `position` installed `files` (a commit's delta) into the index at `prefix`."""

        if not self._running():
            return
        token = self.hold(position)
        fut = self._submit(self._committed(prefix, path, files))
        fut.add_done_callback(_logged)
        fut.add_done_callback(lambda _f: self.release(token))

    def pinned(self, state: IndexState):
        """The engine cache's copies of `state`'s files, pinned — a `Pin`,
        its `handles` by path — when it holds them all, else None. From any
        thread but this service's; `unpin` when done."""

        if not self._running():
            return None
        return self._submit(self._pin(state)).result()

    async def _pin(self, state: IndexState):
        return self.cache.pin(state)

    def corrupt(self, path: str) -> None:
        """A local file failed a check while read: it goes."""

        self._fire(lambda: self._corrupt(path))

    async def _corrupt(self, path: str) -> None:
        self.cache.corrupt(path)

    def unpin(self, pin) -> None:
        self._fire(lambda: self._unpin(pin))

    async def _unpin(self, pin) -> None:
        pin.__exit__(None, None, None)

    async def reads(self, spec: dict, position: float) -> dict | None:
        """The input reads of the attempt `spec` describes, answered from
        local copies (docs/resolved-commits.md §7): a `Reads` record as JSON,
        or None when there is nothing to answer or no time to. An input the
        worker loads whole names its pinned indexes (`index`, or `indexes` per
        fan-in member): those are paged, as its whole read pages them."""

        if not self._running():
            return None
        token = self.hold(position)
        try:
            fut = self._submit(self._admitted(spec, position))
            try:
                return await asyncio.wait_for(asyncio.wrap_future(fut), READS_TIMEOUT)
            except TimeoutError:
                fut.cancel()
            except Exception as e:  # the worker reads the store instead
                log.warning("key cache reads: %s", e)
            return None
        finally:
            self.release(token)

    async def _admitted(self, spec: dict, position: float) -> dict | None:
        """`_reads` under the resolver's admission (`Resolver.admitted`), holding
        the reply's bound of its queue until its work — native threads too —
        has ended, whenever its start stopped waiting."""

        return await self.resolver.admitted(READS_MAX_BYTES, lambda: self._reads(spec, position))

    async def _reads(self, spec: dict, position: float) -> dict | None:
        from solera_worker import each
        from solera_worker.worker import REPAIR_PAGE

        states = {}  # what the reads may touch: inputs, failed keys, outputs (reconcile batches)
        for pin in (spec.get("inputs") or {}).values():
            for js in (
                pin.get("index"),
                (pin.get("each") or {}).get("failures"),
                *(pin.get("indexes") or {}).values(),
            ):
                if js:
                    states[json.dumps(js, sort_keys=True)] = IndexState.from_json(js)
        for info in (spec.get("outputs") or {}).values():
            if info.get("index"):
                states[json.dumps(info["index"], sort_keys=True)] = IndexState.from_json(info["index"])
        pins = [self.cache.held(st) for st in states.values()]
        reads = Reads(recording=True, max_entries=READS_MAX_ENTRIES, max_bytes=READS_MAX_BYTES)
        io = ObjectIO(None, local={p: h for pin in pins for p, h in pin.handles.items()}, served=reads)
        cold = False
        try:
            for pin in (spec.get("inputs") or {}).values():
                try:
                    if "each" in pin:
                        await each.read_each_batch(spec, pin, io)
                    elif "batch" in pin:
                        await each.read_batch(pin, io)
                    elif pin.get("load") == "data":  # a whole read: its locators, a page at a time
                        for js in [pin["index"]] if pin.get("index") else (pin.get("indexes") or {}).values():
                            index, after = KeyIndex(io, None, IndexState.from_json(js)), None
                            while True:
                                *_, after = await index.page(after, REPAIR_PAGE)
                                if after is None:
                                    break
                except Cold:
                    cold = True  # this input's later reads go to the store; fetch it for the next
                except LocalError as e:
                    self.cache.corrupt(e.path)  # as cold: dropped, fetched again, what was recorded kept
                    cold = True
        except Full:
            pass  # the rest go to the store
        finally:
            for pin in pins:
                pin.__exit__(None, None, None)
        if cold:
            for st in states.values():
                if st.files:
                    self.resolver._background_fill(st, position)
        return reads.to_json() if len(reads) else None

    def installed(self, prefix: str, f: FileInfo, path: str, data: bytes) -> None:
        """A file the engine wrote (a compaction output), for the cache. Waiting,
        it holds its bytes: past `INSTALL_QUEUE` waiting it is skipped, and its
        index demoted — a fill fetches what is missing once there is room."""

        if not self._running():
            return
        with self._hold_lock:
            take = self._installing + len(data) <= INSTALL_QUEUE
            if take:
                self._installing += len(data)
        if not take:
            self._fire(lambda: self._demote(prefix))
            return
        fut = self._submit(self.cache.install(prefix, f, path, data))
        fut.add_done_callback(_logged)
        fut.add_done_callback(lambda _f: self._installed(len(data)))

    def _installed(self, n: int) -> None:
        with self._hold_lock:
            self._installing -= n

    async def _demote(self, prefix: str) -> None:
        self.cache.demote(prefix)

    def retired(self, paths: list[str]) -> None:
        """A published compaction let go of these files."""

        self._fire(lambda: self._retire(paths))

    async def _retire(self, paths: list[str]) -> None:
        self.cache.retire(paths)

    def ended(self, attempt: str) -> None:
        """An attempt ended: candidates it did not commit go."""

        self._fire(lambda: self._forget(attempt))

    async def _forget(self, attempt: str) -> None:
        for key in [k for k in self.cache.candidates if f"-{attempt}." in k[0]]:
            self.cache.candidates.pop(key, None)

    async def _committed(self, prefix: str, path, files: list[FileInfo]):
        if not (prefix in self.cache.indexes and self.cache.indexes[prefix].admitted):
            for f in files:  # nothing to install: a candidate it made goes
                self.cache.candidates.pop((path(f.name), f.size, f.digest), None)
            return
        try:
            for f in files:
                p = path(f.name)
                if not await self.cache.committed(prefix, f, p):
                    # A delta the resolver did not produce: fetched once, while it is small.
                    await self.cache.install(
                        prefix, f, p, await self.io.read_whole(p, f.size)
                    )  # verified there
        except (Corrupt, ValueError) as e:
            log.warning("key cache: %s", e)


def _logged(fut: concurrent.futures.Future) -> None:
    if not fut.cancelled() and fut.exception() is not None:
        log.warning("key cache: %s", fut.exception())
