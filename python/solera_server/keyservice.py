"""The engine's key cache and resolver (docs/resolved-commits.md §4–§7), on a
thread of their own: nothing here runs on the engine's event loop.

- `resolve` answers a worker's request from the cache, or declines.
- `committed` keeps the cache warm with what a commit installed — a delta
  the resolver returned is a candidate already, installed without a GET —
  and keeps small deltas' entries in memory as summaries: native sorted
  runs, accounted at the bytes they hold.
- `installed` takes a file the engine wrote (a compaction output).
- `inline` merges summaries into the first page of a pending window, for
  prepare: memory only, never waiting, its work the page's, not the
  window's.
- `reads` answers an attempt's input reads at its `start` (§7.1): the
  worker's own read code over local copies, recorded.

Whatever here reads index files from the object store holds a reader pin
(`hold`) at the event position it read the index at, until its reads are
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
from collections import OrderedDict
from urllib.parse import unquote, urlsplit

from solera.keys import SortedRun
from solera.keys.cache import Corrupt, EngineCache, verify
from solera.keys.index import FileInfo, IndexState, KeyIndex, Options
from solera.keys.io import ObjectIO
from solera.keys.reads import Cold, Full, Reads
from solera.keys.resolver import Limits, Prepared, Resolver

log = logging.getLogger(__name__)

READS_MAX_ENTRIES = 1_000_000  # entries one start reply's reads may carry
READS_MAX_BYTES = 16 * 2**20  # ...and bytes, encoded
READS_TIMEOUT = 2.0  # seconds the engine spends on them before answering without
INLINE_MAX = 10_000  # entries of a delta kept as a summary
INLINE_BATCHES = 64  # summaries one inlined page may merge
INLINE_BYTES = 2**20  # serialized page


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
        summary_bytes: int = 256 * 2**20,
        limits: Limits | None = None,
    ):
        self.objects, self.root, self.options = objects, root, options or Options()
        self.disk, self.candidates, self.window = disk, candidates, window
        self.limits = limits or Limits()
        self.summary_bytes = summary_bytes
        # (prefix, batch) -> (its delta's files as sorted runs, their bytes): read from any thread.
        self.summaries: OrderedDict[tuple, tuple[list[SortedRun], int]] = OrderedDict()
        self._summary_size = 0
        self.loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stopped = False
        self._holds: dict[int, float] = {}  # token -> event position: readers of index files
        self._hold_lock = threading.Lock()
        self._tokens = itertools.count()

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
        self._stopped = True
        if self.loop is None:
            return
        loop, self.loop = self.loop, None

        async def drain():
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(drain(), loop))
        loop.call_soon_threadsafe(loop.stop)
        await asyncio.to_thread(self._thread.join, 5)

    def _running(self) -> bool:
        """Started on first use; not again once stopped, or once it failed to start."""

        if self.loop is None and not self._stopped:
            try:
                self.start()
            except Exception as e:
                log.warning("key cache: cannot start: %s", e)
        return self.loop is not None

    def _submit(self, coro) -> concurrent.futures.Future:
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

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
        self, index, kind: str, run: SortedRun, generation: int, batch: int, path: str, position: float
    ):
        """A resolve of a sorted run against `index` as the engine holds it at
        `position` — a source commit's, in process (docs/resolved-commits.md
        §4): `(answer, delta)`, under the resolver's limits."""

        if not self._running():
            return {"result": "declined", "reason": "busy"}, None
        p = Prepared("", batch, generation, index, batch - 1, True, position)
        token = self.hold(position)
        try:
            return await asyncio.wrap_future(self._submit(self.resolver.compute(p, kind, run, path)))
        finally:
            self.release(token)

    def committed(
        self, prefix: str, path, batch: int, files: list[FileInfo], keep_summary: bool, position: float
    ) -> None:
        """A commit at `position` installed `files` (a batch's delta) into the index at `prefix`."""

        if not self._running():
            return
        token = self.hold(position)
        fut = self._submit(self._committed(prefix, path, batch, files, keep_summary))
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

    async def reads(self, spec: dict, whole: set[str], position: float) -> dict | None:
        """The input reads of the attempt `spec` describes, answered from
        local copies (docs/resolved-commits.md §7.1): a `Reads` record as JSON,
        or None when there is nothing to answer or no time to. `whole`: the
        inputs read whole from an immutable store, paged by their locators."""

        if not self._running():
            return None
        token = self.hold(position)
        try:
            fut = self._submit(self._reads(spec, whole, position))
            try:
                return await asyncio.wait_for(asyncio.wrap_future(fut), READS_TIMEOUT)
            except TimeoutError:
                fut.cancel()
            except Exception as e:  # the worker reads the store instead
                log.warning("key cache reads: %s", e)
            return None
        finally:
            self.release(token)

    async def _reads(self, spec: dict, whole: set[str], position: float) -> dict | None:
        from solera_worker import each
        from solera_worker.worker import REPAIR_PAGE

        states = {}  # what the reads may touch: inputs, failure indexes, outputs (reconcile pages)
        for pin in (spec.get("inputs") or {}).values():
            for js in (pin.get("index"), (pin.get("each") or {}).get("failures")):
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
            for param, pin in (spec.get("inputs") or {}).items():
                try:
                    if "each" in pin:
                        await each.read_page(spec, pin, io)
                    elif "changes" in pin:
                        await each.read_window(pin, io)
                    elif param in whole and pin.get("index"):
                        index, after = KeyIndex(io, None, IndexState.from_json(pin["index"])), None
                        while True:
                            *_, after = await index.page(after, REPAIR_PAGE)
                            if after is None:
                                break
                except Cold:
                    cold = True  # this input's later reads go to the store; fetch it for the next
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
        self._fire(lambda: self.cache.install(prefix, f, path, data))

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

    async def _committed(self, prefix: str, path, batch: int, files: list[FileInfo], keep_summary: bool):
        small = keep_summary and sum(f.entries for f in files) <= INLINE_MAX
        admitted = prefix in self.cache.indexes and self.cache.indexes[prefix].admitted
        parts = []
        try:
            for f in files:
                p = path(f.name)
                cand = self.cache.candidates.get((p, f.size, f.digest))
                data = cand.data if cand is not None else None
                if not await self.cache.committed(prefix, f, p) and admitted:
                    # A delta the resolver did not produce: fetched once, while it is small.
                    data = data or await self.io.read_whole(p, f.size)
                    await self.cache.install(prefix, f, p, data)  # verified there
                if small:
                    data = data or await self.io.read_whole(p, f.size)
                    verify(f, p, data)  # a summary says what the committed file holds, or nothing
                    parts.append(data)
            if small:
                self._summarize(prefix, batch, parts)
        except (Corrupt, ValueError) as e:
            log.warning("key cache: %s", e)

    def _summarize(self, prefix: str, batch: int, parts: list[bytes]) -> None:
        runs = [SortedRun.decode(data) for data in parts]
        size = sum(r.nbytes for r in runs)
        old = self.summaries.pop((prefix, batch), None)
        if old is not None:
            self._summary_size -= old[1]
        self.summaries[(prefix, batch)] = (runs, size)
        self._summary_size += size
        while self._summary_size > self.summary_bytes and self.summaries:
            _, old = self.summaries.popitem(last=False)
            self._summary_size -= old[1]

    # -- inline pages (§7) -----------------------------------------------------------------

    def inline(self, prefix: str, lo: int, hi: int, after: str | None, limit: int) -> dict | None:
        """The first page of the pending window `[lo, hi]` past `after`, merged
        from summaries — newest batch winning, deletions kept — or None when a
        batch has none, or the page would be too big."""

        if hi < lo or hi - lo + 1 > INLINE_BATCHES:
            return None
        parts = [self.summaries.get((prefix, b)) for b in range(hi, lo - 1, -1)]  # newest first
        if any(p is None for p in parts):
            return None
        start = after.encode("utf-8", "surrogateescape") if after is not None else None
        # A batch's files never overlap: each is a run of its own, at its batch's rank.
        runs = [r for p in parts for r in p[0]]
        keys, versions, deleted, locators, more = SortedRun.merge(runs, start, limit)
        # The page's size as the spec serializes it (`json.dumps`, its default separators):
        # with `next` null; a cursor replaces that with a key.
        upserted, removed, size, last = {}, [], len(json.dumps(inline_page({}, [], None))), None
        for k, v, d, loc in zip(keys, versions, deleted, locators, strict=True):
            key = k.decode("utf-8", "surrogateescape")
            entry = len(json.dumps(key) if d else json.dumps({key: [v.hex(), loc]})[1:-1]) + 2
            if size + entry + len(json.dumps(key)) > INLINE_BYTES:  # this key may be the cursor
                if last is None:
                    return None  # the first entry alone is too big: no page that advances
                return inline_page(upserted, removed, last)
            size += entry
            if d:
                removed.append(key)
            else:
                upserted[key] = [v.hex(), loc]
            last = key
        return inline_page(upserted, removed, last if more else None)


def inline_page(upserted: dict, deleted: list, nxt) -> dict:
    return {"upserted": upserted, "deleted": deleted, "next": nxt}


def _logged(fut: concurrent.futures.Future) -> None:
    if not fut.cancelled() and fut.exception() is not None:
        log.warning("key cache: %s", fut.exception())
