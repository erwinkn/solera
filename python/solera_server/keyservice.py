"""The engine's key cache and resolver (docs/resolved-commits.md §4–§7), on a
thread of their own: nothing here runs on the engine's event loop.

- `resolve` answers a worker's request from the cache, or declines.
- `committed` keeps the cache warm with what a commit installed — a delta
  the resolver returned is a candidate already, installed without a GET —
  and keeps small deltas' entries in memory as summaries.
- `installed` takes a file the engine wrote (a compaction output).
- `inline` merges summaries into the first page of a pending window, for
  prepare: memory only, never waiting.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from collections import OrderedDict

from solera.keys import decode_block, parse_index
from solera.keys.cache import EngineCache
from solera.keys.index import FileInfo, Options
from solera.keys.io import ObjectIO
from solera.keys.resolver import Limits, Prepared, Resolver

log = logging.getLogger(__name__)

INLINE_MAX = 10_000  # entries of a delta kept as a summary
INLINE_BATCHES = 64  # summaries one inlined page may merge
INLINE_MERGE = 100_000  # entries one inlined page may merge
INLINE_BYTES = 2**20  # serialized page


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
        # (prefix, batch) -> (keys, versions, deleted, locators), sorted: read from any thread.
        self.summaries: OrderedDict[tuple, tuple] = OrderedDict()
        self._summary_size = 0
        self.loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stopped = False

    # -- the thread -----------------------------------------------------------------------

    def start(self) -> None:
        if self.loop is not None:
            return
        self._stopped = False
        ready = threading.Event()

        def run():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.io = ObjectIO(self.objects)
            self.cache = EngineCache(
                self.root, disk=self.disk, candidates=self.candidates, window=self.window
            )
            self.resolver = Resolver(self.cache, self.io, self.options, self.limits)
            ready.set()
            self.loop.run_forever()

        self._thread = threading.Thread(target=run, name="solera-keys", daemon=True)
        self._thread.start()
        ready.wait()

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
        """Started on first use; not again once stopped."""

        if self.loop is None and not self._stopped:
            self.start()
        return self.loop is not None

    def _submit(self, coro) -> concurrent.futures.Future:
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def _fire(self, make) -> None:
        if self._running():
            self._submit(make()).add_done_callback(_logged)

    # -- requests ---------------------------------------------------------------------------

    async def resolve(self, attempt: str, body: bytes, prepared, live) -> bytes | None:
        if not self._running():
            return None
        return await asyncio.wrap_future(self._submit(self.resolver.resolve(attempt, body, prepared, live)))

    async def direct(self, index, kind: str, run: bytes, generation: int, batch: int, path: str):
        """A resolve of a run against `index` as the engine holds it — a source
        commit's, in process (docs/resolved-commits.md §4): `(answer, delta)`."""

        if not self._running():
            return {"result": "declined", "reason": "busy"}, None
        p = Prepared("", batch, generation, index, batch - 1, True)
        return await asyncio.wrap_future(self._submit(self.resolver.compute(p, kind, run, path)))

    def committed(self, prefix: str, path, batch: int, files: list[FileInfo], keep_summary: bool) -> None:
        """A commit installed `files` (a batch's delta) into the index at `prefix`."""

        self._fire(lambda: self._committed(prefix, path, batch, files, keep_summary))

    def installed(self, prefix: str, f: FileInfo, path: str, data: bytes) -> None:
        self._fire(lambda: self.cache.install(prefix, f, path, data))

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
        for f in files:
            p = path(f.name)
            cand = self.cache.candidates.get((p, f.size, f.digest))
            data = cand.data if cand is not None else None
            if not await self.cache.committed(prefix, f, p) and admitted:
                # A delta the resolver did not produce: fetched once, while it is small.
                data = data or await self.io.read_whole(p, f.size)
                await self.cache.install(prefix, f, p, data)
            if small:
                parts.append(data or await self.io.read_whole(p, f.size))
        if small:
            self._summarize(prefix, batch, parts)

    def _summarize(self, prefix: str, batch: int, parts: list[bytes]) -> None:
        keys, versions, deleted, locators, size = [], [], bytearray(), [], 0
        for data in parts:
            idx = parse_index(data, len(data))
            for _, off, sz, _, _ in idx["blocks"]:
                k, v, d, loc, _ = decode_block(data[off : off + sz], idx["codec"])
                keys += k
                versions += v
                deleted += bytes(d)
                locators += loc
                size += sum(map(len, k)) + sum(map(len, v)) + 24 * len(k)
        self.summaries[(prefix, batch)] = (keys, versions, bytes(deleted), locators, size)
        self._summary_size += size
        while self._summary_size > self.summary_bytes and self.summaries:
            _, old = self.summaries.popitem(last=False)
            self._summary_size -= old[4]

    # -- inline pages (§7) -----------------------------------------------------------------

    def inline(self, prefix: str, lo: int, hi: int, after: str | None, limit: int) -> dict | None:
        """The first page of the pending window `[lo, hi]` past `after`, merged
        from summaries — newest batch winning, deletions kept — or None when a
        batch has none, or the merge or the page would be too big."""

        if hi < lo or hi - lo + 1 > INLINE_BATCHES:
            return None
        parts = [self.summaries.get((prefix, b)) for b in range(hi, lo - 1, -1)]  # newest first
        if any(p is None for p in parts) or sum(len(p[0]) for p in parts) > INLINE_MERGE:
            return None
        start = after.encode("utf-8", "surrogateescape") if after is not None else None
        seen: dict[bytes, tuple] = {}
        for keys, versions, deleted, locators, _ in parts:
            for k, v, d, loc in zip(keys, versions, deleted, locators, strict=True):
                if (start is None or k > start) and k not in seen:
                    seen[k] = (v, d, loc)
        order = sorted(seen)
        upserted, removed, size, last = {}, [], 0, None
        for k in order[:limit]:
            v, d, loc = seen[k]
            key = k.decode("utf-8", "surrogateescape")
            size += len(key) + 2 * len(v) + 24
            if size > INLINE_BYTES:
                if last is None:
                    return None  # the first entry alone is too big: no page that advances
                return {"upserted": upserted, "deleted": removed, "next": last}
            if d:
                removed.append(key)
            else:
                upserted[key] = [v.hex(), loc]
            last = key
        more = len(order) > limit
        return {"upserted": upserted, "deleted": removed, "next": last if more else None}


def _logged(fut: concurrent.futures.Future) -> None:
    if not fut.cancelled() and fut.exception() is not None:
        log.warning("key cache: %s", fut.exception())
