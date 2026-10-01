"""Object I/O for the key index: bounded concurrency, request metrics, an
optional local disk cache, and optional injected latency (for benchmarks).

Index files never change once written, so the disk cache needs no
invalidation: a cached copy is always current.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import threading
from collections import OrderedDict
from dataclasses import dataclass
from urllib.parse import quote

import obstore

from ..objects import create

RANGE = 16 * 2**20  # large reads are split into ranges of this size, fetched in parallel


@dataclass
class Metrics:
    gets: int = 0
    puts: int = 0
    deletes: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    cache_hits: int = 0

    def snapshot(self) -> dict:
        return dict(self.__dict__)

    def reset(self) -> None:
        for name in self.__dict__:
            setattr(self, name, 0)


class DiskCache:
    """A bounded, least-recently-used cache of whole immutable objects on local disk."""

    def __init__(self, path: str | None = None, max_bytes: int = 8 * 2**30):
        self.path = path or os.path.join(tempfile.gettempdir(), "solera-key-cache")
        os.makedirs(self.path, exist_ok=True)
        self.max_bytes = max_bytes
        self._lock = threading.Lock()
        entries = []
        for name in os.listdir(self.path):
            full = os.path.join(self.path, name)
            if name.endswith(".part"):
                os.unlink(full)
                continue
            st = os.stat(full)
            entries.append((st.st_mtime, name, st.st_size))
        self._lru: OrderedDict[str, int] = OrderedDict((n, s) for _, n, s in sorted(entries))
        self._size = sum(self._lru.values())

    def _file(self, key: str) -> str:
        return os.path.join(self.path, quote(key, safe=""))

    def get(self, key: str) -> str | None:
        name = quote(key, safe="")
        with self._lock:
            if name not in self._lru:
                return None
            self._lru.move_to_end(name)
        return self._file(key)

    def forget(self, key: str) -> None:
        name = quote(key, safe="")
        with self._lock:
            self._size -= self._lru.pop(name, 0)

    def put(self, key: str, data: bytes) -> str:
        if len(data) > self.max_bytes:
            raise ValueError("object larger than the cache")
        name = quote(key, safe="")
        full = self._file(key)
        part = f"{full}.{os.getpid()}.{threading.get_ident()}.part"
        with open(part, "wb") as f:
            f.write(data)
        os.replace(part, full)
        with self._lock:
            self._size -= self._lru.pop(name, 0)
            self._lru[name] = len(data)
            self._size += len(data)
            while self._size > self.max_bytes and len(self._lru) > 1:
                old, size = self._lru.popitem(last=False)
                self._size -= size
                try:
                    os.unlink(os.path.join(self.path, old))
                except FileNotFoundError:
                    pass
        return full


class ObjectIO:
    """Reads and writes objects with at most `concurrency` requests in flight.

    With a `cache`, the first read of an object downloads it whole and every
    later read — of any range — is served from local disk. `latency` adds a
    fixed delay per request and `bandwidth` a per-connection transfer rate,
    to model a remote object store in benchmarks."""

    def __init__(
        self,
        store,
        *,
        concurrency: int = 64,
        cache: DiskCache | None = None,
        latency: float = 0.0,
        bandwidth: float | None = None,
        metrics: Metrics | None = None,
    ):
        self.store = store
        self.cache = cache
        self.latency = latency
        self.bandwidth = bandwidth
        self.metrics = metrics or Metrics()
        self._sem = asyncio.Semaphore(concurrency)
        self._inflight: dict[str, asyncio.Future] = {}

    async def _delay(self, nbytes: int) -> None:
        wait = self.latency + (nbytes / self.bandwidth if self.bandwidth else 0.0)
        if wait:
            await asyncio.sleep(wait)

    async def _get(self, path: str, start: int, end: int) -> bytes:
        async with self._sem:
            data = await obstore.get_range_async(self.store, path, start=start, end=end)
            data = bytes(data)
            self.metrics.gets += 1
            self.metrics.bytes_in += len(data)
            await self._delay(len(data))
            return data

    async def _get_whole(self, path: str, size: int) -> bytes:
        parts = await asyncio.gather(
            *(self._get(path, s, min(size, s + RANGE)) for s in range(0, size, RANGE))
        )
        return b"".join(parts)

    async def _cached(self, path: str, size: int) -> str:
        """Local path of the whole object, downloading it once if needed."""

        local = self.cache.get(path)
        if local is not None:
            self.metrics.cache_hits += 1
            return local
        pending = self._inflight.get(path)
        if pending is None:
            pending = asyncio.ensure_future(self._get_whole(path, size))
            self._inflight[path] = pending
            try:
                data = await pending
                return self.cache.put(path, data)
            finally:
                self._inflight.pop(path, None)
        await pending
        return self.cache.get(path) or await self._cached(path, size)

    async def read(self, path: str, start: int, end: int, size: int) -> bytes:
        """Bytes `[start, end)` of an object of `size` bytes."""

        if self.cache is not None:
            for _ in range(2):
                local = await self._cached(path, size)
                try:
                    with open(local, "rb") as f:
                        f.seek(start)
                        return f.read(end - start)
                except FileNotFoundError:
                    self.cache.forget(path)  # evicted by another process sharing the cache
        if end - start > RANGE:
            parts = await asyncio.gather(
                *(self._get(path, s, min(end, s + RANGE)) for s in range(start, end, RANGE))
            )
            return b"".join(parts)
        return await self._get(path, start, end)

    async def read_whole(self, path: str, size: int) -> bytes:
        return await self.read(path, 0, size, size)

    async def write(self, path: str, data: bytes) -> None:
        async with self._sem:
            await create(self.store, path, data)
            self.metrics.puts += 1
            self.metrics.bytes_out += len(data)
            await self._delay(len(data))
        if self.cache is not None:
            self.cache.put(path, data)

    async def delete(self, paths: list[str]) -> None:
        if paths:
            async with self._sem:
                await obstore.delete_async(self.store, paths)
                self.metrics.deletes += len(paths)


_caches: dict[str, DiskCache] = {}


def key_cache(spec: dict | None, objects_url: str) -> DiskCache | None:
    """The process-wide disk cache a project's `key_cache` setting names
    (docs/object-store-state.md §6): `path=None` puts it next to `file://`
    state, or in a temporary directory for remote state. Index files are
    immutable and uniquely named, so every process on the machine can share it."""

    if spec is None:
        return None
    path = spec.get("path")
    if path is None:
        from hashlib import sha256
        from urllib.parse import unquote, urlsplit

        u = urlsplit(objects_url)
        if u.scheme == "file":
            root = os.path.dirname(unquote(u.path).rstrip("/"))
            path = os.path.join(root, ".key-cache", os.path.basename(unquote(u.path).rstrip("/")))
        else:
            digest = sha256(objects_url.encode()).hexdigest()[:16]
            path = os.path.join(tempfile.gettempdir(), "solera-key-cache", digest)
    cache = _caches.get(path)
    if cache is None:
        try:
            cache = _caches[path] = DiskCache(path, int(spec.get("max_bytes") or 8 * 2**30))
        except OSError:
            return None  # no writable data directory: run uncached
    return cache
