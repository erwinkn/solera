"""Object I/O for the key index: bounded concurrency, request metrics, and
optional injected latency (for benchmarks). Reads go to the object store:
the one cache of index files is the engine's (`cache.py`), which answers
small writes before a worker reads anything (docs/resolved-commits.md §5).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

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

    def snapshot(self) -> dict:
        return dict(self.__dict__)

    def reset(self) -> None:
        for name in self.__dict__:
            setattr(self, name, 0)


class ObjectIO:
    """Reads and writes objects with at most `concurrency` requests in flight.
    `latency` adds a fixed delay per request and `bandwidth` a per-connection
    transfer rate, to model a remote object store in benchmarks."""

    def __init__(
        self,
        store,
        *,
        concurrency: int = 64,
        latency: float = 0.0,
        bandwidth: float | None = None,
        metrics: Metrics | None = None,
    ):
        self.store = store
        self.latency = latency
        self.bandwidth = bandwidth
        self.metrics = metrics or Metrics()
        self._sem = asyncio.Semaphore(concurrency)

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

    async def read(self, path: str, start: int, end: int, size: int) -> bytes:
        """Bytes `[start, end)` of an object of `size` bytes."""

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

    async def delete(self, paths: list[str]) -> None:
        if paths:
            async with self._sem:
                await obstore.delete_async(self.store, paths)
                self.metrics.deletes += len(paths)
