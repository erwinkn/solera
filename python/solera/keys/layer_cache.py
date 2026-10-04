"""The engine's cache of layers (docs/key-index-design.md § The engine's cache).

Layer files never change once written, so a cached copy is never stale,
only evicted. Two tiers:

- **in memory**: layer indexes and small parts (read whole), what every read
  opens first; bounded in bytes, least recently used out;
- **on disk**: raw copies of layer files — a block is read with one
  positioned read and checked by its CRC as it decompresses — so lookups at
  the head read no object once an index is warm (the base included: a cold
  write's ~1 GET per key at 100M becomes local reads). Files are filled per
  index, installed straight from a write the engine made, and evicted by
  index, least recently used first; a file a read holds open is not
  evicted.

A copy is the object it claims to be: installed or filled with the size
the state names, and every block read checked; a size mismatch drops it.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections import OrderedDict

from .io import ObjectIO
from .layers import LayerState
from .threads import in_thread


class LayerCache:
    def __init__(self, root: str, *, disk: int, memory: int = 256 * 2**20):
        self.root, self.disk, self.memory = root, disk, memory
        os.makedirs(root, exist_ok=True)
        self._mem: OrderedDict = OrderedDict()  # key -> (value, bytes)
        self._mem_used = 0
        self._files: OrderedDict = OrderedDict()  # object path -> (local path, size, index prefix)
        self._disk_used = 0
        self._open: dict[str, int] = {}  # object path -> readers
        self._filling: dict[str, asyncio.Future] = {}
        for name in os.listdir(
            root
        ):  # nothing is trusted across restarts: the state names sizes, not contents
            if name.endswith(".tmp") or name.endswith(".lay") or name.endswith(".lix"):
                os.remove(os.path.join(root, name))

    # -- memory: indexes and small parts (the mapping LayerIndex takes) -------------------

    def __contains__(self, key) -> bool:
        return key in self._mem

    def __getitem__(self, key):
        v, _ = self._mem[key]
        self._mem.move_to_end(key)
        return v

    def __setitem__(self, key, value) -> None:
        size = (
            len(value)
            if isinstance(value, bytes | bytearray)
            else 64 * (len(getattr(value, "first", ())) + 1)
        )
        if key in self._mem:
            self._mem_used -= self._mem.pop(key)[1]
        self._mem[key] = (value, size)
        self._mem_used += size
        while self._mem_used > self.memory and len(self._mem) > 1:
            _, (_, s) = self._mem.popitem(last=False)
            self._mem_used -= s

    def get(self, key, default=None):
        return self[key] if key in self._mem else default

    # -- disk: raw copies ----------------------------------------------------------------------

    def _local(self, path: str) -> str:
        return os.path.join(self.root, f"{uuid.uuid5(uuid.NAMESPACE_URL, path).hex}.lay")

    def has(self, path: str) -> bool:
        return path in self._files

    def read(self, path: str, start: int, end: int) -> bytes | None:
        """Bytes `[start, end)` of a cached file, or None if not cached."""

        f = self._files.get(path)
        if f is None:
            return None
        self._files.move_to_end(path)
        with open(f[0], "rb") as fh:
            return os.pread(fh.fileno(), end - start, start)

    def _evict(self, need: int, keep: str | None) -> bool:
        for path in list(self._files):
            if self._disk_used + need <= self.disk:
                break
            local, size, prefix = self._files[path]
            if self._open.get(path) or prefix == keep:
                continue
            del self._files[path]
            self._disk_used -= size
            os.remove(local)
        return self._disk_used + need <= self.disk

    def _put(self, path: str, data: bytes, prefix: str) -> bool:
        if path in self._files:
            return True
        if not self._evict(len(data), prefix):
            return False
        local = self._local(path)
        tmp = f"{local}.{uuid.uuid4().hex}.tmp"
        with open(tmp, "wb") as fh:
            fh.write(data)
        os.replace(tmp, local)
        self._files[path] = (local, len(data), prefix)
        self._disk_used += len(data)
        return True

    def install(self, path: str, data: bytes, prefix: str) -> bool:
        """A file the engine just wrote: cached without a GET."""

        return self._put(path, data, prefix)

    def retire(self, paths: list[str]) -> None:
        """Files a published merge let go of: evicted first."""

        for p in paths:
            if p in self._files and not self._open.get(p):
                local, size, _ = self._files.pop(p)
                self._disk_used -= size
                os.remove(local)

    async def fill(self, io: ObjectIO, state: LayerState, *, sides: bool = False) -> bool:
        """Cache every file of the state's main parts (and side parts with
        `sides`), fetched whole, a few at once. Returns whether it all fits."""

        need = [
            (state.path(f.name), f.size)
            for x in state.layers
            for p in ([x.main] + ([x.side] if sides and x.side else []))
            for f in p.files
        ]
        if sum(s for _, s in need) > self.disk:
            return False
        sem = asyncio.Semaphore(4)

        async def one(path: str, size: int) -> bool:
            if path in self._files:
                return True
            fut = self._filling.get(path)
            if fut is None:
                fut = self._filling[path] = asyncio.ensure_future(
                    self._fetch(io, path, size, state.prefix, sem)
                )
            try:
                return await fut
            finally:
                self._filling.pop(path, None)

        return all(await asyncio.gather(*(one(p, s) for p, s in need)))

    async def _fetch(self, io: ObjectIO, path: str, size: int, prefix: str, sem) -> bool:
        async with sem:
            data = await io.read_whole(path, size)
        if len(data) != size:
            return False
        return await in_thread(self._put, path, data, prefix)

    def warm(self, state: LayerState) -> bool:
        """Whether every main part's file is cached."""

        return all(state.path(f.name) in self._files for x in state.layers for f in x.main.files)

    def hold(self, paths: list[str]):
        """Keep `paths` from eviction while a read uses them."""

        cache = self

        class _Hold:
            def __enter__(self):
                for p in paths:
                    cache._open[p] = cache._open.get(p, 0) + 1
                return self

            def __exit__(self, *exc):
                for p in paths:
                    cache._open[p] -= 1
                    if not cache._open[p]:
                        del cache._open[p]

        return _Hold()

    def used(self) -> tuple[int, int]:
        """Bytes on disk, bytes in memory."""

        return self._disk_used, self._mem_used
