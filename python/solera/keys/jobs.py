"""Running a native streaming job (`solera._native.Job`) over an index.

The job does the per-key work; this module does its I/O. Each run — a
level-0 file, or a level's files in key order — is read a segment of
consecutive blocks at a time, a few segments ahead; each file the job
writes is handed to `on_file` as soon as it is full, a few uploads at a
time. Memory is those buffers, whatever the size of the index.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable

from .. import _native
from .io import ObjectIO

SEGMENT = 8 * 2**20  # bytes of consecutive blocks per read
AHEAD = 3  # segments read ahead per run
UPLOADS = 2  # files uploading at once


class _Run:
    """Segments of one run's files, in key order, fetched `AHEAD` at a time."""

    def __init__(self, io: ObjectIO, path: Callable[[str], str], files: list):
        self.io, self.path, self.files = io, path, files
        self.queue: asyncio.Queue = asyncio.Queue(AHEAD)
        self.producer = asyncio.ensure_future(self._produce())

    async def _produce(self):
        try:
            for f in self.files:
                if f.size <= SEGMENT:
                    await self.queue.put(asyncio.ensure_future(self._whole(f)))
                    continue
                part = await self.io.read(self.path(f.name), f.size - f.index, f.size, f.size)
                idx = _native.parse_index(part, f.size)
                blocks = idx["blocks"]
                i = 0
                while i < len(blocks):
                    j, start = i + 1, blocks[i][1]
                    while j < len(blocks) and blocks[j][1] + blocks[j][2] - start <= SEGMENT:
                        j += 1
                    await self.queue.put(asyncio.ensure_future(self._fetch(f, blocks[i:j], idx["codec"])))
                    i = j
            await self.queue.put(None)
        except Exception as e:  # surfaces at the next read
            await self.queue.put(e)

    async def _whole(self, f):
        data = await self.io.read_whole(self.path(f.name), f.size)
        idx = _native.parse_index(data, f.size)
        return data, [(off, size, crc) for _, off, size, _, crc in idx["blocks"]], idx["codec"]

    async def _fetch(self, f, blocks, codec):
        start, end = blocks[0][1], blocks[-1][1] + blocks[-1][2]
        data = await self.io.read(self.path(f.name), start, end, f.size)
        return data, [(off - start, size, crc) for _, off, size, _, crc in blocks], codec

    async def next(self):
        item = await self.queue.get()
        if isinstance(item, Exception):
            raise item
        return None if item is None else await item

    def close(self):
        self.producer.cancel()
        while not self.queue.empty():
            item = self.queue.get_nowait()
            if isinstance(item, asyncio.Future):
                item.cancel()


async def run(
    job,
    io: ObjectIO,
    path: Callable[[str], str],
    runs: list[list],
    on_file: Callable[[int, bytes], Awaitable[None]] | None = None,
    rows: Iterable | None = None,
) -> None:
    """Drive `job` to the end over `runs` (each a list of `FileInfo`, in key
    order; newest run first). Written files go to `on_file(n, data)`, `n`
    counting from 0 in key order. `rows` feeds a streamed replacement its
    sorted chunks; they are pulled off the event loop."""

    readers = [_Run(io, path, files) for files in runs]
    chunks = iter(rows) if rows is not None else None
    uploads: set[asyncio.Future] = set()
    n = 0
    try:
        while (step := await asyncio.to_thread(job.step)) is not None:
            kind, x = step
            if kind == "run":
                seg = await readers[x].next()
                if seg is None:
                    job.end(x)
                else:
                    job.feed(x, *seg)
            elif kind == "rows":
                chunk = await asyncio.to_thread(next, chunks, None)
                if chunk is None:
                    job.end_rows()
                else:
                    job.feed_rows(chunk)
            else:
                if len(uploads) >= UPLOADS:
                    done, uploads = await asyncio.wait(uploads, return_when=asyncio.FIRST_COMPLETED)
                    for t in done:
                        t.result()
                uploads.add(asyncio.ensure_future(on_file(n, x)))
                n += 1
        await asyncio.gather(*uploads)
    finally:
        for r in readers:
            r.close()
        for t in uploads:
            t.cancel()
