"""Running a native streaming job (`solera._native.Merge`) over an index.

The job does the per-key work; this module does its I/O. Each run — a
level-0 file, or a level's files in key order — is read a segment of
consecutive blocks at a time, a few segments ahead; each file the job
writes is handed to `on_file` as soon as it is full, a few uploads at a
time. Memory is those buffers, whatever the size of the index. Given the
runs as the engine cache's local files, the job reads those itself.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable

from .. import _native
from .io import ObjectIO
from .threads import in_thread

SEGMENT = 8 * 2**20  # bytes of consecutive blocks per read
AHEAD = 3  # segments read ahead per run
UPLOADS = 2  # files uploading at once


class _Run:
    """Segments of one run's files, in key order, fetched `AHEAD` at a time.
    It owns every fetch it starts, queued or waiting for room: `close`
    cancels them all and waits for them to end."""

    def __init__(self, io: ObjectIO, path: Callable[[str], str], files: list):
        self.io, self.path, self.files = io, path, files
        self.queue: asyncio.Queue = asyncio.Queue(AHEAD)
        self.fetches: set[asyncio.Task] = set()
        self.producer = asyncio.ensure_future(self._produce())

    def _start(self, coro) -> asyncio.Task:
        task = asyncio.ensure_future(coro)
        self.fetches.add(task)
        task.add_done_callback(self.fetches.discard)
        return task

    async def _produce(self):
        try:
            for f in self.files:
                if f.size <= SEGMENT:
                    await self.queue.put(self._start(self._whole(f)))
                    continue
                part = await self.io.read(self.path(f.name), f.size - f.index, f.size, f.size)
                idx = _native.parse_index(part, f.size)
                blocks = idx["blocks"]
                i = 0
                while i < len(blocks):
                    j, start = i + 1, blocks[i][1]
                    while j < len(blocks) and blocks[j][1] + blocks[j][2] - start <= SEGMENT:
                        j += 1
                    await self.queue.put(self._start(self._fetch(f, blocks[i:j], idx["codec"])))
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

    async def close(self):
        tasks = [self.producer, *self.fetches]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def run(
    job,
    io: ObjectIO,
    path: Callable[[str], str],
    runs: list[list],
    on_file: Callable[[int, bytes], Awaitable[None]] | None = None,
    rows: Iterable | None = None,
    on_garbage: Callable[[int, bytes], Awaitable[None]] | None = None,
    local: list[list] | None = None,
) -> None:
    """Drive `job` to the end over `runs` (each a list of `FileInfo`, in key
    order; newest run first). Written files go to `on_file(n, data)`, `n`
    counting from 0 in key order, and a compaction's garbage files to
    `on_garbage(n, data)`. `rows` feeds a streamed replacement its sorted
    chunks; they are pulled off the event loop. `local`: the runs as
    `LocalFile`s, read in place of the store."""

    if local is not None:
        job.local(local)
    readers = [] if local is not None else [_Run(io, path, files) for files in runs]
    chunks = iter(rows) if rows is not None else None
    uploads: set[asyncio.Future] = set()
    n = g = 0
    try:
        while (step := await in_thread(job.step)) is not None:
            kind, x = step
            if kind == "run":
                seg = await readers[x].next()
                if seg is None:
                    job.end(x)
                else:
                    job.feed(x, *seg)
            elif kind == "rows":
                chunk = await in_thread(next, chunks, None)
                if chunk is None:
                    job.end_rows()
                else:
                    job.feed_rows(chunk)
            else:
                if len(uploads) >= UPLOADS:
                    done, uploads = await asyncio.wait(uploads, return_when=asyncio.FIRST_COMPLETED)
                    for t in done:
                        t.result()
                if kind == "garbage":
                    uploads.add(asyncio.ensure_future(on_garbage(g, x)))
                    g += 1
                else:
                    uploads.add(asyncio.ensure_future(on_file(n, x)))
                    n += 1
        await asyncio.gather(*uploads)
    finally:
        await asyncio.gather(*(r.close() for r in readers))
        for t in uploads:
            t.cancel()
        await asyncio.gather(*uploads, return_exceptions=True)  # none outlives the job
