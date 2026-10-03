"""What a running worker tells the engine (docs/lifecycle.md §5, §6): beats
with its timeline, and its log — live over the channel, durably as chunks.

Both are evidence, never permission: the worker writes on its launch
authorization and its stores' rules, whatever the engine answers.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import gzip
import json
import os
import socket
import threading
import time

from obstore.exceptions import NotFoundError
from solera import lifecycle
from solera.lifecycle import Cancel, Ended
from solera.objects import create

FALLBACK_AFTER = 2  # failed beats before the worker also reports through `.worker`


class Reporter:
    """Beats from a thread of its own, so a producer that blocks its event
    loop still beats. Over the channel every `interval`; while the channel
    fails (or there is none), also through `.worker` every two intervals,
    reading the gate each time — an `aborted` or `closed` gate is the only
    cancel that reaches a worker the engine cannot answer.

    `on_cancel(record)` runs, from this thread, each time the latched cancel
    record gets stronger; `on_ended()` once the engine says the attempt is
    over for this worker."""

    def __init__(self, objects, base, worker_id, channel, interval, timeline, on_cancel, on_ended):
        self.objects, self.base, self.worker_id = objects, base, worker_id
        self.channel, self.interval, self.timeline = channel, interval, timeline
        self.on_cancel, self.on_ended = on_cancel, on_ended
        self.cancel: Cancel | None = None
        self.ended = False
        self.failures = 0
        self.seq = 0
        self._fallback_at = -float("inf")
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"beat {base}", daemon=True)

    def start(self):
        self._thread.start()

    def latch(self, data: dict | None) -> None:
        record = lifecycle.latch(self.cancel, Cancel.from_json(data))
        if record != self.cancel:
            self.cancel = record
            self.on_cancel(record)

    def _run(self):
        import obstore

        while not self._stop.is_set() and not self.ended:
            self.seq += 1
            report = {"worker_id": self.worker_id, "seq": self.seq, **self.timeline.report()}
            if self.channel is not None:
                try:
                    self.latch(self.channel.beat(report).get("cancel"))
                    self.failures = 0
                except Ended:
                    self.ended = True
                    self.on_ended()
                    return
                except Exception:
                    self.failures += 1
            now = time.monotonic()
            if (self.channel is None or self.failures >= FALLBACK_AFTER) and (
                now - self._fallback_at >= 2 * self.interval
            ):
                self._fallback_at = now
                with contextlib.suppress(Exception):
                    body = {**report, "host": socket.gethostname(), "pid": os.getpid(), "at": time.time()}
                    obstore.put(self.objects, f"{self.base}{lifecycle.WORKER}", json.dumps(body).encode())
                    try:
                        gate = json.loads(
                            bytes(obstore.get(self.objects, f"{self.base}{lifecycle.GATE}").bytes())
                        )
                    except (NotFoundError, FileNotFoundError):
                        gate = None
                    if gate is not None and gate["state"] != lifecycle.WRITING:
                        reason = self.cancel.reason if self.cancel else "user"
                        self.latch({"phase": "forced", "reason": reason, "since": 0})
            self._stop.wait(self.interval)

    async def stop(self):
        self._stop.set()
        await asyncio.to_thread(self._thread.join)


LOG_LIVE_SECONDS = 1.0
LOG_CHUNK_SECONDS = 30.0
LOG_CHUNK_BYTES = 1 << 20
LOG_TAIL_BYTES = 64 << 10  # a log end smaller than this travels inside the result
LOG_LIVE_MAX = 10_000  # lines held for the live channel; past it, the oldest are skipped
LOG_CAP = 100 * 2**20  # compressed bytes per attempt
LOG_PENDING_MAX = (
    16 * LOG_CHUNK_BYTES
)  # lines waiting for a chunk; past it (writes failing), new ones are lost


class LogShipper:
    """`ctx.log` lines as gzip-compressed JSON lines (docs/lifecycle.md §2).

    Live, each line goes to the channel within a second, with its offset, so
    a retried batch is not shown twice. Durably, the lines go out as
    create-only chunks, `{attempt}.log.{n:06d}`, every 30 s or 1 MB; never
    joined. At the end, the lines not yet in a chunk travel inside the
    result (`tail`) when they are under 64 KB, else as one last chunk. Past
    `LOG_CAP` a truncation marker is written and shipping stops. While
    chunks cannot be written, lines wait — up to `LOG_PENDING_MAX` bytes;
    past it they are counted as `lost`.

    A chunk is sealed — its number, bytes and lines fixed — before it is
    written, and lines logged meanwhile start the next one: a write retried
    after an unknown outcome rewrites exactly what may have landed."""

    def __init__(self, objects, base: str, channel=None, worker_id: str | None = None):
        self.objects, self.base, self.channel, self.worker_id = objects, base, channel, worker_id
        self.pending: list[tuple[float, str]] = []  # lines not yet in a chunk
        self.pending_bytes = 0
        self.live: list[str] = []  # lines not yet sent live
        self.live_offset = 0  # the offset of live[0]
        self.chunks: list[list] = []
        self.sealed: tuple[int, bytes, list] | None = None  # the chunk being written
        self.size = self.lines = 0
        self.truncated = False
        self.dropped = 0  # lines past LOG_PENDING_MAX
        self._chunked_at = time.monotonic()
        self._flush: asyncio.Task | None = None  # the one chunk write a full buffer started
        self._lock = asyncio.Lock()  # one chunk written at a time
        # The buffers' one owner: lines come from the loop and from threads (a
        # synchronous Each call), while a chunk is compressed and written.
        self._guard = threading.Lock()

    def append(self, entry):
        if self.truncated:
            return
        line = json.dumps(entry, allow_nan=False) + "\n"
        with self._guard:
            if self.pending_bytes + len(line) > LOG_PENDING_MAX:
                self.lines += 1
                self.dropped += 1
                return
            self.pending.append((entry["at"], line))
            self.pending_bytes += len(line)
            self.lines += 1
            if self.channel is not None:
                self.live.append(line)
                if (
                    len(self.live) > LOG_LIVE_MAX
                ):  # the channel is not keeping up: a gap, filled by the chunks
                    drop = len(self.live) - LOG_LIVE_MAX
                    del self.live[:drop]
                    self.live_offset += drop
            full = self.pending_bytes >= LOG_CHUNK_BYTES
        if full and (self._flush is None or self._flush.done()):  # one at a time, however many lines
            try:
                self._flush = asyncio.get_running_loop().create_task(self.chunk())
            except RuntimeError:
                pass  # logging from a thread: the next periodic flush ships it

    def _taken(self) -> list[tuple[float, str]]:
        """The pending lines as they are now: compressed outside the lock,
        then `_took` removes exactly these, whatever came after."""

        with self._guard:
            return list(self.pending)

    def _took(self, lines: list) -> None:
        with self._guard:
            del self.pending[: len(lines)]
            self.pending_bytes -= sum(len(line) for _, line in lines)

    def _member(self, lines: list[tuple[float, str]]) -> tuple[bytes, list[tuple[float, str]]]:
        member = gzip.compress("".join(line for _, line in lines).encode(), compresslevel=6, mtime=0)
        if self.size + len(member) > LOG_CAP:
            self.truncated = True
            marker = {
                "at": lines[0][0],
                "level": "warning",
                "message": f"log truncated: the attempt's log reached {LOG_CAP} bytes",
                "fields": {},
            }
            lines = [(marker["at"], json.dumps(marker) + "\n")]
            member = gzip.compress(lines[0][1].encode(), mtime=0)
        return member, lines

    async def chunk(self) -> bool:
        """Seal the pending lines as the next chunk, unless one is sealed
        already, and write it. False if the write failed: the sealed chunk
        is written again next time, with the same bytes."""

        async with self._lock:
            if self.sealed is None:
                taken = self._taken()
                if not taken:
                    return True
                member, lines = self._member(taken)
                self._took(taken)
                self.sealed = (len(self.chunks), member, lines)
            n, member, lines = self.sealed
            try:
                await create(self.objects, f"{self.base}{lifecycle.chunk(n)}", member)
            except Exception:
                return False
            self.sealed = None
            self.chunks.append([n, len(lines), lines[0][0]])
            self.size += len(member)
            self._chunked_at = time.monotonic()
            return True

    async def send_live(self):
        if self.channel is None or not self.live:
            return
        with self._guard:
            lines, offset = list(self.live), self.live_offset
        with contextlib.suppress(Exception):
            answer = await self.channel.logs({"worker_id": self.worker_id, "offset": offset, "lines": lines})
            acknowledged = offset + max(0, int(answer.get("offset", offset + len(lines))) - offset)
            with self._guard:  # lines dropped meanwhile moved the offset already
                gone = max(0, acknowledged - self.live_offset)
                del self.live[:gone]
                self.live_offset += gone

    async def periodically(self):
        while True:
            await asyncio.sleep(LOG_LIVE_SECONDS)
            await self.send_live()
            if self.sealed or (self.pending and time.monotonic() - self._chunked_at >= LOG_CHUNK_SECONDS):
                await self.chunk()

    async def finish(self) -> dict:
        """The log's index for the result: its chunks and its tail. Never
        raises: the log does not change an attempt's outcome. Lines it
        could not write are counted as `lost`."""

        await self.send_live()
        if self.sealed is not None:
            await self.chunk()
        tail = None
        async with self._lock:
            taken = self._taken()
            if taken and not self.truncated:
                member, lines = self._member(taken)
                if len(member) <= LOG_TAIL_BYTES:
                    tail = base64.b64encode(member).decode()
                    self._took(taken)
        if self.pending:
            await self.chunk()
        index = {
            "chunks": self.chunks,
            "tail": tail,
            "lines": self.lines,
            "bytes": self.size,
            "truncated": self.truncated,
        }
        lost = len(self.sealed[2] if self.sealed else ()) + len(self.pending) + self.dropped
        return {**index, "lost": lost} if lost else index
