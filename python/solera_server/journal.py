"""The engine's durable state on an object store alone (docs/object-store-state.md §3, §10).

Current state lives in memory. Every change to it is an event; the journal
appends events to create-only segment objects and periodically writes the
whole state as a checkpoint. A writer starting up loads the newest
checkpoint and replays the segments after it.

    {prefix}/journal/{seq:020d}.json      {"seq", "writer", "at", "events": [...]}
    {prefix}/checkpoints/{seq:020d}.json  {"seq", "writer", "at", "fences", "state": {...}}

Only create-only puts and LIST are needed — no compare-and-swap, which
obstore's local filesystem backend does not implement.

**Fencing.** A writer's first segment is its fence (`WriterStarted`); the
segment's seq is the writer id. Every later segment is created at `seq+1`.
A replaced writer's next create collides with a segment it did not write
and the journal becomes `fenced`: every later append fails. That segment is
its successor's fence, so fence segments are never deleted: were cleanup to
remove one, the replaced writer's next create would succeed in its slot, and
what it appended would be acknowledged yet never replayed. Each checkpoint
lists them (`fences`) so cleanup can skip them: one small object per writer.

**Durability.** `append` applies nothing; the caller applies an event to
memory and appends it, and a background flusher writes what is buffered
once `flush_interval` has passed or `max_buffer` bytes are buffered. Only
what acts on the outside world on the strength of an event — launching an
attempt, deleting what the event made garbage, answering an API call —
awaits `durable()`, which flushes at once. A failed write is retried with
the very same segment.

**Checkpoints.** After a flush, once the journal written since the last
checkpoint reaches that checkpoint's size (and at least `min_checkpoint`
bytes), the state is snapshotted — at the moment the flushed batch was
sealed, so it matches the segment's seq exactly — and written. Then
checkpoints older than the previous one are deleted, and so are segments
at or below the previous checkpoint's seq but fences: the previous checkpoint and the
journal after it are kept, so a newest checkpoint that turns out
unreadable can be recovered from.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

import obstore
from obstore.exceptions import AlreadyExistsError, NotFoundError
from solera.objects import create

log = logging.getLogger(__name__)


class Fenced(RuntimeError):
    """Another writer took over this namespace; this one must stop."""


class JournalCorrupt(RuntimeError):
    """The journal has a gap or an unreadable segment."""


def _dumps(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


@dataclass
class OpenResult:
    seq: int  # the writer's fence segment — its writer id
    replayed: int  # segments applied after the checkpoint
    checkpoint: int | None  # the checkpoint loaded, if any


class Journal:
    def __init__(
        self,
        store,
        prefix: str = "control",
        *,
        flush_interval: float = 1.0,
        max_buffer: int = 1 << 20,
        min_checkpoint: int = 256 << 10,
        clock: Callable[[], float] = time.time,
    ):
        self.store = store
        self.prefix = prefix.strip("/")
        self.flush_interval = flush_interval
        self.max_buffer = max_buffer
        self.min_checkpoint = min_checkpoint
        self.clock = clock

        self.seq = 0  # last segment written (by anyone) and applied
        self.writer: int | None = None
        self.fenced = False
        self._buffer: list[dict] = []
        self._buffer_bytes = 0
        self._first_buffered: float | None = None
        self.appended = self.written = 0  # events this writer appended, and wrote
        self._waiters: list[tuple[int, asyncio.Future]] = []  # (events appended, waiter)
        self._urgent = False  # someone waits on `durable()`
        self._sealed: tuple | None = None  # (seq, data, events, checkpoint): written next, as is
        self._flushing: asyncio.Lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._snapshot: Callable[[], dict] | None = None
        self._since_checkpoint = 0
        self._last_checkpoint_size = 0
        self._checkpoints: list[int] = []
        self.fences: list[int] = []  # every writer's fence segment: never deleted

    # -- paths ----------------------------------------------------------------------

    def _segment(self, seq: int) -> str:
        return f"{self.prefix}/journal/{seq:020d}.json"

    def _checkpoint(self, seq: int) -> str:
        return f"{self.prefix}/checkpoints/{seq:020d}.json"

    async def _list(self, kind: str, after: int | None = None) -> list[int]:
        prefix = f"{self.prefix}/{kind}/"
        offset = f"{prefix}{after:020d}.json" if after is not None else None
        out = []
        stream = (
            obstore.list(self.store, prefix, offset=offset) if offset else obstore.list(self.store, prefix)
        )
        async for batch in stream:
            for meta in batch:
                name = meta["path"].rsplit("/", 1)[-1]
                if name.endswith(".json") and name[:-5].isdigit():
                    out.append(int(name[:-5]))
        return sorted(out)

    async def _get_json(self, path: str):
        data = await obstore.get_async(self.store, path)
        return json.loads(bytes(await data.bytes_async()))

    # -- opening ----------------------------------------------------------------------

    async def open(
        self,
        restore: Callable[[dict | None], None],
        apply: Callable[[dict], None],
        snapshot: Callable[[], dict],
        *,
        writer: bool = True,
    ) -> OpenResult:
        """Load the newest readable checkpoint, replay the segments after it,
        then fence: from here on this process is the only writer.

        `writer=False` opens read-only: nothing is fenced and appends fail —
        for tools that inspect a namespace a server may be writing."""

        self._snapshot = snapshot
        self._checkpoints = await self._list("checkpoints")
        loaded = None
        for seq in reversed(self._checkpoints):
            try:
                body = await self._get_json(self._checkpoint(seq))
                restore(body["state"])
                self.fences = list(body["fences"])
                loaded = seq
                self._last_checkpoint_size = len(_dumps(body))
                break
            except (NotFoundError, ValueError, KeyError) as error:
                log.warning("checkpoint %s unreadable (%s); trying the previous one", seq, error)
        if loaded is None:
            restore(None)
        self.seq = loaded or 0
        replayed = await self._replay(apply)
        if not writer:
            self.fenced = True  # read-only: every append fails
            return OpenResult(seq=None, replayed=replayed, checkpoint=loaded)
        await self._fence(apply)
        self._task = asyncio.create_task(self._run())
        return OpenResult(seq=self.writer, replayed=replayed, checkpoint=loaded)

    async def _replay(self, apply) -> int:
        count = 0
        for seq in await self._list("journal", after=self.seq):
            if seq != self.seq + 1:
                raise JournalCorrupt(f"journal gap: expected segment {self.seq + 1}, found {seq}")
            body = await self._get_json(self._segment(seq))
            self._apply_segment(seq, body, apply)
            self._since_checkpoint += len(_dumps(body))
            count += 1
        return count

    async def _fence(self, apply) -> None:
        while True:
            seq = self.seq + 1
            fence = {"type": "WriterStarted", "writer": seq}
            body = {"seq": seq, "writer": seq, "at": self.clock(), "events": [fence]}
            try:
                await create(self.store, self._segment(seq), _dumps(body))
            except AlreadyExistsError:
                # Another writer appended since we listed: apply it and try the next seq.
                self._apply_segment(seq, await self._get_json(self._segment(seq)), apply)
                continue
            apply(fence)
            self.seq = seq
            self.writer = seq
            self.fences.append(seq)
            return

    def _apply_segment(self, seq: int, body: dict, apply) -> None:
        for event in body["events"]:
            apply(event)
            if event["type"] == "WriterStarted":
                self.fences.append(seq)
        self.seq = seq

    # -- appending ----------------------------------------------------------------------

    def append(self, *events: dict) -> None:
        """Queue events for the next flush."""

        if self.fenced:
            raise Fenced("this writer was replaced")
        for event in events:
            self._buffer.append(event)
            self._buffer_bytes += len(_dumps(event))
        self.appended += len(events)
        if self._first_buffered is None:
            # Flush timing is monotonic loop time; `clock` only stamps records.
            self._first_buffered = asyncio.get_running_loop().time()
            self._wake.set()  # the flusher starts the flush-interval clock
        elif self._buffer_bytes >= self.max_buffer:
            self._wake.set()

    async def durable(self) -> None:
        """Return once every event appended so far is in the object store.
        What is buffered is flushed now, not after `flush_interval`: waiting
        costs one write."""

        target = self.appended
        if self.written >= target:
            return
        if self.fenced:
            raise Fenced("this writer was replaced")
        if self._task is None:  # closed: nothing flushes in the background
            await self.flush()
            return
        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append((target, waiter))
        self._urgent = True
        self._wake.set()
        await waiter

    async def flush(self) -> None:
        """Write everything buffered, as one segment (and a checkpoint if one
        is due). A segment is sealed before it is written: if the write fails
        or is interrupted, the next flush writes it again, byte for byte."""

        async with self._flushing:
            while self._sealed is not None or self._buffer:
                if self.fenced:
                    self._fail(Fenced("this writer was replaced"))
                    return
                if self._sealed is None:
                    self._seal()
                seq, data, count, snap = self._sealed
                try:
                    await self._put_segment(seq, data)
                except Fenced as error:
                    self._sealed = None
                    self._fail(error)
                    raise
                self._sealed = None
                self.seq = seq
                self.written += count
                self._since_checkpoint += len(data)
                waiting = []
                for target, waiter in self._waiters:
                    if target > self.written:
                        waiting.append((target, waiter))
                    elif not waiter.done():
                        waiter.set_result(None)
                self._waiters = waiting
                if snap is not None:
                    await self._write_checkpoint(seq, snap)

    def _seal(self) -> None:
        events = self._buffer
        self._buffer, self._buffer_bytes, self._first_buffered, self._urgent = [], 0, None, False
        seq = self.seq + 1
        data = _dumps({"seq": seq, "writer": self.writer, "at": self.clock(), "events": events})
        # Sealed: memory now reflects exactly segments 1..seq, so a snapshot
        # taken here matches `seq` — before any later event is applied.
        due = self._snapshot is not None and self._since_checkpoint + len(data) >= max(
            self.min_checkpoint, self._last_checkpoint_size
        )
        snap = _dumps(self._checkpoint_body(seq)) if due else None
        self._sealed = (seq, data, len(events), snap)

    def _checkpoint_body(self, seq: int) -> dict:
        at, fences = self.clock(), list(self.fences)
        return {"seq": seq, "writer": self.writer, "at": at, "fences": fences, "state": self._snapshot()}

    async def _put_segment(self, seq: int, data: bytes) -> None:
        for attempt in range(5):
            try:
                await create(self.store, self._segment(seq), data)  # or finds our own earlier try
                return
            except AlreadyExistsError:
                self.fenced = True
                raise Fenced(f"segment {seq} was written by another writer") from None
            except (OSError, TimeoutError, ConnectionError) as error:
                if attempt == 4:
                    raise
                log.warning("segment %s put failed (%s); retrying", seq, error)
                await asyncio.sleep(0.2 * 2**attempt)

    async def _write_checkpoint(self, seq: int, data: bytes) -> None:
        try:
            await create(self.store, self._checkpoint(seq), data)
        except AlreadyExistsError:
            return
        self._checkpoints.append(seq)
        self._since_checkpoint = 0
        self._last_checkpoint_size = len(data)
        await self._collect()

    async def _collect(self) -> None:
        """Keep the newest checkpoint, the one before it, the journal after
        that, and every fence."""

        if len(self._checkpoints) < 2:
            return
        previous = self._checkpoints[-2]
        old = [c for c in self._checkpoints if c < previous]
        fences = set(self.fences)
        segments = [s for s in await self._list("journal") if s <= previous and s not in fences]
        paths = [self._checkpoint(c) for c in old] + [self._segment(s) for s in segments]
        for i in range(0, len(paths), 1000):
            await obstore.delete_async(self.store, paths[i : i + 1000])
        self._checkpoints = [c for c in self._checkpoints if c >= previous]

    def _fail(self, error: BaseException) -> None:
        self.fenced = True
        for _, waiter in self._waiters:
            if not waiter.done():
                waiter.set_exception(error)
        self._waiters, self._buffer, self._buffer_bytes = [], [], 0

    # -- the flusher ----------------------------------------------------------------------

    async def _run(self) -> None:
        """Flush once the oldest buffered event has waited `flush_interval`,
        `max_buffer` bytes are buffered, or someone waits on `durable()`. A
        failed write is retried after `flush_interval`."""

        loop = asyncio.get_running_loop()
        while True:
            self._wake.clear()
            if self._sealed is None:
                if self._first_buffered is None:
                    await self._wake.wait()
                    continue
                wait = self._first_buffered + self.flush_interval - loop.time()
                if wait > 0 and not self._urgent and self._buffer_bytes < self.max_buffer:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self._wake.wait(), timeout=wait)
                    continue
            try:
                await self.flush()
            except Fenced:
                log.error("journal fenced: another writer took over; stopping")
                return
            except Exception as error:
                log.error("journal flush failed, retrying: %s", error)
                await asyncio.sleep(self.flush_interval)

    async def close(self, *, checkpoint: bool = True) -> None:
        """Flush what is buffered, write a final checkpoint, stop the flusher."""

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self.fenced or self.writer is None:
            return
        await self.flush()
        if checkpoint and self._snapshot is not None and self._since_checkpoint:
            await self._write_checkpoint(self.seq, _dumps(self._checkpoint_body(self.seq)))
