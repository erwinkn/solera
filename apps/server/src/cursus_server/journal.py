"""The engine's durable state on an object store alone (docs/object-store-state.md §3, §10).

Current state lives in memory. Every change to it is an event; the journal
appends events to create-only segment objects and periodically writes the
whole state as a checkpoint. A writer starting up loads the newest
checkpoint and replays the segments after it.

    {prefix}/journal/{seq:020d}.json      {"seq", "writer", "at", "events": [...]}
    {prefix}/checkpoints/{seq:020d}.json  {"seq", "writer", "at", "state": {...}}

Only create-only puts and LIST are needed — no compare-and-swap, which
obstore's local filesystem backend does not implement.

**Fencing.** A writer's first segment is its fence (`WriterStarted`); the
segment's seq is the writer id. Every later segment is created at `seq+1`.
A replaced writer's next create collides with a segment it did not write
and the journal becomes `fenced`: every later append fails.

**Durability.** `append` applies nothing; the caller applies an event to
memory and appends it, and anything the outside world must be able to rely
on (a commit, a run submission) awaits `durable()` — the flush containing
it. Flushes happen when events are pending and either `flush_interval` has
passed or `max_buffer` bytes are buffered.

**Checkpoints.** After a flush, once the journal written since the last
checkpoint reaches that checkpoint's size (and at least `min_checkpoint`
bytes), the state is snapshotted — at the moment the flushed batch was
sealed, so it matches the segment's seq exactly — and written. Then
checkpoints older than the previous one are deleted, and so are segments
at or below the previous checkpoint's seq: the previous checkpoint and the
journal after it are kept, so a newest checkpoint that turns out
unreadable can be recovered from.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

import obstore
from obstore.exceptions import AlreadyExistsError, NotFoundError

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
        self._waiters: list[asyncio.Future] = []
        self._flushing: asyncio.Lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._snapshot: Callable[[], dict] | None = None
        self._since_checkpoint = 0
        self._last_checkpoint_size = 0
        self._checkpoints: list[int] = []

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
        start: bool = True,
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
        if start:
            self._task = asyncio.create_task(self._run())
        return OpenResult(seq=self.writer, replayed=replayed, checkpoint=loaded)

    async def _replay(self, apply) -> int:
        count = 0
        for seq in await self._list("journal", after=self.seq):
            if seq != self.seq + 1:
                raise JournalCorrupt(f"journal gap: expected segment {self.seq + 1}, found {seq}")
            body = await self._get_json(self._segment(seq))
            for event in body["events"]:
                apply(event)
            self.seq = seq
            self._since_checkpoint += len(_dumps(body))
            count += 1
        return count

    async def _fence(self, apply) -> None:
        while True:
            seq = self.seq + 1
            fence = {"type": "WriterStarted", "writer": seq}
            body = {"seq": seq, "writer": seq, "at": self.clock(), "events": [fence]}
            try:
                await obstore.put_async(self.store, self._segment(seq), _dumps(body), mode="create")
            except AlreadyExistsError:
                # Another writer appended since we listed: apply it and try the next seq.
                other = await self._get_json(self._segment(seq))
                for event in other["events"]:
                    apply(event)
                self.seq = seq
                continue
            apply(fence)
            self.seq = seq
            self.writer = seq
            return

    # -- appending ----------------------------------------------------------------------

    def append(self, *events: dict) -> asyncio.Future:
        """Queue events for the next flush; the future resolves once they are durable."""

        if self.fenced:
            raise Fenced("this writer was replaced")
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        for event in events:
            self._buffer.append(event)
            self._buffer_bytes += len(_dumps(event))
        first = self._first_buffered is None
        if first:
            # Flush timing is monotonic loop time; `clock` only stamps records.
            self._first_buffered = loop.time()
        self._waiters.append(fut)
        if self._task is None:
            # No background flusher (tests, tools): flush on the next loop turn.
            if first:
                loop.call_soon(lambda: asyncio.ensure_future(self.flush()))
        elif first or self._buffer_bytes >= self.max_buffer:
            # Wake the flusher: it starts the flush-interval clock, or flushes now when full.
            self._wake.set()
        return fut

    async def durable(self, *events: dict) -> None:
        """Append and wait until durable."""

        await self.append(*events)

    async def flush(self) -> None:
        """Write everything buffered as one segment (and checkpoint if due)."""

        async with self._flushing:
            if not self._buffer:
                return
            if self.fenced:
                self._fail(Fenced("this writer was replaced"))
                return
            events, waiters = self._buffer, self._waiters
            self._buffer, self._waiters, self._buffer_bytes, self._first_buffered = [], [], 0, None
            seq = self.seq + 1
            body = {"seq": seq, "writer": self.writer, "at": self.clock(), "events": events}
            data = _dumps(body)
            # Sealed: memory now reflects exactly segments 1..seq, so a snapshot
            # taken here matches `seq` — before any later event is applied.
            due = self._snapshot is not None and self._since_checkpoint + len(data) >= max(
                self.min_checkpoint, self._last_checkpoint_size
            )
            snap = (
                _dumps({"seq": seq, "writer": self.writer, "at": self.clock(), "state": self._snapshot()})
                if due
                else None
            )
            try:
                await self._put_segment(seq, data)
            except BaseException as error:
                for w in waiters:
                    if not w.done():
                        w.set_exception(error if isinstance(error, Exception) else RuntimeError(str(error)))
                if isinstance(error, Fenced):
                    self._fail(error)
                raise
            self.seq = seq
            self._since_checkpoint += len(data)
            for w in waiters:
                if not w.done():
                    w.set_result(seq)
            if snap is not None:
                await self._write_checkpoint(seq, snap)

    async def _put_segment(self, seq: int, data: bytes) -> None:
        for attempt in range(5):
            try:
                await obstore.put_async(self.store, self._segment(seq), data, mode="create")
                return
            except AlreadyExistsError:
                # Either our own earlier try succeeded without us hearing back, or
                # another writer took this seq: only the first is ours.
                existing = await obstore.get_async(self.store, self._segment(seq))
                if bytes(await existing.bytes_async()) == data:
                    return
                self.fenced = True
                raise Fenced(f"segment {seq} was written by another writer") from None
            except (OSError, TimeoutError, ConnectionError) as error:
                if attempt == 4:
                    raise
                log.warning("segment %s put failed (%s); retrying", seq, error)
                await asyncio.sleep(0.2 * 2**attempt)

    async def _write_checkpoint(self, seq: int, data: bytes) -> None:
        try:
            await obstore.put_async(self.store, self._checkpoint(seq), data, mode="create")
        except AlreadyExistsError:
            return
        self._checkpoints.append(seq)
        self._since_checkpoint = 0
        self._last_checkpoint_size = len(data)
        await self._collect()

    async def _collect(self) -> None:
        """Keep the newest checkpoint, the one before it, and the journal after that."""

        if len(self._checkpoints) < 2:
            return
        previous = self._checkpoints[-2]
        old = [c for c in self._checkpoints if c < previous]
        segments = [s for s in await self._list("journal") if s <= previous]
        paths = [self._checkpoint(c) for c in old] + [self._segment(s) for s in segments]
        for i in range(0, len(paths), 1000):
            await obstore.delete_async(self.store, paths[i : i + 1000])
        self._checkpoints = [c for c in self._checkpoints if c >= previous]

    def _fail(self, error: BaseException) -> None:
        self.fenced = True
        for w in self._waiters:
            if not w.done():
                w.set_exception(error)
        self._waiters, self._buffer, self._buffer_bytes = [], [], 0

    # -- the flusher ----------------------------------------------------------------------

    async def _run(self) -> None:
        while True:
            if self._first_buffered is None:
                self._wake.clear()
                await self._wake.wait()
                continue
            wait = self._first_buffered + self.flush_interval - asyncio.get_running_loop().time()
            if wait > 0 and self._buffer_bytes < self.max_buffer:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=wait)
                except TimeoutError:
                    pass
                continue
            try:
                await self.flush()
            except Fenced:
                log.error("journal fenced: another writer took over; stopping")
                return
            except Exception as error:  # a failed flush already failed its waiters
                log.error("journal flush failed: %s", error)

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
            snap = _dumps(
                {"seq": self.seq, "writer": self.writer, "at": self.clock(), "state": self._snapshot()}
            )
            await self._write_checkpoint(self.seq, snap)
