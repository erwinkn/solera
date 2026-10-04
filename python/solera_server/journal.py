"""The engine's durable state on an object store alone
(docs/object-store-state.md §0, §10; docs/journal-object.md).

Current state lives in memory. Every change to it is an event. The journal
is one object, swapped with `If-Match` on every write (`solera.objects.swap`):
the id of the engine that writes it, the checkpoint it extends, and every
event since that checkpoint. A checkpoint is the whole state, written now
and then under a fresh name.

    {prefix}/journal.json                    {"checkpoint", "engine", "events": [...]}
    {prefix}/checkpoints/{engine}-{n:06d}.json   {"at", "engine", "state": {...}}

**Opening.** Read the journal, load the checkpoint it names and apply its
events; then fence: swap in the same journal under this engine's own id, on
the ETag read. A checkpoint gone, or a conflict, means another engine moved
on meanwhile: read again.

**Fencing.** Every write is a swap on the ETag of this engine's last one,
and no body repeats — each names its engine and adds events or a newer
checkpoint — so once another engine has written, the next swap raises
`Conflict`: the journal is `stopped` (`Fenced`), and every later append
fails. So is a state no checkpoint can hold: no retry would write it.

**Durability.** `append` applies nothing; the caller applies an event to
memory and appends it, and a background flusher swaps in the journal with
what is buffered once `flush_interval` has passed or `max_buffer` bytes are
buffered. What acts on the outside world on the strength of an event awaits
`durable()`, which flushes at once. A flush is sealed before it is written:
a failed one is retried with the very same bytes, which `swap` takes for
its own if an earlier try landed unheard.

**Checkpoints.** Once the journal's events reach max(`min_checkpoint`, a
sixteenth of the last checkpoint's size), and on a clean close: list the
checkpoints, write the state as of the last flush under a fresh name, read
it back byte for byte, move the journal to it (no events), and only then
delete what was listed. Listing after the move could delete a newer engine's checkpoint,
not yet named; one not read back could be named and unreadable.

The journal, its events and checkpoints are encoded with orjson, keys
sorted: the same state gives the same bytes. An event is encoded before
anything is applied, so one the encoding cannot hold exactly is refused
then, not at the next checkpoint.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass

import obstore
import orjson
from obstore.exceptions import NotFoundError
from solera.objects import Conflict, create, read, swap

log = logging.getLogger(__name__)


class Stopped(RuntimeError):
    """This journal writes no more; its engine must stop."""


class Fenced(Stopped):
    """Another engine took over this namespace."""


class JournalCorrupt(RuntimeError):
    """The journal, or the checkpoint it names, cannot be read."""


def encode(event: dict) -> bytes:
    """An event as the journal keeps it, encoded as checkpoints are: what an
    event brings into the model, a checkpoint holds. Raises `TypeError` for
    what JSON cannot hold at all, and `ValueError` for what the encoding
    cannot hold exactly — `inf` and `nan` (orjson writes them as null),
    integers past 64 bits, keys that are not strings — before anything was
    applied."""

    json.dumps(event, allow_nan=False)  # TypeError; ValueError for inf and nan
    try:
        return _dumps(event)
    except orjson.JSONEncodeError as error:
        raise ValueError(f"the journal cannot hold this event: {error}") from None


def _dumps(value) -> bytes:
    """The journal's and the checkpoints' encoding: keys sorted, so a state
    encodes to the same bytes every time (`swap` reads back by bytes)."""

    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS)


@dataclass
class OpenResult:
    engine: str | None  # this engine's id: None when read-only
    replayed: int  # events applied on top of the checkpoint
    checkpoint: str | None  # the checkpoint loaded, if any


class Journal:
    def __init__(
        self,
        store,
        prefix: str = "control",
        *,
        flush_interval: float = 1.0,
        max_buffer: int = 1 << 20,
        min_checkpoint: int = 64 << 10,
        clock: Callable[[], float] = time.time,
    ):
        self.store = store
        self.prefix = prefix.strip("/")
        self.flush_interval = flush_interval
        self.max_buffer = max_buffer
        self.min_checkpoint = min_checkpoint
        self.clock = clock

        self.engine: str | None = None
        self.checkpoint: str | None = None  # the checkpoint the journal names
        self.stopped: Stopped | None = None  # why this journal writes no more
        self.ended = asyncio.Event()  # set with `stopped`: what its engine halts on at once
        self._etag: str | None = None  # of this engine's last write
        self._events: list[bytes] = []  # encoded, every event since `checkpoint`: the journal's
        self._events_bytes = 0
        self._buffer: list[bytes] = []  # encoded events not yet written
        self._buffer_bytes = 0
        self._first_buffered: float | None = None
        self.appended = self.written = 0  # events this engine appended, and wrote
        self._waiters: list[tuple[int, asyncio.Future]] = []  # (events appended, waiter)
        self._urgent = False  # someone waits on `durable()`
        self._sealed: tuple | None = None  # (body, events, count, snapshot): written next, as is
        self._move: tuple | None = None  # (name, size, listed): a checkpoint's move, landed or not
        self._flushing = asyncio.Lock()
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._snapshot: Callable[[], dict] | None = None
        self._last_checkpoint_size = 0
        self._checkpoints = 0  # this engine's, for their names

    @property
    def _journal(self) -> str:
        return f"{self.prefix}/journal.json"

    def _body(self, engine: str | None, checkpoint: str | None, events: list[bytes]) -> bytes:
        head = _dumps({"checkpoint": checkpoint, "engine": engine})  # sorts before "events"
        return head[:-1] + b',"events":[' + b",".join(events) + b"]}"

    # -- opening ----------------------------------------------------------------------

    async def open(
        self,
        restore: Callable[[dict | None], None],
        apply: Callable[[dict], None],
        snapshot: Callable[[], dict],
        *,
        writer: bool = True,
    ) -> OpenResult:
        """Read the journal, load its checkpoint and apply its events, then
        fence: from here on this process is the only writer. `writer=False`
        opens read-only: nothing is fenced and appends fail — for tools that
        inspect a namespace a server may be writing."""

        self._snapshot = snapshot
        while True:
            found = await read(self.store, self._journal)
            try:
                body = orjson.loads(found[0]) if found is not None else {"checkpoint": None, "events": []}
            except orjson.JSONDecodeError as error:
                raise JournalCorrupt(f"{self._journal}: {error}") from None
            checkpoint = body["checkpoint"]
            try:
                state = await self._load(checkpoint) if checkpoint is not None else None
            except (NotFoundError, FileNotFoundError):
                log.warning("checkpoint %s is gone: the journal moved on; reading it again", checkpoint)
                continue
            # Encoded before the model applies them: it keeps what it applies, and later
            # events change it — the journal must keep what was recorded.
            events = [encode(e) for e in body["events"]]
            restore(state)
            for event in body["events"]:
                apply(event)
            self.checkpoint, self._events = checkpoint, events
            self._events_bytes = sum(len(e) for e in self._events)
            replayed = len(self._events)
            if not writer:
                self.stopped = Stopped("opened read-only")
                self.ended.set()
                return OpenResult(engine=None, replayed=replayed, checkpoint=checkpoint)
            engine = secrets.token_hex(8)  # no two processes share one (§10)
            try:
                self._etag = await swap(
                    self.store,
                    self._journal,
                    self._body(engine, checkpoint, self._events),
                    found and found[1],
                )
            except Conflict:
                log.warning("another engine wrote the journal while this one opened; reading it again")
                continue
            self.engine = engine
            break
        self._task = asyncio.create_task(self._run())
        return OpenResult(engine=self.engine, replayed=replayed, checkpoint=checkpoint)

    async def _load(self, name: str) -> dict:
        got = await obstore.get_async(self.store, f"{self.prefix}/checkpoints/{name}.json")
        data = bytes(await got.bytes_async())
        try:
            body = orjson.loads(data)
        except orjson.JSONDecodeError as error:
            raise JournalCorrupt(f"checkpoint {name}: {error}") from None
        self._last_checkpoint_size = len(data)
        return body["state"]

    # -- appending ----------------------------------------------------------------------

    def stop_checkpoints(self) -> None:
        """Take no checkpoint from now on: the state it would snapshot is no
        longer the fold of the journal."""

        self._snapshot = None

    def append(self, *events: bytes, lazy: bool = False) -> None:
        """Queue encoded events (`encode`) for the next flush. A `lazy` event
        does not start the flush clock: it is written with whatever comes
        next, or by `durable()` (docs/lifecycle.md §13)."""

        if self.stopped:
            raise self._stop()
        for event in events:
            self._buffer.append(event)
            self._buffer_bytes += len(event)
        self.appended += len(events)
        if lazy:
            return
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
        if self.stopped:
            raise self._stop()
        if self._task is None:  # closed: nothing flushes in the background
            await self.flush()
            return
        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append((target, waiter))
        self._urgent = True
        self._wake.set()
        await waiter

    async def flush(self) -> None:
        """Swap in the journal with everything buffered (and take a
        checkpoint if one is due). A flush is sealed before it is written: if
        the write fails or is interrupted, the next flush writes it again,
        byte for byte."""

        async with self._flushing:
            if self._move is not None and not self.stopped:
                await self._finish_move()  # pending since an error: before anything else
            while self._sealed is not None or self._buffer:
                if self.stopped:
                    self._fail(self._stop())
                    return
                if self._sealed is None:
                    try:
                        self._seal()
                    except Exception as error:  # the state cannot be encoded: no retry changes that
                        stopped = Stopped(f"the state cannot be written ({error})")
                        self._fail(stopped)
                        raise stopped from error
                body, events, count, snap = self._sealed
                try:
                    self._etag = await swap(self.store, self._journal, body, self._etag)
                except Conflict:
                    self._sealed = None
                    error = Fenced("another engine wrote the journal")
                    self._fail(error)
                    raise error from None
                self._sealed = None
                self._events, self._events_bytes = events, sum(len(e) for e in events)
                self.written += count
                waiting = []
                for target, waiter in self._waiters:
                    if target > self.written:
                        waiting.append((target, waiter))
                    elif not waiter.done():
                        waiter.set_result(None)
                self._waiters = waiting
                if snap is not None:
                    await self._take_checkpoint(snap)

    def _seal(self) -> None:
        """Seal what is buffered, and the state as of it if a checkpoint is
        due. Everything is encoded before the buffer is taken: one that
        fails leaves the buffer as it was."""

        new = self._buffer
        events = self._events + new
        # Memory now reflects exactly these events, so a snapshot taken here is
        # the state as of this flush — before any later event is applied.
        size = sum(len(e) for e in events)
        due = self._snapshot is not None and size >= max(
            self.min_checkpoint, self._last_checkpoint_size // 16
        )
        snap = _dumps({"at": self.clock(), "engine": self.engine, "state": self._snapshot()}) if due else None
        body = self._body(self.engine, self.checkpoint, events)
        self._buffer, self._buffer_bytes, self._first_buffered, self._urgent = [], 0, None, False
        self._sealed = (body, events, len(new), snap)

    async def _take_checkpoint(self, data: bytes) -> None:
        """List, write, read back, move the journal, then delete what was
        listed — in that order (docs/object-store-state.md §10)."""

        listed = [
            meta["path"]
            for batch in obstore.list(self.store, prefix=f"{self.prefix}/checkpoints/")
            for meta in batch
            if meta["path"].endswith(".json")
        ]
        self._checkpoints += 1
        name = f"{self.engine}-{self._checkpoints:06d}"
        path = f"{self.prefix}/checkpoints/{name}.json"
        await create(self.store, path, data)  # a fresh name; a retry finds its own bytes
        # Read back byte for byte: the encoding is deterministic and came from a
        # valid state, so the same bytes are a checkpoint that parses.
        try:
            got = await obstore.get_async(self.store, path)
            back = bytes(await got.bytes_async())
        except Exception as error:  # the journal stays: the next due point tries again
            log.warning("checkpoint %s does not read back (%s); keeping the journal", name, error)
            return
        if back != data:
            log.warning("checkpoint %s reads back other bytes; keeping the journal", name)
            return
        self._move = (name, len(data), listed)
        await self._finish_move()

    async def _finish_move(self) -> None:
        """Move the journal to the checkpoint written, then delete what was
        listed before. An error but a conflict leaves the move pending: it is
        written again, the very same body, before anything else — `swap`
        takes its own bytes for a move that landed unheard."""

        name, size, listed = self._move
        try:
            self._etag = await swap(self.store, self._journal, self._body(self.engine, name, []), self._etag)
        except Conflict:
            self._move = None
            self._fail(Fenced("another engine wrote the journal"))
            return  # stopped: deletes nothing
        self._move = None
        self.checkpoint, self._events, self._events_bytes = name, [], 0
        self._last_checkpoint_size = size
        for i in range(0, len(listed), 1000):
            with contextlib.suppress(NotFoundError, FileNotFoundError):
                await obstore.delete_async(self.store, listed[i : i + 1000])

    def _stop(self) -> Stopped:
        """A fresh error for why this journal stopped, to raise."""

        return type(self.stopped)(*self.stopped.args)

    def _fail(self, error: Stopped) -> None:
        self.stopped = error
        self.ended.set()
        for _, waiter in self._waiters:
            if not waiter.done():
                waiter.set_exception(error)
        self._waiters, self._buffer, self._buffer_bytes, self._first_buffered = [], [], 0, None

    # -- the flusher ----------------------------------------------------------------------

    async def _run(self) -> None:
        """Flush once the oldest buffered event has waited `flush_interval`,
        `max_buffer` bytes are buffered, or someone waits on `durable()`. A
        failed write is retried after `flush_interval`."""

        loop = asyncio.get_running_loop()
        while not self.stopped:
            self._wake.clear()
            if self._sealed is None and self._move is None:
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
            except Stopped as error:
                log.critical("journal stopped: %s", error)
                return
            except Exception as error:
                log.error("journal flush failed, retrying: %s", error)
                await asyncio.sleep(self.flush_interval)

    async def close(self, *, checkpoint: bool = True) -> None:
        """Flush what is buffered, take a final checkpoint, stop the flusher."""

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self.stopped or self.engine is None:
            return
        await self.flush()
        if checkpoint and self._snapshot is not None and self._events and not self.stopped:
            async with self._flushing:
                state = self._snapshot()
                await self._take_checkpoint(
                    _dumps({"at": self.clock(), "engine": self.engine, "state": state})
                )
