"""The HTTP resolver's core (docs/resolved-commits.md §4): framing, admission,
deduplication and the resolve itself, over the engine cache.

The route and its authentication belong to the attempt channel
(`lifecycle.md` §5); this module takes a request body and the attempt's
preparation and returns a response body. It persists nothing: an answer is
a pure function of the pinned snapshot and the request, so a retry is
recomputed, or shares a computation still in flight.

Framing, both ways: `u8` protocol version · `u32` header length (little
endian) · JSON header · payloads, each output's at `offset` (from the end
of the header), `size` bytes long — a `.kx` file. Payloads lie back to back
in output order, so no byte is two outputs'.

Nothing in a request is taken on its word: a run is decoded once, every
fact checked (`SortedEntries.decode`), and the limits apply to what it holds.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import struct
from collections.abc import Callable
from dataclasses import dataclass

from .. import _native
from .._native import LimitError, LocalError, SortedEntries
from .cache import EngineCache
from .index import IndexState, Options
from .io import ObjectIO
from .threads import in_thread

log = logging.getLogger(__name__)

VERSION = 1
CONTENT_TYPE = f"application/vnd.solera.resolve; version={VERSION}"


class UnsupportedVersion(ValueError):
    pass


class Malformed(ValueError):
    """A body whose framing or payloads do not hold together."""


MAX_BODY = 64 * 2**20  # a request's bytes, all its outputs' runs together


def frame(header: dict, payloads: list[bytes]) -> bytes:
    head = json.dumps(header, separators=(",", ":")).encode()
    return bytes([VERSION]) + struct.pack("<I", len(head)) + head + b"".join(payloads)


def unframe(body: bytes) -> tuple[dict, memoryview]:
    if not body or body[0] != VERSION:
        raise UnsupportedVersion(f"resolve protocol version {body[0] if body else None}")
    if len(body) < 5:
        raise Malformed("truncated frame")
    (n,) = struct.unpack_from("<I", body, 1)
    if 5 + n > len(body):
        raise Malformed("header past the end of the body")
    try:
        header = json.loads(body[5 : 5 + n])
    except ValueError as e:
        raise Malformed(f"header: {e}") from e
    if not isinstance(header, dict):
        raise Malformed("header is not an object")
    return header, memoryview(body)[5 + n :]


def _outputs(header: dict, payloads: memoryview) -> list[tuple[dict, memoryview]]:
    """Each output's header and payload, every bound checked: the payloads
    lie back to back, in output order, and fill the body."""

    outputs, names = header.get("outputs"), set()
    if not isinstance(outputs, list):
        raise Malformed("outputs is not a list")
    out, at = [], 0
    for o in outputs:
        if not isinstance(o, dict) or not isinstance(o.get("name"), str) or o["name"] in names:
            raise Malformed("an output without a name of its own")
        names.add(o["name"])
        offset, size = o.get("offset"), o.get("size")
        if not (isinstance(offset, int) and isinstance(size, int) and 0 <= size):
            raise Malformed(f"{o['name']}: bad offset or size")
        if offset != at or offset + size > len(payloads):
            raise Malformed(f"{o['name']}: a payload not right after the one before")
        out.append((o, payloads[offset : offset + size]))
        at += size
    if at != len(payloads):
        raise Malformed("bytes past the last payload")
    return out


@dataclass(frozen=True)
class Prepared:
    """What the engine prepared for one output of a live attempt: the
    request is checked against it, never trusted."""

    partition: str
    commit_number: int
    generation: int
    index: IndexState  # the index the engine holds for the partition now
    head_commit: int
    replace: bool  # whether a replacement is allowed
    at: float = math.inf  # the event counter the index was read at: a fill's reader pin


@dataclass
class Limits:
    max_keys: int = 100_000  # a patch's entries
    max_bytes: int = 16 * 2**20  # a run's bytes, and a delta's
    max_decoded: int = 64 * 2**20  # a run's bytes decoded: decompressed, and its keys and payloads
    max_entries: int = 2_000_000  # a replacement's entries plus the index's
    queue_bytes: int = 64 * 2**20
    concurrency: int = 2


class Resolver:
    """`resolve(attempt, body, prepared)`: `prepared(name)` returns the
    output's `Prepared`, or None when the attempt does not hold it. `pins`,
    when given, keeps fills in collection's reader pins: `pin(at)`
    returns a token for `unpin`."""

    def __init__(
        self, cache: EngineCache, io: ObjectIO, options: Options | None = None, limits=None, pins=None
    ):
        self.cache, self.io, self.pins = cache, io, pins
        self.o = options or Options()
        self.limits = limits or Limits()
        self._sem = asyncio.Semaphore(self.limits.concurrency)
        self._queued = 0
        self._inflight: dict[tuple, asyncio.Future] = {}
        self._fills: set[asyncio.Task] = set()

    def _reserve(self, n: int) -> bool:
        """Room in the queue for `n` bytes, taken now: released by `_release`."""

        if self._queued + n > self.limits.queue_bytes:
            return False
        self._queued += n
        return True

    def _release(self, n: int) -> None:
        self._queued -= n

    async def admitted(self, n: int, work):
        """`work()` under the one admission rule of the engine's cache —
        resolves and start reads alike: `n` bytes of the queue, then one of
        `concurrency` turns, both held until it ends — or None when the queue
        is full (busy: the worker reads the store)."""

        if not self._reserve(n):
            return None
        try:
            async with self._sem:
                return await work()
        finally:
            self._release(n)

    async def resolve(
        self, attempt: str, body: bytes, prepared: Callable[[str], Prepared | None], live: Callable[[], bool]
    ) -> bytes:
        """The answer to a request. Raises `UnsupportedVersion` or `Malformed`."""

        if len(body) > MAX_BODY:
            raise Malformed(f"a body over {MAX_BODY} bytes")
        header, payloads = unframe(body)
        outputs = _outputs(header, payloads)
        out_header, out_payloads, offset = [], [], 0
        for o, data in outputs:
            answer, delta = await self._one(attempt, header.get("worker_id"), o, data, prepared, live)
            if delta is not None:
                answer.update(offset=offset, size=len(delta))
                out_payloads.append(delta)
                offset += len(delta)
            out_header.append({"name": o["name"], **answer})
        return frame({"outputs": out_header}, out_payloads)

    async def _one(self, attempt, worker_id, o, view: memoryview, prepared, live):
        declined = {"result": "declined"}
        p = prepared(o["name"])
        if p is None or not live():
            return {**declined, "reason": "not_live"}, None
        kind, keys = o.get("kind"), o.get("keys")
        if (
            o.get("partition") != p.partition
            or o.get("commit_number") != p.commit_number
            or o.get("generation") != p.generation
            or (o.get("base") or {}).get("prefix") != p.index.prefix
            or kind not in ("patch", "replace")
            or (kind == "replace" and not p.replace)
            or not isinstance(keys, int)
        ):
            return {**declined, "reason": "invalid"}, None
        if (o.get("base") or {}).get("head_commit") != p.head_commit:
            return {**declined, "reason": "stale"}, None
        if len(view) > self.limits.max_bytes:
            return {**declined, "reason": "too_big"}, None
        data = bytes(view)  # an output the attempt holds: its payload, copied once
        actual = _native.content_digest(data)
        if o.get("digest") != actual:
            return {**declined, "reason": "invalid"}, None
        key = (
            attempt,
            worker_id,
            o["name"],
            p.partition,
            kind,
            p.commit_number,
            p.generation,
            p.index.prefix,
            p.head_commit,
            actual,
            keys,
        )
        fut = self._inflight.get(key)
        if fut is None:
            if not self._reserve(len(data)):
                return {**declined, "reason": "busy"}, None
            # The worker uploads the delta under its own name: kept as a candidate under it.
            path = p.index.path(f"{p.commit_number:012d}-{attempt}.0000")
            fut = self._inflight[key] = asyncio.ensure_future(self._compute(p, kind, data, live, path, keys))

            size = len(data)

            def done(_f, key=key, size=size):
                self._inflight.pop(key, None)
                self._release(size)

            fut.add_done_callback(done)
        return await asyncio.shield(fut)

    async def compute(self, p: Prepared, kind: str, run: SortedEntries, path: str):
        """One resolve with no request around it — a source commit, in the engine:
        `(answer, delta)` as for an output of a request, under the same limits."""

        if not self._reserve(run.nbytes):
            return {"result": "declined", "reason": "busy"}, None
        try:
            return await self._compute(p, kind, run, lambda: True, path)
        finally:
            self._release(run.nbytes)

    async def _compute(self, p: Prepared, kind: str, run, live, path: str, keys: int | None = None):
        """The answer for `run` — a `SortedEntries`, or a request's `.kx` payload
        claiming `keys` entries, decoded here — against `p`'s index."""

        declined = {"result": "declined"}
        lim = self.limits
        indexed = sum(f.entries for f in p.index.files)
        most = lim.max_keys if kind == "patch" else lim.max_entries - indexed
        local = self.cache.open(p.index)
        if local is None:
            self._background_fill(p.index, p.at)
            return {**declined, "reason": "cold"}, None
        with local:
            async with self._sem:
                if not live():
                    return {**declined, "reason": "not_live"}, None
                if isinstance(run, bytes):
                    try:
                        run = await in_thread(
                            SortedEntries.decode, run, max_entries=max(most, 0), max_bytes=lim.max_decoded
                        )
                    except LimitError:
                        return {**declined, "reason": "too_big"}, None
                    except ValueError:  # malformed, or a checksum fails
                        return {**declined, "reason": "invalid"}, None
                    if len(run) != keys:
                        return {**declined, "reason": "invalid"}, None
                if len(run) > most:
                    return {**declined, "reason": "too_big"}, None
                if kind == "replace" and run.removes:
                    return {**declined, "reason": "invalid"}, None
                snap = _native.Snapshot(local.runs)
                try:
                    files, added, removed, changed = await in_thread(
                        snap.resolve,
                        run,
                        replace=kind == "replace",
                        generation=p.generation,
                        **_writer(self.o, lim.max_bytes),
                    )
                except LocalError as e:
                    self.cache.corrupt(e.path)  # refetched by the fill
                    self._background_fill(p.index, p.at)
                    return {**declined, "reason": "cold"}, None
                except ValueError:
                    return {**declined, "reason": "invalid"}, None
        if not files:
            return {"result": "empty"}, None
        if len(files) > 1 or len(files[0]) > lim.max_bytes:
            return {**declined, "reason": "too_big"}, None
        delta = files[0]
        return {
            "result": "delta",
            "added": added,
            "removed": removed,
            "entries": added + removed + changed,
            "file": {"size": len(delta), "digest": self.cache.offer(path, delta)},
        }, delta

    def _background_fill(self, index: IndexState, at: float) -> None:
        """Fill a cold index, a reader of its files until every fetch is done:
        collection keeps what it reads (taken now, while the request that
        found it cold still holds its own)."""

        token = self.pins.pin(at) if self.pins is not None else None

        async def fill():
            try:
                await self.cache.fill(self.io, index)
            except Exception as e:  # the next resolve declines and asks again
                log.warning("key cache fill of %s: %s", index.prefix, e)
            finally:
                if token is not None:
                    self.pins.unpin(token)

        t = asyncio.ensure_future(fill())
        self._fills.add(t)
        t.add_done_callback(self._fills.discard)


def _writer(o: Options, max_file_bytes: int) -> dict:
    return {
        "block_size": o.block_size,
        "level": o.level,
        "bits_per_item": o.bits_per_item,
        "k": o.k,
        "max_file_bytes": max_file_bytes,
    }


# -- the worker's side ---------------------------------------------------------------------


@dataclass
class Ask:
    """One output to resolve: its sorted entries, sent as a `.kx` file."""

    name: str
    partition: str
    kind: str  # "patch" or "replace"
    commit_number: int
    generation: int
    prefix: str
    head_commit: int
    run: SortedEntries


def request(worker_id: str, asks: list[Ask]) -> bytes:
    outputs, offset, payloads = [], 0, []
    for a in asks:
        payload = a.run.encode()
        payloads.append(payload)
        outputs.append(
            {
                "name": a.name,
                "partition": a.partition,
                "kind": a.kind,
                "commit_number": a.commit_number,
                "generation": a.generation,
                "base": {"prefix": a.prefix, "head_commit": a.head_commit},
                "keys": len(a.run),
                "offset": offset,
                "size": len(payload),
                "digest": _native.content_digest(payload),
            }
        )
        offset += len(payload)
    return frame({"worker_id": worker_id, "outputs": outputs}, payloads)


def answers(body: bytes) -> dict[str, tuple[dict, bytes | None]]:
    """Per output: its answer, and for a `delta` the file, checked against its digest."""

    header, payloads = unframe(body)
    out = {}
    for o in header["outputs"]:
        delta = None
        if o["result"] == "delta":
            delta = bytes(payloads[o["offset"] : o["offset"] + o["size"]])
            if len(delta) != o["file"]["size"] or _native.content_digest(delta) != o["file"]["digest"]:
                o = {"result": "declined", "reason": "corrupt"}
                delta = None
        out[o.get("name")] = (o, delta)
    return out
