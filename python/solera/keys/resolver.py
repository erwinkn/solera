"""The HTTP resolver's core (docs/resolved-commits.md §4): framing, admission,
deduplication and the resolve itself, over the engine cache.

The route and its authentication belong to the attempt channel
(`lifecycle.md` §5); this module takes a request body and the attempt's
preparation and returns a response body. It persists nothing: an answer is
a pure function of the pinned snapshot and the request, so a retry is
recomputed, or shares a computation still in flight.

Framing, both ways: `u8` protocol version · `u32` header length (little
endian) · JSON header · payloads, each output's at `offset` (from the end
of the header), `size` bytes long — a `.kx` file.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import struct
from collections.abc import Callable
from dataclasses import dataclass

from .. import _native
from . import FOOTER_SIZE, parse_footer
from .cache import EngineCache
from .index import IndexState, Options
from .io import ObjectIO

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


def _outputs(header: dict, payloads: memoryview) -> list[tuple[dict, bytes]]:
    """Each output's header and payload, every bound checked."""

    outputs, names = header.get("outputs"), set()
    if not isinstance(outputs, list):
        raise Malformed("outputs is not a list")
    out = []
    for o in outputs:
        if not isinstance(o, dict) or not isinstance(o.get("name"), str) or o["name"] in names:
            raise Malformed("an output without a name of its own")
        names.add(o["name"])
        offset, size = o.get("offset"), o.get("size")
        if not (isinstance(offset, int) and isinstance(size, int) and 0 <= offset and 0 <= size):
            raise Malformed(f"{o['name']}: bad offset or size")
        if offset + size > len(payloads):
            raise Malformed(f"{o['name']}: payload past the end of the body")
        out.append((o, bytes(payloads[offset : offset + size])))
    return out


@dataclass(frozen=True)
class Prepared:
    """What the engine prepared for one output of a live attempt: the
    request is checked against it, never trusted."""

    scope: str
    batch: int
    generation: int
    index: IndexState  # the index the engine holds for the scope now
    head_batch: int
    replace: bool  # whether a replacement is allowed
    position: float = math.inf  # the event position the index was read at: a fill's reader pin


@dataclass
class Limits:
    max_keys: int = 100_000
    max_bytes: int = 16 * 2**20
    max_entries: int = 2_000_000
    queue_bytes: int = 64 * 2**20
    concurrency: int = 2


class Resolver:
    """`resolve(attempt, body, prepared)`: `prepared(name)` returns the
    output's `Prepared`, or None when the attempt does not hold it. `holds`,
    when given, keeps fills in collection's reader pins: `hold(position)`
    returns a token for `release`."""

    def __init__(
        self, cache: EngineCache, io: ObjectIO, options: Options | None = None, limits=None, holds=None
    ):
        self.cache, self.io, self.holds = cache, io, holds
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
            answer, delta = await self._one(attempt, header.get("invocation"), o, data, prepared, live)
            if delta is not None:
                answer.update(offset=offset, size=len(delta))
                out_payloads.append(delta)
                offset += len(delta)
            out_header.append({"name": o["name"], **answer})
        return frame({"outputs": out_header}, out_payloads)

    async def _one(self, attempt, invocation, o, data, prepared, live):
        declined = {"result": "declined"}
        p = prepared(o["name"])
        if p is None or not live():
            return {**declined, "reason": "not_live"}, None
        kind = o.get("kind")
        if (
            o.get("scope") != p.scope
            or o.get("batch") != p.batch
            or o.get("generation") != p.generation
            or (o.get("base") or {}).get("prefix") != p.index.prefix
            or kind not in ("patch", "replace")
            or (kind == "replace" and not p.replace)
        ):
            return {**declined, "reason": "invalid"}, None
        if (o.get("base") or {}).get("head_batch") != p.head_batch:
            return {**declined, "reason": "stale"}, None
        # What the payload is, from its bytes, not from what the header says.
        actual = _native.content_digest(data)
        try:
            entries = parse_footer(data[-FOOTER_SIZE:])["entries"] if data else -1
        except ValueError:
            entries = -1
        if o.get("digest") != actual or o.get("keys") != entries:
            return {**declined, "reason": "invalid"}, None
        lim = self.limits
        if len(data) > lim.max_bytes or (kind == "patch" and entries > lim.max_keys):
            return {**declined, "reason": "too_big"}, None
        if kind == "replace" and sum(f.entries for f in p.index.files) + entries > lim.max_entries:
            return {**declined, "reason": "too_big"}, None
        key = (
            attempt,
            invocation,
            o["name"],
            p.scope,
            kind,
            p.batch,
            p.generation,
            p.index.prefix,
            p.head_batch,
            actual,
        )
        fut = self._inflight.get(key)
        if fut is None:
            if not self._reserve(len(data)):
                return {**declined, "reason": "busy"}, None
            # The worker uploads the delta under its own name: kept as a candidate under it.
            path = p.index.path(f"{p.batch:012d}-{attempt}.0000")
            fut = self._inflight[key] = asyncio.ensure_future(self._compute(p, kind, data, live, path))

            size = len(data)

            def done(_f, key=key, size=size):
                self._inflight.pop(key, None)
                self._release(size)

            fut.add_done_callback(done)
        return await asyncio.shield(fut)

    async def compute(self, p: Prepared, kind: str, data: bytes, path: str):
        """One resolve with no request around it — a source commit, in the engine:
        `(answer, delta)` as for an output of a request, under the same limits."""

        if not self._reserve(len(data)):
            return {"result": "declined", "reason": "busy"}, None
        try:
            return await self._compute(p, kind, data, lambda: True, path)
        finally:
            self._release(len(data))

    async def _compute(self, p: Prepared, kind: str, data: bytes, live, path: str):
        declined = {"result": "declined"}
        pin = self.cache.pin(p.index)
        if pin is None:
            self._background_fill(p.index, p.position)
            return {**declined, "reason": "cold"}, None
        with pin:
            async with self._sem:
                if not live():
                    return {**declined, "reason": "not_live"}, None
                snap = _native.Snapshot(pin.runs)
                try:
                    files, added, removed, changed = await asyncio.to_thread(
                        snap.resolve,
                        data,
                        replace=kind == "replace",
                        generation=p.generation,
                        **_writer(self.o, self.limits.max_bytes),
                    )
                except ValueError as e:
                    bad = re.match(r"local file (\S+): ", str(e))
                    if bad is None:
                        return {**declined, "reason": "invalid"}, None
                    self.cache.corrupt(bad.group(1))  # refetched by the fill
                    self._background_fill(p.index, p.position)
                    return {**declined, "reason": "cold"}, None
        if not files:
            return {"result": "empty"}, None
        if len(files) > 1 or len(files[0]) > self.limits.max_bytes:
            return {**declined, "reason": "too_big"}, None
        delta = files[0]
        return {
            "result": "delta",
            "added": added,
            "removed": removed,
            "entries": added + removed + changed,
            "file": {"size": len(delta), "digest": self.cache.offer(path, delta)},
        }, delta

    def _background_fill(self, index: IndexState, position: float) -> None:
        """Fill a cold index, a reader of its files until every fetch is done:
        collection keeps what it reads (taken now, while the request that
        found it cold still holds its own)."""

        token = self.holds.hold(position) if self.holds is not None else None

        async def fill():
            try:
                await self.cache.fill(self.io, index)
            except Exception as e:  # the next resolve declines and asks again
                log.warning("key cache fill of %s: %s", index.prefix, e)
            finally:
                if token is not None:
                    self.holds.release(token)

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
    """One output to resolve: its run is a `.kx` file of the sorted entries."""

    name: str
    scope: str
    kind: str  # "patch" or "replace"
    batch: int
    generation: int
    prefix: str
    head_batch: int
    run: bytes
    keys: int


def request(invocation: str, asks: list[Ask]) -> bytes:
    outputs, offset = [], 0
    for a in asks:
        outputs.append(
            {
                "name": a.name,
                "scope": a.scope,
                "kind": a.kind,
                "batch": a.batch,
                "generation": a.generation,
                "base": {"prefix": a.prefix, "head_batch": a.head_batch},
                "keys": a.keys,
                "offset": offset,
                "size": len(a.run),
                "digest": _native.content_digest(a.run),
            }
        )
        offset += len(a.run)
    return frame({"invocation": invocation, "outputs": outputs}, [a.run for a in asks])


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
