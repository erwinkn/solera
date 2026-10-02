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
import re
import struct
from collections.abc import Callable
from dataclasses import dataclass

from .. import _native
from .cache import Corrupt, EngineCache
from .index import IndexState, Options
from .io import ObjectIO

VERSION = 1
CONTENT_TYPE = f"application/vnd.solera.resolve; version={VERSION}"


class UnsupportedVersion(ValueError):
    pass


def frame(header: dict, payloads: list[bytes]) -> bytes:
    head = json.dumps(header, separators=(",", ":")).encode()
    return bytes([VERSION]) + struct.pack("<I", len(head)) + head + b"".join(payloads)


def unframe(body: bytes) -> tuple[dict, memoryview]:
    if not body or body[0] != VERSION:
        raise UnsupportedVersion(f"resolve protocol version {body[0] if body else None}")
    (n,) = struct.unpack_from("<I", body, 1)
    return json.loads(body[5 : 5 + n]), memoryview(body)[5 + n :]


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


@dataclass
class Limits:
    max_keys: int = 100_000
    max_bytes: int = 16 * 2**20
    max_entries: int = 2_000_000
    queue_bytes: int = 64 * 2**20
    concurrency: int = 2


class Resolver:
    """`resolve(attempt, body, prepared)`: `prepared(name)` returns the
    output's `Prepared`, or None when the attempt does not hold it."""

    def __init__(self, cache: EngineCache, io: ObjectIO, options: Options | None = None, limits=None):
        self.cache, self.io = cache, io
        self.o = options or Options()
        self.limits = limits or Limits()
        self._sem = asyncio.Semaphore(self.limits.concurrency)
        self._queued = 0
        self._inflight: dict[tuple, asyncio.Future] = {}
        self._fills: set[asyncio.Task] = set()

    async def resolve(
        self, attempt: str, body: bytes, prepared: Callable[[str], Prepared | None], live: Callable[[], bool]
    ) -> bytes:
        header, payloads = unframe(body)
        out_header, out_payloads, offset = [], [], 0
        for o in header.get("outputs") or []:
            data = bytes(payloads[o["offset"] : o["offset"] + o["size"]])
            answer, delta = await self._one(attempt, header.get("invocation"), o, data, prepared, live)
            if delta is not None:
                answer.update(offset=offset, size=len(delta))
                out_payloads.append(delta)
                offset += len(delta)
            out_header.append({"name": o.get("name"), **answer})
        return frame({"outputs": out_header}, out_payloads)

    async def _one(self, attempt, invocation, o, data, prepared, live):
        declined = {"result": "declined"}
        p = prepared(o.get("name"))
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
        lim = self.limits
        if len(data) > lim.max_bytes or (kind == "patch" and o.get("keys", 0) > lim.max_keys):
            return {**declined, "reason": "too_big"}, None
        if kind == "replace" and sum(f.entries for f in p.index.files) + o.get("keys", 0) > lim.max_entries:
            return {**declined, "reason": "too_big"}, None
        key = (
            attempt,
            invocation,
            o.get("name"),
            p.scope,
            kind,
            p.batch,
            p.generation,
            p.index.prefix,
            p.head_batch,
            o.get("digest"),
        )
        fut = self._inflight.get(key)
        if fut is None:
            if self._queued + len(data) > lim.queue_bytes:
                return {**declined, "reason": "busy"}, None
            # The worker uploads the delta under its own name: kept as a candidate under it.
            path = p.index.path(f"{p.batch:012d}-{attempt}.0000")
            fut = self._inflight[key] = asyncio.ensure_future(self._compute(p, kind, data, live, path))
            fut.add_done_callback(lambda _f: self._inflight.pop(key, None))
        return await asyncio.shield(fut)

    async def compute(self, p: Prepared, kind: str, data: bytes, path: str):
        """One resolve with no request around it — a source commit, in the engine:
        `(answer, delta)` as for an output of a request."""

        return await self._compute(p, kind, data, lambda: True, path)

    async def _compute(self, p: Prepared, kind: str, data: bytes, live, path: str):
        declined = {"result": "declined"}
        pin = self.cache.pin(p.index)
        if pin is None:
            self._background_fill(p.index)
            return {**declined, "reason": "cold"}, None
        self._queued += len(data)
        try:
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
                        self._background_fill(p.index)
                        return {**declined, "reason": "cold"}, None
        finally:
            self._queued -= len(data)
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

    def _background_fill(self, index: IndexState) -> None:
        async def fill():
            try:
                await self.cache.fill(self.io, index)
            except Corrupt:
                pass

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
