"""Index reads answered ahead (docs/resolved-commits.md §7).

The engine runs an attempt's input reads — the worker's own code — over
its cache's local copies, with a recording `Reads` on the `ObjectIO`:
every `KeyIndex` call (`page`, `pending`, `lookup`), its arguments and
its result are kept, up to the bounds. The record rides the `start` reply;
the worker runs the same code with the record on its `ObjectIO`, and a
call found in it for the same pinned index is answered from it — any
other reads the store.

A call names its index by the digest of its pinned state, so an answer
is only ever used for the snapshot it was read from. Results travel as
`.kx` files (keys, generations, deletions, payloads) — the format
resolves use, checked as it is decoded.
"""

from __future__ import annotations

import base64
import json

from .. import _native
from .._native import SortedEntries, encode_file

VERSION = 2


class Cold(Exception):
    """A recording read needs a file the engine does not hold."""


class Full(Exception):
    """A record reached its bounds."""


def _hex(b: bytes | None) -> str | None:
    return b.hex() if b is not None else None


def _unhex(s: str | None) -> bytes | None:
    return bytes.fromhex(s) if s is not None else None


def _key(identity: str, call: str, args: tuple) -> str:
    if call == "lookup":  # the keys, sorted and unique, by their digest
        (keys,) = args
        packed = b"".join(len(k).to_bytes(4, "little") + k for k in keys)
        args = (_native.content_digest(packed),)
    else:
        args = tuple(_hex(a) if isinstance(a, bytes) else a for a in args)
    return json.dumps([identity, call, args], separators=(",", ":"))


class Reads:
    """A record of index reads: `recording` on the engine, answering on the worker."""

    def __init__(
        self, *, recording: bool = False, max_entries: int = 0, max_bytes: int = 0, max_decoded: int = 0
    ):
        self.recording = recording
        self.max_entries, self.max_bytes = max_entries, max_bytes
        self.max_decoded = max_decoded or 4 * max_bytes  # keys and payloads read, before encoding
        self.entries = self.bytes = self.decoded = 0
        self.calls: dict[str, dict] = {}

    def __len__(self) -> int:
        return len(self.calls)

    # -- the engine's side -----------------------------------------------------------

    def admit(self, call: str, args: tuple) -> None:
        """`Full` before a call is read when what it may return — its limit, or
        the keys it looks up — cannot fit what is left of the record."""

        asked = len(args[0]) if call == "lookup" else args[-1]
        if self.entries + asked > self.max_entries:
            raise Full(call)

    def decoded_left(self) -> int:
        """Bytes of keys and payloads a read may still decode for this record."""

        return max(0, self.max_decoded - self.decoded)

    def record(self, identity: str, call: str, args: tuple, result, page=None) -> None:
        """Keep `result` — from the native `page` (a `SortedEntries`) when there is
        one, encoded as it is; `Full` once it would pass the bounds, its
        entries checked before anything is encoded, its bytes after."""

        entries = len(result) if call == "lookup" else len(result[0])
        if self.entries + entries > self.max_entries:
            raise Full(call)
        if page is not None:
            keys = result[0]
            run = page.encode()
            nxt = result[-1]
            self.decoded += page.nbytes
        elif call == "lookup":
            keys = sorted(result)
            run = encode_file(
                keys,
                [result[k][0] for k in keys],
                bytes(len(keys)),
                payloads=[result[k][1] for k in keys],
            )
            nxt = None
        elif call == "page":
            keys, generations, payloads, nxt = result
            run = encode_file(keys, generations, bytes(len(keys)), payloads=payloads)
        else:
            keys, generations, deleted, payloads, nxt = result
            run = encode_file(keys, generations, deleted, payloads=payloads)
        if self.entries + len(keys) > self.max_entries or self.bytes + len(run) > self.max_bytes:
            raise Full(call)
        self.entries += len(keys)
        self.bytes += len(run)
        self.calls[_key(identity, call, args)] = {"run": run, "next": nxt}

    def to_json(self) -> dict:
        return {
            "version": VERSION,
            "calls": [
                {"key": k, "run": base64.b64encode(c["run"]).decode(), "next": _hex(c["next"])}
                for k, c in self.calls.items()
            ],
        }

    # -- the worker's side ------------------------------------------------------------

    @classmethod
    def from_json(cls, d: dict | None) -> Reads | None:
        """The record a `start` reply carries; None for none, or one this
        worker cannot read."""

        if not d or d.get("version") != VERSION:
            return None
        reads = cls()
        for c in d.get("calls") or []:
            reads.calls[c["key"]] = {"run": base64.b64decode(c["run"]), "next": _unhex(c.get("next"))}
        return reads

    def answer(self, identity: str, call: str, args: tuple):
        """The recorded result of this call, as `KeyIndex` returns it, or None."""

        c = self.calls.get(_key(identity, call, args))
        if c is None:
            return None
        try:
            keys, generations, deleted, payloads = SortedEntries.decode(c["run"]).entries()
        except ValueError:
            return None  # read from the store instead
        if call == "lookup":
            return {k: (g, p) for k, g, p in zip(keys, generations, payloads, strict=True)}
        if call == "page":
            return keys, generations, payloads, c["next"]
        return keys, generations, deleted, payloads, c["next"]
