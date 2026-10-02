"""Index reads answered ahead (docs/resolved-commits.md §7.1).

The engine runs an attempt's input reads — the worker's own code — over
its cache's local copies, with a recording `Reads` on the `ObjectIO`:
every `KeyIndex` call (`page`, `pending`, `lookup`), its arguments and
its result are kept, up to the bounds. The record rides the `start` reply;
the worker runs the same code with the record on its `ObjectIO`, and a
call found in it for the same pinned index is answered from it — any
other reads the store.

A call names its index by the digest of its pinned state, so an answer
is only ever used for the snapshot it was read from. Results travel as
`.kx` files (keys, versions, deletions, locators) — the format resolves
use, checked as it is decoded.
"""

from __future__ import annotations

import base64
import json

from .. import _native
from .._native import SortedRun, encode_file

VERSION = 1


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

    def __init__(self, *, recording: bool = False, max_entries: int = 0, max_bytes: int = 0):
        self.recording = recording
        self.max_entries, self.max_bytes = max_entries, max_bytes
        self.entries = self.bytes = 0
        self.calls: dict[str, dict] = {}

    def __len__(self) -> int:
        return len(self.calls)

    # -- the engine's side -----------------------------------------------------------

    def record(self, identity: str, call: str, args: tuple, result) -> None:
        """Keep `result`; `Full` once it would pass the bounds — its entries
        checked before anything is encoded, its bytes after."""

        entries = len(result) if call == "lookup" else len(result[0])
        if self.entries + entries > self.max_entries:
            raise Full(call)
        if call == "lookup":
            keys = sorted(result)
            run = encode_file(
                keys,
                [result[k][0] for k in keys],
                bytes(len(keys)),
                locators=[result[k][1] for k in keys],
            )
            nxt = None
        elif call == "page":
            keys, versions, locators, nxt = result
            run = encode_file(keys, versions, bytes(len(keys)), locators=locators)
        else:
            keys, versions, deleted, locators, nxt = result
            run = encode_file(keys, versions, deleted, locators=locators)
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
            keys, versions, deleted, locators = SortedRun.decode(c["run"]).entries()
        except ValueError:
            return None  # read from the store instead
        if call == "lookup":
            return {k: (v, loc) for k, v, loc in zip(keys, versions, locators, strict=True)}
        if call == "page":
            return keys, versions, locators, c["next"]
        return keys, versions, deleted, locators, c["next"]
