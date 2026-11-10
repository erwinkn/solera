"""A batch on the new tracking core (docs/tracking-design.md; build plan B5):
what a worker does differently when its spec says `"tracking": "core"`.

A keyed incremental input loads the keys its plan names: the plan's bytes
are in the spec, and `Tracking.plan_keys` reads each input's keys, classes
and versions out of them. A source served live may serve a key at another
version than planned, or not at all: the batch's classes follow what it
served, and each such key goes back in the result with the version it was
served at (`delivered[input]["loaded"]`), for the engine to record.

A keyed whole read names its objects from the output's layers in the change
index (`pages`).

A keyed output's write is stored as its store takes it, every key at the
attempt's generation, and the keys it changed go back to the engine, which
builds the commit (D207): in the result when small, else in one file beside
the attempt's control file (N13).
"""

from __future__ import annotations

import asyncio
from base64 import b64decode, b64encode

import obstore
from solera import lifecycle
from solera._native import Tracking, encode_changes
from solera.change_index import shared
from solera.keys.layers import key_str
from solera.stores import KeyedWrite, WriteError, prepare_for

CHANGES_INLINE = 64 << 10  # changed keys' bytes a result carries; past it, a file (N13)
CLASSES = ("added", "updated", "removed", "unchanged")


def on_core(spec: dict) -> bool:
    return spec.get("tracking") == "core"


def planned(spec: dict, pin: dict) -> tuple[list, list, list]:
    """A keyed input's keys in this batch, with their classes and the
    versions they are planned at."""

    return Tracking.plan_keys(b64decode(spec["plan"]), int(pin["core"]))


def observe(keys: list, classes: list, versions: list, served: dict | None) -> tuple[dict, list]:
    """The batch's classes, and the keys a source served otherwise than
    planned, each with the version it served (None: absent). A key served
    absent is removed if the consumer held it, else not delivered; one
    served at another word than planned is updated if it was unchanged. A
    key planned at a generation has no word to compare."""

    out: dict[str, list[str]] = {c: [] for c in CLASSES}
    loaded = []
    for key, cls, version in zip(keys, classes, versions, strict=True):
        if served is not None and cls != "removed":
            got = served.get(key)
            if got is None:
                loaded.append([key, None])
                cls = None if cls == "added" else "removed"
            elif isinstance(version, str) and got != version:
                loaded.append([key, got])
                cls = "updated" if cls == "unchanged" else cls
        if cls is not None:
            out[cls].append(key)
    return out, loaded


async def pages(pin: dict | None, size: int):
    """A keyed output's live keys at its head, each with its version, a page
    at a time: always one page at least."""

    if pin is None:
        yield {}
        return
    index, after = shared(pin["url"]), None
    while True:
        rows, after = await asyncio.to_thread(
            index.scan, pin["layers"], int(pin["head"]), after=after, limit=size
        )
        yield {row[0]: row[2] for row in rows}
        if after is None:
            return


async def prepare(o) -> None:
    """A keyed output's write, read once and given to its store whole: a
    replacement or a first write resets the content, a patch keeps every key
    it does not name."""

    if o.opaque:
        raise WriteError(f"{o.output.name}: opaque writes do not run on the new tracking core yet")
    o.prepared = await asyncio.to_thread(prepare_for, o.store, o.value, o.output)
    removes = frozenset() if o.replace else frozenset(o.prepared.removes)
    o.write = KeyedWrite(o.prepared, removes=removes, reset=o.replace, value=o.value)


def unchanged(o) -> bool:
    """A patch that names no key: nothing to store, and the head stands."""

    return not o.replace and len(o.prepared.rows) == 0 and not o.prepared.removes


async def changes(objects, spec: dict, name: str, o) -> dict:
    """The keys a stored write changed, as the engine builds its commit from
    them: `whole` for a replacement, and `changes` (base64) or, when too big
    for the result, `changes_path`, a file beside the attempt's control file."""

    def encode() -> bytes:
        keys = [key_str(k) for k in o.prepared.rows.entries()[0]]
        upserts = dict.fromkeys(keys, "") if o.output.is_dynamic_partitions else keys
        return encode_changes(upserts, [] if o.replace else [str(k) for k in o.prepared.removes])

    data = await asyncio.to_thread(encode)
    entry = {"whole": o.replace}
    if len(data) <= CHANGES_INLINE:
        entry["changes"] = b64encode(data).decode()
        return entry
    path = f"{lifecycle.base(spec['run']['id'], spec['attempt'])}changes-{name}"
    await obstore.put_async(objects, path, data)
    entry["changes_path"] = path
    return entry
