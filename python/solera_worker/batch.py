"""A batch on the new tracking core (docs/tracking-design.md; build plan B5):
what a worker does differently when its spec says `"tracking": "core"`.

A keyed incremental input loads the keys its plan names: the plan's bytes
are in the spec, and `PlannedKeys` reads each input's keys, classes and
versions out of them as columns, which the batch keeps (Python and Rust
cross once per batch, never per key). A source served live may serve a key
at another version than planned, or not at all: the batch's classes follow
what it served, and each such key goes back in the result with the version
it was served at (`delivered[input]["loaded"]`), for the engine to record.

A keyed whole read names its objects from the output's layers in the change
index (`pages`).

A keyed output's write is stored as its store takes it, every key at the
attempt's generation, and the keys it changed go back to the engine, which
builds the commit (D207): in the result when small, else in one file beside
the attempt's control file (N13).
"""

from __future__ import annotations

import asyncio
import tempfile
from base64 import b64decode, b64encode
from collections.abc import Mapping

import obstore
from solera import lifecycle
from solera._native import PlannedKeys
from solera.change_index import shared
from solera.stores import KeyedWrite, WriteError, prepare_for

CHANGES_INLINE = 64 << 10  # changed keys' bytes a result carries; past it, a file (N13)
CHANGES_CHUNK = 64 << 10  # keys encoded at a time


def on_core(spec: dict) -> bool:
    return spec.get("tracking") == "core"


def planned(spec: dict, pin: dict) -> PlannedKeys:
    """A keyed input's keys in this batch, with their classes and the
    versions they are planned at: none once its walk is done (no plan)."""

    if spec.get("plan") is None:
        return PlannedKeys()
    return PlannedKeys.decode(b64decode(spec["plan"]), int(pin["core"]))


class KeyVersions(Mapping):
    """A batch's `Keys.generations`: the keys it loads, each with the version
    it is planned at, in the plan's order. Read from the plan's columns when
    a store first asks, a whole column in one call: the keys alone (`iter`,
    `sorted()` for a source's loader), or the dict of their versions, for a
    store that looks keys up."""

    __slots__ = ("_planned", "_versions")

    def __init__(self, planned: PlannedKeys):
        self._planned, self._versions = planned, None

    def _dict(self) -> dict:
        if self._versions is None:
            self._versions = self._planned.versions()
        return self._versions

    def __getitem__(self, key):
        return self._dict()[key]

    def __contains__(self, key) -> bool:
        return key in self._dict()

    def get(self, key, default=None):
        return self._dict().get(key, default)

    def items(self):
        return self._dict().items()

    def values(self):
        return self._dict().values()

    def __repr__(self) -> str:
        return f"KeyVersions({len(self)} keys)"

    def __iter__(self):
        return iter(self._planned.load_keys())

    def __len__(self) -> int:
        return self._planned.loads()

    def sorted(self) -> list[str]:
        return self._planned.load_keys(sorted=True)


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
    for the result, `changes_path`, a file beside the attempt's control file.
    Encoded a chunk of keys at a time into a local file, so a write of any
    size is handed over in bounded memory."""

    entry = {"whole": o.replace}
    with tempfile.TemporaryFile() as f:
        size = await asyncio.to_thread(_encode, o, f)
        f.seek(0)
        if size <= CHANGES_INLINE:
            entry["changes"] = b64encode(f.read()).decode()
            return entry
        path = f"{lifecycle.base(spec['run']['id'], spec['attempt'])}changes-{name}"
        await obstore.put_async(objects, path, f)
    entry["changes_path"] = path
    return entry


def _encode(o, f) -> int:
    """Writes the changes' transport form to `f`: its header, then the
    blocks of each `CHANGES_CHUNK` changes in key order, upserts and
    removals merged (a block stands alone, and keys ascend across chunks).
    Returns its size."""

    word = "" if o.output.is_dynamic_partitions else None
    for part in o.prepared.rows.changes(() if o.replace else o.prepared.removes, word, CHANGES_CHUNK):
        f.write(part)
    return f.tell()
