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

A log's rows are the core's to keep, in the output's layers of the change
index. A write is one commit: its rows, encoded a part at a time, written as
the layer the engine named, which goes back in the result for the engine to
install. A read is the rows of a range of commits, or of all of them. On a
store that writes in place (a table), the store keeps the rows, each stamped
with its commit, and the layer says only that the commit is.
"""

from __future__ import annotations

import asyncio
import json
import pickle
import tempfile
import time
from base64 import b64decode, b64encode
from collections.abc import Mapping

import obstore
from solera import lifecycle
from solera._native import PlannedKeys
from solera.change_index import shared
from solera.stores import KeyedWrite, Patch, StoreError, WriteError, encode, frames, prepare_for

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


async def repair(o, context) -> None:
    """Delete what dead writers left in a keyed output's table (D196): the
    attempts whose versions `info["repairs"]` lists wrote, perhaps, and ended
    without a commit. The store says which keys the partition holds at those
    versions, after taking this attempt's version check, which locks every
    older writer out for good. A key the output's log lacks is deleted: no
    commit ever recorded it. A key the log has keeps its dead row until its
    producer writes it again; the log's version says it is not the row's."""

    held = await o.store.written_at(o.info["repairs"], o.prior, context)
    pin, lacking = o.info.get("core"), []
    for chunk in held:
        found = set()
        if pin is not None:
            rows, _ = await asyncio.to_thread(shared(pin["url"]).get, pin["layers"], int(pin["head"]), chunk)
            found = {row[0] for row in rows}
        lacking += [key for key in chunk if key not in found]
    if lacking:
        prepared = await asyncio.to_thread(prepare_for, o.store, Patch([], remove=lacking), o.output)
        await o.store.store(KeyedWrite(prepared, removes=frozenset(lacking)), o.prior, context)


# -- logs ----------------------------------------------------------------------------

PART_BYTES = 1 << 20  # about what a part of a log's commit holds, encoded
PART_MAX = 32 << 20  # past this a part is split: the index holds an entry in one block
FORMATS = {"json": b"j", "pkl": b"p", "kept": b"s"}  # a part's first byte


def log_rows(o) -> list:
    """What a write appends to a log: a `Patch` of rows, removing none."""

    name, write = o.output.name, o.value
    if not isinstance(write, Patch):
        raise WriteError(f"{name}: an unkeyed incremental output only accepts Patch writes")
    if write.remove:
        raise WriteError(f"{name}: remove is not allowed on an unkeyed incremental output")
    rows = write.rows if isinstance(write.rows, list) else frames.rows_of(write.rows, name)
    return rows


def log_parts(rows: list) -> list[bytes]:
    """A commit's rows as its parts: consecutive rows, about `PART_BYTES`
    encoded each, as JSON when it round-trips exactly, pickle otherwise
    (`encode`). Rows are sized from the first of them; a part that comes out
    far larger is split."""

    if not rows:
        return []
    sample = rows[:64]
    each = max(1, len(encode(sample)[0]) // len(sample))
    step, out = max(1, PART_BYTES // each), []

    def put(chunk: list) -> None:
        data, fmt = encode(chunk)
        if len(data) > PART_MAX and len(chunk) > 1:
            put(chunk[: len(chunk) // 2])
            put(chunk[len(chunk) // 2 :])
        else:
            out.append(FORMATS[fmt] + data)

    for start in range(0, len(rows), step):
        put(rows[start : start + step])
    return out


def _part_rows(part: bytes) -> list:
    body = memoryview(part)[1:]
    if part[:1] == FORMATS["json"]:
        return json.loads(bytes(body))
    if part[:1] == FORMATS["pkl"]:
        return pickle.loads(body)
    raise StoreError("this log's rows are its store's to read, not the engine's")


def log_kept(rows: list) -> list[bytes]:
    """The one part of a commit whose rows a store keeps (a store that
    writes in place, each row stamped with its commit): how many they are.
    The core holds the commit all the same, so a reader's place, a start
    over and staleness are one mechanism for every log."""

    return [FORMATS["kept"] + str(len(rows)).encode()]


async def log_write(spec: dict, o, parts, rows: list) -> dict:
    """Write a log's commit, the layer the engine named for this attempt
    (`info["log"]`), and return it: its rows as `parts` makes them. A commit
    that starts the log over (`clear`) is written even with no row."""

    planned = o.info["log"]
    # As the engine names a commit's producer: JSON, keys sorted.
    producer = json.dumps(
        {"attempt": spec["attempt"], "run": spec["run"]["id"]}, sort_keys=True, separators=(",", ":")
    ).encode()
    parts = await asyncio.to_thread(parts, rows)
    return await asyncio.to_thread(
        shared(planned["url"]).log_write,
        planned["path"],
        int(planned["commit"]),
        parts,
        time_ms=int(time.time() * 1000),
        producer=producer,
        live_before=int(planned["live"]),
        clear=bool(planned["clear"]),
    )


async def log_read(pin: dict | None, lo: int = 0, hi: int | None = None) -> list:
    """The rows of a log's commits `lo + 1..=hi` (to its head by default),
    in the order they were appended: none before its first commit."""

    if pin is None:
        return []
    index, head, after, rows = shared(pin["url"]), int(pin["head"]), None, []
    while True:
        parts, after = await asyncio.to_thread(
            index.log_read, pin["layers"], head, lo, head if hi is None else hi, after=after
        )
        for _, part in parts:
            rows += _part_rows(part)
        if after is None:
            return rows
