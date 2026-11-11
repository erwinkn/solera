"""The scale harness's project (README.md): the cast, its store, and what
each producer and store call costs.

    feed       a keyed source: keys k000000000 … at the day each last changed
      └─▶ items      a keyed copy of feed (key "id")
            ├─▶ tally      a count per group of keys, built on ctx.load()
            └─▶ copy       a keyed copy of items
                  └─▶ archive    a keyed copy of copy

Every asset runs on the pool `scale`, whose workers the driver (run.py)
starts. `$SCALE_ROOT` holds the data (`data/`), the day each key last changed
(`versions`, one byte per key: 0 to 254, 255 for a key that is absent), and
one line per producer or store call in `stats/{pid}.jsonl`, with its wall and
CPU seconds. `$SCALE_FORM` picks the data's form: `dict`, a `Loaded` and a
dict per row and packs pickled by key, or `arrow`, one Arrow table per load
and batch and packs written from the producer's table. A producer's line
counts the rows it was handed (`keys`) and the keys its batch planned
(`planned`: added, updated, removed and unchanged); a keyed write's line
counts the keys it writes (`keys`) and removes (`removed`)."""

from __future__ import annotations

import asyncio
import json
import mmap
import os
import pickle
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc
from solera.executors import Pool
from solera.objects import create
from solera.objects import read as read_object
from solera.sdk import KEYS, Incremental, Loaded, Output, Project, asset, is_ref_type, source
from solera.stores import MISSING, FileStore, Keys, Patch, Read, StoreError, Written, by_key_type, frames

ROOT = Path(os.environ.get("SCALE_ROOT", ".solera/scale"))
BATCH = int(os.environ.get("SCALE_BATCH", "10000"))
ARROW = os.environ.get("SCALE_FORM", "dict") == "arrow"
Rows = pa.Table if ARROW else list
EMPTY = pa.table({"id": pa.array([], pa.string()), "v": pa.array([], pa.int64())})
GROUPS = 16
ABSENT = 255
POOL = Pool("scale")()
_versions: mmap.mmap | None = None
_stats: int | None = None


def clocks() -> tuple[float, float]:
    return time.perf_counter(), time.process_time()


def record(what: str, start: tuple[float, float], **fields) -> None:
    """One line of `stats/{pid}.jsonl`: `what` took the wall and CPU seconds
    since `start` (`clocks()`)."""

    global _stats
    if _stats is None:
        (ROOT / "stats").mkdir(parents=True, exist_ok=True)
        _stats = os.open(ROOT / "stats" / f"{os.getpid()}.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND)
    wall, cpu = time.perf_counter() - start[0], time.process_time() - start[1]
    line = {"what": what, "at": time.time(), "wall": wall, "cpu": cpu, **fields}
    os.write(_stats, (json.dumps(line) + "\n").encode())


def versions() -> mmap.mmap:
    global _versions
    if _versions is None:
        with open(ROOT / "versions", "rb") as f:
            _versions = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    return _versions


class PackStore(FileStore):
    """FileStore with a keyed write packed into one object per attempt:
    `{base}/{generation}.pkl` holding `{key: rows}`, or, for a producer's
    Arrow table, `{base}/{generation}.arrow` holding the rows it writes, in
    key order, taken from the table by each chunk's `indices`. A FileStore
    keeps a directory and an object per key, two inodes, so a disk holds a
    few million keys of it, not 100M (ERW-21). A keyed read groups its keys by
    generation and reads each pack once. Cleanup deletes nothing: a pack
    still holds other keys' live rows, and no number here measures it."""

    async def store(self, write, prior, context) -> Written:
        if context.output.key is not None:
            return await super().store(write, prior, context)
        start = clocks()
        written = await super().store(write, prior, context)
        record("store_write", start, output=context.output.name, keys=0, removed=0)
        return written

    async def _store_keyed(self, write, prior, context, base, generation) -> Written:
        start, value, keys = clocks(), write.value, 0
        table = value.rows if isinstance(value, Patch) else value
        if isinstance(table, pa.Table):
            parts = []
            async for chunk in write.chunks():
                parts.append(await asyncio.to_thread(_take, table, chunk.indices))
                keys += len(chunk)
            form, data = "arrow", await asyncio.to_thread(_ipc, parts, table.schema)
        else:
            rows: dict = {}
            async for chunk in write.chunks():
                rows.update(await asyncio.to_thread(chunk.groups))
            form, data, keys = "pkl", pickle.dumps(rows, protocol=5), len(rows)
        await create(self._objects(), f"{base}/{generation}.{form}", data)
        record("store_write", start, output=context.output.name, keys=keys, removed=len(write.removes))
        ref = self._ref(context, {"mode": "keyed", "path": base, "key": context.output.key, "form": form})
        return Written(ref, changed=write.changed())

    async def read(self, ref, t, selection) -> Read:
        start, handle = clocks(), ref.handle or {}
        if handle.get("mode") != "keyed" or is_ref_type(t):
            read = await super().read(ref, t, selection)
            record("store_read", start, output=ref.output, keys=0, packs=1)
            return read
        if not isinstance(selection, Keys):
            raise StoreError(f"{ref.output}: a keyed read takes Keys")
        base, wanted = handle["path"], defaultdict(list)
        for key, generation in selection.generations.items():
            wanted[int(generation)].append(key)
        if handle.get("form") == "arrow":
            value = await self._read_arrow(base, handle["key"], wanted, t)
            record("store_read", start, output=ref.output, keys=len(selection.generations), packs=len(wanted))
            return Read(value, selection.generations)
        packs = await self._many(lambda g: self._found(f"{base}/{g}"), list(wanted))
        content = {}
        for (generation, keys), pack in zip(wanted.items(), packs, strict=True):
            for key in keys:
                if key not in pack:
                    raise StoreError(f"key {key!r}: {base}/{generation} does not hold it")
                content[key] = pack[key]
        inner = by_key_type(t)
        if handle.get("key") == KEYS:
            value = content
        elif inner is not MISSING:
            value = {k: frames.materialize(rows, inner) for k, rows in content.items()}
        else:
            value = frames.materialize([row for rows in content.values() for row in rows], t)
        record("store_read", start, output=ref.output, keys=len(content), packs=len(wanted))
        return Read(value, selection.generations)

    async def _read_arrow(self, base: str, column: str, wanted: dict, t):
        async def pack(generation: int) -> pa.Table:
            found = await read_object(self._objects(), f"{base}/{generation}.arrow")
            if found is None:
                raise StoreError(f"{base}/{generation}.arrow is gone")
            return pa.ipc.open_file(found[0]).read_all()

        parts = []
        for (generation, keys), table in zip(
            wanted.items(), await self._many(pack, list(wanted)), strict=True
        ):
            part = table.filter(pc.is_in(table[column], value_set=pa.array(keys, pa.string())))
            if pc.count_distinct(part[column]).as_py() != len(keys):
                missing = set(keys) - set(part[column].to_pylist())
                raise StoreError(f"key {min(missing)!r}: {base}/{generation} does not hold it")
            parts.append(part)
        table = pa.concat_tables(parts) if parts else None
        inner = by_key_type(t)
        if inner is MISSING:
            return frames.materialize(table if table is not None else [], t)
        groups = defaultdict(list)
        for row in table.to_pylist() if table is not None else []:
            groups[row[column]].append(row)
        return {k: frames.materialize(rows, inner) for k, rows in groups.items()}

    async def cleanup(self, output, **pattern) -> None:
        return None


def _take(table: pa.Table, indices) -> pa.Table:
    """The table's rows at a chunk's `indices`: a run, or unsigned ints."""

    if isinstance(indices, range) and indices.step == 1:
        return table.slice(indices.start, len(indices))
    return table.take(pa.Array.from_buffers(pa.uint32(), len(indices), [None, pa.py_buffer(indices)]))


def _ipc(parts: list[pa.Table], schema: pa.Schema) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, schema) as writer:
        for part in parts:
            writer.write_table(part)
    return sink.getvalue().to_pybytes()


@source(key="id")
def feed(keys, ctx):
    start, days = clocks(), versions()
    if ARROW:
        ids = pa.array(keys, pa.string())
        index = pc.cast(pc.utf8_slice_codeunits(ids, 1), pa.int64()).to_numpy()
        day = np.frombuffer(days, np.uint8)[index].astype(np.int64)
        table = pa.table({"id": ids, "v": day, "version": pc.cast(day, pa.string())})
        served = Loaded(table.filter(pa.array(day != ABSENT)), version="version")
        record("produce", start, asset="feed", keys=len(keys))
        return served
    out = {}
    for key in keys:
        day = days[int(key[1:])]
        if day != ABSENT:
            out[key] = Loaded({"id": key, "v": day}, version=str(day))
    record("produce", start, asset="feed", keys=len(keys))
    return out


def planned(batch) -> int:
    return len(batch.added) + len(batch.updated) + len(batch.removed) + len(batch.unchanged)


def copied(name: str, changes, rows: list[dict], start):
    """A keyed copy's write: a full run starts over, every other batch patches."""

    if ARROW:
        rows = rows.select(["id", "v"]) if rows.num_columns else EMPTY
    else:
        rows = [{"id": r["id"], "v": r["v"]} for r in rows]
    write = rows if changes.reset else Patch(rows, remove=list(changes.removed))
    record("produce", start, asset=name, keys=len(rows), planned=planned(changes))
    return write


@asset(inputs={"feed": Incremental(batch_size=BATCH)}, outputs=Output("items", key="id"), executor=POOL)
def items(ctx, feed: Rows):
    return copied("items", ctx.batch["feed"], feed, clocks())


@asset(inputs={"items": Incremental(batch_size=BATCH)}, outputs=Output("copy", key="id"), executor=POOL)
def copy(ctx, items: Rows):
    return copied("copy", ctx.batch["items"], items, clocks())


@asset(inputs={"copy": Incremental(batch_size=BATCH)}, outputs=Output("archive", key="id"), executor=POOL)
def archive(ctx, copy: Rows):
    return copied("archive", ctx.batch["copy"], copy, clocks())


@asset(inputs={"items": Incremental(batch_size=BATCH)}, outputs=Output("tally"), executor=POOL)
async def tally(ctx, items: Rows):
    """Its own load is the store's (`store_read:tally`), so its clock starts after it."""

    batch = ctx.batch["items"]
    held = None if batch.reset else await ctx.load()
    start = clocks()
    counts = list(held or [0] * GROUPS)
    for key in batch.added:
        counts[int(key[1:]) % GROUPS] += 1
    for key in batch.removed:
        counts[int(key[1:]) % GROUPS] -= 1
    record("produce", start, asset="tally", keys=len(items), planned=planned(batch))
    return counts


project = Project(
    name="scale",
    assets=[items, tally, copy, archive],
    sources=[feed],
    default_store=PackStore(ROOT / "data"),
)
