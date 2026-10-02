"""FileStore and S3Store (§4): one object per value, key version or batch,
each written once under a name no other attempt writes. They take plain
Python, pandas DataFrames and Arrow data (`frames`)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import pickle
import typing
from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote

from ..sdk import KEYS, ObjectRef, Ref, is_ref_type
from . import (
    MISSING,
    PARALLEL,
    Batches,
    KeyedWrite,
    Keys,
    Patch,
    Prepared,
    Scope,
    StoreError,
    WriteError,
    Written,
    _digest,
    _segment,
    by_key_type,
    encode,
    frames,
    resolve_env,
)


class FileStore:
    """The default store (§4): what an asset returns, as files — one per
    value, per key, or per batch — each written once, under a name no other
    attempt writes (docs/lifecycle.md §9.8):

        {root}/rollup@184467.json               a value, by generation 184467
        {root}/site_status/alpha@184467.json    a value, partition alpha
        {root}/uploads/u-7/9c41e0….184467.pkl   a keyed output: one object per key and version
        {root}/site_files/alpha/f-1/5d2a….184467.json
                                                keyed and partitioned: the key's rows
        {root}/site_events/alpha/000000000042.184467.json
                                                an unkeyed incremental output: one per batch

    A name carries the logical version and the writing attempt's generation
    (`Scope.generation`), and a write is create-only: a dead writer only
    ever leaves objects nothing references, and a reader gets exactly the
    version it was pinned to. A keyed read names its objects from the key
    index's `(version, locator)` (`Keys`); a range of batches keeps, per
    batch, the highest generation, which committed it. Superseded objects
    are deleted by `discard`, once nothing can read them.

    Content is JSON when it round-trips exactly, pickle otherwise. `path`
    defaults to `$SOLERA_DATA`, else `.solera/data` next to the project
    file."""

    version = "2"
    writes = "immutable"
    ref_type = ObjectRef
    shared_table = False

    def __init__(self, path: str | os.PathLike | None = None):
        self.path = path
        self.home: str | None = None  # the project's directory, for the default path
        self._stores: dict[str, Any] = {}

    def _objects(self):
        from obstore.store import LocalStore

        if self.path is not None:
            root = os.fspath(self.path)
        else:
            root = os.environ.get("SOLERA_DATA") or os.path.join(self.home or os.getcwd(), ".solera", "data")
        root = os.path.abspath(root)
        if root not in self._stores:
            self._stores[root] = LocalStore(root, mkdir=True)
        return self._stores[root]

    def can_load(self, t, selection) -> bool:
        if is_ref_type(t):
            return issubclass(self.ref_type, t)
        return True

    def can_store(self, t, output) -> bool:
        if t is None:
            return True
        if output.key == KEYS:
            return t in (dict, Mapping) or (
                typing.get_origin(t) in (dict, Mapping) and typing.get_args(t)[:1] == (str,)
            )
        if output.incremental:  # rows, elements or batches: a list
            return t is list or typing.get_origin(t) is list or frames.can_store(t)
        return True  # a value: anything

    def prepare(self, write, output) -> Prepared:
        """Keyed writes of plain Python, DataFrames and Arrow (`frames`)."""

        return frames.prepare(write, output)

    # -- writes ---------------------------------------------------------------

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        output = scope.output
        if prior is not None and prior.meta.get("external"):
            raise WriteError(f"{output.name}: cannot write an external source ref")
        patch = isinstance(write, Patch)
        if patch and not output.incremental:
            raise WriteError(f"{output.name}: Patch requires an incremental output")
        base = self._base(output, scope, prior)
        generation = int(scope.generation or 0)
        if output.key is not None:
            if not isinstance(write, KeyedWrite):
                write = await asyncio.to_thread(KeyedWrite.of, self, write, output, prior)
            if output.is_partition_set:
                return await self._store_set(write, prior, scope, base, generation)
            return await self._store_keyed(write, prior, scope, base, generation)
        if output.incremental:
            if not patch:
                raise WriteError(f"{output.name}: an unkeyed incremental output only accepts Patch writes")
            return await self._store_batch(write, prior, scope, base, generation)
        name = f"{base}@{generation}"
        version = await self._put(name, write)
        return Written(self._ref(scope, {"mode": "value", "path": name, "base": base}, version))

    async def _store_set(self, write: KeyedWrite, prior, scope, base, generation) -> Written:
        """A partition set: its element list, as one value."""

        elements = write.prepared.take(None)
        if not write.whole and prior is not None:
            drop = set(write.removes) | set(write.prepared.removes) | set(elements)
            elements = [e for e in await self._elements(prior) if e not in drop] + elements
        name = f"{base}@{generation}"
        version = await self._put(name, elements)
        return Written(self._ref(scope, {"mode": "set", "path": name, "base": base}, version))

    async def _store_keyed(self, write: KeyedWrite, prior, scope, base, generation) -> Written:
        """One object per key and version: the keys the write changes, each
        named by the version the key index will hold, so no object goes
        unnamed — only their groups are read from the write. Removed keys
        need no write: the index stops naming them, and `discard` deletes
        what nothing reads."""

        async def put(entry) -> None:
            key, version, group = entry
            await self._put(self.key_name(base, key, version, generation), group)

        async for page in write.pages():
            await self._many(put, page)
        handle = {"mode": "keyed", "path": base, "key": scope.output.key}
        return Written(self._ref(scope, handle, write.version(prior)))

    async def _store_batch(self, write: Patch, prior, scope, base, generation) -> Written:
        """An unkeyed incremental write: its items, as one object per batch.
        With no prior (a first write or a full run) the output starts over at
        this batch; earlier ones are no longer read, and go with `discard`."""

        output = scope.output
        if write.remove:
            raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
        items = write.rows
        if not isinstance(items, list):
            items = frames.rows_of(items, output.name)
        if not isinstance(items, list):
            raise WriteError(f"{output.name}: a batch is a list, got {type(items).__name__}")
        if not items and prior is not None:
            return Written(prior)
        if scope.batch is not None:
            batch = scope.batch
        else:
            batch = int(prior.handle["batches"][1]) + 1 if prior is not None else 0
        version = await self._put(f"{base}/{batch:012d}.{generation}", items)
        if prior is None:
            first = batch
        else:
            first = int(prior.handle["batches"][0])
            version = _digest([prior.version, version])
        handle = {"mode": "batches", "path": base, "batches": [first, batch]}
        return Written(self._ref(scope, handle, version))

    @staticmethod
    def version_name(version: bytes) -> str:
        """A key's version as a file name: its hex, or — past 32 bytes, a long
        declared revision — the hex of the first 16 bytes of its SHA-256."""

        if len(version) > 32:
            version = hashlib.sha256(version).digest()[:16]
        return version.hex()

    @classmethod
    def key_name(cls, base: str, key: str, version: bytes, locator: int) -> str:
        return f"{base}/{_segment(key)}/{cls.version_name(version)}.{int(locator)}"

    async def discard(self, scope: Scope, prior: Ref | None, items: list) -> None:
        """Delete objects nothing reads any more (docs/lifecycle.md §9.8):
        superseded versions, and what attempts that never committed wrote.
        `items` name them: `("key", key, version_hex, locator)`,
        `("path", path)`, `("value", generation)`, `("batch", n, generation)`,
        or `("batches", lo, hi)` — every object of batches lo..hi. Names are
        never reused, so deleting one twice is no harm."""

        base = self._base(scope.output, scope, prior)
        names, ranges = [], []
        for item in items:
            kind = item[0]
            if kind == "key":
                names.append(self.key_name(base, item[1], bytes.fromhex(item[2]), item[3]))
            elif kind == "path":
                names.append(item[1])
            elif kind == "value":
                names.append(f"{base}@{int(item[1])}")
            elif kind == "batch":
                names.append(f"{base}/{int(item[1]):012d}.{int(item[2])}")
            elif kind == "batches":
                ranges.append((int(item[1]), int(item[2])))
            else:
                raise StoreError(f"{scope.output.name}: cannot discard {item!r}")
        if ranges:
            for name in await self._keys(base):
                batch = name.partition(".")[0]
                if batch.isdigit() and any(lo <= int(batch) <= hi for lo, hi in ranges):
                    names.append(f"{base}/{name}")
        await self._many(lambda n: self._delete(n), names)

    # -- reads ------------------------------------------------------------------

    async def load(self, ref: Ref, t, selection: Keys | Batches | None) -> Any:
        if is_ref_type(t):
            return ref
        handle = ref.handle or {}
        mode, base = handle.get("mode"), handle.get("path")
        if base is None:
            raise StoreError(f"{ref.output}: not a {type(self).__name__} ref")
        if mode == "batches":
            if isinstance(selection, Keys):
                raise StoreError(f"{ref.output}: an unkeyed incremental output takes Batches")
            first, last = (int(b) for b in handle["batches"])
            lo, hi = (max(first, selection.lo), selection.hi) if selection is not None else (first, last)
            names = await self._batches(base, lo, hi)
            # The ref's batches run first..last without a gap: every one a commit wrote.
            if missing := [b for b in range(lo, min(hi, last) + 1) if b not in names]:
                raise StoreError(f"{ref.output}: batch {missing[0]} of {base} is gone")
            batches = await self._many(self._found, [names[b] for b in sorted(names)])
            return frames.materialize([item for b in batches for item in b], t)
        if mode == "keyed":
            if not isinstance(selection, Keys):
                raise StoreError(
                    f"{ref.output}: a keyed read names its objects from the key index: it takes Keys"
                )
            keys = list(selection.revisions)
            found = await self._many(
                lambda k: self._found(self.key_name(base, k, *selection.revisions[k]), k), keys
            )
            content = dict(zip(keys, found, strict=True))
            if handle.get("key") == KEYS:
                return content
            inner = by_key_type(t)
            if inner is not MISSING:  # dict[str, T]: each key's group, as T
                return {k: frames.materialize(rows, inner) for k, rows in content.items()}
            return frames.materialize([row for rows in content.values() for row in rows], t)
        value = await self._get(base)
        if value is MISSING:
            raise StoreError(f"{ref.output}: {base} is gone")
        if mode == "set":
            if selection is not None:
                value = [e for e in value if e in selection.revisions]
            return frames.materialize(value, t)
        if selection is not None:
            raise StoreError(f"{ref.output}: an unkeyed output cannot serve a selection")
        return value

    async def _elements(self, ref: Ref) -> list[str]:
        return list(await self._found((ref.handle or {}).get("path", "")))

    async def _found(self, base: str, key: str | None = None):
        """The object at `base`, which a commit names: gone, it is an error,
        never an empty read."""

        value = await self._get(base)
        if value is MISSING:
            what = f"key {key!r}: " if key is not None else ""
            raise StoreError(f"{what}{base} is gone")
        return value

    async def _batches(self, base: str, lo: int, hi: int) -> dict[int, str]:
        """Batch `n` -> its committed object, in `[lo, hi]`: of the objects
        named after `n`, the highest generation's. The attempts that used
        batch `n` all ran between the commits of `n - 1` and `n`, one at a
        time, and the one that committed `n` was the last of them."""

        import obstore

        objects = self._objects()
        # Listed from `lo` on: an object store lists in key order, so it stops past
        # `hi`; a local directory comes in any order, and is read to its end.
        ordered = type(objects).__name__ != "LocalStore"
        best: dict[int, tuple[int, str]] = {}
        async for chunk in obstore.list(objects, prefix=f"{base}/", offset=f"{base}/{lo:012d}"):
            for meta in chunk:
                name = meta["path"][len(base) + 1 :]
                if "/" in name or not name.endswith((".json", ".pkl")):
                    continue
                batch, _, generation = unquote(name.rsplit(".", 1)[0]).partition(".")
                if not (batch.isdigit() and generation.isdigit()) or int(batch) < lo:
                    continue
                if int(batch) > hi:
                    if ordered:
                        return {b: n for b, (_, n) in best.items()}
                    continue
                if int(generation) >= best.get(int(batch), (-1, ""))[0]:
                    best[int(batch)] = (int(generation), f"{base}/{batch}.{generation}")
        return {b: n for b, (_, n) in best.items()}

    # -- objects ----------------------------------------------------------------

    @staticmethod
    def _base(output, scope, prior) -> str:
        """Where the scope's content lives: the prior's place, so a renamed
        output keeps its objects where they are (§2)."""

        handle = (prior.handle or {}) if prior is not None else {}
        if "base" in handle or "path" in handle:
            return handle.get("base") or handle["path"]
        name = _segment(output.name)
        return f"{name}/{_segment(scope.partition)}" if scope.partition else name

    async def _put(self, base: str, value) -> str:
        """Create `value` at `base`, once; returns its revision. The same name
        written again is the same attempt's, with the same bytes."""

        from ..objects import create

        data, fmt = encode(value)
        await create(self._objects(), f"{base}.{fmt}", data)
        return hashlib.sha256(data).hexdigest()

    async def _get(self, base: str):
        import obstore
        from obstore.exceptions import NotFoundError

        for fmt in ("json", "pkl"):
            try:
                result = await obstore.get_async(self._objects(), f"{base}.{fmt}")
            except (NotFoundError, FileNotFoundError):
                continue
            data = bytes(await result.bytes_async())
            return json.loads(data) if fmt == "json" else pickle.loads(data)
        return MISSING

    async def _delete(self, base: str, *formats: str) -> None:
        import obstore
        from obstore.exceptions import NotFoundError

        for fmt in formats or ("json", "pkl"):
            try:
                await obstore.delete_async(self._objects(), f"{base}.{fmt}")
            except (NotFoundError, FileNotFoundError):
                pass

    async def _keys(self, base: str) -> set[str]:
        """The names of the objects in a directory, without their format."""

        import obstore

        found = set()
        async for chunk in obstore.list(self._objects(), prefix=f"{base}/"):
            for meta in chunk:
                name = meta["path"][len(base) + 1 :]
                if "/" not in name and name.endswith((".json", ".pkl")):
                    found.add(unquote(name.rsplit(".", 1)[0]))
        return found

    @staticmethod
    async def _many(fn, items) -> list:
        """`fn` of each item, `PARALLEL` at a time, results in order: a fixed
        set of workers takes the items one by one, so a million items never
        become a million waiting tasks."""

        items = list(items)
        results: list = [None] * len(items)
        todo = iter(enumerate(items))

        async def worker():
            for i, item in todo:
                results[i] = await fn(item)

        await asyncio.gather(*(worker() for _ in range(min(PARALLEL, len(items)))))
        return results

    @staticmethod
    def _ref(scope, handle, version) -> ObjectRef:
        return ObjectRef(
            output=scope.output.name, store="", handle=handle, version=version, partition=scope.partition
        )


class S3Store(FileStore):
    """FileStore's layout and behavior in a bucket: `S3Store("s3://bucket/prefix")`.
    `options` go to obstore (`region`, `endpoint`, credentials, …); `env:NAME`
    values are resolved in the harness. A PUT is atomic, so there is nothing
    to rename."""

    def __init__(self, url: str, **options: Any):
        super().__init__()
        self.url, self.options = url, options

    def _objects(self):
        if "" not in self._stores:
            import obstore

            self._stores[""] = obstore.store.from_url(resolve_env(self.url), **resolve_env(self.options))
        return self._stores[""]
