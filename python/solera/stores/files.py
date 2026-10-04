"""FileStore and S3Store (§4): one object per value, key or commit, each
written once under a name no other attempt writes. They take plain
Python, pandas DataFrames and Arrow data (`frames`)."""

from __future__ import annotations

import asyncio
import json
import os
import pickle
from typing import Any

from ..sdk import KEYS, ObjectRef, Ref, is_ref_type
from . import (
    MISSING,
    PARALLEL,
    Commits,
    KeyedWrite,
    Keys,
    Patch,
    Prepared,
    StoreError,
    WriteContext,
    WriteError,
    Written,
    _segment,
    by_key_type,
    encode,
    frames,
    resolve_env,
    takes,
)


class FileStore:
    """The default store (§4): what an asset returns, as files — one per
    value, per key, or per commit — each written once, under a name no other
    attempt writes (docs/lifecycle.md §9.8):

        {root}/rollup@184467.json               a value, by generation 184467
        {root}/site_status/alpha@184467.json    a value, partition alpha
        {root}/uploads/u-7/184467.pkl           a keyed output: one object per key and generation
        {root}/site_files/alpha/f-1/184467.json keyed and partitioned: the key's rows
        {root}/site_events/alpha/000000000042/184467.json
                                                an unkeyed incremental output: one per commit

    A name carries the writing attempt's generation (`WriteContext.generation`) —
    a key's version (docs/versions.md) — and a write is create-only: a dead
    writer only ever leaves objects nothing references, and a reader gets
    exactly what it was pinned to. A keyed read names its objects from the
    generations the key index holds (`Keys`); a range of commits keeps, per
    commit, the highest generation, which committed it — listed commit by
    commit, so reading one never lists the others. Superseded objects
    are deleted by `cleanup`, once nothing can read them.

    Content is JSON when it round-trips exactly, pickle otherwise. `path`
    defaults to `$SOLERA_DATA`, else `.solera/data` next to the project
    file."""

    version = "4"
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
        return takes(t, output, frames=True)  # every form, DataFrames and Arrow too

    def prepare(self, write, output) -> Prepared:
        """Keyed writes of plain Python, DataFrames and Arrow (`frames`)."""

        return frames.prepare(write, output)

    # -- writes ---------------------------------------------------------------

    async def store(self, write, prior: Ref | None, context: WriteContext) -> Written:
        output = context.output
        if prior is not None and prior.meta.get("source"):
            raise WriteError(f"{output.name}: cannot write an external source ref")
        patch = isinstance(write, Patch)
        if patch and not output.incremental:
            raise WriteError(f"{output.name}: Patch requires an incremental output")
        base = self._base(output, context, prior)
        if context.reset:
            prior = None  # where the content is, but nothing of it is kept
        generation = int(context.generation or 0)
        if output.key is not None:
            if not isinstance(write, KeyedWrite):
                write = await asyncio.to_thread(KeyedWrite.of, self, write, output, prior)
            if output.is_dynamic_partitions:
                return await self._store_set(write, prior, context, base, generation)
            return await self._store_keyed(write, prior, context, base, generation)
        if output.incremental:
            if not patch:
                raise WriteError(f"{output.name}: an unkeyed incremental output only accepts Patch writes")
            return await self._store_commit(write, prior, context, base, generation)
        name = f"{base}@{generation}"
        await self._put(name, write)
        return Written(self._ref(context, {"mode": "value", "path": name, "base": base}))

    async def _store_set(self, write: KeyedWrite, prior, context, base, generation) -> Written:
        """A dynamic partitions: its element list, as one value."""

        partitions = write.prepared.take(None)
        if not write.reset and prior is not None:
            drop = set(write.removes) | set(write.prepared.removes) | set(partitions)
            partitions = [e for e in await self._partitions(prior) if e not in drop] + partitions
        name = f"{base}@{generation}"
        await self._put(name, partitions)
        return Written(self._ref(context, {"mode": "set", "path": name, "base": base}))

    async def _store_keyed(self, write: KeyedWrite, prior, context, base, generation) -> Written:
        """One object per key and generation: the keys the write's delta
        writes, each named by the generation the key index will hold, so no
        object goes unnamed — only their groups are read from the write.
        Removed keys need no write: the index stops naming them, and
        `cleanup` deletes what nothing reads."""

        async def put(entry) -> None:
            key, group = entry
            await self._put(self.key_name(base, key, generation), group)

        async for chunk in write.chunks():
            await self._many(put, chunk)
        return Written(self._ref(context, {"mode": "keyed", "path": base, "key": context.output.key}))

    async def _store_commit(self, write: Patch, prior, context, base, generation) -> Written:
        """An unkeyed incremental write: its items, as one object per commit.
        With no prior (a first write, or a reset) the output starts over at
        this commit; earlier ones are no longer read, and go with `cleanup`."""

        output = context.output
        if write.remove:
            raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
        items = write.rows
        if not isinstance(items, list):
            items = frames.rows_of(items, output.name)
        if not isinstance(items, list):
            raise WriteError(f"{output.name}: a commit is a list, got {type(items).__name__}")
        if not items and prior is not None:
            return Written(prior)
        if context.commit_number is not None:
            commit_number = context.commit_number
        else:
            commit_number = int(prior.handle["commits"][1]) + 1 if prior is not None else 0
        await self._put(f"{base}/{commit_number:012d}/{generation}", items)
        first = commit_number if prior is None else int(prior.handle["commits"][0])
        handle = {"mode": "commits", "path": base, "commits": [first, commit_number]}
        return Written(self._ref(context, handle))

    @staticmethod
    def key_name(base: str, key: str, generation: int) -> str:
        return f"{base}/{_segment(key)}/{int(generation)}"

    async def cleanup(self, context: WriteContext, prior: Ref | None, items: list) -> None:
        """Delete objects nothing reads any more (docs/lifecycle.md §9.8):
        superseded ones, and what attempts that never committed wrote.
        `items` name them: `("key", key, generation)`,
        `("path", path)`, `("value", generation)`, `("commit_number", n, generation)`,
        or `("commits", lo, hi)` — every object of commits lo..hi. Names are
        never reused, so deleting one twice is no harm."""

        base = self._base(context.output, context, prior)
        names, ranges = [], []
        for item in items:
            kind = item[0]
            if kind == "key":
                names.append(self.key_name(base, item[1], item[2]))
            elif kind == "path":
                names.append(item[1])
            elif kind == "value":
                names.append(f"{base}@{int(item[1])}")
            elif kind == "commit_number":
                names.append(f"{base}/{int(item[1]):012d}/{int(item[2])}")
            elif kind == "commits":
                ranges.append((int(item[1]), int(item[2])))
            else:
                raise StoreError(f"{context.output.name}: cannot clean up {item!r}")
        if ranges:
            import obstore

            async for chunk in obstore.list(self._objects(), prefix=f"{base}/"):
                for meta in chunk:
                    found = _commit_of(base, meta["path"])
                    if found and any(lo <= found[0] <= hi for lo, hi in ranges):
                        names.append(f"{base}/{found[0]:012d}/{found[1]}")
        await self._many(lambda n: self._delete(n), names)

    # -- reads ------------------------------------------------------------------

    async def load(self, ref: Ref, t, selection: Keys | Commits | None) -> Any:
        if is_ref_type(t):
            return ref
        handle = ref.handle or {}
        mode, base = handle.get("mode"), handle.get("path")
        if base is None:
            raise StoreError(f"{ref.output}: not a {type(self).__name__} ref")
        if mode == "commits":
            if isinstance(selection, Keys):
                raise StoreError(f"{ref.output}: an unkeyed incremental output takes Batches")
            first, last = (int(b) for b in handle["commits"])
            lo, hi = (
                (max(first, selection.lo), min(last, selection.hi))
                if selection is not None
                else (first, last)
            )
            names = await self._commits(base, lo, hi)
            # The ref's commits run first..last without a gap: every one was committed.
            if missing := [b for b in range(lo, hi + 1) if b not in names]:
                raise StoreError(f"{ref.output}: commit {missing[0]} of {base} is gone")
            commits = await self._many(self._found, [names[b] for b in sorted(names)])
            return frames.materialize([item for b in commits for item in b], t)
        if mode == "keyed":
            if not isinstance(selection, Keys):
                raise StoreError(
                    f"{ref.output}: a keyed read names its objects from the key index: it takes Keys"
                )
            keys = list(selection.generations)
            found = await self._many(
                lambda k: self._found(self.key_name(base, k, selection.generations[k]), k), keys
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
                value = [e for e in value if e in selection.generations]
            return frames.materialize(value, t)
        if selection is not None:
            raise StoreError(f"{ref.output}: an unkeyed output cannot serve a selection")
        return value

    async def _partitions(self, ref: Ref) -> list[str]:
        return list(await self._found((ref.handle or {}).get("path", "")))

    async def _found(self, base: str, key: str | None = None):
        """The object at `base`, which a commit names: gone, it is an error,
        never an empty read."""

        value = await self._get(base)
        if value is MISSING:
            what = f"key {key!r}: " if key is not None else ""
            raise StoreError(f"{what}{base} is gone")
        return value

    async def _commits(self, base: str, lo: int, hi: int) -> dict[int, str]:
        """Commit `n` -> its committed object, in `[lo, hi]`: of the objects
        under `n`'s name, the highest generation's. The attempts that used
        commit number `n` all ran one at a time, after commit `n - 1`, and
        the one that committed `n` was the last of them."""

        import obstore
        from obstore.store import LocalStore

        objects = self._objects()
        best: dict[int, int] = {}

        async def scan(prefix: str, offset: str | None = None) -> None:
            async for chunk in obstore.list(objects, prefix=prefix, offset=offset):
                for meta in chunk:
                    if (found := _commit_of(base, meta["path"])) is None:
                        continue
                    commit_number, generation = found
                    if commit_number > hi:
                        return  # listed in key order: none further is in range
                    if commit_number >= lo and generation > best.get(commit_number, -1):
                        best[commit_number] = generation

        if isinstance(objects, LocalStore):
            # A directory lists in any order, to its end: each commit's own, then.
            await self._many(lambda b: scan(f"{base}/{b:012d}/"), range(lo, hi + 1))
        else:
            # An object store lists in key order, from `lo`: it stops past `hi`.
            await scan(f"{base}/", f"{base}/{lo:012d}")
        return {b: f"{base}/{b:012d}/{g}" for b, g in best.items()}

    # -- objects ----------------------------------------------------------------

    @staticmethod
    def _base(output, context, prior) -> str:
        """Where the partition's content lives: the prior's place, else under
        the output life's home, so every partition of a renamed output stays
        in one place (§2, K25)."""

        handle = (prior.handle or {}) if prior is not None else {}
        if "base" in handle or "path" in handle:
            return handle.get("base") or handle["path"]
        name = _segment(context.home or output.name)
        return f"{name}/{_segment(context.partition)}" if context.partition else name

    async def _put(self, base: str, value) -> None:
        """Create `value` at `base`, once. The same name written again is the
        same attempt's, with the same bytes."""

        from ..objects import create

        data, fmt = encode(value)
        await create(self._objects(), f"{base}.{fmt}", data)

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
    def _ref(context, handle) -> ObjectRef:
        return ObjectRef(output=context.output.name, store="", handle=handle, partition=context.partition)


def _commit_of(base: str, path: str) -> tuple[int, int] | None:
    """An object's `(commit, generation)`, from its name under `base`:
    `{commit:012d}/{generation}.{format}`. None for any other object."""

    commit_number, _, name = path[len(base) + 1 :].partition("/")
    generation, _, fmt = name.partition(".")
    if commit_number.isdigit() and generation.isdigit() and fmt in ("json", "pkl"):
        return int(commit_number), int(generation)
    return None


class S3Store(FileStore):
    """FileStore's layout and behavior in a bucket: `S3Store("s3://bucket/prefix")`.
    `options` go to obstore (`region`, `endpoint`, credentials, …); `env:NAME`
    values are resolved in the worker. A PUT is atomic, so there is nothing
    to rename."""

    def __init__(self, url: str, **options: Any):
        super().__init__()
        self.url, self.options = url, options

    def _objects(self):
        if "" not in self._stores:
            import obstore

            self._stores[""] = obstore.store.from_url(resolve_env(self.url), **resolve_env(self.options))
        return self._stores[""]
