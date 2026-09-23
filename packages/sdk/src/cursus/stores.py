"""Store protocol and the built-in stores (§3, §4). Runs in the harness."""

from __future__ import annotations

import inspect
import json
import os
import tempfile
import typing
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote

from .sdk import BlobRef, JsonRef, Output, Ref, digest, is_ref_type


class StoreError(Exception):
    retryable = False


class StoreConflict(StoreError):
    """A fenced write refused: the live marker differs from `prior` (§3)."""


class StaleRead(StoreError):
    """A pinned read refused: live marker != ref.version (§3). Retryable."""

    retryable = True


class WriteError(StoreError):
    """Malformed write: duplicate keys, wrong shape, disallowed op."""


@dataclass(frozen=True)
class Patch:
    """Partial write: replace the named keys, delete `remove` (§4)."""

    rows: Any
    remove: Any = ()


@dataclass(frozen=True)
class Sql:
    """PostgresStore only: materialize a SELECT, or run a statement verbatim (§4)."""

    stmt: str


@dataclass(frozen=True)
class Keys:
    """A `key -> revision` selection passed to `store.load` (§4)."""

    revisions: Mapping[str, str]


@dataclass(frozen=True)
class Batches:
    """An inclusive `[lo, hi]` batch-range selection passed to `store.load`
    on an unkeyed incremental output (§2.2)."""

    lo: int
    hi: int


@dataclass(frozen=True)
class Scope:
    """A write scope (§9): `batch` is the engine-assigned batch number for
    incremental outputs, `attempt` the writing attempt's id (unique, so object
    names built from it never collide), `aliases` the output's former names."""

    output: Output
    partition: str
    batch: int | None = None
    attempt: str | None = None
    aliases: tuple = ()


@dataclass(frozen=True)
class Written:
    """What a store wrote. `keys` is only for writes the harness never sees as
    rows (`Sql` materialized inside Postgres): the scope's complete new
    `key -> version` map. For every other write the harness derives keys from
    the rows itself (§6, §9)."""

    ref: Ref
    keys: Mapping[str, str] | None = None


@runtime_checkable
class Store(Protocol):
    version: str = "1"
    ref_type: type[Ref] = Ref

    def can_load(self, t: type | None, selection: type | None) -> bool: ...
    def can_store(self, t: type | None, output: Output) -> bool: ...
    async def store(self, write: Any, prior: Ref | None, scope: Scope) -> Written: ...
    async def load(self, ref: Ref, t: type, selection: Keys | Batches | None) -> Any: ...

    # Optional: `async def expire(self, head: Ref, before: float) -> None` deletes
    # what no version written after `before` needs; the head stays loadable (§9).


def resolve_env(value: Any) -> Any:
    """`env:NAME` indirection for store/resource config, resolved in the harness.
    Dicts and lists are walked; anything else passes through."""

    if isinstance(value, str) and value.startswith("env:"):
        name = value[4:]
        if name not in os.environ:
            raise StoreError(f"Environment variable {name} is not set")
        return os.environ[name]
    if isinstance(value, dict):
        return {k: resolve_env(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(resolve_env(v) for v in value)
    return value


def _is_dataframe_type(t: Any) -> bool:
    return (
        inspect.isclass(t)
        and t.__name__ in ("DataFrame", "GeoDataFrame")
        and t.__module__.split(".")[0] in ("pandas", "geopandas")
    )


def _is_list_of_dicts(t: Any) -> bool:
    return typing.get_origin(t) is list and typing.get_args(t) == (dict,)


def _rows(value: Any, output_name: str) -> list[dict]:
    """Coerce a write payload into a row list."""

    if _is_dataframe(value):
        return value.to_dict(orient="records")
    if isinstance(value, list) and all(isinstance(r, dict) for r in value):
        return [dict(r) for r in value]
    if value is None:
        return []
    raise WriteError(f"{output_name}: expected rows (list[dict] or DataFrame), got {type(value).__name__}")


def _is_dataframe(value: Any) -> bool:
    return type(value).__name__ in ("DataFrame", "GeoDataFrame") and type(value).__module__.split(".")[0] in (
        "pandas",
        "geopandas",
    )


def key_map(output: Output, rows: list[dict]) -> dict[str, str]:
    """The scope's complete `key -> revision` map for a row payload (§4)."""

    if output.is_partition_set:
        return {str(k): "1" for k in rows}
    result = {}
    for row in rows:
        if output.key not in row:
            raise WriteError(f"{output.name}: row lacks the declared key column {output.key!r}")
        key = str(row[output.key])
        if key in result:
            raise WriteError(f"{output.name}: duplicate key {key!r} in write")
        if output.revision:
            if output.revision not in row:
                raise WriteError(f"{output.name}: row lacks the declared revision column {output.revision!r}")
            result[key] = str(row[output.revision])
        else:
            result[key] = digest(row)
    return result


def _attempt_of(scope: Scope) -> str:
    from .ids import ulid

    return scope.attempt or ulid()


def _parse(path: str) -> tuple[str, int, str] | None:
    """`(kind, batch, attempt)` of a batch or snapshot object name
    `{b|s}{batch:012d}-{attempt}.json`; `("v", -1, attempt)` for `v{attempt}.json`."""

    name = path.rsplit("/", 1)[-1]
    if not name.endswith(".json"):
        return None
    if name[:1] in ("b", "s") and len(name) > 19 and name[13] == "-":
        try:
            return name[0], int(name[1:13]), name[14:-5]
        except ValueError:
            return None
    if name[:1] == "v":
        return "v", -1, name[1:-5]
    return None


def remove_empty_dirs(objects, prefixes) -> None:
    """A local filesystem store keeps directories its objects left behind and
    lists them; object stores have no directories. Remove the empty ones."""

    root = getattr(objects, "prefix", None)
    if type(objects).__name__ != "LocalStore" or root is None:
        return
    for prefix in sorted(set(prefixes), key=len, reverse=True):
        path = os.path.join(str(root), prefix.strip("/"))
        while os.path.normpath(path) != os.path.normpath(str(root)):
            try:
                os.rmdir(path)
            except OSError:
                break
            path = os.path.dirname(path)


def _batch_path(prefix: str, kind: str, batch: int, attempt: str) -> str:
    return f"{prefix}{batch // 1000:09d}/{kind}{batch:012d}-{attempt}.json"


def _written_at(attempt: str) -> float | None:
    from .ids import ulid_time

    try:
        return ulid_time(attempt)
    except ValueError:
        return None


class JsonStore:
    """Default store (§4): one object per batch for incremental outputs, one
    object per version for values and partition sets.

    Object layout under the attempt's `objects` namespace, `{esc(scope)}`
    being `_` for the unpartitioned scope:

        data/{output}/{esc(scope)}/v{attempt}.json          a value or partition-set version
        data/{output}/{esc(scope)}/{k}/b{batch}-{attempt}.json
                                one batch: {"rows": …} for an unkeyed incremental
                                output, {"upsert"/"reset", "remove": …} for a keyed one
        data/{output}/{esc(scope)}/{k}/s{batch}-{attempt}.json
                                keyed snapshot {"rows": …}, every `snapshot_every`
                                batches, so loads fold a bounded tail

    `{k}` is `batch // 1000`: a load lists only the directories its batches
    are in, and `expire` walks the oldest ones first.

    Every object is named after the attempt that wrote it, so no attempt ever
    overwrites another's objects. When attempts wrote the same batch (a retry
    after a failed commit), the newest attempt's object is the committed one:
    later attempts only get that batch number while it is still uncommitted.
    The attempt id also dates each object, which is how `expire` decides.

    The store never works out what changed: the harness does, against the
    output's key index, and skips writes that change nothing (§6).
    """

    version = "1"
    ref_type = JsonRef
    shared_table = False

    def __init__(self, snapshot_every: int = 64):
        self._objects = None
        self.snapshot_every = max(1, int(snapshot_every))

    def bind_objects(self, objects) -> None:
        """The harness hands the attempt's object store to stores without a URL."""

        self._objects = objects

    def can_load(self, t, selection) -> bool:
        if t is None:
            return selection is None
        if is_ref_type(t):
            return issubclass(self.ref_type, t)
        if _is_dataframe_type(t) or _is_list_of_dicts(t) or t in (list, dict):
            return True
        if t in (str, int, float, bool, type(None)):
            return selection is None
        return False

    def can_store(self, t, output) -> bool:
        if output.is_partition_set:
            return t is None or t is list or _is_list_of(t, str) or _is_list_of_dicts(t)
        if output.incremental:
            return t is None or _is_list_of_dicts(t) or _is_dataframe_type(t) or t is list
        return True  # unkeyed value: any JSON value

    # -- writes ---------------------------------------------------------------

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        objects = self._require_objects()
        output = scope.output
        if prior is not None and prior.meta.get("external"):
            raise WriteError(f"{output.name}: cannot write an external source ref")

        if isinstance(write, Patch):
            if not output.incremental:
                raise WriteError(f"{output.name}: Patch requires an incremental output")
            remove = {str(k) for k in write.remove}
            if output.is_partition_set:
                return await self._store_set(
                    objects, output, scope, prior, write.rows or [], remove, patch=True
                )
            rows = _rows(write.rows, output.name)
            if output.key is None:
                return await self._store_batch(objects, output, scope, prior, rows, remove)
            return await self._store_keyed(objects, output, scope, prior, rows, remove, patch=True)
        if output.is_partition_set:
            return await self._store_set(objects, output, scope, prior, write or [], set(), patch=False)
        if output.key is not None:
            return await self._store_keyed(
                objects, output, scope, prior, _rows(write, output.name), set(), patch=False
            )
        if output.incremental:
            raise WriteError(f"{output.name}: an unkeyed incremental output only accepts Patch writes")
        return await self._store_version(objects, output, scope, prior, write, "value")

    async def _store_version(self, objects, output, scope, prior, payload, mode) -> Written:
        """One object per version; rewriting the prior version writes nothing."""

        version = digest(_canonical(payload))
        if prior is not None and version == prior.version:
            return Written(prior)
        path = f"{self._prefix(output, scope)}v{_attempt_of(scope)}.json"
        await self._put(objects, path, payload)
        key = output.key if mode == "set" else None
        return Written(self._ref(output, scope, {"object": path, "mode": mode, "key": key}, version))

    async def _store_set(self, objects, output, scope, prior, rows, remove, *, patch) -> Written:
        """Partition-set write: the element list, one object per version."""

        elements = [str(e) for e in rows]
        if patch and prior is not None:
            old = await self._payload(objects, prior)
            payload = [e for e in old if e not in remove and e not in elements] + elements
        else:
            payload = elements
        return await self._store_version(objects, output, scope, prior, payload, "set")

    async def _store_batch(self, objects, output, scope, prior, rows, remove) -> Written:
        """Unkeyed incremental write: one `{"rows": …}` object per batch. With
        no prior the batch starts the output over (a first write or a full run)."""

        if remove:
            raise WriteError(f"{output.name}: remove is not allowed on an unkeyed incremental output")
        if not rows and prior is not None:
            return Written(prior)
        batch = self._batch(scope, prior)
        payload = {"rows": rows}
        prefix = self._prefix(output, scope, prior)
        first = int(prior.handle["batches"][0]) if prior is not None else batch
        if prior is None:
            payload["reset"] = True
        await self._put(objects, _batch_path(prefix, "b", batch, _attempt_of(scope)), payload)
        version = digest([prior.version if prior else "", digest({"rows": _canonical(rows)})])
        handle = {"mode": "batches", "prefix": prefix, "batches": [first, batch]}
        return Written(self._ref(output, scope, handle, version))

    async def _store_keyed(self, objects, output, scope, prior, rows, remove, *, patch) -> Written:
        """Keyed incremental write: one `{"upsert"/"reset", "remove"}` object
        per batch. A replacement, or any write with no prior, starts the batch
        window over; a Patch extends it."""

        key_map(output, rows)  # validates the key column and duplicate keys
        batch = self._batch(scope, prior)
        prefix = self._prefix(output, scope, prior)
        attempt = _attempt_of(scope)
        extend = patch and prior is not None and "batches" in (prior.handle or {})
        if extend:
            payload = {"upsert": rows, "remove": sorted(remove)}
            first = int(prior.handle["batches"][0])
            version = digest([prior.version, digest({"rows": _canonical(rows), "remove": sorted(remove)})])
        else:
            payload = {"reset": True, "upsert": rows, "remove": []}
            first = batch
            version = digest(_canonical(rows))
        await self._put(objects, _batch_path(prefix, "b", batch, attempt), payload)
        snap = (prior.handle or {}).get("snapshot", -1) if extend else -1
        base = snap if snap >= first else first - 1
        if batch - base >= self.snapshot_every:
            state = await self._fold(objects, prefix, output.key, first, batch, snap)
            await self._put(objects, _batch_path(prefix, "s", batch, attempt), {"rows": list(state.values())})
            snap = batch
        handle = {
            "mode": "keyed",
            "key": output.key,
            "prefix": prefix,
            "batches": [first, batch],
            "snapshot": snap if snap >= first else -1,
        }
        return Written(self._ref(output, scope, handle, version))

    @staticmethod
    def _batch(scope, prior) -> int:
        if scope.batch is not None:
            return scope.batch
        batches = (prior.handle or {}).get("batches") if prior is not None else None
        return int(batches[1]) + 1 if batches else 0

    # -- reads ------------------------------------------------------------------

    async def load(self, ref: Ref, t, selection: Keys | Batches | None) -> Any:
        if is_ref_type(t):
            return ref
        objects = self._require_objects()
        handle = ref.handle or {}
        mode = handle.get("mode")
        if mode == "batches":
            if isinstance(selection, Batches):
                first, last = selection.lo, selection.hi
            elif selection is None:
                first, last = handle["batches"]
            else:
                raise StoreError(f"{ref.output}: an unkeyed incremental output takes Batches")
            rows = []
            for batch in await self._batch_objects(objects, handle["prefix"], int(first), int(last)):
                if batch.get("reset"):
                    rows = []
                rows += batch.get("rows", [])
            return self._materialize(rows, t)
        if mode == "keyed":
            if isinstance(selection, Batches):
                raise StoreError(f"{ref.output}: a keyed output takes Keys, not Batches")
            first, last = handle["batches"]
            state = await self._fold(
                objects, handle["prefix"], handle.get("key"), int(first), int(last), handle.get("snapshot")
            )
            rows = list(state.values())
            if selection is not None:
                rows = [r for r in rows if str(r.get(handle.get("key"))) in selection.revisions]
            return self._materialize(rows, t)
        payload = await self._read(objects, ref)
        if mode == "set":
            elements = payload or []
            if selection is not None:
                elements = [e for e in elements if str(e) in selection.revisions]
            return self._materialize(elements, t)
        if selection is not None:
            raise StoreError(f"{ref.output}: unkeyed output cannot serve a selection")
        return self._materialize(payload, t)

    async def _fold(self, objects, prefix, key_col, first, last, snap) -> dict:
        """The keyed content at batch `last`: the snapshot at `snap` (if any),
        then batches up to `last`; last writer wins per key, removes apply,
        `reset` batches start over. Missing batches — expired — are skipped."""

        state: dict[str, dict] = {}
        start = first
        if isinstance(snap, int) and first <= snap <= last:
            found = await self._winners(objects, prefix, "s", snap, snap)
            if snap in found:
                for row in (await self._get(objects, found[snap]) or {}).get("rows", []):
                    state[str(row[key_col])] = row
                start = snap + 1
        for batch in await self._batch_objects(objects, prefix, start, last):
            if batch.get("reset"):
                state.clear()
            for row in batch.get("upsert", []):
                state[str(row[key_col])] = row
            for key in batch.get("remove", []):
                state.pop(str(key), None)
        return state

    async def _batch_objects(self, objects, prefix, first, last) -> list[dict]:
        """The committed batch objects in `[first, last]`, in order, fetched in parallel."""

        import asyncio

        if last < first:
            return []
        found = await self._winners(objects, prefix, "b", first, last)
        limit = asyncio.Semaphore(32)

        async def get(path):
            async with limit:
                return await self._get(objects, path)

        batches = await asyncio.gather(*(get(found[b]) for b in sorted(found)))
        return [b for b in batches if b is not None]

    async def _winners(self, objects, prefix, kind, first, last) -> dict[int, str]:
        """Per batch in `[first, last]`, the object the newest attempt wrote."""

        import asyncio

        listed = await asyncio.gather(
            *(self._list(objects, f"{prefix}{k:09d}/") for k in range(first // 1000, last // 1000 + 1))
        )
        best: dict[int, tuple[str, str]] = {}
        for path in (p for paths in listed for p in paths):
            parsed = _parse(path)
            if parsed is None or parsed[0] != kind:
                continue
            _, batch, attempt = parsed
            if first <= batch <= last and (batch not in best or attempt > best[batch][0]):
                best[batch] = (attempt, path)
        return {b: path for b, (_, path) in best.items()}

    # -- retention (§9, §11) ------------------------------------------------------

    async def expire(self, head: Ref, before: float) -> None:
        """Delete what no version written after `before` needs; the head always
        stays loadable.

        Values: every older version object but the head's. Keyed batches:
        everything below the newest snapshot a version after `before` folds
        from. Unkeyed batches: the batches written before `before` — an event
        log's content is the batches it retains. Objects of attempts that
        never committed go once they are older than `before`."""

        import obstore

        objects = self._require_objects()
        handle = head.handle or {}
        mode = handle.get("mode")
        doomed = []
        if mode in ("value", "set"):
            folder = handle["object"].rsplit("/", 1)[0] + "/"
            for path in await self._list(objects, folder):
                parsed = _parse(path)
                at = _written_at(parsed[2]) if parsed else None
                if path != handle["object"] and at is not None and at < before:
                    doomed.append(path)
        elif mode in ("keyed", "batches"):
            first, last = (int(v) for v in handle["batches"])
            prefix = handle["prefix"]
            listing = await obstore.list_with_delimiter_async(objects, prefix)
            folders = sorted(p.rstrip("/") + "/" for p in listing["common_prefixes"])
            # Oldest directories first, up to the one holding the first batch
            # written after `before`: everything older is below it.
            winners: dict[tuple[str, int], tuple[str, str]] = {}
            horizon = None
            for folder in folders:
                for path in sorted(await self._list(objects, folder)):
                    parsed = _parse(path)
                    if parsed is None or parsed[0] == "v":
                        continue
                    kind, batch, attempt = parsed
                    current = winners.get((kind, batch))
                    if current is None or attempt > current[0]:
                        if current is not None and (_written_at(current[0]) or before) < before:
                            doomed.append(current[1])  # an attempt that never committed
                        winners[(kind, batch)] = (attempt, path)
                    elif (_written_at(attempt) or before) < before:
                        doomed.append(path)
                recent = [
                    b
                    for (kind, b), (attempt, _) in winners.items()
                    if kind == "b" and b <= last and (_written_at(attempt) or 0) >= before
                ]
                if recent:
                    horizon = min(recent)
                    break
            horizon = last if horizon is None else horizon
            if mode == "batches":
                keep = horizon
            else:
                snaps = [b for kind, b in winners if kind == "s"]
                inside = [b for b in snaps if first <= b <= horizon] if first <= horizon else []
                older = [b for b in snaps if b <= horizon]
                keep = max(inside) if inside else first if first <= horizon else max(older, default=0)
            doomed += [path for (_, b), (_, path) in winners.items() if b < keep]
        for i in range(0, len(doomed), 1000):
            await obstore.delete_async(objects, doomed[i : i + 1000])
        remove_empty_dirs(objects, [p.rsplit("/", 1)[0] for p in doomed])

    async def _list(self, objects, prefix: str) -> list[str]:
        import obstore

        out = []
        async for chunk in obstore.list(objects, prefix=prefix):
            out.extend(meta["path"] for meta in chunk)
        return out

    # -- helpers ----------------------------------------------------------------

    @staticmethod
    def _prefix(output, scope, prior=None) -> str:
        """The scope's directory: the prior window's, so a renamed output keeps
        extending its batches where they are (§2)."""

        if prior is not None and "prefix" in (prior.handle or {}):
            return prior.handle["prefix"]
        # `_` is the unscoped directory — object paths cannot hold an empty segment.
        return f"data/{output.name}/{quote(scope.partition or '_', safe='')}/"

    async def _payload(self, objects, ref) -> list:
        payload = await self._read(objects, ref)
        return payload if isinstance(payload, list) else []

    def _materialize(self, payload, t):
        if t is None or t is inspect.Parameter.empty:
            return payload
        if _is_dataframe_type(t):
            import pandas as pd

            return pd.DataFrame(payload if isinstance(payload, list) else [payload])
        return payload

    def _ref(self, output, scope, handle, version) -> JsonRef:
        return JsonRef(
            output=output.name,
            store="",
            handle=handle,
            version=version,
            partition=scope.partition,
        )

    async def _put(self, objects, path: str, payload):
        import obstore

        body = json.dumps(payload, sort_keys=True, allow_nan=False).encode()
        await obstore.put_async(objects, path, body, mode="overwrite", use_multipart=False)

    async def _get(self, objects, path: str):
        import obstore
        from obstore.exceptions import NotFoundError

        try:
            result = await obstore.get_async(objects, path)
        except (NotFoundError, FileNotFoundError):
            return None
        return json.loads(bytes(await result.bytes_async()))

    async def _read(self, objects, ref: Ref):
        import obstore

        result = await obstore.get_async(objects, ref.handle["object"])
        return json.loads(bytes(await result.bytes_async()))

    def _require_objects(self):
        if self._objects is None:
            raise StoreError("JsonStore is not bound to an object store")
        return self._objects


def _is_list_of(t: Any, inner: type) -> bool:
    return typing.get_origin(t) is list and typing.get_args(t) == (inner,)


def _canonical(value: Any) -> Any:
    if _is_dataframe(value):
        return value.to_dict(orient="records")
    return value


class BlobStore:
    """`bytes` / `Path` payloads, content-addressed (§4)."""

    version = "1"
    ref_type = BlobRef
    shared_table = False

    def __init__(self, url: str | None = None):
        self.url = url
        self._objects = None
        self._migrate_lock = None

    def bind_objects(self, objects) -> None:
        if self.url is None:
            self._objects = objects

    def _resolve(self):
        if self._objects is not None:
            return self._objects
        if self.url:
            import obstore

            self._objects = obstore.store.from_url(resolve_env(self.url))
            return self._objects
        raise StoreError("BlobStore is not bound to an object store")

    def can_load(self, t, selection) -> bool:
        if selection is not None:
            return False
        return t is None or t in (bytes, Path) or (is_ref_type(t) and issubclass(self.ref_type, t))

    def can_store(self, t, output) -> bool:
        if output.incremental or output.is_partition_set:
            return False
        return t is None or t in (bytes, Path, Callable)

    async def migrate(self, output: Output, migrations) -> list[str]:
        """Apply pending callable migrations over the output's blob prefix,
        recording each in `_migrations.json` next to the data (§4)."""

        import asyncio

        import obstore
        from obstore.exceptions import NotFoundError

        objects = self._resolve()
        prefix = f"blobs/{output.name}/"
        ledger_path = prefix + "_migrations.json"
        if self._migrate_lock is None:
            self._migrate_lock = asyncio.Lock()
        async with self._migrate_lock:
            try:
                result = await obstore.get_async(objects, ledger_path)
                ledger = json.loads(bytes(await result.bytes_async()))
            except (NotFoundError, FileNotFoundError):
                ledger = {"applied": []}
            applied = {entry["name"] for entry in ledger["applied"]}
            for migration in migrations:
                if migration.name in applied:
                    continue
                if not callable(migration.payload):
                    raise StoreError(
                        f"{output.name}: BlobStore migration {migration.name!r} requires a callable payload"
                    )
                value = migration.payload(objects, prefix)
                if inspect.isawaitable(value):
                    await value
                ledger["applied"].append({"name": migration.name, "at": self._now()})
                applied.add(migration.name)
                body = json.dumps(ledger, sort_keys=True, allow_nan=False).encode()
                await obstore.put_async(objects, ledger_path, body, mode="overwrite", use_multipart=False)
            return [m.name for m in migrations if m.name in applied]

    @staticmethod
    def _now() -> str:
        import datetime as dt

        return dt.datetime.now(dt.UTC).isoformat()

    async def store(self, write, prior: Ref | None, scope: Scope) -> Written:
        import hashlib

        import obstore

        objects = self._resolve()
        if isinstance(write, Path):
            data = write.read_bytes()
        elif isinstance(write, (bytes, bytearray)):
            data = bytes(write)
        else:
            raise WriteError(
                f"{scope.output.name}: BlobStore accepts bytes or Path, not {type(write).__name__}"
            )
        version = digest({"blob": True, "sha": hashlib.sha256(data).hexdigest()})
        if prior is not None and prior.version == version:
            return Written(prior)
        folder = f"blobs/{scope.output.name}/{quote(scope.partition or '_', safe='')}/"
        path = f"{folder}v{_attempt_of(scope)}.bin"
        await obstore.put_async(objects, path, data, mode="overwrite", use_multipart=False)
        return Written(
            BlobRef(
                output=scope.output.name,
                store="",
                handle={"object": path, "bytes": len(data)},
                version=version,
                partition=scope.partition,
            )
        )

    async def expire(self, head: Ref, before: float) -> None:
        """Delete every older version but the head's (§9, §11)."""

        import obstore

        objects = self._resolve()
        keep = head.handle["object"]
        folder = keep.rsplit("/", 1)[0] + "/"
        doomed = []
        async for chunk in obstore.list(objects, prefix=folder):
            for meta in chunk:
                name = meta["path"].rsplit("/", 1)[-1]
                at = _written_at(name[1:-4]) if name.startswith("v") and name.endswith(".bin") else None
                if meta["path"] != keep and at is not None and at < before:
                    doomed.append(meta["path"])
        for i in range(0, len(doomed), 1000):
            await obstore.delete_async(objects, doomed[i : i + 1000])

    async def load(self, ref: Ref, t, selection: Keys | None) -> Any:
        import obstore

        if is_ref_type(t):
            return ref
        if selection is not None:
            raise StoreError("BlobStore cannot serve a Keys selection")
        result = await obstore.get_async(self._resolve(), ref.handle["object"])
        data = bytes(await result.bytes_async())
        if t is Path or t is None or t is bytes:
            if t is Path:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".blob")
                tmp.write(data)
                tmp.close()
                return Path(tmp.name)
            return data
        raise StoreError(f"BlobStore cannot load {t}")
