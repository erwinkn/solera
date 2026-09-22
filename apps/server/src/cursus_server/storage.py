from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import unquote, urlsplit

import obstore
from obstore.exceptions import AlreadyExistsError
from obstore.store import LocalStore, MemoryStore
from slatedb.uniffi import (
    AdminBuilder,
    DbBuilder,
    FlushOptions,
    FlushType,
    GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions,
    IsolationLevel,
    KeyRange,
    ObjectStore,
    Settings,
)


class Unavailable(RuntimeError):
    """The current writer cannot safely acknowledge further commands."""


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


class Transaction:
    def __init__(self, native):
        self.native = native
        self.dirty = False

    async def get(self, key: str, default=None):
        value = await self.native.get(key.encode())
        return json.loads(value) if value is not None else default

    async def put(self, key: str, value):
        self.dirty = True
        await self.native.put(key.encode(), encode(value))

    async def delete(self, key: str):
        self.dirty = True
        await self.native.delete(key.encode())

    async def scan(self, prefix: str, limit: int | None = None):
        raw = prefix.encode()
        it = await self.native.scan(
            KeyRange(start=raw, start_inclusive=True, end=raw + b"\xff", end_inclusive=False)
        )
        values = []
        while limit is None or len(values) < limit:
            item = await it.next()
            if item is None:
                break
            values.append((item.key.decode(), json.loads(item.value)))
        return values


class SlateState:
    """Single active writer; SlateDB owns WAL, fencing, replay and compaction.

    The lock spans remote durability so API reads cannot see an unacknowledged
    memory-only write. Ambiguous commit errors poison the instance. Restart and
    resolve the command receipt; never blindly retry an uncertain publication.
    """

    def __init__(self, db, objects, url, namespace):
        self.db, self.objects = db, objects
        self.url, self.namespace = url, namespace
        self.lock = asyncio.Lock()
        self.poisoned = False
        self.last_sequence = 0
        self.objects_url = None
        self.db_path = None
        self.native = None

    @classmethod
    async def open(cls, url: str, namespace="default", *, flush_interval=None):
        native, db_path, objects, objects_url = _resolve(url, namespace, create=True)
        builder = DbBuilder(db_path, native)
        settings = Settings.default()
        # §4.4 of the storage redesign: `await_durable` resolves on the flush
        # tick, so the interval doubles as durable-commit latency; 1 s caps WAL
        # object creation at ~1/s under the single-writer model. Tests and the
        # soak gate set CURSUS_SLATE_FLUSH_INTERVAL lower to keep wall time
        # reasonable. `l0_sst_size_bytes` bounds restart replay.
        flush_interval = flush_interval or os.environ.get("CURSUS_SLATE_FLUSH_INTERVAL", "1s")
        settings.set("flush_interval", json.dumps(flush_interval))
        settings.set("max_unflushed_bytes", str(32 * 1024 * 1024))
        settings.set("l0_sst_size_bytes", str(1024 * 1024))
        builder.with_settings(settings)
        state = cls(await builder.build(), objects, url, namespace)
        state.objects_url = objects_url
        state.db_path, state.native = db_path, native
        try:
            async with state.transaction() as tx:
                schema = await tx.get("system/schema")
                if schema is None:
                    await tx.put("system/schema", 1)
                elif schema != 1:
                    raise ValueError(f"Unsupported state schema: {schema}")
        except BaseException:
            await state.close()
            raise
        return state

    @asynccontextmanager
    async def transaction(self):
        async with self.lock:
            if self.poisoned:
                raise Unavailable("Writer lost authority or a commit outcome is uncertain; restart required")
            try:
                native = await self.db.begin(IsolationLevel.SERIALIZABLE_SNAPSHOT)
            except Exception as error:
                self.poisoned = True
                raise Unavailable("Native writer is unavailable; restart required") from error
            tx, committing = Transaction(native), False
            try:
                yield tx
                if tx.dirty:
                    committing = True
                    handle = await native.commit()
                    if handle is not None:
                        await handle.await_durable()
                        self.last_sequence = handle.seqnum()
                else:
                    await native.rollback()
            except BaseException as error:
                if committing:
                    self.poisoned = True
                    if isinstance(error, Exception):
                        raise Unavailable(
                            "Commit outcome is uncertain; restart and resolve its receipt"
                        ) from error
                else:
                    await native.rollback()
                raise

    async def get(self, key, default=None):
        async with self.transaction() as tx:
            return await tx.get(key, default)

    async def scan(self, prefix, limit=None):
        async with self.transaction() as tx:
            return await tx.scan(prefix, limit)

    async def stage(self, value, *, complete=True):
        data = encode(value)
        if len(data) > 64 * 1024 * 1024:
            raise ValueError("Output exceeds the alpha's 64 MiB JSON snapshot limit")
        checksum = hashlib.sha256(data).hexdigest()
        key = f"sha256/{checksum}.json"
        try:
            await obstore.put_async(self.objects, key, data, mode="create", use_multipart=False)
        except AlreadyExistsError:
            await self.load({"key": key, "sha256": checksum})
        return {
            "key": key,
            "sha256": checksum,
            "bytes": len(data),
            "rows": len(value) if isinstance(value, list) else None,
            "complete": complete,
        }

    async def load(self, ref):
        expected = f"sha256/{ref['sha256']}.json"
        if ref["key"] != expected or not re.fullmatch(r"[0-9a-f]{64}", ref["sha256"]):
            raise ValueError("Invalid artifact reference")
        result = await obstore.get_async(self.objects, ref["key"])
        data = bytes(await result.bytes_async())
        if hashlib.sha256(data).hexdigest() != ref["sha256"]:
            raise ValueError("Artifact checksum mismatch")
        return json.loads(data)

    async def gc_once(self, *, min_age_ms: int = 300_000, dry_run: bool = False):
        """Flush the memtable so sealed WAL SSTs become collectible, then run
        one SlateDB garbage-collection pass over manifests, WAL, compacted
        SSTs and compaction state — safe alongside the open writer (§4.4)."""

        if self.db_path is None:
            return
        if not dry_run:
            # WAL SSTs stay referenced until their contents reach L0; the L0
            # flush thresholds (max_wal_flushes_before_l0_flush,
            # max_unflushed_bytes) are sized for far heavier write load, so a
            # durable-commit-heavy engine needs the explicit flush.
            await self.db.flush_with_options(FlushOptions(flush_type=FlushType.MEM_TABLE))
        admin = AdminBuilder(self.db_path, self.native).build()
        await admin.run_gc_once(_gc_options(min_age_ms, dry_run))

    async def close(self):
        await self.db.shutdown()


def _resolve(url: str, namespace: str, *, create: bool):
    """(native ObjectStore, db_path, objects, objects_url) for a state URL."""

    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", namespace):
        raise ValueError("Namespace must contain 1–64 letters, digits, underscores or hyphens")
    u = urlsplit(url)
    if u.query or u.fragment or u.username or u.password:
        raise ValueError("Credentials and query parameters do not belong in storage URLs")
    if u.scheme == "file":
        if u.netloc not in ("", "localhost") or not u.path.startswith("/"):
            raise ValueError("File storage requires an absolute local path")
        root = Path(unquote(u.path)).resolve() / namespace
        if create:
            root.mkdir(parents=True, exist_ok=True)
        return (
            ObjectStore.resolve("file:///"),
            str(root / "metadata"),
            LocalStore(root / "objects", mkdir=create),
            (root / "objects").as_uri(),
        )
    if u.scheme == "s3" and u.netloc:
        prefix = u.path.strip("/")
        if ".." in prefix.split("/"):
            raise ValueError("Invalid object prefix")
        base = "/".join(filter(None, (prefix, namespace)))
        objects_url = f"s3://{u.netloc}/{base}/objects"
        return (
            ObjectStore.resolve(f"s3://{u.netloc}"),
            f"{base}/metadata",
            obstore.store.from_url(objects_url),
            objects_url,
        )
    if u.scheme == "memory":
        return ObjectStore.resolve("memory:///"), namespace, MemoryStore(), "memory:///"
    raise ValueError("Use file:///absolute/path, s3://bucket/prefix, or memory:///")


def _gc_options(min_age_ms: int, dry_run: bool) -> GarbageCollectorOptions:
    def directory():
        return GarbageCollectorDirectoryOptions(min_age_ms=min_age_ms, dry_run=dry_run)

    return GarbageCollectorOptions(
        manifest_options=directory(),
        wal_options=directory(),
        compacted_options=directory(),
        compactions_options=directory(),
    )


async def gc_once(url: str, namespace: str, *, min_age_ms: int = 300_000, dry_run: bool = False):
    """One-shot GC without opening a writer — the `cursus gc` path (§4.4)."""

    native, db_path, _, _ = _resolve(url, namespace, create=True)
    admin = AdminBuilder(db_path, native).build()
    await admin.run_gc_once(_gc_options(min_age_ms, dry_run))
