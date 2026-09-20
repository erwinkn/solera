from __future__ import annotations

import asyncio
import hashlib
import json
import re
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import unquote, urlsplit

import obstore
from obstore.exceptions import AlreadyExistsError
from obstore.store import LocalStore, MemoryStore
from slatedb.uniffi import DbBuilder, IsolationLevel, KeyRange, ObjectStore, Settings


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

    @classmethod
    async def open(cls, url: str, namespace="default", *, flush_interval="100ms"):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", namespace):
            raise ValueError("Namespace must contain 1–64 letters, digits, underscores or hyphens")
        u = urlsplit(url)
        if u.query or u.fragment or u.username or u.password:
            raise ValueError("Credentials and query parameters do not belong in storage URLs")
        if u.scheme == "file":
            if u.netloc not in ("", "localhost") or not u.path.startswith("/"):
                raise ValueError("File storage requires an absolute local path")
            root = Path(unquote(u.path)).resolve() / namespace
            root.mkdir(parents=True, exist_ok=True)
            native, db_path = ObjectStore.resolve("file:///"), str(root / "metadata")
            objects = LocalStore(root / "objects", mkdir=True)
            objects_url = (root / "objects").as_uri()
        elif u.scheme == "s3" and u.netloc:
            prefix = u.path.strip("/")
            if ".." in prefix.split("/"):
                raise ValueError("Invalid object prefix")
            base = "/".join(filter(None, (prefix, namespace)))
            native, db_path = ObjectStore.resolve(f"s3://{u.netloc}"), f"{base}/metadata"
            objects_url = f"s3://{u.netloc}/{base}/objects"
            objects = obstore.store.from_url(objects_url)
        elif u.scheme == "memory":
            native, db_path, objects = ObjectStore.resolve("memory:///"), namespace, MemoryStore()
            objects_url = "memory:///"
        else:
            raise ValueError("Use file:///absolute/path, s3://bucket/prefix, or memory:///")
        builder = DbBuilder(db_path, native)
        settings = Settings.default()
        settings.set("flush_interval", json.dumps(flush_interval))
        settings.set("max_unflushed_bytes", str(32 * 1024 * 1024))
        settings.set("l0_sst_size_bytes", str(8 * 1024 * 1024))
        builder.with_settings(settings)
        state = cls(await builder.build(), objects, url, namespace)
        state.objects_url = objects_url
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

    async def close(self):
        await self.db.shutdown()
