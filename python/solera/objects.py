"""Object-store writes the worker and the engine both decide by
(docs/object-store-state.md §0).

A create-only PUT decides who owns a name. Its outcome can be ambiguous:
the object lands but the response is lost, and the retry — obstore's or
the caller's — finds it there. So an object found in the way is read back,
and if it holds exactly the bytes being written, the earlier try was this
write's own and it succeeded.

An object that is overwritten is read with its ETag and replaced with
`swap`, a compare-and-swap (`If-Match`). A swap refused or unanswered is
settled the same way, by reading the object back: so no two writes may
produce the same bytes for one object — every body `swap` writes names its
writer and never repeats. `file://` has no `If-Match`: there `swap` takes
an `fcntl` lock and compares SHA-256 digests, which serve as its ETags.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import os
import uuid

import obstore
from obstore.exceptions import (
    AlreadyExistsError,
    InvalidPathError,
    NotFoundError,
    NotSupportedError,
    PermissionDeniedError,
    UnauthenticatedError,
)

ATTEMPTS = 5  # writes of one swap whose outcome reads back as "nothing landed"
_FINAL = (InvalidPathError, NotSupportedError, PermissionDeniedError, UnauthenticatedError)


class Conflict(Exception):
    """Another writer's write is there: the swap's ETag is no longer current."""


async def create(store, path: str, data: bytes) -> None:
    """Create `path` holding `data`. Raises `AlreadyExistsError` only if
    another writer's object is there."""

    try:
        await obstore.put_async(store, path, data, mode="create", use_multipart=False)
    except AlreadyExistsError:
        existing = await obstore.get_async(store, path)
        if bytes(await existing.bytes_async()) != data:
            raise


async def read(store, path: str) -> tuple[bytes, str] | None:
    """`(data, etag)` of the object at `path`, or None if there is none."""

    try:
        got = await obstore.get_async(store, path)
    except (NotFoundError, FileNotFoundError):
        return None
    data = bytes(await got.bytes_async())
    return data, (_digest(data) if _local(store) else got.meta["e_tag"])


async def swap(store, path: str, data: bytes, etag: str | None) -> str:
    """Write `data` at `path` only if its ETag is still `etag` (None: only if
    there is no object); returns the new ETag. Raises `Conflict` if another
    writer's write is there. A refused or unanswered write reads the object
    back: holding exactly `data`, it landed; still at `etag`, it is written
    again; anything else is a conflict."""

    if _local(store):
        return await asyncio.to_thread(_swap_file, _file(store, path), data, etag)
    error: BaseException | None = None
    for _ in range(ATTEMPTS):
        mode = "create" if etag is None else {"e_tag": etag}
        try:
            put = await obstore.put_async(store, path, data, mode=mode, use_multipart=False)
            return put["e_tag"]
        except _FINAL:
            raise
        except Exception as e:  # refused (412, 409, exists), or the answer lost: read back
            error = e
        found = await read(store, path)
        if found is not None and found[0] == data:
            return found[1]  # it landed: the earlier try was this write
        if (found[1] if found is not None else None) != etag:
            raise Conflict(f"{path}: another writer's write is there") from error
        # still at `etag`: nothing landed (409: a concurrent conditional write lost), so again
    raise error  # type: ignore[misc]


def _local(store) -> bool:
    return type(store).__name__ == "LocalStore"


def _file(store, path: str) -> str:
    root = store.prefix
    return os.path.join(str(root), path) if root is not None else os.path.join("/", path)


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _swap_file(full: str, data: bytes, etag: str | None) -> str:
    """`swap` on a local file: under an `flock` on `{full}.lock`, which the
    kernel drops if the process dies, compare the file's digest with
    `etag`, then write a temporary file, fsync it and replace the object."""

    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(f"{full}.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            try:
                with open(full, "rb") as f:
                    current = f.read()
            except FileNotFoundError:
                current = None
            if current == data:
                return _digest(data)  # an earlier try of this write
            if (_digest(current) if current is not None else None) != etag:
                raise Conflict(f"{full}: another writer's write is there")
            tmp = f"{full}.{uuid.uuid4().hex}.tmp"
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, full)
            directory = os.open(os.path.dirname(full), os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return _digest(data)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
