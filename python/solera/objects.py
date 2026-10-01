"""Object-store writes the harness and the engine both decide by.

A create-only PUT decides who owns a name. Its outcome can be ambiguous:
the object lands but the response is lost, and the retry — obstore's or
the caller's — finds it there. So an object found in the way is read back,
and if it holds exactly the bytes being written, the earlier try was this
write's own and it succeeded.
"""

from __future__ import annotations

import obstore
from obstore.exceptions import AlreadyExistsError


async def create(store, path: str, data: bytes) -> None:
    """Create `path` holding `data`. Raises `AlreadyExistsError` only if
    another writer's object is there."""

    try:
        await obstore.put_async(store, path, data, mode="create", use_multipart=False)
    except AlreadyExistsError:
        existing = await obstore.get_async(store, path)
        if bytes(await existing.bytes_async()) != data:
            raise
