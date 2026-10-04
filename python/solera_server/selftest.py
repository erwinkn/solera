"""An explicit, destructive-to-its-own-namespace integration probe.

Uses only generated synthetic data under a fresh `probe-UUID` namespace. It does
not delete or rewrite any existing workspace. No credentials are printed.
"""

from __future__ import annotations

import time
import uuid
from urllib.parse import urlsplit

import obstore
from obstore.exceptions import AlreadyExistsError

from .executors.local import load_manifest
from .state import State, Unavailable


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


async def create_only_probe(objects):
    """The journal's fencing rests on create-only writes being enforced."""

    key = "conformance/" + uuid.uuid4().hex
    await obstore.put_async(objects, key, b"first", mode="create", use_multipart=False)
    try:
        await obstore.put_async(objects, key, b"duplicate", mode="create", use_multipart=False)
    except AlreadyExistsError:
        pass
    else:
        raise RuntimeError("Backend ignored create-if-absent")
    await obstore.delete_async(objects, key)


async def key_index_probe(objects):
    """Index files are written create-only and read back by range (§6)."""

    from solera.keys import Rows
    from solera.keys.io import ObjectIO
    from solera.keys.layers import LayerIndex, LayerState

    io = ObjectIO(objects)
    state = LayerState(prefix="conformance/keys/", life="0")
    name = f"{0:012d}-{uuid.uuid4().hex}"
    files, _ = await LayerIndex(io, state).write_replace(
        Rows.keys([b"b", b"a"], b"1"), name=name, generation=7
    )
    state = state.committed(0, files)
    try:
        index = LayerIndex(io, state)
        rows, _ = await index.delta(None, first=10)
        check(
            [(r[0], r[3], r[4]) for r in rows] == [(b"a", 7, b"1"), (b"b", 7, b"1")],
            "Key index page read back wrong",
        )
        check(await index.lookup([b"b"]) == {b"b": (7, b"1")}, "Key index lookup read back wrong")
        rows, _ = await index.delta(None, keys=[b"a", b"c"])
        check([r[0] for r in rows] == [b"a"], "Key index key list read back wrong")
    finally:
        await io.delete([state.path(n) for n in state.referenced()])


async def selftest(url):
    started = time.monotonic()
    namespace = "probe-" + uuid.uuid4().hex
    manifest = await load_manifest("solera_server.demo:project")
    checks = []
    state = await State.open(url, namespace)
    try:
        await create_only_probe(state.objects)
        checks.append("create-if-absent writes are enforced")
        await key_index_probe(state.objects)
        checks.append("key index files write and read back by range")
        from .engine import Engine

        engine = Engine(state, manifest, project="solera_server.demo:project")
        await engine.initialize()
        checks.append("manifest registration and control-plane initialization")
    finally:
        await state.close()

    # A new writer restores everything from the object store and fences the old one.
    old = await State.open(url, namespace)
    new = await State.open(url, namespace)
    try:
        check(new.model.deploy == manifest["deploy"], "Registration did not survive a restart")
        new.record({"type": "AutomationChanged", "name": "__probe__", "enabled": True})
        await new.durable()
        try:
            old.record({"type": "AutomationChanged", "name": "__probe__", "enabled": False})
            await old.durable()
        except Unavailable:
            pass
        else:
            raise RuntimeError("Superseded writer acknowledged a new write")
        checks.append("state restores from the object store; a new writer fences the old one")
    finally:
        await new.close()
        await old.close()
    return {
        "status": "passed",
        "scheme": urlsplit(url).scheme,
        "namespace": namespace,
        "checks": checks,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
