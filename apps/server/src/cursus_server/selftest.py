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

from .placements.local import load_manifest
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


async def selftest(url):
    started = time.monotonic()
    namespace = "probe-" + uuid.uuid4().hex
    manifest = await load_manifest("cursus_server.demo:project")
    checks = []
    state = await State.open(url, namespace)
    try:
        await create_only_probe(state.objects)
        checks.append("create-if-absent writes are enforced")
        from .engine import Engine

        engine = Engine(state, manifest, project="cursus_server.demo:project")
        await engine.initialize()
        checks.append("manifest registration and control-plane initialization")
    finally:
        await state.close()

    # A new writer restores everything from the object store and fences the old one.
    old = await State.open(url, namespace)
    new = await State.open(url, namespace)
    try:
        check(new.model.revision == manifest["revision"], "Registration did not survive a restart")
        await new.emit({"type": "AutomationChanged", "name": "__probe__", "enabled": True})
        try:
            await old.emit({"type": "AutomationChanged", "name": "__probe__", "enabled": False})
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
