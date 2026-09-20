"""An explicit, destructive-to-its-own-namespace integration probe.

Uses only generated synthetic data under a fresh `probe-UUID` namespace. It does
not delete or rewrite any existing workspace. No credentials are printed.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from urllib.parse import urlsplit

import obstore
from obstore.exceptions import AlreadyExistsError, PreconditionError

from .placements.local import load_manifest
from .state import State
from .storage import SlateState, Unavailable


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


async def conditional_probe(objects):
    key = "conformance/" + uuid.uuid4().hex
    version = await obstore.put_async(objects, key, b"first", mode="create", use_multipart=False)
    try:
        await obstore.put_async(objects, key, b"duplicate", mode="create", use_multipart=False)
    except AlreadyExistsError:
        pass
    else:
        raise RuntimeError("Backend ignored create-if-absent")
    expected = {k: version[k] for k in ("e_tag", "version") if version.get(k) is not None}
    check(bool(expected.get("e_tag")), "Backend did not return an ETag")
    results = await asyncio.gather(
        *[
            obstore.put_async(objects, key, value, mode=expected, use_multipart=False)
            for value in (b"winner-a", b"winner-b")
        ],
        return_exceptions=True,
    )
    check(
        sum(not isinstance(r, BaseException) for r in results) == 1,
        "Conditional-update race did not have exactly one winner",
    )
    failures = [r for r in results if isinstance(r, BaseException)]
    check(
        isinstance(failures[0], PreconditionError),
        f"Unexpected conditional-update failure: {type(failures[0]).__name__}",
    )
    await obstore.delete_async(objects, key)


async def selftest(url):
    started = time.monotonic()
    namespace = "probe-" + uuid.uuid4().hex
    manifest = await load_manifest("dorc.demo:project")
    slate = await SlateState.open(url, namespace)
    state = State(slate)
    checks = []
    try:
        # LocalStore need not implement ETag-based update; the production S3
        # conformance probe is mandatory for a remote run, never silently skipped.
        if urlsplit(url).scheme == "s3":
            await conditional_probe(state.objects)
            checks.append("create-if-absent and competing conditional updates")
        from .engine import Engine

        engine = Engine(state, manifest, project="dorc.demo:project")
        await engine.initialize()
        checks.append("manifest registration and control-plane initialization")
    finally:
        await state.close()
    # New native writer: all authority is restored from the selected object
    # store. No local database files or coordinator checkpoint are reused.
    slate = await SlateState.open(url, namespace)
    state = State(slate)
    try:
        replacement = await SlateState.open(url, namespace)
        try:
            async with replacement.transaction() as tx:
                await tx.put("probe/new-writer", True)
            try:
                async with asyncio.timeout(20):
                    async with state.transaction() as tx:
                        await tx.put("probe/stale-writer", True)
            except (Unavailable, TimeoutError):
                pass
            else:
                raise RuntimeError("Superseded native writer acknowledged a new write")
            check(await replacement.get("probe/stale-writer") is None, "Stale write became durable")
            checks.append("native writer takeover fences the old writer")
        finally:
            await replacement.close()
    finally:
        with contextlib.suppress(Exception):
            await state.close()
    return {
        "status": "passed",
        "scheme": urlsplit(url).scheme,
        "namespace": namespace,
        "checks": checks,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
