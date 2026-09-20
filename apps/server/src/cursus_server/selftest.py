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

from .engine import Engine
from .execution import LocalSubprocess
from .storage import SlateState, Unavailable


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


async def drain(engine, run_id, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        await engine.execute_next()
        detail = await engine.run_detail(run_id)
        if detail["request"]["status"] in {"succeeded", "failed", "canceled"}:
            check(detail["request"]["status"] == "succeeded", f"Run failed: {detail}")
            return detail
        await asyncio.sleep(0.02)
    raise TimeoutError("Materialization did not finish")


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
    backend = LocalSubprocess("cursus_server.demo:project")
    manifest = await backend.manifest()
    state = await SlateState.open(url, namespace)
    checks = []
    try:
        # LocalStore need not implement ETag-based update; the production S3
        # conformance probe is mandatory for a remote run, never silently skipped.
        if urlsplit(url).scheme == "s3":
            await conditional_probe(state.objects)
            checks.append("create-if-absent and competing conditional updates")
        engine = Engine(state, manifest, backend)
        await engine.initialize()
        run = await engine.submit(["sample_quality"], command_id="first-request")
        await drain(engine, run["id"])
        samples = await engine.asset_detail("samples")
        measurements = await engine.asset_detail("measurements")
        check(
            samples["head"]["commit_id"] == measurements["head"]["commit_id"],
            "Multi-output commit was not shared",
        )
        check(len(samples["preview"]) == 3, "Incorrect initial inventory")
        checks.append("real subprocess DAG and atomic multi-output batches")
        before = samples["head"]["commit_id"]
        second = await engine.submit(["sample_quality"])
        await drain(engine, second["id"])
        check(
            (await engine.asset_detail("samples"))["head"]["commit_id"] == before,
            "Unchanged input was not skipped",
        )
        checks.append("unchanged keyed inventory does not republish")
        third = await engine.submit(
            ["sample_quality"],
            config={"files": [{"id": "LAB-001", "revision": "2", "sample": "Revised", "calcium": None}]},
        )
        await drain(engine, third["id"])
        check(
            (await engine.asset_detail("measurements"))["preview"] == [],
            "Empty replacement did not remove old rows",
        )
        check(len((await engine.asset_detail("samples"))["preview"]) == 1, "Deletion was not propagated")
        checks.append("revisions, source deletion, and empty child replacement")
        backfill = await engine.submit(["daily_report"], partitions=["2026-01-01", "2026-01-02"])
        await drain(engine, backfill["id"])
        check(
            (await engine.asset_detail("daily_report", "2026-01-02"))["preview"][0]["date"] == "2026-01-02",
            "Partition mismatch",
        )
        checks.append("bounded daily backfill")
    finally:
        await state.close()
    # New native writer: all authority is restored from the selected object
    # store. No local database files or coordinator checkpoint are reused.
    state = await SlateState.open(url, namespace)
    try:
        engine = Engine(state, manifest, backend)
        await engine.initialize()
        recovered = await engine.submit(["sample_quality"], command_id="first-request")
        check(recovered["id"] == run["id"], "Command receipt did not survive reopen")
        check(
            (await engine.asset_detail("samples"))["preview"][0]["name"] == "Revised",
            "Output did not survive reopen",
        )
        checks.append("new-writer recovery and durable idempotency receipt")
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
