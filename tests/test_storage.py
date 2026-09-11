import asyncio
import contextlib
import os
import subprocess
import sys
import uuid

import pytest
from conftest import finish

from data_orchestrator.engine import Engine
from data_orchestrator.execution import LocalSubprocess
from data_orchestrator.selftest import selftest
from data_orchestrator.storage import SlateState, Unavailable


async def test_transaction_rollback_and_reopen(tmp_path):
    state = await SlateState.open(tmp_path.as_uri(), flush_interval="1ms")
    async with state.transaction() as tx:
        await tx.put("a", 1)
        await tx.put("b", 2)
    with pytest.raises(ValueError):
        async with state.transaction() as tx:
            await tx.put("a", 3)
            await tx.delete("b")
            raise ValueError("abort")
    await state.close()
    state = await SlateState.open(tmp_path.as_uri(), flush_interval="1ms")
    try:
        assert await state.get("a") == 1
        assert await state.get("b") == 2
    finally:
        await state.close()


async def test_artifact_integrity_and_namespace_isolation(tmp_path):
    first = await SlateState.open(tmp_path.as_uri(), "first", flush_interval="1ms")
    second = await SlateState.open(tmp_path.as_uri(), "second", flush_interval="1ms")
    try:
        ref = await first.stage([{"value": 1}])
        assert await first.stage([{"value": 1}]) == ref
        assert await first.load(ref) == [{"value": 1}]
        async with first.transaction() as tx:
            await tx.put("private/key", ref)
        assert await second.get("private/key") is None
        with pytest.raises(ValueError):
            await first.load({**ref, "key": "../outside"})
        import obstore

        await obstore.put_async(first.objects, ref["key"], b"corrupt")
        with pytest.raises(ValueError, match="checksum"):
            await first.load(ref)
    finally:
        await first.close()
        await second.close()


async def test_memory_object_store():
    state = await SlateState.open("memory:///", flush_interval="1ms")
    try:
        async with state.transaction() as tx:
            await tx.put("hello", "world")
        assert await state.get("hello") == "world"
        assert await state.load(await state.stage([1, 2])) == [1, 2]
    finally:
        await state.close()


@pytest.mark.parametrize(
    "url,namespace",
    [
        ("https://example.com", "default"),
        ("file://remote/path", "default"),
        ("file:///tmp/test", "../escape"),
        ("s3://key:secret@bucket/path", "default"),
    ],
)
async def test_invalid_configuration(url, namespace):
    with pytest.raises(ValueError):
        await SlateState.open(url, namespace)


class GateHandle:
    def __init__(self, reached, release, fail):
        self.reached, self.release, self.fail = reached, release, fail

    async def await_durable(self):
        self.reached.set()
        await self.release.wait()
        if self.fail:
            raise OSError("Lost durable acknowledgement")

    def seqnum(self):
        return 7


class GateTransaction:
    def __init__(self, db):
        self.db = db

    async def put(self, key, value):
        self.db.value = value

    async def get(self, key):
        return self.db.value

    async def commit(self):
        return GateHandle(self.db.reached, self.db.release, self.db.fail)

    async def rollback(self):
        pass


class GateDatabase:
    def __init__(self, fail=False):
        self.reached, self.release = asyncio.Event(), asyncio.Event()
        self.fail, self.value = fail, None

    async def begin(self, isolation):
        return GateTransaction(self)


async def test_acknowledgement_and_reads_wait_for_remote_durability():
    db = GateDatabase()
    state = SlateState(db, None, "memory:///", "test")

    async def write():
        async with state.transaction() as tx:
            await tx.put("key", 42)

    writer = asyncio.create_task(write())
    await db.reached.wait()
    reader = asyncio.create_task(state.get("key"))
    await asyncio.sleep(0.01)
    assert not writer.done() and not reader.done()
    db.release.set()
    await writer
    assert await reader == 42
    assert state.last_sequence == 7


async def test_ambiguous_commit_poison_is_fail_closed():
    db = GateDatabase(fail=True)
    db.release.set()
    state = SlateState(db, None, "memory:///", "test")
    with pytest.raises(Unavailable, match="uncertain"):
        async with state.transaction() as tx:
            await tx.put("key", 42)
    assert state.poisoned
    with pytest.raises(Unavailable):
        await state.get("key")


async def test_killed_process_recovers_accepted_request(tmp_path):
    url = tmp_path.as_uri()
    script = """
import asyncio, os, sys
from data_orchestrator.storage import SlateState
from data_orchestrator.execution import LocalSubprocess
from data_orchestrator.engine import Engine
async def main():
    backend = LocalSubprocess('data_orchestrator.demo:project')
    state = await SlateState.open(sys.argv[1], flush_interval='1ms')
    engine = Engine(state, await backend.manifest(), backend)
    await engine.initialize()
    await engine.submit(['sample_quality'], command_id='crash-request')
    await engine.claim()
    os._exit(0)
asyncio.run(main())
"""
    result = await asyncio.to_thread(
        subprocess.run, [sys.executable, "-c", script, url], capture_output=True, timeout=40
    )
    assert result.returncode == 0, result.stderr.decode()
    state = await SlateState.open(url, flush_interval="1ms")
    try:
        backend = LocalSubprocess("data_orchestrator.demo:project")
        engine = Engine(state, await backend.manifest(), backend)
        await engine.initialize()
        run = await engine.submit(["sample_quality"], command_id="crash-request")
        assert len(await engine.list_runs()) == 1
        assert (await finish(engine, run))["request"]["status"] == "succeeded"
        assert len((await engine.asset_detail("samples"))["preview"]) == 3
    finally:
        await state.close()


async def test_real_local_end_to_end_and_native_fencing(tmp_path):
    result = await selftest(tmp_path.as_uri())
    assert result["status"] == "passed"
    assert len(result["checks"]) == 6


@pytest.mark.s3
async def test_s3_http_emulator_contract(tmp_path, monkeypatch):
    import boto3
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    settings = {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ENDPOINT": endpoint,
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
        "AWS_CONDITIONAL_PUT": "etag",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    for k, v in settings.items():
        monkeypatch.setenv(k, v)
    bucket = "orchestrator-" + uuid.uuid4().hex
    boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1").create_bucket(Bucket=bucket)
    try:
        result = await selftest("s3://" + bucket + "/contracts")
        assert result["status"] == "passed"
        assert len(result["checks"]) == 7
    finally:
        server.stop()


@pytest.mark.live
async def test_live_s3_contract():
    url = os.getenv("DORC_TEST_S3_URL")
    if not url:
        pytest.skip("Set DORC_TEST_S3_URL to explicitly authorize a real-bucket test")
    assert url.startswith("s3://")
    result = await selftest(url)
    assert result["status"] == "passed"


async def test_native_writer_takeover_rejects_old_writes(tmp_path):
    old = await SlateState.open(tmp_path.as_uri(), flush_interval="1ms")
    newer = await SlateState.open(tmp_path.as_uri(), flush_interval="1ms")
    try:
        async with newer.transaction() as tx:
            await tx.put("owner", "new")
        with pytest.raises(Unavailable):
            async with old.transaction() as tx:
                await tx.put("owner", "old")
        assert await newer.get("owner") == "new"
    finally:
        await newer.close()
        with contextlib.suppress(Exception):
            await old.close()
