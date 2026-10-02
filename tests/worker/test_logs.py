"""An attempt's log chunks (docs/lifecycle.md §2.1): sealed before they
are written, so lines logged during an upload are not lost and a retry
after an unknown outcome writes the same bytes; and whatever the log's
fate, the result is published."""

import asyncio
import base64
import gzip
import json
import os

from obstore.store import LocalStore
from solera import lifecycle
from solera_worker import reporting
from solera_worker.reporting import LogShipper


def entry(message: str) -> dict:
    return {"at": 1.0, "level": "info", "message": message, "fields": {}}


async def chunk_lines(objects, n: int) -> list[str]:
    import obstore

    data = await (await obstore.get_async(objects, f"a{lifecycle.chunk(n)}")).bytes_async()
    return [json.loads(line)["message"] for line in gzip.decompress(bytes(data)).decode().splitlines()]


def tail_lines(index: dict) -> list[str]:
    text = gzip.decompress(base64.b64decode(index["tail"])).decode()
    return [json.loads(line)["message"] for line in text.splitlines()]


async def test_a_line_logged_during_an_upload_goes_in_the_next_chunk(tmp_path, monkeypatch):
    """Review P2-5: a line appended while chunk 0 uploads is neither lost
    nor counted in chunk 0."""

    objects = LocalStore(tmp_path)
    shipper = LogShipper(objects, "a")
    paused, go = asyncio.Event(), asyncio.Event()
    real = reporting.create

    async def slow(store, path, data):
        paused.set()
        await go.wait()
        await real(store, path, data)

    monkeypatch.setattr(reporting, "create", slow)
    shipper.append(entry("one"))
    upload = asyncio.create_task(shipper.chunk())
    await paused.wait()
    shipper.append(entry("two"))
    go.set()
    assert await upload
    assert shipper.chunks == [[0, 1, 1.0]] and await chunk_lines(objects, 0) == ["one"]
    index = await shipper.finish()
    assert tail_lines(index) == ["two"] and index["lines"] == 2 and "lost" not in index


async def test_an_upload_that_landed_unacknowledged_is_retried_with_its_bytes(tmp_path, monkeypatch):
    """Chunk 0 lands but its response is lost; more lines arrive, then a
    tail too big for the result. The retry writes chunk 0's own bytes, the
    big tail becomes chunk 1, and `finish` never raises."""

    objects = LocalStore(tmp_path)
    shipper = LogShipper(objects, "a")
    real, calls = reporting.create, []

    async def lossy(store, path, data):
        calls.append(path)
        await real(store, path, data)
        if len(calls) == 1:
            raise OSError("connection reset")  # landed, but the writer cannot know

    monkeypatch.setattr(reporting, "create", lossy)
    shipper.append(entry("one"))
    assert not await shipper.chunk()
    shipper.append(entry(os.urandom(64 << 10).hex()))  # incompressible: no tail
    index = await shipper.finish()
    assert [c[:2] for c in index["chunks"]] == [[0, 1], [1, 1]] and index["tail"] is None
    assert await chunk_lines(objects, 0) == ["one"] and "lost" not in index


async def test_a_log_that_cannot_be_written_does_not_stop_the_result(tmp_path, monkeypatch):
    async def down(store, path, data):
        raise OSError("the object store is down")

    monkeypatch.setattr(reporting, "create", down)
    shipper = LogShipper(LocalStore(tmp_path), "a")
    shipper.append(entry("one"))
    assert not await shipper.chunk()
    shipper.append(entry(os.urandom(64 << 10).hex()))
    index = await shipper.finish()
    assert index["chunks"] == [] and index["lost"] == 2


async def test_a_line_logged_while_a_chunk_compresses_is_kept(tmp_path, monkeypatch):
    """Review round 3, #4: a synchronous Each call logs from its thread
    while chunk 0 is being compressed (gzip releases the GIL). The line
    goes into the next chunk or the tail, never nowhere."""

    objects = LocalStore(tmp_path)
    shipper = LogShipper(objects, "a")
    compress, during = reporting.gzip.compress, []

    def compressing(data, *args, **kw):
        if not during:
            during.append(True)
            shipper.append(entry("during"))  # as a thread would, mid-compression
        return compress(data, *args, **kw)

    monkeypatch.setattr(reporting.gzip, "compress", compressing)
    shipper.append(entry("before"))
    assert await shipper.chunk()
    assert await chunk_lines(objects, 0) == ["before"] and shipper.chunks[0][1] == 1
    index = await shipper.finish()
    assert tail_lines(index) == ["during"] and index["lines"] == 2 and "lost" not in index
