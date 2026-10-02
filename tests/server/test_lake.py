"""The lake (docs/object-store-state.md §7): buffered rows, flushed to Parquet,
forgotten by key, and queried — the buffer through an in-memory mirror."""

import asyncio
import threading
from pathlib import Path

from solera_server.lake import Lake, LakeState, Table

SCHEMA = {"events": Table("run", "at", {"run": "VARCHAR", "at": "DOUBLE", "n": "INTEGER"})}


class Store:
    """Just enough of `State`: objects on local disk, and events applied to
    the lake's state as the model would."""

    def __init__(self, root: Path):
        self.root, self.objects_url = root, root.as_uri()
        self.lake = LakeState(SCHEMA)
        self.garbage = []

    async def create_object(self, path, data):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    async def get_object(self, path):
        target = self.root / path
        return target.read_bytes() if target.exists() else None

    async def delete_objects(self, paths):
        for path in paths:
            (self.root / path).unlink(missing_ok=True)

    def record(self, e):
        if e["type"] == "LogFlushed":
            self.lake.flushed(e["files"], e["upto"])
        elif e["type"] == "LogCompacted":
            self.garbage.extend(self.lake.compacted(e["changes"]))


def rows(con):
    return con.execute("SELECT run, n FROM events ORDER BY n").fetchall()


async def test_buffer_flush_forget_and_restore(tmp_path):
    store = Store(tmp_path)
    lake = Lake(store, SCHEMA, lambda: store.lake, name="Log", clock=lambda: 100.0, merge_width=2)
    for n in range(3):
        store.lake.append("events", {"run": f"r{n}", "at": float(n), "n": n})
    assert await lake.query(rows, ("events",)) == [("r0", 0), ("r1", 1), ("r2", 2)]

    await lake.flush(force=True)  # the buffer empties into a file
    store.lake.append("events", {"run": "r3", "at": 3.0, "n": 3})
    extra = {"events": [{"run": "live", "at": 9.0, "n": 9}]}
    assert await lake.query(rows, ("events",), extra=extra) == [
        ("r0", 0),
        ("r1", 1),
        ("r2", 2),
        ("r3", 3),
        ("live", 9),
    ]
    assert await lake.query(rows, ("events",), key="r3") == [("r3", 3)]  # the file can't hold it

    store.lake.forget({"r1", "r3"}, at=100.0)  # hidden in the file, dropped from the buffer
    assert await lake.query(rows, ("events",)) == [("r0", 0), ("r2", 2)]
    assert store.lake.files["events"][0]["hidden"] == ["r1"]

    store.lake.append("events", {"run": "r4", "at": 4.0, "n": 4})
    store.lake = LakeState(SCHEMA, store.lake.to_json())  # as after a restart
    assert await lake.query(rows, ("events",)) == [("r0", 0), ("r2", 2), ("r4", 4)]

    await lake.flush(force=True)
    while lake.plan():  # two files merge; the hidden row goes with the rewrite
        lake.maintain()
        await lake.job
    [merged] = store.lake.files["events"]
    assert merged["rows"] == 3 and "hidden" not in merged and len(store.garbage) == 2


async def test_a_query_sees_the_buffer_as_it_was(tmp_path):
    store = Store(tmp_path)
    lake = Lake(store, SCHEMA, lambda: store.lake, name="Log")
    store.lake.append("events", {"run": "a", "at": 1.0, "n": 1})
    started, go = threading.Event(), threading.Event()

    def slow(con):
        started.set()
        go.wait(5)
        return rows(con)

    pending = asyncio.create_task(lake.query(slow, ("events",)))
    await asyncio.to_thread(started.wait, 5)
    store.lake.forget({"a"}, at=2.0)
    store.lake.append("events", {"run": "b", "at": 2.0, "n": 2})
    assert await lake.query(rows, ("events",)) == [("b", 2)]  # mirrored anew meanwhile
    go.set()
    assert await pending == [("a", 1)]


async def test_a_flush_during_the_download_neither_hides_nor_repeats_a_row(tmp_path, monkeypatch):
    """System review #2: `a` is in a file, `b` in the buffer. A query
    starts; while it downloads `a`'s file, `b` is flushed. The query saw one
    moment: both rows, once."""

    store = Store(tmp_path)
    lake = Lake(store, SCHEMA, lambda: store.lake, name="Log", cache=str(tmp_path / "cache"))
    store.lake.append("events", {"run": "a", "at": 1.0, "n": 1})
    await lake.flush(force=True)
    lake._evict(store.lake.files["events"][0]["path"])  # it must be downloaded
    store.lake.append("events", {"run": "b", "at": 2.0, "n": 2})
    fetch, fetching, go = lake._fetch, asyncio.Event(), asyncio.Event()

    async def slow(paths):
        fetching.set()
        await go.wait()
        await fetch(paths)

    monkeypatch.setattr(lake, "_fetch", slow)
    pending = asyncio.create_task(lake.query(rows, ("events",)))
    await fetching.wait()
    await lake.flush(force=True)  # `b` leaves the buffer for a file
    go.set()
    assert await pending == [("a", 1), ("b", 2)]
    assert await lake.query(rows, ("events",)) == [("a", 1), ("b", 2)]


async def test_rows_arriving_during_an_upload_do_not_void_it(tmp_path):
    """Review round 2, system #1: `b` is buffered while `a`'s file uploads.
    The file is installed, and `b` waits for the next flush; a row
    forgotten during the upload still voids it."""

    store = Store(tmp_path)
    lake = Lake(store, SCHEMA, lambda: store.lake, name="Log")
    create, during = store.create_object, []

    async def uploading(path, data):
        await create(path, data)
        for change in during:
            change()

    store.create_object = uploading
    store.lake.append("events", {"run": "a", "at": 1.0, "n": 1})
    during.append(lambda: store.lake.append("events", {"run": "b", "at": 2.0, "n": 2}))
    await lake.flush(force=True)
    assert len(store.lake.files["events"]) == 1 and [
        values[0] for _, values in store.lake.rows["events"]
    ] == ["b"]
    during[:] = [lambda: store.lake.forget({"b"}, at=3.0)]
    store.lake.append("events", {"run": "c", "at": 3.0, "n": 3})
    await lake.flush(force=True)
    assert len(store.lake.files["events"]) == 1  # `b` went meanwhile: that file is not installed
    during.clear()
    await lake.flush(force=True)
    assert await lake.query(rows, ("events",)) == [("a", 1), ("c", 3)]


async def test_a_compaction_leaves_cached_files_a_query_still_reads(tmp_path):
    """Review round 2, system #3: on a remote store, files are read from a
    local cache. A query chose two files; they are compacted before it
    reads them. Their cached copies stay until collection deletes the
    files themselves."""

    store = Store(tmp_path)
    store.objects_url = "s3://bucket/ns"  # remote: queries read the cache
    lake = Lake(store, SCHEMA, lambda: store.lake, name="Log", cache=str(tmp_path / "cache"), merge_width=2)
    for n, run in enumerate("ab", 1):
        store.lake.append("events", {"run": run, "at": float(n), "n": n})
        await lake.flush(force=True)
    old = [f["path"] for f in store.lake.files["events"]]
    started, go = threading.Event(), threading.Event()

    def slow(con):
        started.set()
        go.wait(5)
        return rows(con)

    pending = asyncio.create_task(lake.query(slow, ("events",)))
    await asyncio.to_thread(started.wait, 5)
    lake.maintain()
    await lake.job
    assert set(store.garbage) == set(old)  # compacted away, not yet collected
    go.set()
    assert await pending == [("a", 1), ("b", 2)]
    lake.evict(store.garbage)  # what collection does as it deletes them
    assert not any(Path(lake._local(p)).exists() for p in old)


async def test_a_canceled_query_keeps_its_pin_until_its_thread_is_done(tmp_path):
    """Review round 3, B4: a query is canceled while its thread reads. The
    files it chose stay pinned until the thread has finished."""

    import contextlib

    store = Store(tmp_path)
    pins = []

    @contextlib.contextmanager
    def pin():
        pins.append(1)
        try:
            yield
        finally:
            pins.pop()

    lake = Lake(store, SCHEMA, lambda: store.lake, name="Log", pin=pin)
    store.lake.append("events", {"run": "a", "at": 1.0, "n": 1})
    await lake.flush(force=True)
    started, go = threading.Event(), threading.Event()

    def slow(con):
        started.set()
        go.wait(5)
        return rows(con)

    pending = asyncio.create_task(lake.query(slow, ("events",)))
    await asyncio.to_thread(started.wait, 5)
    pending.cancel()
    await asyncio.sleep(0.05)
    assert pins == [1]  # the thread still reads: still pinned
    go.set()
    with contextlib.suppress(asyncio.CancelledError):
        await pending
    assert pins == []


async def test_a_query_parses_nothing_on_the_event_loop(tmp_path, monkeypatch):
    """Review round 5, engine #6: mirroring 100,000 buffered rows into
    DuckDB blocked the event loop for half a second before the threaded
    query began. Mirroring, and opening the transaction, happen on the
    preparing thread; queries prepare in the order they took their
    snapshots, so each still sees its own."""

    store = Store(tmp_path)
    lake = Lake(store, SCHEMA, lambda: store.lake, name="Log")
    loop_thread, threads = threading.get_ident(), []
    mirror = lake._mirror

    def watched(*args):
        threads.append(threading.get_ident())
        return mirror(*args)

    monkeypatch.setattr(lake, "_mirror", watched)
    store.lake.append("events", {"run": "a", "at": 1.0, "n": 1})
    first = asyncio.create_task(lake.query(rows, ("events",)))
    await asyncio.sleep(0)  # its snapshot taken: then another row, and a second query
    store.lake.append("events", {"run": "b", "at": 2.0, "n": 2})
    second = asyncio.create_task(lake.query(rows, ("events",)))
    assert await first == [("a", 1)] and await second == [("a", 1), ("b", 2)]
    assert threads and loop_thread not in threads
    await lake.stop()
