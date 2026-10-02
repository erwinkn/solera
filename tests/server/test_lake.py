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
