"""Versions are generations (docs/versions.md): the sequences its review
asked for, end to end through the engine — provenance of a paged
delivery, a failure record kept in its entry across a restart, and repair
by presence after a writer died."""

from solera import Transient
from solera.sdk import Each, Incremental, Output, Project, Ref, asset
from solera.stores import Patch

from ..conftest import whole
from .test_engine import drive, make_engine, state, status_of  # noqa: F401
from .test_fence import LiveStore


async def test_a_paged_delta_window_says_the_generation_it_read(state):  # noqa: F811
    """Review finding 3: a delta window delivered over pages reads the index
    as of its start. The upstream writes `b` again (g3) after the window's
    first page; its second page reads `b` as of g2 — the object it was
    pinned to — and lineage says g2, not the head's g3. The change then
    arrives as a delta of its own, read at g3."""

    content = {"rows": [{"id": "a", "v": 1}, {"id": "b", "v": 1}]}
    seen, moved = [], {"done": False}

    @asset(outputs=Output("items", key="id"))
    def items():
        return Patch(content["rows"])

    @asset(inputs={"items": Incremental(batch_size=1)}, outputs=Output("copy", key="id"))
    async def copy(ctx, items: list):
        changes = ctx.batch["items"]
        seen.append((changes.full, [(r["id"], r["v"]) for r in items]))
        if not changes.full and changes.first and not moved["done"]:
            moved["done"] = True  # the upstream moves while the window is half delivered
            content["rows"] = [{"id": "b", "v": 3}]
            assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
        return Patch(items, remove=list(changes.removed))

    project = Project(assets=[items, copy])
    engine = make_engine(state, project)
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["copy"], upstream=True))) == "succeeded"
    content["rows"] = [{"id": "a", "v": 2}, {"id": "b", "v": 2}]
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    g2 = state.model.heads[("items", "")]["ref"]["generation"]
    seen.clear()
    assert status_of(await drive(engine, await engine.submit(["copy"]))) == "succeeded"
    g3 = state.model.heads[("items", "")]["ref"]["generation"]
    assert g3 > g2
    assert seen == [(False, [("a", 2)]), (False, [("b", 2)]), (False, [("b", 3)])]

    made = (await engine.history.commits(outputs=["copy"]))["commits"]
    pages = sorted(m["generation"] for m in made)[-3:]  # the three pages of the second run
    read = []
    for generation in pages:
        [edge] = (await engine.history.lineage("copy", "", generation))["edges"]
        read.append(edge["from"]["generation"])
    assert read == [g2, g2, g3]
    rows = await project.stores["default"].load(
        Ref.from_json(state.model.heads[("copy", "")]["ref"]), list[dict], await whole(state, "copy")
    )
    assert sorted((r["id"], r["v"]) for r in rows) == [("a", 2), ("b", 3)]


async def test_a_failure_record_lives_in_its_entry_across_a_restart(state, tmp_path):  # noqa: F811
    """Review finding 2: the failure index's entry carries the key's record —
    its outcome, tries, retry deadline and the upstream generation it failed
    at — as its payload. After a restart, the replayed engine reads it back
    whole, and a retry takes the key at the upstream generation it failed at."""

    import time

    from solera.failed_keys import RETRYING
    from solera.keys.index import KeyIndex, key_str
    from solera.keys.io import ObjectIO

    from .test_each import records

    tries, skew = {"n": 0}, {"seconds": 0.0}

    def clock():
        return time.time() + skew["seconds"]

    @asset(outputs=Output("files", keyed=True))
    def files():
        return {"a.csv": 1}

    def parse(ctx, file: int):
        tries["n"] += 1
        if tries["n"] == 1:
            raise Transient("not yet", retry_after=3600, retry_for=86400)
        return [{"n": file, "at": ctx.generation}]

    parse = asset(parse, inputs={"file": Each("files")}, outputs=Output("rows", key="path"))
    project = Project(assets=[files, parse])
    engine = make_engine(state, project, clock=clock)
    await engine.initialize()
    await drive(engine, await engine.submit(["parse"], upstream=True))
    upstream = state.model.heads[("files", "")]["ref"]["generation"]
    [(key, record)] = (await records(engine, "parse")).items()
    assert key == "a.csv" and record.outcome == RETRYING and record.tries == 1
    assert record.upstream == upstream and record.next_at > 0 and record.until > record.next_at
    await engine.stop()

    from solera_server.state import State

    await state.close()
    again = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = make_engine(again, project, clock=clock)
    await engine.initialize()
    index = KeyIndex(ObjectIO(again.objects), None, again.model.index("@parse", "").pinned())
    found = await index.lookup([b"a.csv"])
    from solera.failed_keys import Record

    assert {key_str(k): Record.decode(p) for k, (_, p) in found.items()} == {"a.csv": record}
    skew["seconds"] = 7200.0  # two hours on: its retry is due
    await drive(engine, await engine.submit(["parse"]))
    assert tries["n"] == 2 and await records(engine, "parse") == {}
    rows = await project.stores["default"].load(
        Ref.from_json(again.model.heads[("rows", "")]["ref"]), list[dict], await whole(again, "rows")
    )
    assert rows == [{"path": "a.csv", "n": 1, "at": upstream}]  # it ran at the generation it failed at
    await engine.stop()
    await again.close()


async def _dead_write(state, landed: bool):  # noqa: F811
    """`k` is not in the index. Attempt g12 patches it into a fenced store and
    dies after its gate — its insert landed, or not — leaving its intent;
    the next attempt, g15, patches `x` alone. Returns the engine's index of
    `items` and the store."""

    live = LiveStore()
    writes = [[{"id": "a", "v": 1}], Patch([{"id": "x", "v": 1}])]

    @asset(outputs=Output("items", key="id", store="live"))
    def items():
        return writes.pop(0)

    engine = make_engine(state, Project(assets=[items], stores={"live": live}))
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    if landed:
        live.rows["k"] = {"id": "k", "v": 9}
    # The dead attempt's intent, as its gate left it: its delta file names `k`.
    from solera.keys import SortedEntries
    from solera.keys.index import KeyIndex
    from solera.keys.io import ObjectIO

    index = state.model.index("items", "")
    files, _ = await KeyIndex(ObjectIO(state.objects), None, index.pinned()).resolve(
        SortedEntries.of([b"k"]), commit_number=1, attempt="dead", generation=12
    )
    state.model.repairs[("items", "")] = [{**files.to_json(), "run": "r", "attempt": "dead"}]
    assert status_of(await drive(engine, await engine.submit(["items"]))) == "succeeded"
    assert ("items", "") not in state.model.repairs
    return engine, live


async def test_a_repair_keeps_a_dead_writers_key_it_finds(state):  # noqa: F811
    """docs/versions.md §5, landed: the store holds `k`, so it takes the
    repairing attempt's generation, as `x` does; the index and the store
    agree."""

    engine, live = await _dead_write(state, landed=True)
    keys = (await engine.list_keys("items"))["keys"]
    g15 = state.model.heads[("items", "")]["ref"]["generation"]
    assert keys["k"] == keys["x"] == g15 and sorted(keys) == sorted(live.rows) == ["a", "k", "x"]


async def test_a_repair_drops_a_dead_writers_key_it_does_not_find(state):  # noqa: F811
    """docs/versions.md §5, not landed: the store lacks `k` and the index
    never held it: nothing — the index lists no key without rows."""

    engine, live = await _dead_write(state, landed=False)
    assert sorted((await engine.list_keys("items"))["keys"]) == sorted(live.rows) == ["a", "x"]
