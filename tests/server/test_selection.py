"""What a run reads of an incremental input, and what a producer holds
(docs/behaviors.md SEL-7, SEL-8, SEL-9, INC-11): `keys=` is a list,
`"all"` or the default; a full run is the run's mode; its first batch says
`reset`; `ctx.load()` always returns what is materialized."""

from solera.sdk import Incremental, Output, Project, asset
from solera.stores import FileStore, Patch

from .engines import drive, make_engine, status_of


def project(root, content: dict, seen: list, batch_size: int = 10) -> Project:
    """`files` (keyed: `content["v"]`, its rows or a Patch) -> `tally`, a plain consumer
    keeping a count by the changes; each batch, and what `ctx.load()`
    returned, is recorded in `seen`."""

    @asset(outputs=Output("files", key="id"))
    def files():
        return content["v"]

    @asset(inputs={"files": Incremental(batch_size=batch_size)})
    async def tally(ctx, files: list):
        b, held = ctx.batch["files"], await ctx.load()
        seen.append(
            {
                "reset": b.reset,
                "index": b.index,
                "added": list(b.added),
                "updated": list(b.updated),
                "removed": list(b.removed),
                "unchanged": list(b.unchanged),
                "rows": sorted(r["id"] for r in files),
                "held": held,
            }
        )
        before = 0 if b.reset else (held or {"count": 0})["count"]
        return {"count": before + len(b.added) - len(b.removed)}

    return Project(assets=[files, tally], default_store=FileStore(root / "data"))


def rows(*keys) -> dict:
    return {"v": [{"id": k, "v": 1} for k in keys]}


async def built(state, tmp_path, content, seen, batch_size=10):
    engine = make_engine(state, project(tmp_path, content, seen, batch_size))
    await engine.initialize()
    assert status_of(await drive(engine, await engine.submit(["tally"], upstream=True))) == "succeeded"
    return engine


async def test_sel_7_keys_all_loads_every_key_without_starting_over(state, tmp_path):
    content, seen = rows("a", "b", "c"), []
    engine = await built(state, tmp_path, content, seen)
    content["v"] = Patch([{"id": "a", "v": 2}])
    await drive(engine, await engine.submit(["files"]))
    seen.clear()
    detail = await drive(engine, await engine.submit(["tally"], keys={"files": "all"}))
    assert status_of(detail) == "succeeded"
    [b] = seen
    assert not b["reset"] and b["updated"] == ["a"] and b["unchanged"] == ["b", "c"] and not b["added"]
    assert b["rows"] == ["a", "b", "c"] and b["held"] == {"count": 3}  # owed in its class, the rest unchanged


async def test_sel_8_a_full_run_owes_every_key_and_a_plain_consumer_starts_over(state, tmp_path):
    content, seen = rows("a", "b", "c"), []
    engine = await built(state, tmp_path, content, seen, batch_size=2)
    seen.clear()
    detail = await drive(engine, await engine.submit(["tally"], mode="full"))
    assert status_of(detail) == "succeeded"
    assert [(b["index"], b["reset"], b["added"]) for b in seen] == [(0, True, ["a", "b"]), (1, False, ["c"])]
    assert seen[1]["held"] == {"count": 2}  # batch by batch: the second builds on the first, from zero


async def test_sel_8_keys_full_is_no_longer_a_full_run(state, tmp_path):
    engine = await built(state, tmp_path, rows("a"), [])
    try:
        await engine.submit(["tally"], keys={"files": "full"})
    except ValueError as error:
        assert "mode='full'" in str(error)
    else:
        raise AssertionError("keys={'files': 'full'} was accepted")


async def test_sel_9_a_full_runs_first_batch_says_reset_and_load_returns_what_is_materialized(
    state, tmp_path
):
    content, seen = rows("a", "b", "c"), []
    engine = await built(state, tmp_path, content, seen, batch_size=2)
    seen.clear()
    await drive(engine, await engine.submit(["tally"], mode="full"))
    first, second = seen
    assert first["reset"] and first["held"] == {"count": 3}  # what is materialized, which it ignores
    assert not second["reset"] and second["held"] == {"count": 2}  # what batch 0 committed


async def test_inc_11_ctx_load_returns_the_output_as_materialized(state, tmp_path):
    seen = []
    await built(state, tmp_path, rows("a", "b", "c"), seen, batch_size=2)
    assert [b["held"] for b in seen] == [None, {"count": 2}]  # None before its first commit
