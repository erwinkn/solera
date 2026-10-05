"""Key outcomes (docs/per-key-processing.md §9): each key's latest outcome,
stored where it is not ok, else derived from the observation record — ok,
removed, unmatched — and whether its input owes the key."""

from .engines import drive, make_engine, status_of
from .test_each import files_project


def parse(file: dict):
    if file["text"] == "bug":
        raise ValueError("unexpected header")
    return [{"value": int(file["text"])}]


async def outcomes_of(engine, **kw) -> dict:
    page = await engine.latest_outcomes("parse", **kw)
    return {r["key"]: (r["outcome"], r["owed"]) for r in page["keys"]}


async def one(engine, key: str) -> dict:
    [row] = (await engine.latest_outcomes("parse", key=key))["keys"]
    return row


async def run(engine, targets, **kw):
    detail = await drive(engine, await engine.submit(targets, **kw))
    assert status_of(detail) == "succeeded", detail
    return detail


async def test_derived_outcomes_follow_the_observation_record(state):
    content = {
        k: {"text": v} for k, v in {"a.csv": "1", "b.csv": "2", "bug.csv": "bug", "notes.txt": "3"}.items()
    }
    written = {}
    engine = make_engine(state, files_project(content, parse, include="*.csv", written=written))
    await engine.initialize()
    await run(engine, ["parse"], upstream=True)

    assert await outcomes_of(engine) == {
        "a.csv": ("ok", False),
        "b.csv": ("ok", False),
        "bug.csv": ("failed", False),
        "notes.txt": ("unmatched", False),  # upstream has it, the patterns leave it out
    }
    a = await one(engine, "a.csv")
    logged = await engine.history.key_outcomes("parse", key="a.csv", limit=1)
    assert a["version"] == logged["outcomes"][0]["generation"] and "tries" not in a
    bug = await one(engine, "bug.csv")
    assert (bug["tries"], bug["message"]) == (1, "ValueError: unexpected header")
    assert await one(engine, "ghost.csv") == {
        "partition": "",
        "key": "ghost.csv",
        "outcome": None,
        "version": None,
        "owed": False,
    }

    # Upstream moves on: each key keeps its latest outcome, owed until processed.
    content["a.csv"] = {"text": "5"}
    del content["b.csv"]
    content["bug.csv"] = {"text": "4"}
    content["new.csv"] = {"text": "6"}
    await run(engine, ["files"])
    assert await outcomes_of(engine) == {
        "a.csv": ("ok", True),
        "b.csv": ("ok", True),  # owed its removal
        "bug.csv": ("failed", True),
        "new.csv": (None, True),  # never processed
        "notes.txt": ("unmatched", False),
    }

    await run(engine, ["parse"])
    assert await outcomes_of(engine) == {
        "a.csv": ("ok", False),
        "b.csv": ("removed", False),  # upstream had it at the cut, and the removal is delivered
        "bug.csv": ("ok", False),  # a failed key turned ok
        "new.csv": ("ok", False),
        "notes.txt": ("unmatched", False),
    }
    assert (await one(engine, "a.csv"))["version"] != a["version"]
    assert list(await outcomes_of(engine, outcomes=["removed", "unmatched"])) == ["b.csv", "notes.txt"]
    assert await outcomes_of(engine, outcomes=["failed"]) == {}

    # New patterns leave keys out: owed until a run delivers them, unmatched since.
    narrow = files_project(content, parse, include="a*.csv", written=written)
    engine = make_engine(state, narrow)
    await engine.initialize()
    assert (await one(engine, "new.csv"))["outcome"] == "ok" and (await one(engine, "new.csv"))["owed"]
    await run(engine, ["parse"])
    assert await outcomes_of(engine, outcomes=["ok", "unmatched"]) == {
        "a.csv": ("ok", False),
        "bug.csv": ("unmatched", False),
        "new.csv": ("unmatched", False),
        "notes.txt": ("unmatched", False),
    }


async def test_a_listing_pages_in_key_order(state):
    content = {f"{i:02}.csv": {"text": str(i)} for i in range(7)}
    engine = make_engine(state, files_project(content, parse))
    await engine.initialize()
    await run(engine, ["parse"], upstream=True)

    every = await engine.latest_outcomes("parse")
    assert [r["key"] for r in every["keys"]] == sorted(content) and every["next"] is None
    paged, after = [], None
    while True:
        page = await engine.latest_outcomes("parse", limit=3, after=after)
        paged += page["keys"]
        if (after := page["next"]) is None:
            break
    assert paged == every["keys"]
