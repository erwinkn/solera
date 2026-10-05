"""The console's read models over the API (§8, §10): asset rollups, a per-key
asset's key outcomes and their log, explain, inputs with their positions,
holds, and when schedules next fire."""

import json

import httpx
import pytest
from solera import Rejected
from solera.sdk import (
    Cron,
    Every,
    Incremental,
    OnChange,
    Output,
    Project,
    StaticPartitions,
    asset,
    job,
)
from solera_server import views
from solera_server.api import create_app
from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement
from solera_server.state import State


class Unprocessable(Rejected):
    pass


CONTENT = {
    "a.csv": {"text": "1"},
    "bad.csv": {"text": "reject"},
    "bug.csv": {"text": "bug"},
    "draft-1.csv": {"text": "2"},
    "notes.txt": {"text": "3"},
}


def build_project(content, parts):
    @asset(outputs=Output("files", keyed=True))
    def files():
        return dict(content)

    @asset(
        inputs={"file": Incremental("files", include="*.csv", exclude={"drafts": "draft-*"}, each=True)},
        outputs=Output("samples", key="path"),
    )
    def parse(file: dict):
        if file["text"] == "reject":
            raise Unprocessable("empty file")
        if file["text"] == "bug":
            raise ValueError("unexpected header")
        return [{"value": int(file["text"])}]

    days = StaticPartitions(["x", "y"])

    @asset(outputs=Output("parts", keyed=True), partitions={"day": days})
    def parted(ctx):
        return dict(parts[ctx.partition])

    @asset(inputs={"parts": Incremental(batch_size=1)}, partitions={"day": days}, deps=["files"])
    def consume(parts: dict):
        return {"n": len(parts)}

    @job
    def notify(ctx):
        ctx.log("done")

    return Project(assets=[files, parse, parted, consume, notify], name="console")


async def open_engine(tmp_path, project):
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = Engine(
        state,
        project.manifest,
        placements={"Local": lambda s, c: InlinePlacement(c, project)},
        clock=state.clock,
        eval_interval=0.05,
    )
    await engine.initialize()
    return state, engine


@pytest.fixture
async def world(tmp_path):
    content = {k: dict(v) for k, v in CONTENT.items()}
    parts = {"x": {"k1": 1, "k2": 2}, "y": {"k1": 1}}
    state, engine = await open_engine(tmp_path, build_project(content, parts))
    app = create_app(engine=engine, insecure=True)
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        yield engine, client, "/api/projects/console", content, parts
    await state.close()


async def run(engine, targets, **kw):
    detail = await engine.run_until((await engine.submit(targets, **kw))["id"])
    assert detail["request"]["status"] == "succeeded", detail
    return detail


async def test_assets_status_rolls_up_every_asset(world):
    engine, client, base, *_ = world
    await run(engine, ["parse"], upstream=True)
    await run(engine, ["parted"], partitions="all")
    await run(engine, ["consume"], partitions=["x"])  # its dep, `files`, ran above
    await run(engine, ["notify"])

    response = await client.get(f"{base}/assets:status")
    assert response.status_code == 200
    status = response.json()["assets"]
    assert set(status) == {"files", "parse", "parted", "consume", "notify"}
    parse = status["parse"]
    assert parse["partitions"] == {
        "total": 1,
        "materialized": 1,
        "stale": 0,
        "missing": 0,
        "failed": 0,
        "running": 0,
        "removed": 0,
    }
    assert parse["partitioned"] is False and parse["stored_counts"] == {"rejected": 1, "failed": 1}
    assert parse["last"]["outcome"] == "succeeded" and parse["last"]["attempt"].count("/") == 1
    assert parse["repairs"] == 0 and parse["updated_at"]
    assert status["files"]["stored_counts"] is None  # no per-key input
    consume = status["consume"]
    assert consume["partitioned"] and consume["partitions"]["total"] == 2
    assert (consume["partitions"]["materialized"], consume["partitions"]["missing"]) == (1, 1)
    # A job has no head: it is complete once it succeeded.
    assert status["notify"]["partitions"]["materialized"] == 1 and status["notify"]["updated_at"] is None

    # The rollup is not an asset name, and /partitions reads the same statuses.
    assert (await client.get(f"{base}/assets/assets:status")).status_code == 404
    parts = (await client.get(f"{base}/partitions/consume")).json()["partitions"]
    assert {p["partition"]: p["status"] for p in parts} == {"x": "materialized", "y": "missing"}


async def test_stored_outcomes_list_page_and_filter(world, monkeypatch):
    engine, client, base, *_ = world
    await run(engine, ["parse"], upstream=True)

    stored = [("outcome", o) for o in ("rejected", "failed", "retrying", "canceled", "timed_out")]
    found = (await client.get(f"{base}/assets/parse/outcomes", params=stored)).json()
    assert [s["partition"] for s in found["partitions"]] == [""]
    [partition] = found["partitions"]
    assert partition["stored_counts"] == {"rejected": 1, "failed": 1} and partition["last"] == "changes"
    assert found["deploy"] == engine.m.deploy_number and found["next"] is None
    by_key = {k["key"]: k for k in found["keys"]}
    assert set(by_key) == {"bad.csv", "bug.csv"}
    bad = by_key["bad.csv"]
    assert (bad["outcome"], bad["tries"], bad["message"]) == ("rejected", 1, "Unprocessable: empty file")
    assert bad["next_at"] is None and bad["until"] is None and bad["eligible"] is False
    # The upstream key's generation: as key_outcomes shows it.
    rows = (await client.get(f"{base}/assets/parse/outcomes/history", params={"key": "bad.csv"})).json()
    assert bad["version"] == rows["outcomes"][0]["generation"]
    assert by_key["bug.csv"]["message"] == "ValueError: unexpected header"

    first = (await client.get(f"{base}/assets/parse/outcomes", params=[*stored, ("limit", 1)])).json()
    assert [k["key"] for k in first["keys"]] == ["bad.csv"] and json.loads(first["next"]) == ["", "bad.csv"]
    params = [*stored, ("limit", 1), ("after", first["next"])]
    second = (await client.get(f"{base}/assets/parse/outcomes", params=params)).json()
    assert [k["key"] for k in second["keys"]] == ["bug.csv"] and second["next"] is None

    # Every key, its outcome derived where none is stored; one key's alone.
    every = (await client.get(f"{base}/assets/parse/outcomes")).json()
    assert {k["key"]: (k["outcome"], k["owed"]) for k in every["keys"]} == {
        "a.csv": ("ok", False),
        "bad.csv": ("rejected", False),
        "bug.csv": ("failed", False),
        "draft-1.csv": ("unmatched", False),
        "notes.txt": ("unmatched", False),
    }
    one = (await client.get(f"{base}/assets/parse/outcomes", params={"key": "a.csv"})).json()
    assert [(k["key"], k["outcome"]) for k in one["keys"]] == [("a.csv", "ok")]

    failed = (await client.get(f"{base}/assets/parse/outcomes", params={"outcome": "failed"})).json()
    assert [k["key"] for k in failed["keys"]] == ["bug.csv"] and len(failed["partitions"]) == 1
    retrying = (await client.get(f"{base}/assets/parse/outcomes", params={"outcome": "retrying"})).json()
    assert retrying["keys"] == [] and retrying["partitions"][0]["stored_counts"]["rejected"] == 1

    # A page reads a bounded number of entries: a rare class comes back short, with a `next`.
    monkeypatch.setattr(views, "SCAN", 1)
    params = {"outcome": "failed", "limit": 1}
    short = (await client.get(f"{base}/assets/parse/outcomes", params=params)).json()
    assert short["keys"] == [] and json.loads(short["next"]) == ["", "bad.csv"]
    rest = (
        await client.get(f"{base}/assets/parse/outcomes", params={**params, "after": short["next"]})
    ).json()
    assert [k["key"] for k in rest["keys"]] == ["bug.csv"] and rest["next"] is None
    monkeypatch.undo()

    assert (await client.get(f"{base}/assets/files/outcomes")).status_code == 400  # no per-key input
    assert (await client.get(f"{base}/assets/parse/outcomes", params={"outcome": "odd"})).status_code == 400
    assert (await client.get(f"{base}/assets/ghost/outcomes")).status_code == 404


async def test_key_outcomes_page_newest_first(world):
    engine, client, base, content, _ = world
    await run(engine, ["parse"], upstream=True)
    content["a.csv"] = {"text": "5"}
    await run(engine, ["parse"], upstream=True)

    every = (await client.get(f"{base}/assets/parse/outcomes/history")).json()
    rows = every["outcomes"]
    assert every["asset"] == "parse" and every["next"] is None
    assert [r["at"] for r in rows] == sorted((r["at"] for r in rows), reverse=True)
    assert [r["key"] for r in rows if r["key"] == "a.csv"] == ["a.csv", "a.csv"]
    assert {r["outcome"] for r in rows if r["key"] == "bad.csv"} == {"rejected"}

    paged, before = [], None
    while True:
        params = {"limit": 2, **({"before": before} if before else {})}
        page = (await client.get(f"{base}/assets/parse/outcomes/history", params=params)).json()
        paged += page["outcomes"]
        before = page["next"]
        if before is None:
            break
    assert paged == rows

    exact = (await client.get(f"{base}/assets/parse/outcomes/history", params={"key": "a.csv"})).json()
    assert [r["outcome"] for r in exact["outcomes"]] == ["ok", "ok"]
    assert exact["outcomes"][0]["generation"] != exact["outcomes"][1]["generation"]
    searched = (await client.get(f"{base}/assets/parse/outcomes/history", params={"q": "BAD"})).json()
    assert {r["key"] for r in searched["outcomes"]} == {"bad.csv"}
    failing = (
        await client.get(
            f"{base}/assets/parse/outcomes/history", params=[("outcome", "failed"), ("outcome", "ok")]
        )
    ).json()
    assert {r["outcome"] for r in failing["outcomes"]} == {"failed", "ok"}
    run_id = exact["outcomes"][-1]["run"]
    one = (await client.get(f"{base}/assets/parse/outcomes/history", params={"run": run_id})).json()
    assert {r["run"] for r in one["outcomes"]} == {run_id}


async def test_explain_says_why_a_key_is_or_is_not_there(world):
    engine, client, base, content, _ = world
    await run(engine, ["parse"], upstream=True)

    async def explain(key, **params):
        response = await client.get(f"{base}/assets/parse/explain", params={"key": key, **params})
        assert response.status_code == 200, response.text
        return response.json()

    ok = await explain("a.csv")
    assert (ok["verdict"], ok["input"], ok["upstream"], ok["upstream_partition"]) == (
        "ok",
        "file",
        "files",
        "",
    )
    assert ok["outputs"] == {
        "samples": {"present": True, "generation": ok["outputs"]["samples"]["generation"]}
    }
    assert ok["last"]["outcome"] == "ok" and ok["last_ok"] == ok["last"]
    assert ok["last_ok"]["generation"] == ok["upstream_generation"]
    assert ok["patterns"]["included"] and ok["patterns"]["excluded_by"] is None
    assert ok["patterns"]["pending"] is None and ok["outcome"] is None

    failing = await explain("bad.csv")
    assert failing["verdict"] == "failing" and failing["outcome"]["outcome"] == "rejected"
    assert failing["last"]["outcome"] == "rejected" and failing["last_ok"] is None
    excluded = await explain("draft-1.csv")
    assert excluded["verdict"] == "excluded" and excluded["patterns"]["excluded_by"] == "drafts"
    assert (await explain("notes.txt"))["verdict"] == "not_matched"
    assert (await explain("ghost.csv"))["verdict"] == "absent"

    # The upstream moves on: a changed key is pending, a deleted one removed.
    content["a.csv"] = {"text": "7"}
    del content["bug.csv"]
    content["new.csv"] = {"text": "8"}
    await run(engine, ["files"])
    pending = await explain("a.csv")
    assert pending["verdict"] == "pending"
    assert pending["last_ok"]["generation"] != pending["upstream_generation"]
    assert (await explain("new.csv"))["verdict"] == "pending"
    assert (await explain("bug.csv"))["verdict"] == "failing"  # its record outlives the key until delivered
    await run(engine, ["parse"])
    assert (await explain("bug.csv"))["verdict"] == "removed"
    assert (await explain("a.csv"))["verdict"] == "ok"

    assert (await client.get(f"{base}/assets/files/explain", params={"key": "a.csv"})).status_code == 400
    bad_partition = await client.get(
        f"{base}/assets/parse/explain", params={"key": "a.csv", "partition": "x"}
    )
    assert bad_partition.status_code == 404
    await run(engine, ["parted"], partitions=["y"])  # a plain Incremental input never delivered
    consume = await client.get(f"{base}/assets/consume/explain", params={"key": "k1", "partition": "y"})
    assert consume.json()["verdict"] == "pending"
    assert consume.json()["last"] is None and consume.json()["outputs"] == {}


async def test_explain_a_key_restored_after_its_removal_is_pending(world):
    engine, client, base, content, _ = world
    await run(engine, ["parse"], upstream=True)
    original = content.pop("a.csv")
    await run(engine, ["parse"], upstream=True)  # the removal is delivered: no row holds it
    content["a.csv"] = original  # back as it was: written again, at a generation of its own
    await run(engine, ["files"])

    async def explain(key):
        response = await client.get(f"{base}/assets/parse/explain", params={"key": key})
        assert response.status_code == 200, response.text
        return response.json()

    restored = await explain("a.csv")
    assert restored["last"]["outcome"] == "removed"
    assert restored["last_ok"]["generation"] != restored["upstream_generation"]  # a new write of it
    assert restored["outputs"]["samples"]["present"] is False
    assert restored["verdict"] == "pending"
    await run(engine, ["parse"])
    delivered = await explain("a.csv")
    assert delivered["verdict"] == "ok" and delivered["outputs"]["samples"]["present"] is True


async def test_edges_report_every_scope_and_what_it_owes(world):
    engine, client, base, _, parts = world
    await run(engine, ["files", "parted"], partitions="all")
    await run(engine, ["consume"], partitions=["x"])

    found = (await client.get(f"{base}/assets/consume/inputs")).json()
    assert found["asset"] == "consume"
    inputs = {e["param"]: e for e in found["inputs"]}
    assert (inputs["parts"]["kind"], inputs["files"]["kind"]) == ("incremental", "dep")
    assert inputs["parts"]["upstream_asset"] == "parted" and inputs["parts"]["batch_size"] == 1
    assert inputs["files"]["partitions"] == [] and inputs["files"]["source"] is False
    partitions = {s["partition"]: s for s in inputs["parts"]["partitions"]}
    none = {"added": 0, "updated": 0, "removed": 0}
    head = engine.m.heads[("parts", "x")]["commit_number"]
    assert partitions["x"]["observed"] == {"owed": none, "full_run_due": None, "observed_at": head}
    assert partitions["y"] == {
        "partition": "y",
        "upstream_partition": "y",
        "observed": {"owed": {**none, "added": len(parts["y"])}, "full_run_due": None, "observed_at": None},
    }

    parts["x"]["k3"] = 3
    await run(engine, ["parted"], partitions=["x"])
    parts["x"]["k1"] = 9
    await run(engine, ["parted"], partitions=["x"])
    behind = {
        s["partition"]: s
        for s in (await client.get(f"{base}/assets/consume/inputs")).json()["inputs"][0]["partitions"]
    }
    assert behind["x"]["observed"]["owed"] == {"added": 1, "updated": 2, "removed": 0}  # k3; k1, k2 rewritten

    each = {e["param"]: e for e in (await client.get(f"{base}/assets/parse/inputs")).json()["inputs"]}["file"]
    assert each["kind"] == "each" and (each["batch_size"], each["concurrency"]) == (10_000, 64)  # D111
    assert each["patterns"]["exclude"] == [["drafts", {"glob": "draft-*"}]]
    assert [s["observed"]["observed_at"] for s in each["partitions"]] == [None]


async def test_a_domain_too_big_to_list_still_rolls_up(tmp_path):
    grid = {d: StaticPartitions([f"{d}{i}" for i in range(1000)]) for d in "ab"}  # 1,000,000 partitions

    @asset(outputs=Output("rows", keyed=True), partitions=grid)
    def rows(ctx):
        return {"k1": {"text": "1"}, "k2": {"text": "2"}}

    @asset(
        inputs={"row": Incremental("rows", each=True)}, outputs=Output("cells", key="path"), partitions=grid
    )
    def cells(row: dict):
        return [{"value": int(row["text"])}]

    state, engine = await open_engine(tmp_path, Project(assets=[rows, cells], name="grid"))
    app = create_app(engine=engine, insecure=True)
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        base, partition = "/api/projects/grid", "a=a7,b=b9"
        await run(engine, ["rows"], partitions=[partition])
        await run(engine, ["cells"], partitions=[partition])

        response = await client.get(f"{base}/assets:status")
        assert response.status_code == 200, response.text
        for name in ("rows", "cells"):
            assert response.json()["assets"][name]["partitions"] == {
                "total": 1_000_000,
                "materialized": 1,
                "stale": 0,
                "missing": 999_999,
                "failed": 0,
                "running": 0,
                "removed": 0,
            }
        explained = await client.get(
            f"{base}/assets/cells/explain", params={"key": "k1", "partition": partition}
        )
        assert explained.status_code == 200, explained.text
        assert explained.json()["verdict"] == "ok"
        other = {"key": "k1", "partition": "a=a8,b=b9"}  # a current partition, never run
        assert (await client.get(f"{base}/assets/cells/explain", params=other)).json()["verdict"] == "absent"
        ghost = {"key": "k1", "partition": "a=a7,b=b1000"}
        assert (await client.get(f"{base}/assets/cells/explain", params=ghost)).status_code == 404
        # Listing every partition stays bounded: refused, as an `all` run would be.
        assert (await client.get(f"{base}/partitions/rows")).status_code == 400
    await state.close()


async def test_what_an_operator_may_clear_starts_empty(world):
    engine, client, base, *_ = world
    assert (await client.get(f"{base}/repairs")).json() == {"repairs": []}
    assert (await client.get(f"{base}/cleanups")).json() == {"cleanups": []}


async def test_automations_say_when_they_next_fire(tmp_path):
    @asset(outputs=Output("feed", keyed=True))
    def feed():
        return {"a": 1}

    @asset(inputs={"feed": Incremental()})
    def total(feed: dict):
        return len(feed)

    project = Project(
        assets=[feed, total],
        automations=[
            Every(3600, name="hourly", targets=[feed]),
            Cron("0 7 * * 1", timezone="Europe/Paris", name="weekly", targets=[feed]),
            OnChange("feed", name="follow", targets=[total]),
        ],
        name="timed",
    )
    state, engine = await open_engine(tmp_path, project)
    app = create_app(engine=engine, insecure=True)
    app.state.engine = engine
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        base = "/api/projects/timed"

        async def next_at():
            autos = (await client.get(f"{base}/automations")).json()["automations"]
            return {a["name"]: a["next_at"] for a in autos}

        due = await next_at()
        # Never fired: a schedule waits for its next time after it was declared.
        assert due["hourly"] == engine.m.automations["hourly"]["since"] + 3600 > engine.clock()
        assert due["weekly"] > engine.clock()
        assert due["follow"] is None

        for name in ("hourly", "weekly"):
            assert (await client.post(f"{base}/automations/{name}/run-now")).status_code == 202
        auto = engine.m.automations
        due = await next_at()
        assert due["hourly"] == auto["hourly"]["last_fired"] + 3600
        assert due["weekly"] == engine._due_at(auto["weekly"]) > engine.clock()
        assert "next_at" not in auto["hourly"]  # a copy: the model's record is untouched

        detail = (await client.get(f"{base}/assets/feed")).json()
        assert {a["name"]: a["next_at"] for a in detail["automations"]} == {
            "hourly": due["hourly"],
            "weekly": due["weekly"],
        }
        off = (await client.post(f"{base}/automations/hourly/disable")).json()
        assert off["enabled"] is False and off["next_at"] is None
    await state.close()
