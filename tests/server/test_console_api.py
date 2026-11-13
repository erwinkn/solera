"""The console's read models over the API (§8, §10): asset rollups, a per-key
asset's key outcomes and their log, explain, inputs with their positions,
holds, and when schedules next fire."""

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


async def test_what_an_operator_may_clear_starts_empty(world):
    engine, client, base, *_ = world
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
