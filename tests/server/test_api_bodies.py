"""Request bodies the API takes into the journal, fuzzed (docs/verification.md,
"Fuzzing"): whatever a body holds — wrong types, missing or extra fields,
deep nesting, integers past 64 bits, NaN and infinities (Python's JSON
reader accepts them), odd strings — the API answers it, never with a 500,
and what it accepted the journal holds: a read-only replay equals the live
model, a checkpoint is taken, and a reopened state equals it again. A body
that broke replay would leave the engine unable to restart."""

import asyncio
import json
import math
import os
import tempfile
from pathlib import Path

import httpx
import pytest
from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st
from solera_server.api import create_app
from solera_server.state import State

from tests.conftest import worker_finished

from .engines import make_engine
from .test_api import build_project


def _normal(model) -> dict:
    return json.loads(json.dumps(model.snapshot(), sort_keys=True, default=str))


# JSON values as a client may send them: Python's reader also takes NaN,
# Infinity and integers of any size.
scalars = st.one_of(
    st.none(),
    st.booleans(),
    # Integers past 64 bits are left out while F27 is open: its own test has them.
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.floats(allow_nan=True, allow_infinity=True),
    st.text(max_size=12),
    st.sampled_from(
        ["", "\x00", "a" * 300, "../x", "feed", "total", "daily", "uploads", "2026-09-18", "full"]
    ),
)
values = st.recursive(
    scalars,
    lambda inner: st.one_of(
        st.lists(inner, max_size=4), st.dictionaries(st.text(max_size=8), inner, max_size=4)
    ),
    max_leaves=12,
)


def _mutate(valid: dict):
    """A valid body with one field dropped, replaced by any value, or added."""

    keys = sorted(valid)
    return st.one_of(
        st.just(valid),
        st.sampled_from(keys).map(lambda k: {f: v for f, v in valid.items() if f != k}),
        st.tuples(st.sampled_from(keys), values).map(lambda kv: {**valid, kv[0]: kv[1]}),
        st.tuples(st.text(max_size=8), values).map(lambda kv: {**valid, kv[0]: kv[1]}),
    )


ROUTES = {
    "runs": {"targets": ["total"], "partitions": "latest", "mode": "incremental", "keys": None, "config": {}},
    "runs-daily": {"targets": ["daily"], "partitions": ["2026-09-18"], "upstream": True, "tags": {"t": "1"}},
    "runs-keys": {"targets": ["total"], "keys": {"feed": {"keys": ["a"]}}},
    "commit-keys": {"keys": {"u-1": "v1"}},
    "commit-upsert": {"upsert": ["u-2"], "remove": ["u-1"]},
    "retry": {"classes": ["failed"], "partition": "2026-09-18"},
    "prune": {"before": 0.0, "keep": 1, "dry_run": True},
    "clear": {"output": "feed", "partition": ""},
}
RAW = {"retry", "clear"}  # routes that read the body as raw JSON, not through a model


def _path(base: str, route: str) -> str:
    return {
        "runs": f"{base}/runs",
        "runs-daily": f"{base}/runs",
        "runs-keys": f"{base}/runs",
        "commit-keys": f"{base}/sources/uploads/commit",
        "commit-upsert": f"{base}/sources/uploads/commit",
        "retry": f"{base}/assets/flaky/keys:retry",
        "prune": f"{base}/runs:prune",
        "clear": f"{base}/cleanups:clear",
    }[route]


def _body(route: str):
    if route in RAW:  # while F28 is open: their valid body only
        return st.just(ROUTES[route])
    return st.one_of(_mutate(ROUTES[route]), values)  # a near miss, or anything at all


requests = st.lists(
    st.sampled_from(sorted(ROUTES)).flatmap(lambda r: st.tuples(st.just(r), _body(r))),
    min_size=1,
    max_size=6,
)


async def _exchange(root: Path, sent: list[tuple[str, object]]) -> None:
    project = build_project()
    state = await State.open(root.as_uri(), "test", flush_interval=0.001)
    engine = make_engine(state, project, eval_interval=0.05)
    await engine.initialize()
    app = create_app(engine=engine, insecure=True)
    app.state.engine = engine
    base = f"/api/projects/{engine.manifest['name']}"
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as client:
            for route, body in sent:
                content = json.dumps(body, allow_nan=True)
                answer = await client.post(
                    _path(base, route), content=content, headers={"content-type": "application/json"}
                )
                assert answer.status_code < 500, (
                    f"{route} {content[:200]}: {answer.status_code} {answer.text[:300]}"
                )
        await engine.tick()
    finally:
        await engine.stop()
    await worker_finished()  # nothing records past here: the model is what was accepted
    await state.durable()
    live = _normal(state.model)
    replayed = await State.open(root.as_uri(), "test", writer=False)
    assert _normal(replayed.model) == live, "the journal replays to another state"
    await state.close()  # a final checkpoint of everything accepted
    again = await State.open(root.as_uri(), "test", writer=False)
    assert _normal(again.model) == live, "the checkpoint reopens to another state"


@settings(
    max_examples=int(os.environ.get("SOLERA_FUZZ_EXAMPLES", 60)),  # CI; thousands for a campaign
    deadline=None,
    suppress_health_check=list(HealthCheck),
)
@given(sent=requests)
@example(sent=[("runs", {"targets": ["total"], "config": {"n": math.nan, "m": math.inf}})])
@example(sent=[("runs", {"targets": ["total"], "tags": {"t": "x" * 300}, "by": ""})])
def test_any_body_the_api_takes_survives_replay_and_checkpoint(sent, tmp_path):
    with tempfile.TemporaryDirectory(dir=tmp_path) as d:
        asyncio.run(_exchange(Path(d), sent))


@pytest.mark.xfail(strict=True, reason="F27: open")
@pytest.mark.parametrize(
    "route,body",
    [
        ("runs", {"targets": ["total"], "config": {"n": 2**70}}),
        ("runs", {"targets": ["total"], "config": {"n": -(2**64)}}),
    ],
)
def test_an_integer_past_64_bits_never_wedges_the_journal(route, body, tmp_path):
    """F27: a run's config with an integer past 64 bits was accepted and
    journaled, then every checkpoint failed on it, and with them every
    flush: nothing more was journaled and every write hung. Refused (4xx),
    or held whole by the journal and the checkpoint."""

    asyncio.run(_exchange(tmp_path, [(route, body)]))


@pytest.mark.xfail(strict=True, reason="F28: open")
@pytest.mark.parametrize(
    "route,body",
    [(r, b) for r in sorted(RAW) for b in (None, [], "x", 1)] + [("clear", {"output": []})],
)
def test_a_malformed_body_on_a_raw_route_is_refused(route, body, tmp_path):
    """F28: the routes that read their body as raw JSON, with no model,
    called `.get` on whatever it was and used its fields unchecked, so
    `null`, a list, a string or a field of the wrong type answered 500."""

    asyncio.run(_exchange(tmp_path, [(route, body)]))
