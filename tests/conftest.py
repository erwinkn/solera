import asyncio
import contextlib

import pytest
from obstore.store import LocalStore
from solera.sdk import Output
from solera.stores import Scope


def pytest_addoption(parser):
    parser.addoption("--slow", action="store_true", help="also run the long growth soaks")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--slow"):
        return
    skip = pytest.mark.skip(reason="a long soak: run with --slow")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(autouse=True)
async def world(monkeypatch):
    """Every engine and state a test makes, torn down after it in order:
    engines stopped (their key services' threads with them), then states
    closed. Whatever a test stops or closes itself is not done twice."""

    from solera_server.engine import Engine
    from solera_server.state import State

    engines, states = [], []
    init, open_state = Engine.__init__, State.open

    def made(self, *args, **kw):
        init(self, *args, **kw)
        engines.append(self)

    async def opened(*args, **kw):
        state = await open_state(*args, **kw)
        states.append(state)
        return state

    monkeypatch.setattr(Engine, "__init__", made)
    monkeypatch.setattr(State, "open", opened)
    yield
    for engine in engines:
        with contextlib.suppress(Exception):
            await engine.stop()
    for state in states:
        with contextlib.suppress(Exception):
            await state.close()


async def worker_finished() -> None:
    """Every in-process worker done — past its commit, its discards too
    (docs/lifecycle.md §9.8): a run is settled before its worker ends."""

    from solera_server.placements.inline import InlinePlacement

    while running := [t for t in InlinePlacement._tasks.values() if not t.done()]:
        await asyncio.wait(running)


async def maintenance_drained(engine) -> None:
    """Upkeep with nothing left in flight: compactions and recounts done,
    their results recorded, garbage collected."""

    upkeep = engine.upkeep
    for _ in range(50):
        upkeep.truncate()
        upkeep.maintain()
        if not upkeep.jobs:
            await upkeep.collect()
            if not upkeep.jobs:
                return
        await asyncio.gather(*list(upkeep.jobs.values()), return_exceptions=True)
    raise AssertionError("upkeep never drained")


@pytest.fixture
def objects(tmp_path):
    """A real object store on file:// (never an in-memory dict)."""

    return LocalStore(tmp_path / "objects", mkdir=True)


@pytest.fixture(autouse=True)
def data(tmp_path, monkeypatch):
    """Where default stores keep outputs: a fresh directory per test."""

    path = tmp_path / "data"
    monkeypatch.setenv("SOLERA_DATA", str(path))
    return path


def scope(output: Output, partition: str = "", batch=None, **kw) -> Scope:
    return Scope(output=output, partition=partition, batch=batch, attempt="test", **kw)


async def whole(state, output: str, scope: str = ""):
    """A whole keyed read's selection, as the harness builds it for an
    immutable store: every live entry of the output's key index, with its
    version and locator (docs/lifecycle.md §9.8)."""

    from solera.keys.index import KeyIndex, key_str
    from solera.keys.io import ObjectIO
    from solera.stores import Keys

    index = KeyIndex(ObjectIO(state.objects), None, state.model.index(output, scope).pinned())
    entries, after = {}, None
    while True:
        keys, versions, locators, after = await index.page(after, 100_000)
        entries.update({key_str(k): (v, loc) for k, v, loc in zip(keys, versions, locators, strict=True)})
        if after is None:
            return Keys(entries)
