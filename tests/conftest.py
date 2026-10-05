import asyncio
from types import SimpleNamespace

import pytest
from obstore.store import LocalStore
from solera.sdk import Output
from solera.stores import WriteContext


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
    """Every engine and state a test makes on its own event loop (a server
    thread's loop tears down its own), torn down after it in order: engines
    stopped (their key services' threads with them), then states
    closed. A failure tearing down is the test's, unless the test expects
    it: it names its type in `world.expected`. A state that breaks would
    exit the process: here its exit codes are recorded in `world.exits`
    instead."""

    from solera_server.engine import Engine
    from solera_server.state import State

    engines, states = [], []
    init, open_state = Engine.__init__, State.open

    loop = asyncio.get_running_loop()

    def made(self, *args, **kw):
        init(self, *args, **kw)
        try:
            mine = asyncio.get_running_loop() is loop
        except RuntimeError:  # made outside any loop: the test's
            mine = True
        if mine:
            engines.append(self)

    async def opened(*args, **kw):
        state = await open_state(*args, **kw)
        if asyncio.get_running_loop() is loop:
            states.append(state)
        return state

    monkeypatch.setattr(Engine, "__init__", made)
    monkeypatch.setattr(State, "open", opened)
    exits: list[int] = []
    monkeypatch.setattr(State, "_exit", staticmethod(exits.append))
    world = SimpleNamespace(engines=engines, states=states, exits=exits, expected=())
    yield world
    errors = []
    for close in [engine.stop for engine in engines] + [state.close for state in states]:
        try:
            await close()
        except world.expected:
            pass
        except Exception as error:
            errors.append(error)
    if errors:
        raise errors[0]


async def worker_finished() -> None:
    """Every in-process worker done — past its commit, its cleanups too
    (docs/lifecycle.md §9.8): a run is settled before its worker ends."""

    from solera_server.executors import inline

    while running := [inline._workers.get(a) for a in inline._workers]:
        await asyncio.wait(running)


async def maintenance_drained(engine) -> None:
    """Upkeep with nothing left in flight: span merges done,
    their results recorded, garbage collected."""

    upkeep = engine.upkeep
    for _ in range(50):
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
    monkeypatch.setenv("SOLERA_DATA_URL", path.as_uri())
    return path


def context(output: Output, partition: str = "", commit_number=None, **kw) -> WriteContext:
    return WriteContext(output=output, partition=partition, commit_number=commit_number, attempt="test", **kw)


async def whole(state, output: str, partition: str = ""):
    """A whole keyed read's selection, as the worker builds it for an
    immutable store: every live entry of the output's key index, with the
    generation that wrote it (docs/lifecycle.md §9.8)."""

    from solera.keys.index import KeyIndex, key_str
    from solera.keys.io import ObjectIO
    from solera.stores import Keys

    index = KeyIndex(ObjectIO(state.objects), None, state.model.index(output, partition).slice())
    entries, after = {}, None
    while True:
        keys, generations, _, after = await index.page(after, 100_000)
        entries.update(zip(map(key_str, keys), generations, strict=True))
        if after is None:
            return Keys(entries)
