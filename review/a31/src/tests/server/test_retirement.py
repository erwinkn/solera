"""K25: an output removed, or moved to another store, leaves its data in
the store it was on; a cleanup task deletes it — `store.cleanup(output,
home, before=G)`, G the first generation after the deploy — once its
`cleanup_after` has passed and no pin predates the deploy."""

import asyncio
import datetime as dt

from solera.sdk import Output, Project, asset
from solera.stores import FileStore
from solera_server.state import State

from .engines import drive, make_engine


def files(root) -> set[str]:
    return {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()}


async def _engine(state, project):
    engine = make_engine(state, project)
    await engine.initialize()
    return engine


async def _settle(engine, rounds=300):
    """Tick until no run is under way, twice running: a due cleanup task has
    been submitted, and has run."""

    idle = 0
    for _ in range(rounds):
        await engine.tick()
        await asyncio.sleep(0.01)
        idle = 0 if any(r["status"] == "running" for r in engine.m.runs.values()) else idle + 1
        if idle >= 3:
            return
    raise AssertionError("runs still under way")


def orders_project(root, *, with_orders=True, cleanup_after=None, store_after=None):
    store = FileStore(root / "data")
    if store_after is not None:
        store.cleanup_after = store_after

    @asset(outputs=Output("orders", key="id", **({"cleanup_after": cleanup_after} if cleanup_after else {})))
    def orders():
        return [{"id": "o1"}, {"id": "o2"}]

    @asset
    def other():
        return 1

    return Project(assets=[orders, other] if with_orders else [other], default_store=store)


async def test_a_removed_outputs_files_go(tmp_path):
    """`orders` is removed: a cleanup task deletes its files, nothing else's."""

    state = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)
    engine = await _engine(state, orders_project(tmp_path))
    await drive(engine, await engine.submit(["orders", "other"]))
    assert any(f.startswith("orders/") for f in files(tmp_path / "data"))
    await engine.stop()
    engine = await _engine(state, orders_project(tmp_path, with_orders=False))
    await _settle(engine)
    left = files(tmp_path / "data")
    assert not any(f.startswith("orders") for f in left) and any(f.startswith("other") for f in left), left
    assert engine.m.retired == {}
    await engine.stop()
    await state.close()


class Clock:
    """Wall time, moved on by hand: a grace period passes in a test."""

    def __init__(self):
        self.skew = 0.0

    def __call__(self) -> float:
        import time

        return time.time() + self.skew


DAY = 86_400.0


async def _engine_at(state, project, clock):
    engine = make_engine(state, project, clock=clock)
    await engine.initialize()
    return engine


async def test_a_moved_outputs_old_files_go_and_its_new_ones_stay(tmp_path):
    """`orders` moves from store a to store b: its files in a go, those it
    then writes in b stay."""

    def project(store):
        @asset(outputs=Output("orders", key="id", store=store))
        def orders():
            return [{"id": "o1"}]

        return Project(
            assets=[orders], stores={"a": FileStore(tmp_path / "a"), "b": FileStore(tmp_path / "b")}
        )

    state = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)
    engine = await _engine(state, project("a"))
    await drive(engine, await engine.submit(["orders"]))
    await engine.stop()
    engine = await _engine(state, project("b"))
    await drive(engine, await engine.submit(["orders"]))
    await _settle(engine)
    assert files(tmp_path / "a") == set() and files(tmp_path / "b")
    await engine.stop()
    await state.close()


async def test_the_grace_period_is_the_stores_unless_the_output_says(tmp_path):
    """A store's `cleanup_after` (here a week) holds a removed output's files
    until it passes; an output's own overrides it (a day)."""

    state = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)
    clock = Clock()
    for override, wait in ((None, 7 * DAY), (dt.timedelta(days=1), DAY)):
        project = orders_project(tmp_path, cleanup_after=override, store_after=dt.timedelta(days=7))
        engine = await _engine_at(state, project, clock)
        await drive(engine, await engine.submit(["orders"]))
        await engine.stop()
        engine = await _engine_at(state, orders_project(tmp_path, with_orders=False), clock)
        clock.skew += wait - 3600
        await _settle(engine)
        assert any(f.startswith("orders/") for f in files(tmp_path / "data")), "not yet due"
        clock.skew += 7200
        await _settle(engine)
        assert not any(f.startswith("orders/") for f in files(tmp_path / "data")), "due"
        await engine.stop()
    await state.close()


async def test_an_output_added_back_within_the_grace_period_keeps_its_new_data(tmp_path):
    """The engine audit's case: `orders` is removed with a week's grace; on
    day 3 a deploy adds it back to the same store and it writes; on day 7
    the old life's cleanup runs — `before` the reset — and the new life's
    files stay."""

    state = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)
    clock, week = Clock(), dt.timedelta(days=7)
    engine = await _engine_at(state, orders_project(tmp_path, store_after=week), clock)
    await drive(engine, await engine.submit(["orders"]))
    old = {f for f in files(tmp_path / "data") if f.startswith("orders/")}
    await engine.stop()
    engine = await _engine_at(state, orders_project(tmp_path, with_orders=False, store_after=week), clock)
    await engine.stop()
    clock.skew += 3 * DAY
    engine = await _engine_at(state, orders_project(tmp_path, store_after=week), clock)
    await drive(engine, await engine.submit(["orders"]))
    new = {f for f in files(tmp_path / "data") if f.startswith("orders/")} - old
    clock.skew += 5 * DAY
    await _settle(engine)
    left = files(tmp_path / "data")
    assert new and new <= left and not (old & left), (old, new, left)
    await engine.stop()
    await state.close()


async def test_a_cleanup_waits_for_a_reader_pinned_before_the_reset(tmp_path):
    """A reader pinned before the deploy that removed `orders` may still read
    its files: the cleanup task waits until it lets go."""

    state = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)
    engine = await _engine(state, orders_project(tmp_path))
    await drive(engine, await engine.submit(["orders"]))
    await engine.stop()
    engine = await _engine(state, orders_project(tmp_path, with_orders=False))
    [entry] = engine.m.retired.values()
    token = object()
    engine.m.readers[token] = (entry["before"] - 1, None)  # pinned before the reset
    await _settle(engine)
    assert any(f.startswith("orders/") for f in files(tmp_path / "data"))
    del engine.m.readers[token]
    await _settle(engine)
    assert not any(f.startswith("orders/") for f in files(tmp_path / "data"))
    await engine.stop()
    await state.close()


async def test_a_custom_store_the_project_no_longer_declares_leaves_the_cleanup_stuck(tmp_path):
    """`orders` lived on a store of the project's own, and the deploy that
    removes it removes that store too: nothing can rebuild it, so the
    cleanup task fails for good, the entry stuck with why — shown, its files
    left — until an operator clears it."""

    class Mine(FileStore):
        """A store of the project's own: not rebuilt from the manifest."""

    def project(with_orders):
        @asset(outputs=Output("orders", key="id", store="mine"))
        def orders():
            return [{"id": "o1"}]

        @asset
        def other():
            return 1

        if with_orders:
            return Project(assets=[orders, other], stores={"mine": Mine(tmp_path / "mine")})
        return Project(assets=[other], default_store=FileStore(tmp_path / "data"))

    state = await State.open((tmp_path / "state").as_uri(), "test", flush_interval=0.001)
    engine = await _engine(state, project(True))
    await drive(engine, await engine.submit(["orders"]))
    await engine.stop()
    engine = await _engine(state, project(False))
    await _settle(engine)
    [entry] = engine.m.retired.values()
    assert "no longer declared" in entry["stuck"], entry
    assert files(tmp_path / "mine")
    [row] = [c for c in engine.cleanups_view() if c.get("stuck")]
    assert row["output"] == "orders" and "no longer declared" in row["stuck"]
    assert engine.clear_cleanups("orders", "", "test")["cleared"] == [entry["id"]]
    assert engine.m.retired == {} and files(tmp_path / "mine")  # given up on: its files stay
    await engine.stop()
    await state.close()


def test_a_built_in_store_with_a_secret_written_in_the_open_is_refused():
    """K25 (b): a built-in store's config enters the manifest, so registration
    refuses a literal secret there, naming the field and the env:NAME form;
    a DSN without a password (local development) passes."""

    import pytest
    from solera.sdk import RegistrationError
    from solera.stores import S3Store
    from solera_postgres import PostgresStore

    @asset
    def one():
        return 1

    def register(store):
        return Project(assets=[one], default_store=store).manifest

    with pytest.raises(RegistrationError, match="dsn='env:NAME'"):
        register(PostgresStore("postgresql://solera:hunter2@db:5432/solera"))
    with pytest.raises(RegistrationError, match="dsn='env:NAME'"):
        register(PostgresStore("host=db dbname=solera password=hunter2"))
    with pytest.raises(RegistrationError, match="secret_access_key='env:NAME'"):
        register(S3Store("s3://bucket/data", secret_access_key="hunter2"))
    for fine in (
        PostgresStore("postgresql://solera@localhost/solera"),
        PostgresStore("env:SOLERA_DSN"),
        S3Store("s3://bucket/data", secret_access_key="env:AWS_SECRET"),
    ):
        assert register(fine)["stores"]["default"]["built_in"]["class"] == type(fine).__name__
