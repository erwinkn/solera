"""The store conformance kit (`solera.testing.stores`, docs/stores.md) run
against the shipped stores, and against a minimal SQL store fenced with
`solera.fencing.fence` — the guide's example — so the kit itself is proven."""

import contextlib
import os
import uuid
from urllib.parse import unquote, urlsplit

import pytest
from solera.sdk import Output
from solera.stores import FileStore, S3Store
from solera.testing.stores import Harness, scenarios

DSN = os.environ.get("SOLERA_TEST_DATABASE_URL")
S3_URL = os.getenv("SOLERA_TEST_S3")


def fresh(store_name: str):
    return lambda **decl: Output(f"t_{uuid.uuid4().hex[:12]}", store=store_name, **decl)


def file_harness(tmp_path):
    return Harness(FileStore(tmp_path / "data"), fresh("default"))


def s3_harness(tmp_path):
    if not S3_URL:
        pytest.skip("set SOLERA_TEST_S3 to run against an S3-compatible server")
    u = urlsplit(S3_URL)
    store = S3Store(
        f"s3://{u.path.strip('/')}/conformance-{uuid.uuid4().hex}",
        endpoint=f"{u.scheme}://{u.netloc.rpartition('@')[2]}",
        access_key_id=unquote(u.username),
        secret_access_key=unquote(u.password),
        region="us-east-1",
        client_options={"allow_http": True},
    )
    return Harness(store, fresh("default"))


def postgres_harness(tmp_path):
    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    from solera_postgres import PostgresStore

    store = PostgresStore(DSN)

    @contextlib.asynccontextmanager
    async def hold(partition):
        """The older writer's write transaction, open: its fence row held."""

        import asyncio

        table, _, _ = store._table(partition.output)
        conn = store._connect()
        cur = conn.cursor()
        store._domain(cur, table)
        store._fence(cur, table, partition)
        try:
            yield
        finally:
            await asyncio.to_thread(conn.commit)
            conn.close()

    return Harness(store, fresh("postgres"), hold)


def example_store(fenced=True):
    """The guide's example, examples/json_table_store.py."""

    import importlib.util
    from pathlib import Path

    if not DSN:
        pytest.skip("SOLERA_TEST_DATABASE_URL is not set")
    path = Path(__file__).parents[2] / "examples" / "json_table_store.py"
    spec = importlib.util.spec_from_file_location("json_table_store", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not fenced:  # a store that forgets its fence
        module.fence = lambda *args, **kw: None
    return module.JsonTableStore(DSN)


def example_harness(tmp_path, store=None):
    store = store or example_store()

    @contextlib.asynccontextmanager
    async def hold(partition):
        import asyncio

        from solera.fencing import fence

        table = store._table(partition.output, None)
        conn, cur = store._transaction(table)
        fence(cur, partition, table)
        try:
            yield
        finally:
            await asyncio.to_thread(conn.commit)
            conn.close()

    return Harness(store, fresh("example"), hold)


HARNESSES = {"file": file_harness, "s3": s3_harness, "postgres": postgres_harness, "example": example_harness}


def cases():
    from solera.testing.stores import EVERY, FENCED, IMMUTABLE, READS

    for name, kind in (
        ("file", "immutable"),
        ("s3", "immutable"),
        ("postgres", "fenced"),
        ("example", "fenced"),
    ):
        # A fenced store here reads the current rows: it reports what it read (`reads`).
        for scenario in EVERY + (IMMUTABLE if kind == "immutable" else FENCED + READS):
            yield pytest.param(name, scenario, id=f"{name}-{scenario.__name__}")


@pytest.mark.parametrize(("store", "scenario"), list(cases()))
async def test_shipped_stores_conform(store, scenario, tmp_path):
    harness = HARNESSES[store](tmp_path)
    assert scenario in scenarios(harness.store)
    await scenario(harness)


async def test_the_kit_catches_a_store_that_forgets_its_fence():
    """The kit has teeth: the example store with its fence left out fails
    the stale-writer scenario."""

    from solera.testing.stores import a_stale_writer_is_refused

    with pytest.raises(AssertionError, match="accepted a write it must refuse"):
        await a_stale_writer_is_refused(example_harness(None, example_store(fenced=False)))


def test_a_fenced_stores_harness_holds_a_transaction():
    """The waiting scenario needs `Worker.hold`: a fenced store's worker
    without one is refused, rather than a scenario passing untested."""

    with pytest.raises(ValueError, match="needs hold"):
        Harness(example_store(), fresh("example"))
