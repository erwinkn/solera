"""The generated conformance kit (`solera.testing.storemachine`) against the
shipped stores, the guide's example store and the simulation's in-memory
fenced store: random sequences of attempts, stale writers, duplicates,
retries, pinned readers, discards and batches, checked after every step."""

import os
import tempfile
from pathlib import Path

import pytest
from hypothesis import HealthCheck, settings
from solera.testing.storemachine import stateful

from .test_store_conformance import HARNESSES, fresh

RUNS = int(os.environ.get("SOLERA_STORE_MACHINE_RUNS", 30))


def table_harness(_tmp):
    import contextlib

    from solera.testing.stores import Harness

    from ..sim.stores import Database, TableStore

    store = TableStore(Database())

    @contextlib.asynccontextmanager
    async def hold(scope):
        """The older writer's transaction, open: the slice's lock held, its fence taken."""

        table = store._table(scope.output, None)
        async with store.db.lock(table, scope.partition):
            store.db.fences[(table, scope.partition)] = store._fence(scope, table)
            yield

    return Harness(store, fresh("db"), hold)


@pytest.mark.parametrize("name", ["file", "table", "postgres", "example", "s3"])
def test_a_store_conforms_under_random_sequences(name, request):
    make = table_harness if name == "table" else HARNESSES[name]
    make(Path(tempfile.mkdtemp()))  # skips here, not inside Hypothesis, when its backend is absent
    machine = stateful(lambda: make(Path(tempfile.mkdtemp())))
    slow = request.config.getoption("--slow")
    machine.TestCase.settings = settings(
        max_examples=RUNS * (10 if slow else 1),
        stateful_step_count=30,
        deadline=None,
        derandomize=not slow,
        suppress_health_check=list(HealthCheck),
    )
    machine.TestCase().runTest()
