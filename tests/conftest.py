import os

import pytest
from cursus.sdk import Output
from cursus.stores import JsonStore, Scope
from obstore.store import LocalStore

# Durable-commit latency equals the WAL flush interval; the suite commits far
# too often for the 1 s deployment default.
os.environ.setdefault("CURSUS_SLATE_FLUSH_INTERVAL", "10ms")


@pytest.fixture
def objects(tmp_path):
    """A real object store on file:// (never an in-memory dict)."""

    return LocalStore(tmp_path / "objects", mkdir=True)


@pytest.fixture
def json_store(objects):
    store = JsonStore()
    store.bind_objects(objects)
    return store


def scope(output: Output, partition: str = "", baseline=None, batch=None) -> Scope:
    return Scope(output=output, partition=partition, baseline=baseline, batch=batch)
