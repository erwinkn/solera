import pytest
from data_orchestrator.sdk import Output
from data_orchestrator.stores import JsonStore, Scope
from obstore.store import LocalStore


@pytest.fixture
def objects(tmp_path):
    """A real object store on file:// (never an in-memory dict)."""

    return LocalStore(tmp_path / "objects", mkdir=True)


@pytest.fixture
def json_store(objects):
    store = JsonStore()
    store.bind_objects(objects)
    return store


def scope(output: Output, partition: str = "", prior_keys=None) -> Scope:
    return Scope(output=output, partition=partition, prior_keys=prior_keys)
