import pytest
from obstore.store import LocalStore
from solera.sdk import Output
from solera.stores import JsonStore, Scope


@pytest.fixture
def objects(tmp_path):
    """A real object store on file:// (never an in-memory dict)."""

    return LocalStore(tmp_path / "objects", mkdir=True)


@pytest.fixture
def json_store(objects):
    store = JsonStore()
    store.bind_objects(objects)
    return store


def scope(output: Output, partition: str = "", batch=None) -> Scope:
    return Scope(output=output, partition=partition, batch=batch, attempt="test")
