import pytest
from obstore.store import LocalStore
from solera.sdk import Output
from solera.stores import FileStore, Scope


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


@pytest.fixture
def file_store(data):
    return FileStore(data)


def scope(output: Output, partition: str = "", batch=None, **kw) -> Scope:
    return Scope(output=output, partition=partition, batch=batch, attempt="test", **kw)
