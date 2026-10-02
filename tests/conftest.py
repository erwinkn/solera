import pytest
from obstore.store import LocalStore
from solera.sdk import Output
from solera.stores import Scope


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
