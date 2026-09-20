"""§4: BlobStore — content-addressed bytes and the `_migrations.json` ledger
that records callable migrations applied over an output's prefix."""

import json

import obstore
import pytest
from cursus.sdk import Migration, Output
from cursus.stores import BlobStore, StoreError

from tests.conftest import scope


@pytest.fixture
def blob_store(objects):
    store = BlobStore()
    store.bind_objects(objects)
    return store


async def test_bytes_round_trip(blob_store):
    """§4: a bytes write is content-addressed and loads back verbatim."""

    out = Output("t", store="blobs")
    written = await blob_store.store(b"payload", None, scope(out))
    assert written.ref.handle["object"].startswith("blobs/t/")
    assert await blob_store.load(written.ref, bytes, None) == b"payload"


async def test_migration_ledger_round_trip(blob_store, objects):
    """§4: callable payloads run over the output prefix and each applied name
    lands in `_migrations.json`; a second call applies nothing."""

    calls = []

    def seed(store, prefix):
        calls.append("seed")
        obstore.put(store, prefix + "marker.txt", b"seeded")

    out = Output("docs", store="blobs", migrations=[Migration("seed", seed)])
    applied = await blob_store.migrate(out, out.migrations)
    assert applied == ["seed"]
    assert calls == ["seed"]

    result = await obstore.get_async(objects, "blobs/docs/_migrations.json")
    ledger = json.loads(bytes(await result.bytes_async()))
    assert [e["name"] for e in ledger["applied"]] == ["seed"]

    again = await blob_store.migrate(out, out.migrations)
    assert again == ["seed"]
    assert calls == ["seed"]  # applied once


async def test_migration_payload_must_be_callable(blob_store):
    """§4: a non-callable payload is a StoreError, not a silent skip."""

    out = Output("docs", store="blobs")
    out.migrations = (Migration("bad", "SELECT 1"),)
    with pytest.raises(StoreError, match="callable"):
        await blob_store.migrate(out, out.migrations)
