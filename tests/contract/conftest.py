"""The contract's fixture: `solera`, a namespace and the engine that serves it.

An entry a later unit builds (`entries.LATER`) is skipped with the unit's name."""

import pytest

from tests.contract.check import entry_of
from tests.contract.entries import LATER
from tests.contract.harness import Solera


def pytest_collection_modifyitems(config, items):
    for item in items:
        if item.path.parent.name != "contract":
            continue
        entry = entry_of(item.name) or ""
        later = LATER.get(item.name.split("[")[0]) or LATER.get(entry) or LATER.get(entry.split("-")[0])
        if later is not None:
            item.add_marker(pytest.mark.skip(reason=f"{entry}: {later}"))


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path)
    yield namespace
    await namespace.close()
