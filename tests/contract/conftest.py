"""The contract's fixture: `solera`, a namespace served on the tracking path
`SOLERA_TRACKING` names (`today`, the default, or `core`).

On today's path the tests of `entries.TARGET` are skipped;
`SOLERA_CONTRACT_TARGET=1` runs them anyway, to see them fail there."""

import os

import pytest

from tests.contract.check import entry_of
from tests.contract.entries import TARGET
from tests.contract.harness import PATHS, Solera

PATH = os.environ.get("SOLERA_TRACKING", "today")
if PATH not in PATHS:
    raise ValueError(f"SOLERA_TRACKING={PATH!r}: one of {PATHS}")


def pytest_collection_modifyitems(config, items):
    """On today's path, a target entry's test asserts a behaviour it does not have."""

    if PATH != "today" or os.environ.get("SOLERA_CONTRACT_TARGET"):
        return
    for item in items:
        entry = entry_of(item.name)
        if item.path.parent.name == "contract" and entry in TARGET:
            item.add_marker(pytest.mark.skip(reason=f"{entry}: the target behaviour ({TARGET[entry]})"))


@pytest.fixture
async def solera(tmp_path):
    namespace = Solera(tmp_path, PATH)
    yield namespace
    await namespace.close()
