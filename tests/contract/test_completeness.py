"""The contract is complete: every entry is tested or pending with a reason,
no contract test reaches today's internals, and every test a *Covered* line
names exists."""

from tests.contract import check


def test_every_entry_has_a_test_or_a_pending_reason():
    print(check.summary())
    assert not check.problems(), "\n".join(check.problems())


def test_the_check_catches_every_way_to_reach_an_internal():
    source = """
import solera_server.engine
from solera import keys
from tests import server
from solera.keys.layers import LayerIndex

async def test_x(solera):
    solera.engine.m
    solera.state
"""
    found = check.internal_uses(source)
    assert "solera_server.engine" in found and "solera.keys" in found and "tests.server" in found
    assert "solera.keys.layers" in found
    assert sum(use.startswith("line ") for use in found) == 2
    assert check.internal_uses("from solera.sdk import asset\nfrom solera import Rejected\n") == []
