"""Index files nothing names (docs/key-index-design.md § Lifecycles): what
the orphan collector judges a commit's delta by, and the event that makes
an orphan garbage."""

from __future__ import annotations

from solera_server.model import Model
from solera_server.upkeep import _attempt_of


def test_a_delta_names_its_attempt():
    assert _attempt_of("000000000012-01J8ZD3Q-0.lay") == "01J8ZD3Q"
    assert _attempt_of("000000000012-01J8ZD3Q-17.lay") == "01J8ZD3Q"
    assert _attempt_of("000000000012-01J8ZD3Q.lix") == "01J8ZD3Q"
    for other in (
        "l000000000000-000000000056-e3-01J8ZE2-m0.lay",  # a merge output: its epoch decides
        "000000000012-01J8ZD3Q.0000",  # a span-era delta: not a layer file
        "12-01J8ZD3Q-0.lay",  # not a commit number
        "000000000012-01J8ZD3Q-x.lay",
        "000000000012--0.lay",
    ):
        assert _attempt_of(other) is None, other


def test_orphans_become_garbage_once():
    m = Model()
    m.garbage = [["keys/o/_/a.lay", 3]]
    m.apply({"type": "OrphansFound", "paths": ["keys/o/_/a.lay", "keys/o/_/b.lay"]})
    paths = [p for p, _ in m.garbage]
    assert paths == ["keys/o/_/a.lay", "keys/o/_/b.lay"]  # a.lay kept its own event counter
    assert m.garbage[0][1] == 3
