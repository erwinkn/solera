"""Simulation runs that found a bug, replayed step for step: each a strict
xfail while its finding is open (docs/verification.md, "Findings"), an
ordinary test once fixed. Open findings are not set aside here."""

import pytest

from .machine import Simulation


@pytest.fixture(autouse=True)
def unmasked(monkeypatch):
    monkeypatch.setenv("SOLERA_SIM_KNOWN", "1")


@pytest.mark.xfail(strict=True, reason="F9: a consumer keeps a key its upstream dropped when it moved store")
def test_f9_a_key_dropped_by_a_moved_output_leaves_its_consumers():
    """`items` on the table store; `k10`, `k11` committed; `items` moves to
    FileStore (its first write there starts over, with an index of its own);
    the feed drops `k11`. `checks` (an Each consumer) keeps `k11`."""

    state = Simulation()
    state.boot(seed=2188, store="table")
    state.commit_feed(keys={"k10", "k11"}, op="upsert", version="1")
    state.redeploy(change="table", clean=True)
    state.commit_feed(keys={"k10", "k3", "k2"}, op="replace", version="1")
    state.teardown()
