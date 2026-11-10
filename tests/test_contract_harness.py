"""The contract harness's own guarantees, which the contract tests rely on."""

import sys
import threading

import pytest

from tests.contract.harness import Solera


async def test_a_timed_out_cli_call_restores_the_process_and_holds_off_the_next(tmp_path, monkeypatch):
    stuck, release = threading.Event(), threading.Event()

    def blocks():
        stuck.set()
        release.wait(30)

    monkeypatch.setattr("solera_server.cli.main", blocks)
    solera = Solera(tmp_path)
    before = sys.argv, sys.stdout, sys.stderr
    try:
        with pytest.raises(AssertionError, match="did not return"):
            await solera.cli("migrate", timeout=0.2)
        assert stuck.is_set()
        assert (sys.argv, sys.stdout, sys.stderr) == before
        with pytest.raises(AssertionError, match="still running"):
            await solera.cli("migrate", timeout=5)
        assert (sys.argv, sys.stdout, sys.stderr) == before
    finally:
        release.set()

    monkeypatch.setattr("solera_server.cli.main", lambda: print(sys.argv[-1]))
    assert await solera.cli("migrate", timeout=5) == (0, "migrate\n", "")
    assert (sys.argv, sys.stdout, sys.stderr) == before
