"""The generated conformance kit (`solera.testing.storemachine`) against the
shipped stores and the guide's example store: random sequences of attempts, stale writers, duplicates,
retries, pinned readers, cleanups and batches, checked after every step."""

import os
import tempfile
from pathlib import Path

import pytest
from hypothesis import HealthCheck, settings
from solera.testing.storemachine import stateful

from .test_store_conformance import HARNESSES

RUNS = int(os.environ.get("SOLERA_STORE_MACHINE_RUNS", 30))


@pytest.mark.parametrize("name", ["file", "postgres", "example", "s3"])
def test_a_store_conforms_under_random_sequences(name, request):
    make = HARNESSES[name]
    make(Path(tempfile.mkdtemp()))  # skips here, not inside Hypothesis, when its backend is absent
    machine = stateful(lambda: make(Path(tempfile.mkdtemp())))
    slow = request.config.getoption("--slow")
    machine.TestCase.settings = settings(
        max_examples=RUNS * (10 if slow else 1),
        stateful_step_count=30,
        deadline=None,
        derandomize=not slow,
        suppress_health_check=list(HealthCheck),
    )
    machine.TestCase().runTest()
