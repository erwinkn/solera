"""The deterministic simulation (tests/sim): random sequences of commits,
runs, cancels, crashes, takeovers, re-registrations, worker deaths and
object-store faults, checked after every step and at convergence. CI runs
a short budget; `--slow` (or SOLERA_SIM_EXAMPLES) a long one."""

import os

from hypothesis import HealthCheck, Phase, Verbosity, settings

import time

from .machine import STATS, Simulation


def _settings(slow: bool):
    examples = int(os.environ.get("SOLERA_SIM_EXAMPLES", 400 if slow else 20))
    return settings(
        max_examples=examples,
        stateful_step_count=int(os.environ.get("SOLERA_SIM_STEPS", 40)),
        deadline=None,
        suppress_health_check=list(HealthCheck),
        derandomize=not slow,
        print_blob=True,
        verbosity=Verbosity.normal,
        phases=[Phase.explicit, Phase.reuse, Phase.generate, Phase.target, Phase.shrink],
    )


def test_simulation(request):
    slow = request.config.getoption("--slow")
    Simulation.TestCase.settings = _settings(slow)
    started = time.monotonic()
    try:
        Simulation.TestCase().runTest()
    finally:
        STATS["seconds"] += time.monotonic() - started
