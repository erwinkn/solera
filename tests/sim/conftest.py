import pytest


@pytest.fixture(autouse=True)
def world():
    """The simulation keeps its engines on loops of its own: the suite's
    `world` fixture, which stops engines on pytest's loop, does not apply."""

    return None


def pytest_terminal_summary(terminalreporter):
    from .machine import STATS

    if not STATS["examples"]:
        return
    hours = STATS["seconds"] / 3600
    rate = f" ({STATS['steps'] / hours:,.0f} steps/hour)" if hours else ""  # replays alone time nothing
    terminalreporter.write_line(
        f"simulation: {STATS['examples']} runs, {STATS['steps']} steps, "
        f"{STATS['virtual'] / 3600:.1f} h virtual in {hours * 60:.1f} min{rate}"
    )
