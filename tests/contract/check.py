"""The contract's completeness check: every entry of docs/behaviors.md has a
test in tests/contract/ or a pending reason, no test names an entry the
contract lacks, and no contract test imports today's internals.

`uv run python -m tests.contract.check` prints the count."""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

from tests.contract.entries import PENDING

HERE = Path(__file__).parent
BEHAVIORS = HERE.parents[1] / "docs" / "behaviors.md"

# What a contract test may not import: the engine, the worker and the SDK's
# tracking internals, and the tests that read them. Only harness.py and
# conftest.py reach the engine, and only through its API and its lifecycle.
INTERNAL = (
    "solera_server",
    "solera_worker",
    "solera.keys",
    "solera.key_outcomes",
    "solera.lifecycle",
    "solera.fencing",
    "solera._native",
    "tests.server",
    "tests.sim",
    "tests.sdk",
    "tests.worker",
    "tests.staleness",
)
TRUSTED = {"harness.py", "conftest.py"}
ENTRY = re.compile(r"^\*\*([A-Z]{3}-\d+)\. ", re.M)
TEST = re.compile(r"^test_([a-z]{3})_(\d+)_")


def entries() -> list[str]:
    return ENTRY.findall(BEHAVIORS.read_text())


def entry_of(test: str) -> str | None:
    match = TEST.match(test)
    return f"{match[1].upper()}-{match[2]}" if match else None


def modules() -> list[Path]:
    return sorted(p for p in HERE.glob("*.py") if p.name not in TRUSTED)


def tests() -> dict[str, list[str]]:
    """Each entry's tests, from every `test_*` function of the area files."""

    found: dict[str, list[str]] = {}
    for path in HERE.glob("test_*.py"):
        if path.name == "test_completeness.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name.startswith("test_"):
                found.setdefault(entry_of(node.name) or "?", []).append(f"{path.name}::{node.name}")
    return found


def internal_imports() -> list[str]:
    out = []
    for path in modules():
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or ""]
            else:
                continue
            out += [
                f"{path.name}: {n}" for n in names if any(n == i or n.startswith(i + ".") for i in INTERNAL)
            ]
    return out


def problems() -> list[str]:
    ids, found = entries(), tests()
    known = set(ids)
    out = [f"{e}: no test and no pending reason" for e in ids if e not in found and e not in PENDING]
    out += [f"{e}: has tests and a pending reason" for e in ids if e in found and e in PENDING]
    out += [f"{t}: names no entry of the contract" for e, ts in found.items() if e not in known for t in ts]
    out += [f"{e}: pending, but the contract has no such entry" for e in PENDING if e not in known]
    if len(ids) != len(known):
        out.append("docs/behaviors.md names an entry twice")
    out += [f"imports an internal: {line}" for line in internal_imports()]
    return out


def summary() -> str:
    ids, found = entries(), tests()
    tested = [e for e in ids if e in found]
    pending = [e for e in ids if e in PENDING and e not in found]
    return (
        f"{len(ids)} entries: {len(tested)} with tests ({sum(len(found[e]) for e in tested)} tests), "
        f"{len(pending)} pending"
    )


def main() -> int:
    print(summary())
    for problem in problems():
        print(f"  {problem}")
    return 1 if problems() else 0


if __name__ == "__main__":
    sys.exit(main())
