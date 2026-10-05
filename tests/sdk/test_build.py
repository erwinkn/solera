"""The build identity in the deploy (docs/per-key-processing.md §13)."""

import json
import subprocess
import sys

import pytest
from solera.build import identity


def git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


PROJECT = """
from helpers import f

from solera.sdk import Project, asset


@asset
def numbers():
    return [f()]


project = Project(assets=[numbers])
"""


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A project in a git work tree: `project.py` builds it, `helpers.py`
    is what its asset calls; `other.py` and `notes.md` are beside it."""

    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    git(repo, "init", "-q")
    git(repo, "config", "user.email", "t@example.com")
    git(repo, "config", "user.name", "t")
    (repo / "pkg" / "project.py").write_text(PROJECT)
    (repo / "pkg" / "helpers.py").write_text("def f(): return 1\n")
    (repo / "pkg" / "other.py").write_text("x = 1\n")
    (repo / "pkg" / "notes.md").write_text("# notes\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "one")
    return repo


def build(pkg) -> dict:
    """The build identity a fresh process importing the project computes, as a worker does."""

    script = "import json, sys; sys.path.insert(0, sys.argv[1]); import project; "
    script += "print(json.dumps(project.project.manifest['build']))"
    out = subprocess.run([sys.executable, "-c", script, str(pkg)], capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


def test_explicit_build_wins(repo, monkeypatch):
    assert identity(str(repo / "pkg"), "img@sha256:ab") == {"id": "img@sha256:ab", "source": "explicit"}
    monkeypatch.setenv("SOLERA_BUILD", "ci-42")
    assert identity(str(repo / "pkg"))["id"] == "ci-42"


def test_the_build_is_the_code_the_project_runs(repo):
    """The modules its objects reach — `project.py`, and `helpers.py`, which
    its asset calls — committed or not; the commit only for display. An
    edit beside them it does not run (a note, a module it never imports,
    data) changes nothing, so a local serve keeps its deploy."""

    pkg = repo / "pkg"
    clean = build(pkg)
    assert clean["source"] == "modules" and len(clean["commit"]) == 40
    (pkg / "notes.md").write_text("# other notes\n")
    (pkg / "other.py").write_text("x = 2\n")
    (pkg / "data.json").write_text("{}")
    assert build(pkg)["id"] == clean["id"]
    (pkg / "helpers.py").write_text("def f(): return 2\n")
    first = build(pkg)
    (pkg / "helpers.py").write_text("def f(): return 3\n")
    second = build(pkg)
    assert len({clean["id"], first["id"], second["id"]}) == 3  # two uncommitted fixes, two builds
    (pkg / "helpers.py").write_text("def f(): return 1\n")
    assert build(pkg)["id"] == clean["id"]


def test_outside_git_the_modules_are_the_build(tmp_path, monkeypatch):
    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    pkg = tmp_path / "proj"
    pkg.mkdir()
    (pkg / "project.py").write_text(PROJECT)
    (pkg / "helpers.py").write_text("def f(): return 1\n")
    first = build(pkg)
    assert first["source"] == "modules" and "commit" not in first
    (pkg / "helpers.py").write_text("def f(): return 2\n")
    assert build(pkg)["id"] != first["id"]


def test_revision_follows_the_build(monkeypatch):
    from solera.sdk import Project, asset

    @asset
    def a():
        return 1

    monkeypatch.setenv("SOLERA_BUILD", "one")
    one = Project(assets=[a]).manifest
    monkeypatch.setenv("SOLERA_BUILD", "two")
    two = Project(assets=[a]).manifest
    assert one["deploy"] != two["deploy"] and "code_hash" not in one["assets"]["a"]
    assert Project(assets=[a], build="two").manifest["deploy"] == two["deploy"]


def test_the_deploy_number_counts_served_deploys():
    from solera_server.model import Model

    m = Model()
    manifest = {"automations": {}, "sources": {}, "assets": {}, "outputs": {}}
    for deploy in ("r1", "r1", "r2", "r1"):
        m.apply({"type": "ProjectRegistered", "deploy": deploy, "manifest": manifest, "at": 0})
    assert m.deploy_number == 3
    restored = Model()
    restored.restore(m.snapshot())
    assert restored.deploy_number == 3


def test_a_method_mismatch_is_named():
    from solera.build import method_note

    assert method_note({"source": "modules"}, {"source": "modules"}) is None
    assert method_note({"source": "modules"}, None) is None
    note = method_note({"source": "explicit"}, {"source": "modules"})
    assert "explicit build id" in note and "project's modules" in note and "SOLERA_BUILD" in note


async def test_the_engine_warns_when_a_worker_computed_its_revision_another_way(tmp_path, caplog):
    """A worker whose deploy differs because it hashed its modules while the
    engine used an explicit id fails its attempt as before, and the engine
    says why."""

    import logging

    from solera.sdk import Project, asset
    from solera_server.state import State

    from tests.server.engines import make_engine

    @asset
    def numbers():
        return [1]

    project = Project(assets=[numbers], build="served")
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = make_engine(state, project)
    engine.manifest = {**engine.manifest, "build": {"id": "x", "source": "modules"}, "deploy": "elsewhere"}
    await engine.initialize()
    with caplog.at_level(logging.WARNING):
        detail = await engine.run_until((await engine.submit(["numbers"]))["id"], 10)
    assert detail["request"]["status"] == "failed"
    assert any("never match" in r.getMessage() for r in caplog.records)
    assert "SOLERA_BUILD" in detail["attempts"][detail["tasks"][0]["id"]][0]["error"]
    await state.close()


async def test_the_engine_warns_a_sensor_host_once(tmp_path, caplog):
    import logging

    from solera.sdk import Project, asset
    from solera_server.state import State

    from tests.server.engines import make_engine

    @asset
    def numbers():
        return [1]

    project = Project(assets=[numbers], build="served")
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = make_engine(state, project)
    engine.manifest = {**engine.manifest, "build": {"id": "x", "source": "modules"}}
    await engine.initialize()
    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            await engine.sensor_next("local", "other", "h1", 1, 0, "files")
    assert sum("never match" in r.getMessage() for r in caplog.records) == 1
    await state.close()


def test_the_error_policy_changes_the_revision(monkeypatch):
    """Review 9: classifying an error differently is a different project."""

    from solera import Failed, Rejected, Transient
    from solera.sdk import Project, asset

    monkeypatch.setenv("SOLERA_BUILD", "same")

    @asset
    def a():
        return 1

    class Slow(Transient):
        retry_for = "2h"

    class Slower(Transient):
        retry_for = "3h"

    revisions = {
        Project(assets=[a], errors=mapping).manifest["deploy"]
        for mapping in (
            {ValueError: Failed},
            {ValueError: Rejected},
            {ValueError: Slow},
            {ValueError: Slower},
            {},
        )
    }
    assert len(revisions) == 5
