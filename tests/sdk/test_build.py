"""The build identity in the deploy (docs/per-key-processing.md §13)."""

import subprocess

import pytest
from solera.build import identity


def git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    git(repo, "init", "-q")
    git(repo, "config", "user.email", "t@example.com")
    git(repo, "config", "user.name", "t")
    (repo / "pkg" / "project.py").write_text("x = 1\n")
    (repo / "pkg" / "helpers.py").write_text("def f(): return 1\n")
    (repo / ".gitignore").write_text("data/\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "one")
    return repo


def test_explicit_build_wins(repo, monkeypatch):
    assert identity(str(repo / "pkg"), "img@sha256:ab") == {"id": "img@sha256:ab", "source": "explicit"}
    monkeypatch.setenv("SOLERA_BUILD", "ci-42")
    assert identity(str(repo / "pkg"))["id"] == "ci-42"


def test_uncommitted_edits_are_distinct_builds(repo):
    """Two different uncommitted fixes of a helper are two builds — a
    commit with a dirty flag could not tell them apart."""

    pkg = str(repo / "pkg")
    clean = identity(pkg)
    assert clean["source"] == "git" and clean["dirty"] is False
    (repo / "pkg" / "helpers.py").write_text("def f(): return 2\n")
    first = identity(pkg)
    (repo / "pkg" / "helpers.py").write_text("def f(): return 3\n")
    second = identity(pkg)
    assert first["dirty"] and second["dirty"] and first["commit"] == second["commit"]
    assert len({clean["id"], first["id"], second["id"]}) == 3
    (repo / "pkg" / "helpers.py").write_text("def f(): return 1\n")
    assert identity(pkg)["id"] == clean["id"]


def test_untracked_files_count_ignored_ones_do_not(repo):
    pkg = str(repo / "pkg")
    before = identity(pkg)["id"]
    (repo / "data").mkdir()
    (repo / "data" / "out.json").write_text("{}")
    assert identity(pkg)["id"] == before
    (repo / "pkg" / "new_helper.py").write_text("y = 2\n")
    assert identity(pkg)["id"] != before
    (repo / "pkg" / "new_helper.py").unlink()
    (repo / "pkg" / "helpers.py").unlink()  # a deletion is a change too
    assert identity(pkg)["id"] != before


def test_outside_git_python_files_only(tmp_path, monkeypatch):
    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    (tmp_path / "proj").mkdir()
    (tmp_path / "proj" / "project.py").write_text("x = 1\n")
    first = identity(str(tmp_path / "proj"))
    assert first["source"] == "files"
    (tmp_path / "proj" / "data.json").write_text("{}")  # what a run writes next to it
    (tmp_path / "proj" / "__pycache__").mkdir()
    (tmp_path / "proj" / "__pycache__" / "x.py").write_text("")
    assert identity(str(tmp_path / "proj"))["id"] == first["id"]
    (tmp_path / "proj" / "project.py").write_text("x = 2\n")
    assert identity(str(tmp_path / "proj"))["id"] != first["id"]


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

    assert method_note({"source": "git"}, {"source": "git"}) is None
    assert method_note({"source": "git"}, None) is None
    note = method_note({"source": "git"}, {"source": "files"})
    assert "git work tree" in note and "Python files" in note and "SOLERA_BUILD" in note


async def test_the_engine_warns_when_a_worker_computed_its_revision_another_way(tmp_path, caplog):
    """A worker whose deploy differs because it hashed files while the engine
    used git fails its attempt as before, and the engine says why."""

    import logging

    from solera.sdk import Project, asset
    from solera_server.state import State

    from tests.server.test_engine import make_engine

    @asset
    def numbers():
        return [1]

    project = Project(assets=[numbers], build="served")
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = make_engine(state, project)
    engine.manifest = {**engine.manifest, "build": {"id": "x", "source": "git"}, "deploy": "elsewhere"}
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

    from tests.server.test_engine import make_engine

    @asset
    def numbers():
        return [1]

    project = Project(assets=[numbers], build="served")
    state = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    engine = make_engine(state, project)
    engine.manifest = {**engine.manifest, "build": {"id": "x", "source": "git"}}
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


def test_submodule_contents_are_part_of_the_build(tmp_path, monkeypatch):
    """Review 10: two different edits inside a submodule are two builds."""

    monkeypatch.delenv("SOLERA_BUILD", raising=False)
    sub = tmp_path / "sub"
    sub.mkdir()
    git(sub, "init", "-q")
    git(sub, "config", "user.email", "t@example.com")
    git(sub, "config", "user.name", "t")
    (sub / "lib.py").write_text("x = 1\n")
    git(sub, "add", "-A")
    git(sub, "commit", "-qm", "sub")
    top = tmp_path / "top"
    top.mkdir()
    git(top, "init", "-q")
    git(top, "config", "user.email", "t@example.com")
    git(top, "config", "user.name", "t")
    (top / "project.py").write_text("y = 1\n")
    git(top, "-c", "protocol.file.allow=always", "submodule", "add", "-q", str(sub), "vendored")
    git(top, "commit", "-qam", "top")
    clean = identity(str(top))
    (top / "vendored" / "lib.py").write_text("x = 2\n")
    first = identity(str(top))
    (top / "vendored" / "lib.py").write_text("x = 3\n")
    second = identity(str(top))
    assert len({clean["id"], first["id"], second["id"]}) == 3
