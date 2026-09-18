import pytest
from data_orchestrator import Project
from dorc.execution import LocalSubprocess
from dorc_worker.worker import load_project

SOURCE = """
from data_orchestrator import asset, Project

@asset
def answer():
    return [{"value": 42}]

project = Project([answer])
"""


def test_module_entrypoint():
    project = load_project("dorc.demo:project")
    assert isinstance(project, Project)
    assert "sample_quality" in project.manifest["producers"]


def test_bare_module_defaults_to_project_attribute():
    project = load_project("dorc.demo")
    assert isinstance(project, Project)


def test_file_entrypoint_default_attribute(tmp_path):
    path = tmp_path / "defs.py"
    path.write_text(SOURCE)
    project = load_project(str(path))
    assert isinstance(project, Project)
    assert "answer" in project.manifest["producers"]


def test_file_entrypoint_with_attribute(tmp_path):
    path = tmp_path / "defs.py"
    path.write_text(SOURCE.replace("project =", "other ="))
    project = load_project(f"{path}:other")
    assert isinstance(project, Project)


def test_file_entrypoint_auto_detects_single_project(tmp_path):
    path = tmp_path / "defs.py"
    path.write_text(SOURCE.replace("project =", "defs ="))
    project = load_project(str(path))
    assert isinstance(project, Project)


def test_file_entrypoint_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_project(str(tmp_path / "nope.py"))


async def test_manifest_across_process_boundary(tmp_path):
    path = tmp_path / "defs.py"
    path.write_text(SOURCE)
    manifest = await LocalSubprocess(str(path)).manifest()
    assert "answer" in manifest["producers"]
