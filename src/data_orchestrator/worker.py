"""User code runs here, never in the API process."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import sys
from pathlib import Path

from .sdk import AssetContext, Project, normalize_result


def _load_module(path: Path):
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_project(entrypoint):
    target, separator, attribute = entrypoint.rpartition(":")
    if target.endswith(".py") or (not separator and entrypoint.endswith(".py")):
        path = Path(target or entrypoint).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Project file not found: {path}")
        module = _load_module(path)
        if separator:
            project = getattr(module, attribute)
        else:
            project = getattr(module, "project", None)
            if project is None:
                candidates = [value for value in vars(module).values() if isinstance(value, Project)]
                if len(candidates) != 1:
                    raise TypeError("Project file must define a `project` or use file.py:attribute")
                project = candidates[0]
    else:
        project = getattr(
            importlib.import_module(target if separator else entrypoint),
            attribute if separator else "project",
        )
    if callable(project):
        project = project()
    if not isinstance(project, Project):
        raise TypeError("Entrypoint must be a Project or a factory returning one")
    return project


async def main():
    mode, entrypoint, *paths = sys.argv[1:]
    project = load_project(entrypoint)
    if mode == "manifest":
        Path(paths[0]).write_text(json.dumps(project.manifest, allow_nan=False))
        return
    spec = json.loads(Path(paths[0]).read_text())
    if project.manifest["revision"] != spec["revision"]:
        raise RuntimeError("Code revision changed; register the new project before executing")
    producer = project.producers[spec["producer"]]
    args = dict(spec["inputs"])
    ctx = None
    if "ctx" in inspect.signature(producer.fn).parameters:
        if "ctx" in args:
            raise ValueError("ctx is reserved for AssetContext")
        ctx = AssetContext(**spec["context"])
        args["ctx"] = ctx
    for name, value in project.resources.items():
        if name in inspect.signature(producer.fn).parameters:
            if name in args:
                raise ValueError(f"Input and resource conflict: {name}")
            args[name] = value
    result = producer.fn(**args)
    if inspect.isawaitable(result):
        result = await result
    payload = normalize_result(result, list(producer.outputs))
    if ctx is not None:
        payload["log_entries"] = ctx._records
    Path(paths[1]).write_text(json.dumps(payload, allow_nan=False))


if __name__ == "__main__":
    asyncio.run(main())
