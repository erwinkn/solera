"""User code runs here, never in the API process (§10 worker protocol).

`python -m dorc_worker run --objects URL --attempt ID`:
fetch spec -> refuse on revision mismatch -> resolve `env:` -> load inputs per
annotation -> build ctx -> run the producer -> store() each returned output,
stage key maps -> write the result last, in one PUT.
"""

from __future__ import annotations

import asyncio
import dataclasses
import importlib
import importlib.util
import inspect
import json
import os
import sys
import time
import traceback
import typing
from pathlib import Path

from data_orchestrator.sdk import (
    UNSET,
    Asset,
    Changes,
    Project,
    Ref,
    Result,
    TimePartitions,
    is_ref_type,
    split_partition,
)
from data_orchestrator.stores import Keys, Scope, StoreError


def _load_module(path: Path):
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_project(entrypoint: str) -> Project:
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
                candidates = [v for v in vars(module).values() if isinstance(v, Project)]
                if len(candidates) != 1:
                    raise TypeError("Project file must define a `project` or use file.py:attribute")
                project = candidates[0]
    else:
        project = getattr(
            importlib.import_module(target if separator else entrypoint),
            attribute if separator else "project",
        )
    if callable(project) and not isinstance(project, Project):
        project = project()
    if not isinstance(project, Project):
        raise TypeError("Entrypoint must be a Project or a factory returning one")
    return project


def _objects(url: str):
    import obstore

    return obstore.store.from_url(url)


async def _put(objects, key: str, value: bytes):
    import obstore

    await obstore.put_async(objects, key, value, mode="overwrite", use_multipart=False)


async def _get(objects, key: str) -> bytes | None:
    import obstore
    from obstore.exceptions import NotFoundError

    try:
        result = await obstore.get_async(objects, key)
    except NotFoundError:
        return None
    return bytes(await result.bytes_async())


class Ctx:
    """The `ctx` argument handed to producers (§2)."""

    def __init__(self, spec, asset: Asset, project: Project, objects, changes, shipper):
        self._spec, self._objects, self._shipper = spec, objects, shipper
        self.partition: str = spec["partition"]
        declared = project.manifest["assets"][asset.name]["partitions"]
        self.partitions: dict[str, str] = (
            split_partition(declared["dims"], self.partition) if declared else {}
        )
        self.partition_window = self._window(project, asset)
        self.cursor = spec.get("cursor")
        self.changes = changes
        self.run_id = spec["run"]["id"]
        self.config = spec["run"].get("config") or {}
        self.execution = spec.get("execution") or {"kind": "Local", "environment": {}, "placement": {}}
        self._stores = project.stores

    def _window(self, project, asset):
        declared = project.manifest["assets"][asset.name]["partitions"]
        if not declared:
            return None
        windows = {}
        for name, spec in declared["dims"].items():
            if spec["kind"] == "time":
                tp = TimePartitions(
                    spec["start"],
                    spec["every"],
                    end=spec.get("end"),
                    end_offset=spec.get("end_offset"),
                    timezone=spec["timezone"],
                    format=spec["format"],
                )
                windows[name] = tp.window(self.partitions[name])
        if len(windows) == 1:
            return next(iter(windows.values()))
        return windows or None

    def log(self, message: str, **fields):
        entry = {"at": time.time(), "message": str(message), "fields": fields}
        json.dumps(entry, allow_nan=False)
        self._shipper.append(entry)

    async def load(self, ref: Ref, t):
        store = self._stores[ref.store]
        return await store.load(ref, t, None)


class LogShipper:
    """Streams ctx.log lines to logs/{attempt}/{seq}.jsonl chunks."""

    def __init__(self, objects, attempt: str):
        self.objects, self.attempt = objects, attempt
        self.entries: list[dict] = []
        self.seq = 0

    def append(self, entry):
        self.entries.append(entry)

    async def flush(self):
        if not self.entries:
            return
        body = "".join(json.dumps(e, allow_nan=False) + "\n" for e in self.entries).encode()
        await _put(self.objects, f"logs/{self.attempt}/{self.seq:06d}.jsonl", body)
        self.seq += 1
        self.entries.clear()


async def _key_map(objects, ref: Ref) -> dict[str, str] | None:
    info = (ref.meta or {}).get("keys")
    if not info:
        return None
    data = await _get(objects, info["object"])
    return json.loads(data) if data else None


async def _resolve_inputs(spec, project, asset, objects):
    """Load each pin by annotation; build call args + ctx.changes (§5, §10)."""

    manifest_asset = project.manifest["assets"][asset.name]
    edges = manifest_asset["inputs"]
    hints = typing.get_type_hints(asset.fn)
    args, changes = {}, {}
    for name, pin in spec["inputs"].items():
        edge = edges.get(name)
        if edge is None:  # a dep pin: recorded, never bound
            continue
        param = name
        t = hints.get(param)
        if "refs" in pin:  # AllPartitions
            inner = _dict_inner(t)
            out = {}
            for key, ref_json in pin["refs"].items():
                ref = Ref.from_json(ref_json)
                out[key] = (
                    ref
                    if (inner is not None and is_ref_type(inner))
                    else await project.stores[ref.store].load(ref, inner, None)
                )
            args[param] = out
            continue
        ref = Ref.from_json(pin["ref"])
        store = project.stores[ref.store]
        if "changes" in pin:  # ByKey: selection + ctx.changes
            upserted = pin["changes"].get("upserted") or {}
            deleted = pin["changes"].get("deleted") or []
            changes[param] = Changes(upserted=sorted(upserted), deleted=list(deleted))
            if pin["changes"].get("full"):
                args[param] = await store.load(ref, t, None)
            else:
                args[param] = await store.load(ref, t, Keys(upserted))
            continue
        if t is not None and is_ref_type(t):
            args[param] = ref
        else:
            args[param] = await store.load(ref, t, None)
    return args, changes


def _dict_inner(t):
    if t is None:
        return None
    if typing.get_origin(t) in (dict, dict):
        args_ = typing.get_args(t)
        if len(args_) == 2:
            return args_[1]
    return None


async def _store_outputs(spec, project, asset, objects, result_value):
    """store() each returned output and stage key maps (§4, §6)."""

    manifest_asset = project.manifest["assets"][asset.name]
    declared = {o["name"]: o for o in manifest_asset["outputs"]}
    decls = {o.name or asset.name: o for o in asset.outputs}
    priors = {name: Ref.from_json(r) for name, r in (spec.get("prior") or {}).items()}

    if isinstance(result_value, Result):
        values, cursor = result_value.outputs, result_value.cursor
    elif len(decls) == 1:
        values, cursor = {next(iter(decls)): result_value}, UNSET
    elif isinstance(result_value, dict) and set(result_value) <= set(decls):
        values, cursor = result_value, UNSET
    else:
        raise StoreError(f"{asset.name}: multi-output assets must return Result(outputs={{...}})")

    refs = {}
    for name, value in values.items():
        if name not in decls:
            raise StoreError(f"{asset.name}: returned undeclared output {name!r}")
        output = decls[name]
        store_name = declared[name]["store"]
        store = project.stores[store_name]
        if hasattr(store, "bind_objects"):
            store.bind_objects(objects)
        prior = priors.get(name)
        prior_keys = await _key_map(objects, prior) if prior is not None else None
        scope = Scope(output=output, partition=spec["partition"], prior_keys=prior_keys)
        written = await store.store(value, prior, scope)
        if written.ref is None:
            continue
        ref = dataclasses.replace(written.ref, store=store_name)
        if written.keys is not None:
            body = json.dumps(written.keys, sort_keys=True, allow_nan=False).encode()
            import hashlib

            sha = hashlib.sha256(body).hexdigest()
            await _put(objects, f"keys/{sha}.json", body)
            ref = dataclasses.replace(
                ref, meta={**ref.meta, "keys": {"object": f"keys/{sha}.json", "count": len(written.keys)}}
            )
        refs[name] = ref.to_json()
    return refs, cursor


async def run_attempt(objects_url: str, attempt: str, entrypoint: str):
    objects = _objects(objects_url)
    spec_data = await _get(objects, f"specs/{attempt}.json")
    if spec_data is None:
        raise StoreError(f"No spec at specs/{attempt}.json")
    spec = json.loads(spec_data)
    result_key = f"results/{attempt}.json"

    async def fail(error: BaseException, retryable: bool):
        payload = {
            "attempt": attempt,
            "status": "failed",
            "error": {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": "".join(traceback.format_exception(error))[-32000:],
                "retryable": retryable,
            },
        }
        await _put(objects, result_key, json.dumps(payload, allow_nan=False).encode())

    try:
        project = load_project(entrypoint)
    except Exception as error:
        await fail(error, False)
        return 1
    if project.manifest["revision"] != spec["revision"]:
        await fail(
            StoreError(
                f"revision mismatch: spec {spec['revision'][:12]} != project {project.manifest['revision'][:12]}"
            ),
            False,
        )
        return 1
    asset = project.assets[spec["asset"]]
    shipper = LogShipper(objects, attempt)
    try:
        args, changes = await _resolve_inputs(spec, project, asset, objects)
        ctx = Ctx(spec, asset, project, objects, changes, shipper)
        signature = inspect.signature(asset.fn)
        if "ctx" in signature.parameters:
            args["ctx"] = ctx
        for name, resource in project.resources.items():
            if name in signature.parameters:
                args[name] = resource
        value = asset.fn(**args)
        if inspect.isawaitable(value):
            value = await value
        refs, cursor = await _store_outputs(spec, project, asset, objects, value)
        await shipper.flush()
        payload = {"attempt": attempt, "status": "succeeded", "outputs": refs}
        if cursor is not UNSET:
            payload["cursor"] = cursor
        await _put(objects, result_key, json.dumps(payload, allow_nan=False).encode())
        return 0
    except StoreError as error:
        await fail(error, getattr(error, "retryable", False))
    except Exception as error:
        await fail(error, True)
    finally:
        try:
            await shipper.flush()
        except Exception:
            pass
    return 1


async def _pool_worker(pool: str, server: str, token: str | None):
    """Pull path: register, claim, run the stage, complete (§10)."""

    import httpx

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(server, headers=headers, timeout=30) as client:
        capacity = {"cpu": os.cpu_count(), "memory": None, "gpu": None}
        registered = (await client.post("/api/workers/register", json={"pool": pool, **capacity})).json()
        worker_id = registered["worker"]
        print(f"[pool] worker {worker_id} registered in pool {pool!r}", flush=True)
        while True:
            response = await client.post("/api/tasks/claim", json={"worker": worker_id})
            if response.status_code == 204:
                await asyncio.sleep(1.0)
                continue
            response.raise_for_status()
            claim = response.json()
            task_id, stage = claim["task"], claim["stage"]
            print(f"[pool] claimed {task_id}", flush=True)
            lease = float(claim.get("lease_seconds", 30))

            async def renew(lease=lease, task_id=task_id):
                while True:
                    await asyncio.sleep(max(lease / 3, 1.0))
                    try:
                        await client.post(f"/api/tasks/{task_id}/renew", json={"worker": worker_id})
                    except Exception:
                        return

            renewal = asyncio.create_task(renew())
            try:
                await run_attempt(stage["objects"], stage["attempt"], os.environ["DORC_PROJECT"])
                await client.post(f"/api/tasks/{task_id}/complete", json={"worker": worker_id})
                print(f"[pool] completed {task_id}", flush=True)
            finally:
                renewal.cancel()


async def main():
    args = sys.argv[1:]
    if not args:
        raise SystemExit("usage: dorc_worker run|manifest|pool ...")
    mode, rest = args[0], args[1:]
    if mode == "manifest":
        # dorc_worker manifest PROJECT OUT
        entrypoint, out = rest[0], rest[1]
        project = load_project(entrypoint)
        Path(out).write_text(json.dumps(project.manifest, allow_nan=False))
        return
    if mode == "run":
        # dorc_worker run --objects URL --attempt ID (DORC_PROJECT env entrypoint)
        options = dict(zip(rest[::2], rest[1::2], strict=True))
        code = await run_attempt(options["--objects"], options["--attempt"], os.environ["DORC_PROJECT"])
        raise SystemExit(code)
    if mode == "pool":
        options = dict(zip(rest[::2], rest[1::2], strict=True))
        await _pool_worker(options["--pool"], options["--server"].rstrip("/"), options.get("--token"))
        return
    raise SystemExit(f"unknown mode: {mode}")
