"""User code runs here, never in the API process (§10 worker protocol).

`python -m solera_worker run --objects URL --run RUN --attempt ID`:
read the spec from the attempt file -> refuse on revision mismatch -> resolve `env:` -> load inputs per
annotation (Incremental edges through the upstream key index) -> build ctx ->
run the producer -> for each returned output, work out what changed against
its key index, expire what retention no longer needs, store() it unless
nothing did, write the delta file -> rewrite the attempt file with the spec,
the result and the log index, in one PUT.
"""

from __future__ import annotations

import asyncio
import dataclasses
import gzip
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

from solera.keys.index import DeltaFiles, IndexState, KeyIndex, key_bytes, key_str
from solera.keys.io import ObjectIO, key_cache
from solera.sdk import (
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
from solera.stores import (
    Batches,
    Keys,
    Patch,
    Scope,
    Sql,
    StoreError,
    WriteError,
    _rows,
    key_map,
    resolve_env,
)


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

    def log(self, message: str, level: str = "info", **fields):
        entry = {"at": time.time(), "level": level, "message": str(message), "fields": fields}
        json.dumps(entry, allow_nan=False)
        self._shipper.append(entry)

    async def load(self, ref: Ref, t):
        store = self._stores[ref.store]
        return await store.load(ref, t, None)


LOG_FLUSH_SECONDS = 2.0
LOG_FLUSH_BYTES = 256 * 1024
LOG_CAP = 100 * 2**20  # compressed bytes per attempt


class LogShipper:
    """`ctx.log` lines as gzip-compressed JSON lines (docs/object-store-state.md §8).

    While the attempt runs, each flush — every 2 s or 256 KB — writes one
    gzip member as `{attempt}.log.{n}`, so the console can tail it. At the
    end the members are joined into `{attempt}.log` (concatenated gzip
    members are one valid gzip file) and the chunks deleted; `index()` lists
    `[byte offset, lines, first timestamp]` per member for range reads. Past
    `LOG_CAP` a truncation marker is written and shipping stops."""

    def __init__(self, objects, base: str):
        self.objects, self.base = objects, base
        self.pending: list[tuple[float, str]] = []
        self.pending_bytes = 0
        self.members: list[bytes] = []
        self.blocks: list[list] = []
        self.size = self.lines = 0
        self.truncated = False
        self._lock = asyncio.Lock()

    def append(self, entry):
        if self.truncated:
            return
        line = json.dumps(entry, allow_nan=False) + "\n"
        self.pending.append((entry["at"], line))
        self.pending_bytes += len(line)
        if self.pending_bytes >= LOG_FLUSH_BYTES:
            try:
                asyncio.get_running_loop().create_task(self.flush())
            except RuntimeError:
                pass  # logging from a thread: the next periodic flush ships it

    async def flush(self):
        async with self._lock:
            if not self.pending or self.truncated:
                return
            lines, self.pending, self.pending_bytes = self.pending, [], 0
            member = gzip.compress("".join(line for _, line in lines).encode(), compresslevel=6, mtime=0)
            if self.size + len(member) > LOG_CAP:
                self.truncated = True
                marker = {
                    "at": lines[0][0],
                    "level": "warning",
                    "message": f"log truncated: the attempt's log reached {LOG_CAP} bytes",
                    "fields": {},
                }
                lines = [(marker["at"], json.dumps(marker) + "\n")]
                member = gzip.compress(lines[0][1].encode(), mtime=0)
            await _put(self.objects, f"{self.base}.log.{len(self.members):06d}", member)
            self.blocks.append([self.size, len(lines), lines[0][0]])
            self.members.append(member)
            self.size += len(member)
            self.lines += len(lines)

    async def periodically(self):
        while True:
            await asyncio.sleep(LOG_FLUSH_SECONDS)
            await self.flush()

    async def finish(self) -> dict:
        """Join the chunks into the attempt's log; returns its index."""

        await self.flush()
        if self.members:
            import obstore

            await _put(self.objects, f"{self.base}.log", b"".join(self.members))
            await obstore.delete_async(
                self.objects, [f"{self.base}.log.{n:06d}" for n in range(len(self.members))]
            )
        return {"blocks": self.blocks, "lines": self.lines, "bytes": self.size, "truncated": self.truncated}


async def _resolve_inputs(spec, project, asset, keys_io):
    """Load each pin by annotation; build call args + ctx.changes (§5, §10).

    An Incremental edge over a keyed upstream reads its page from the pinned
    key index — the pending deltas in `[from, to]`, or the whole index for a
    full delivery — and loads just those keys. `delivered` reports where the
    page ended, for the engine's watermark (§6)."""

    manifest_asset = project.manifest["assets"][asset.name]
    edges = manifest_asset["inputs"]
    hints = typing.get_type_hints(asset.fn)
    args, changes, delivered = {}, {}, {}
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
        if "changes" in pin:  # Incremental: selection + ctx.changes (§5.1)
            ch = pin["changes"]
            full = bool(ch.get("full"))
            if "batches" in ch:
                lo, hi = (int(v) for v in ch["batches"])
                args[param] = await store.load(ref, t, Batches(lo, hi))
                changes[param] = Changes(rows=args[param], batches=range(lo, hi + 1), full=full)
                continue
            if "keys" in ch:  # a run's keys= override: a one-off selection
                upserted, deleted, after = {str(k): "" for k in ch["keys"]}, (), None
            else:
                index = KeyIndex(keys_io, None, IndexState.from_json(pin["index"]))
                start = key_bytes(ch["after"]) if ch.get("after") is not None else None
                if full:
                    keys, versions, nxt = await index.page(start, int(ch["limit"]))
                    flags = bytes(len(keys))
                else:
                    keys, versions, flags, nxt = await index.pending(
                        int(ch["from"]), int(ch["to"]), start, int(ch["limit"])
                    )
                upserted = {
                    key_str(k): key_str(v) for k, v, d in zip(keys, versions, flags, strict=True) if not d
                }
                deleted = tuple(key_str(k) for k, d in zip(keys, flags, strict=True) if d)
                after = key_str(nxt) if nxt is not None else None
            args[param] = await store.load(ref, t, Keys(upserted))
            changes[param] = Changes(
                rows=args[param], deleted=deleted, full=full, upserted=tuple(sorted(upserted))
            )
            delivered[param] = {"after": after, "upserted": sorted(upserted), "deleted": list(deleted)}
            continue
        if t is not None and is_ref_type(t):
            args[param] = ref
        else:
            args[param] = await store.load(ref, t, None)
    return args, changes, delivered


def _dict_inner(t):
    if t is None:
        return None
    if typing.get_origin(t) in (dict, dict):
        args_ = typing.get_args(t)
        if len(args_) == 2:
            return args_[1]
    return None


async def _store_outputs(spec, project, asset, objects, keys_io, result_value):
    """Store each returned output (§4, §6, §9).

    A keyed output's write is compared with its key index as pinned in the
    spec: a write that changes nothing is not stored at all and keeps the
    head; otherwise the store writes it and the changed entries become the
    batch's delta file. Returns `{name: entry}` and the cursor."""

    manifest_asset = project.manifest["assets"][asset.name]
    declared = {o["name"]: o for o in manifest_asset["outputs"]}
    decls = {o.name or asset.name: o for o in asset.outputs}
    priors = {name: Ref.from_json(r) for name, r in (spec.get("prior") or {}).items()}
    pinned = spec.get("outputs") or {}

    if isinstance(result_value, Result):
        values, cursor = result_value.outputs, result_value.cursor
    elif not decls:
        values, cursor = {}, UNSET  # a job: no outputs, return value ignored
    elif len(decls) == 1:
        values, cursor = {next(iter(decls)): result_value}, UNSET
    elif isinstance(result_value, dict) and set(result_value) <= set(decls):
        values, cursor = result_value, UNSET
    else:
        raise StoreError(f"{asset.name}: multi-output assets must return Result(outputs={{...}})")

    entries = {}
    for name, value in values.items():
        if name not in decls:
            raise StoreError(f"{asset.name}: returned undeclared output {name!r}")
        output = decls[name]
        store_name = declared[name]["store"]
        store = project.stores[store_name]
        if hasattr(store, "bind_objects"):
            store.bind_objects(objects)
        schema = None
        if output.migrations:
            migrate = getattr(store, "migrate", None)
            if not callable(migrate):
                raise StoreError(
                    f"{output.name}: store {store_name!r} has no migrate for declared migrations"
                )
            try:
                applied = await migrate(output, output.migrations)
            except StoreError:
                raise
            except Exception as error:
                raise StoreError(f"{output.name}: migration failed: {error}") from error
            schema = applied[-1] if applied else output.migrations[-1].name
        info = pinned.get(name) or {}
        prior = priors.get(name)
        before = (spec.get("retention") or {}).get("before")
        committed = Ref.from_json(info["head"]) if info.get("head") else None
        if before is not None and committed is not None and callable(getattr(store, "expire", None)):
            # Retention reaches data here (§11): whatever no version written
            # after `before` needs goes, and the committed head stays loadable.
            await store.expire(committed, before)
        scope = Scope(
            output=output,
            partition=spec["partition"],
            batch=info.get("batch"),
            attempt=spec["attempt"],
            aliases=tuple(info.get("aliases") or ()),
        )
        entry = {}
        if info.get("index") is not None:
            index = KeyIndex(keys_io, None, IndexState.from_json(info["index"]))
            written = None
            if isinstance(value, Sql):
                # Rows the harness never sees: the store reports the new key map.
                written = await store.store(value, prior, scope)
                if written.keys is None:
                    raise StoreError(f"{output.name}: store {store_name!r} reported no keys for a Sql write")
                new, removes, replace = dict(written.keys), [], True
            else:
                patch = isinstance(value, Patch)
                rows = value.rows if patch else value
                if output.is_partition_set:
                    rows = list(rows or [])
                else:
                    rows = _rows(rows, output.name)
                new = key_map(output, rows)
                # With no prior (a first write, or a full run) a Patch is the whole content.
                replace = not patch or prior is None
                removes = [] if replace else [str(k) for k in value.remove if str(k) not in new]
            delta = await index.changes(
                [key_bytes(k) for k in new],
                [key_bytes(v) for v in new.values()],
                [key_bytes(k) for k in removes],
                replace=replace,
            )
            if written is None:
                if not len(delta) and info.get("exists"):
                    entries[name] = {"unchanged": True}
                    continue
                written = await store.store(value, prior, scope)
            files = (
                await index.write(int(info["batch"]), spec["attempt"], delta)
                if len(delta)
                else DeltaFiles([], 0, 0, True)
            )
            entry["keys"] = files.to_json()
            if output.is_partition_set:
                if replace:
                    elements = set(new)
                else:
                    elements = (set(info.get("elements") or ()) - set(removes)) | set(new)
                entry["elements"] = sorted(elements)
        else:
            if isinstance(value, Sql) and output.incremental:
                raise WriteError(f"{output.name}: Sql writes need a keyed output")
            written = await store.store(value, prior, scope)
        if written.ref is None:
            continue
        ref = dataclasses.replace(written.ref, store=store_name)
        if schema is not None:
            ref = dataclasses.replace(ref, handle={**(ref.handle or {}), "schema": schema})
        entry["ref"] = ref.to_json()
        entries[name] = entry
    return entries, cursor


def _key_io(objects, objects_url: str, project: Project) -> ObjectIO:
    return ObjectIO(objects, cache=key_cache(project.manifest.get("key_cache"), objects_url))


async def run_attempt(objects_url: str, attempt: str, entrypoint: str | Project, *, run: str):
    """Run one attempt. The engine created `runs/{run}/{attempt}.json` holding
    the spec; the harness rewrites it once, at the end, with the spec, the
    result and the log index (docs/object-store-state.md §8)."""

    objects = _objects(objects_url)
    base = f"runs/{run}/{attempt}"
    record = await _get(objects, f"{base}.json")
    if record is None:
        raise StoreError(f"No attempt file at {base}.json")
    spec = json.loads(record)["spec"]
    shipper = LogShipper(objects, base)
    flusher = asyncio.create_task(shipper.periodically())

    async def finish(result: dict):
        flusher.cancel()
        log = await shipper.finish()
        body = {"spec": spec, "result": result, "log": log}
        await _put(objects, f"{base}.json", json.dumps(body, allow_nan=False).encode())

    async def fail(error: BaseException, retryable: bool):
        await finish(
            {
                "status": "failed",
                "error": {
                    "type": type(error).__name__,
                    "message": str(error),
                    "traceback": "".join(traceback.format_exception(error))[-32000:],
                    "retryable": retryable,
                },
            }
        )

    try:
        project = entrypoint if isinstance(entrypoint, Project) else load_project(entrypoint)
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
    for store in project.stores.values():
        if hasattr(store, "bind_objects"):
            store.bind_objects(objects)
    try:
        keys_io = _key_io(objects, objects_url, project)
        args, changes, delivered = await _resolve_inputs(spec, project, asset, keys_io)
        ctx = Ctx(spec, asset, project, objects, changes, shipper)
        signature = inspect.signature(asset.fn)
        if "ctx" in signature.parameters:
            args["ctx"] = ctx
        for name, resource in project.resources.items():
            if name in signature.parameters:
                args[name] = resolve_env(resource)  # env: secrets resolve in the harness (§5)
        value = asset.fn(**args)
        if inspect.isawaitable(value):
            value = await value
        outputs, cursor = await _store_outputs(spec, project, asset, objects, keys_io, value)
        result = {"status": "succeeded", "outputs": outputs, "delivered": delivered}
        if cursor is not UNSET:
            result["cursor"] = cursor
        await finish(result)
        return 0
    except StoreError as error:
        await fail(error, getattr(error, "retryable", False))
    except Exception as error:
        await fail(error, True)
    finally:
        flusher.cancel()
    return 1


async def run_pool(pool: str, server: str, token: str | None = None):
    """Pull path: register, claim, run the stage, complete (§10)."""

    import httpx

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(base_url=server, headers=headers, timeout=30) as client:
        capacity = {"cpu": os.cpu_count(), "memory": None, "gpu": None}
        registered = (
            await client.post(
                "/api/workers/register",
                json={"pools": [pool], "capacity": capacity},
            )
        ).json()
        worker_id = registered["worker"]
        print(f"[pool] worker {worker_id} registered in pool {pool!r}", flush=True)
        while True:
            try:
                response = await client.post(
                    "/api/tasks/claim", json={"worker": worker_id, "capacity": capacity}
                )
            except httpx.HTTPError:
                # The server may be briefly unreachable (engine tick pressure,
                # restart) — a pool worker polls forever rather than dying.
                await asyncio.sleep(1.0)
                continue
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
                await run_attempt(
                    stage["objects"], stage["attempt"], os.environ["SOLERA_PROJECT"], run=stage["run"]
                )
                await client.post(f"/api/tasks/{task_id}/complete", json={"worker": worker_id})
                print(f"[pool] completed {task_id}", flush=True)
            finally:
                renewal.cancel()


async def main():
    args = sys.argv[1:]
    if not args:
        raise SystemExit("usage: solera_worker run|manifest|pool ...")
    mode, rest = args[0], args[1:]
    if mode == "manifest":
        # solera_worker manifest PROJECT OUT
        entrypoint, out = rest[0], rest[1]
        project = load_project(entrypoint)
        Path(out).write_text(json.dumps(project.manifest, allow_nan=False))
        return
    if mode == "run":
        # solera_worker run --objects URL --attempt ID --run RUN (SOLERA_PROJECT env entrypoint)
        options = dict(zip(rest[::2], rest[1::2], strict=True))
        code = await run_attempt(
            options["--objects"], options["--attempt"], os.environ["SOLERA_PROJECT"], run=options["--run"]
        )
        raise SystemExit(code)
    if mode == "pool":
        options = dict(zip(rest[::2], rest[1::2], strict=True))
        await run_pool(options["--pool"], options["--server"].rstrip("/"), options.get("--token"))
        return
    raise SystemExit(f"unknown mode: {mode}")
