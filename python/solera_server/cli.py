"""The solera CLI. `SOLERA_SERVER_URL` selects a running server (token auth via
`SOLERA_API_TOKEN`); otherwise commands drive an in-process engine against
`SOLERA_STATE_URL`. `solera manifest` always runs locally."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

TERMINAL = {"succeeded", "failed", "canceled"}


def _server_url():
    return os.getenv("SOLERA_SERVER_URL")


def _client():
    import httpx

    token = os.getenv("SOLERA_API_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.AsyncClient(base_url=_server_url(), headers=headers, timeout=60)


async def _remote_project(client):
    response = await client.get("/api/diagnostics")
    response.raise_for_status()
    return response.json()["project"]


async def _stale_pages(page) -> dict:
    """Every page of an asset partition's stale keys, put together."""

    first = await page(None)
    keys, after = list(first["keys"]), first.get("next")
    while after is not None:
        more = await page(after)
        keys.extend(more["keys"])
        after = more.get("next")
    return {
        "stale": bool(first["reasons"]),
        "reasons": first["reasons"],
        "tracked": first["tracked"],
        "keys": keys,
    }


def _reads(args) -> bool:
    """Commands that only read: they open the namespace as a reader, and
    leave a running engine its writer."""

    command, action = args.command, getattr(args, "runs_command", None)
    if command == "runs":
        return action is None or (action == "prune" and args.dry_run)
    if command == "cleanups":
        return not args.clear
    if command == "automations":
        return not args.action
    return command in ("run-show", "logs", "stale")


async def _local_reader(args):
    """The engine's queries over a reader: the persisted manifest, nothing
    initialized, nothing fenced — `record` fails if anything tried."""

    from .engine import Engine
    from .state import State

    state = await State.open(args.state_url, args.namespace, writer=False)
    if state.model.manifest is None:
        await state.close()
        raise SystemExit("no project registered in this namespace")
    return Engine(state, state.model.manifest, project=state.model.project or "", resolve_cache=False)


async def _local_engine(args):
    """An in-process engine: re-registers --project when given, else serves the
    manifest persisted by the last registration."""

    from .engine import Engine
    from .executors.local import load_manifest
    from .state import State

    state = await State.open(args.state_url, args.namespace)
    project = getattr(args, "project", None)
    try:
        if project:
            manifest = await load_manifest(project)
        else:
            manifest = state.model.manifest
            if manifest is None:
                raise SystemExit("no project registered in this namespace; pass --project")
            project = state.model.project or manifest["name"]
        runtime = Engine(state, manifest, project=project)
        await runtime.initialize()
    except BaseException:
        await state.close()
        raise
    return runtime


async def _wait_remote(client, p, run_id, poll=0.5):
    while True:
        response = await client.get(f"/api/projects/{p}/runs/{run_id}")
        response.raise_for_status()
        detail = response.json()
        if detail["request"]["status"] in TERMINAL:
            return detail
        await asyncio.sleep(poll)


SERVER_MODULES = {"fastapi", "uvicorn", "duckdb", "starlette"}


def main():
    """`solera …`. The engine's commands need `solera[server]`; `worker` and
    `manifest` run on the base install, as a worker's host has it."""

    try:
        _main()
    except ModuleNotFoundError as e:
        if (e.name or "").split(".")[0] not in SERVER_MODULES:
            raise
        raise SystemExit(
            f"this command runs the engine, which needs {e.name}: pip install 'solera[server]'"
        ) from None


def _main():
    parser = argparse.ArgumentParser(description="Asset orchestration on object storage")
    parser.add_argument(
        "--state-url", default=os.getenv("SOLERA_STATE_URL", Path(".solera").resolve().as_uri())
    )
    parser.add_argument("--namespace", default=os.getenv("SOLERA_NAMESPACE", "default"))
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--project", default=os.getenv("SOLERA_PROJECT"))
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="Serve the API and console", parents=[common])
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    serve.add_argument("--insecure", action="store_true", help="Loopback-only: disable token auth")

    commands.add_parser("manifest", help="Print the project manifest", parents=[common])

    run = commands.add_parser("run", help="Run assets", parents=[common])
    run.add_argument("targets", nargs="+")
    run.add_argument("--partition", action="append", default=[])
    run.add_argument("--partitions", choices=["latest", "all", "missing"])
    run.add_argument("--full", action="store_true", help="Full run: reset positions, no prior (§8)")
    run.add_argument("--upstream", action="store_true")
    run.add_argument("--config", default="{}", help="Run configuration as a JSON object")
    run.add_argument("--keys", action="append", default=[], help="EDGE=full or EDGE=k1,k2")
    run.add_argument("--tag", action="append", default=[], help="Label the run: NAME=VALUE (repeatable)")

    runs = commands.add_parser("runs", help="List, delete or prune runs (§7, §11)", parents=[common])
    runs.add_argument("--status", action="append", default=[], help="Only runs with this status (repeatable)")
    runs.add_argument("--asset", action="append", default=[], help="Only runs of this asset (repeatable)")
    runs.add_argument("--tag", action="append", default=[], help="Only runs tagged NAME=VALUE, or NAME")
    runs.add_argument("-q", dest="q", help="Only runs whose error contains this, or whose id starts with it")
    runs.add_argument(
        "--before", dest="cursor", help="Only runs older than this run id (the previous page's `next`)"
    )
    runs.add_argument("--limit", type=int, default=50)
    runs_sub = runs.add_subparsers(dest="runs_command")
    runs_delete = runs_sub.add_parser("delete", help="Delete a finished run")
    runs_delete.add_argument("run_id")
    runs_prune = runs_sub.add_parser("prune", help="Delete finished runs")
    runs_prune.add_argument("--before", help="Only runs created before this ISO date or time")
    runs_prune.add_argument("--asset", help="Only runs of this asset")
    runs_prune.add_argument("--keep", type=int, help="Keep the N newest matching runs")
    runs_prune.add_argument("--dry-run", action="store_true")
    cleanups = commands.add_parser(
        "cleanups",
        help="An output partition's pending cleanups, and the stuck ones; a removed or moved output's "
        "leftovers, and when they are due (docs/lifecycle.md §9.8)",
    )
    cleanups.add_argument("output")
    cleanups.add_argument("partition", nargs="?", default="")
    cleanups.add_argument("--clear", action="store_true", help="Forget the stuck entries; their objects stay")

    stale = commands.add_parser(
        "stale", help="Whether an asset's partition is stale, why, and its stale keys", parents=[common]
    )
    stale.add_argument("asset")
    stale.add_argument("partition", nargs="?", default="", help="Its partition; none when unpartitioned")

    keys = commands.add_parser(
        "keys", help="A per-key asset's failing keys (per-key-processing.md §9)", parents=[common]
    )
    keys_sub = keys.add_subparsers(dest="keys_command", required=True)
    keys_retry = keys_sub.add_parser("retry", help="Retry a per-key asset's failing keys now")
    keys_retry.add_argument("asset")
    keys_retry.add_argument("--partition", default=None, help="One partition only")
    for name in ("failed", "rejected", "canceled", "retrying", "timed-out", "all"):
        keys_retry.add_argument(f"--{name}", action="store_true")

    run_show = commands.add_parser("run-show", help="Show a run's tasks and attempts", parents=[common])
    run_show.add_argument("run_id")

    logs = commands.add_parser("logs", help="Print an attempt's log", parents=[common])
    logs.add_argument("run_id")
    logs.add_argument("attempt_id")
    logs.add_argument("--tail", type=int, help="Only the last N lines")

    automations = commands.add_parser("automations", help="List or control automations", parents=[common])
    automations.add_argument("action", nargs="?", choices=["enable", "disable", "run-now"])
    automations.add_argument("name", nargs="?")

    migrate = commands.add_parser(
        "migrate", help="Apply pending output migrations locally (§4)", parents=[common]
    )
    migrate.add_argument("outputs", nargs="*", help="Outputs to migrate (default: all declaring)")

    commit = commands.add_parser("commit", help="Advance a source (§5)", parents=[common])
    commit.add_argument("source")
    commit.add_argument("--version")
    commit.add_argument("--keys", help="Complete key map as JSON, or a JSON list for a partition set")
    commit.add_argument("--upsert", help="Key patch as JSON object (or JSON list for a partition set)")
    commit.add_argument("--remove", action="append", default=None)
    commit.add_argument("--by", default="cli", help="Who is committing (recorded on the commit's run)")

    worker = commands.add_parser("worker", help="Run a pool worker (§10), or a pool's sensor host")
    worker_sub = worker.add_subparsers(dest="worker_command", required=True)
    pool = worker_sub.add_parser("pool")
    pool.add_argument("name")
    pool.add_argument("--server", default=_server_url())
    hosts = worker_sub.add_parser("sensors", help="Run the sensors of a Pool (docs/lifecycle.md §11.2)")
    hosts.add_argument("name")
    hosts.add_argument("--server", default=_server_url())

    commands.add_parser("selftest", help="Check state and object storage connectivity")

    args = parser.parse_args()

    if args.command == "serve":
        if args.insecure and args.host not in {"127.0.0.1", "localhost", "::1"}:
            parser.error(
                "--insecure is limited to loopback listeners; set SOLERA_API_TOKEN for remote access"
            )
        if os.getenv("SOLERA_SELFTEST") == "1":
            from .selftest import selftest

            print(json.dumps(asyncio.run(selftest(args.state_url)), indent=2), flush=True)
        import uvicorn

        from .api import create_app

        project = args.project or "solera_server.demo:project"
        uvicorn.run(
            create_app(
                state_url=args.state_url,
                namespace=args.namespace,
                project=project,
                insecure=args.insecure,
                # Where workers reach this engine: set SOLERA_ENGINE_URL to a
                # public HTTPS name for remote workers (docs/lifecycle.md §5.2).
                engine_url=os.getenv("SOLERA_ENGINE_URL")
                or f"http://{'127.0.0.1' if args.host in ('0.0.0.0', '::') else args.host}:{args.port}",
            ),
            host=args.host,
            port=args.port,
        )
        return

    if args.command == "selftest":
        from .selftest import selftest

        print(json.dumps(asyncio.run(selftest(args.state_url)), indent=2), flush=True)
        return

    asyncio.run(_dispatch(args, parser))


async def _dispatch(args, parser):
    if args.command == "manifest":
        from .executors.local import load_manifest

        print(json.dumps(await load_manifest(args.project), indent=2))
        return

    if args.command == "worker":
        if not args.server:
            parser.error(f"solera worker {args.worker_command} needs --server or SOLERA_SERVER_URL")
        if args.worker_command == "sensors":  # the host replaces itself now and then: give it the process
            command = [
                "-m",
                "solera_worker",
                "sensors",
                "--pool",
                args.name,
                "--server",
                args.server.rstrip("/"),
            ]
            os.execv(sys.executable, [sys.executable, *command])
        from solera_worker.worker import run_pool

        token = os.getenv("SOLERA_POOL_TOKEN") or os.getenv("SOLERA_API_TOKEN")
        await run_pool(args.name, args.server.rstrip("/"), token=token)
        return

    if args.command == "migrate":
        await _migrate(args, parser)
        return

    if _server_url():
        await _remote(args, parser)
    else:
        await _local(args, parser)


async def _migrate(args, parser):
    """Apply declared migrations through the local worker path (§4): load the
    project, bind its stores to the namespace's object store, migrate."""

    from solera_worker.worker import load_project

    from .state import State

    # Read-only: a running server keeps its place as the namespace's writer.
    state = await State.open(args.state_url, args.namespace, writer=False)
    try:
        entrypoint = args.project or state.model.project
        if not entrypoint:
            parser.error("solera migrate needs --project or a previously registered project")
        project = load_project(entrypoint)
        migrating = {
            output.name: output
            for asset in project.assets.values()
            for output in asset.outputs
            if output.migrations
        }
        if args.outputs:
            for name in args.outputs:
                if name not in migrating:
                    parser.error(f"{name}: no such output declares migrations")
            selected = [migrating[name] for name in dict.fromkeys(args.outputs)]
        else:
            selected = list(migrating.values())
        for output in selected:
            record = project.manifest["outputs"][output.name]
            store = project.stores[record["store"]]
            applied = await store.migrate(output, output.migrations)
            for name in applied:
                print(f"{output.name}: applied {name}")
            if not applied:
                print(f"{output.name}: up to date")
    finally:
        await state.close()


def _parse_keys(specs):
    if not specs:
        return None
    out = {}
    for spec in specs:
        input, _, value = spec.partition("=")
        out[input] = value if value == "full" else [k for k in value.split(",") if k]
    return out


def _parse_tags(specs) -> dict[str, str]:
    tags = {}
    for spec in specs:
        name, eq, value = spec.partition("=")
        if not eq:
            raise SystemExit(f"--tag {spec!r}: expected NAME=VALUE")
        tags[name] = value
    return tags


def _runs_query(args) -> dict:
    query = {"status": args.status, "asset": args.asset, "tag": args.tag, "limit": args.limit}
    if args.q:
        query["q"] = args.q
    if args.cursor:
        query["before"] = args.cursor
    return query


def _prune_payload(args):
    import datetime as dt

    before = None
    if args.before:
        moment = dt.datetime.fromisoformat(args.before)
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=dt.UTC)
        before = moment.timestamp()
    return {"before": before, "asset": args.asset, "keep": args.keep, "dry_run": args.dry_run}


def _key_classes(args) -> list[str]:
    """`solera keys retry --failed --rejected …`; `--failed` when none is given."""

    names = ("failed", "rejected", "canceled", "retrying", "timed_out", "all")
    return [n for n in names if getattr(args, n)] or ["failed"]


def _commit_payload(args):
    keys = json.loads(args.keys) if args.keys else None
    upsert = json.loads(args.upsert) if args.upsert else None
    return {"version": args.version, "keys": keys, "upsert": upsert, "remove": args.remove, "by": args.by}


async def _remote(args, parser):
    async with _client() as client:
        p = await _remote_project(client)
        base = f"/api/projects/{p}"
        if args.command == "run":
            config = json.loads(args.config)
            if not isinstance(config, dict):
                parser.error("--config must be a JSON object")
            partitions = args.partition or args.partitions or "latest"
            body = {
                "targets": args.targets,
                "partitions": partitions,
                "mode": "full" if args.full else "incremental",
                "upstream": args.upstream,
                "config": config,
                "keys": _parse_keys(args.keys),
                "by": "cli",
                "tags": _parse_tags(args.tag),
            }
            response = await client.post(f"{base}/runs", json=body)
            response.raise_for_status()
            run = response.json()
            if "id" not in run:
                print(json.dumps(run, indent=2))
                return
            detail = await _wait_remote(client, p, run["id"])
            print(json.dumps(detail, indent=2))
            if detail["request"]["status"] != "succeeded":
                raise SystemExit(1)
        elif args.command == "cleanups" and args.clear:
            body = {"output": args.output, "partition": args.partition, "by": "cli"}
            response = await client.post(f"{base}/cleanups:clear", json=body)
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "cleanups":
            response = await client.get(f"{base}/cleanups")
            response.raise_for_status()
            rows = [r for r in response.json()["cleanups"] if r["output"] == args.output]
            print(json.dumps(rows, indent=2))
        elif args.command == "stale":

            async def page(after):
                params = {"partition": args.partition, **({"after": after} if after else {})}
                response = await client.get(f"{base}/assets/{args.asset}/stale-keys", params=params)
                response.raise_for_status()
                return response.json()

            print(json.dumps(await _stale_pages(page), indent=2))
        elif args.command == "keys":
            body = {"classes": _key_classes(args), "partition": args.partition, "by": "cli"}
            response = await client.post(f"{base}/assets/{args.asset}/keys:retry", json=body)
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "runs" and args.runs_command == "delete":
            response = await client.delete(f"{base}/runs/{args.run_id}")
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "runs" and args.runs_command == "prune":
            response = await client.post(f"{base}/runs:prune", json=_prune_payload(args))
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "runs":
            response = await client.get(f"{base}/runs", params=_runs_query(args))
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "run-show":
            response = await client.get(f"{base}/runs/{args.run_id}")
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "logs":
            params = {"tail": args.tail} if args.tail else {}
            response = await client.get(
                f"{base}/runs/{args.run_id}/attempts/{args.attempt_id}/logs", params=params
            )
            response.raise_for_status()
            sys.stdout.write(response.text)
        elif args.command == "automations":
            if not args.action:
                response = await client.get(f"{base}/automations")
                response.raise_for_status()
                print(json.dumps(response.json()["automations"], indent=2))
            else:
                if not args.name:
                    parser.error("automations enable|disable|run-now needs a name")
                response = await client.post(f"{base}/automations/{args.name}/{args.action}")
                response.raise_for_status()
                print(json.dumps(response.json(), indent=2))
        elif args.command == "commit":
            response = await client.post(f"{base}/sources/{args.source}/commit", json=_commit_payload(args))
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))


async def _local(args, parser):
    runtime = await (_local_reader(args) if _reads(args) else _local_engine(args))
    try:
        if args.command == "run":
            config = json.loads(args.config)
            if not isinstance(config, dict):
                parser.error("--config must be a JSON object")
            partitions = args.partition or args.partitions or "latest"
            run = await runtime.submit(
                args.targets,
                partitions=partitions,
                mode="full" if args.full else "incremental",
                upstream=args.upstream,
                config=config,
                keys=_parse_keys(args.keys),
                by="cli",
                tags=_parse_tags(args.tag),
            )
            if run is None:
                print(json.dumps({"status": "skipped-active"}))
                return
            detail = await runtime.run_until(run["id"])
            print(json.dumps(detail, indent=2))
            if detail["request"]["status"] != "succeeded":
                raise SystemExit(1)
        elif args.command == "stale":

            async def page(after):
                return await runtime.stale_keys(args.asset, args.partition, after=after)

            print(json.dumps(await _stale_pages(page), indent=2))
        elif args.command == "cleanups":
            done = runtime.clear_cleanups if args.clear else None
            view = (
                done(args.output, args.partition, "cli")
                if done
                else runtime.partition_cleanups(args.output, args.partition)
            )
            print(json.dumps(view, indent=2))
        elif args.command == "keys":
            found = runtime.retry_keys(args.asset, _key_classes(args), args.partition, "cli")
            runs = (
                await runtime.submit_retries(args.asset, found["partitions"], "cli")
                if found["partitions"]
                else []
            )
            found["runs"] = [(await runtime.run_until(r["id"]))["request"]["status"] for r in runs]
            print(json.dumps(found, indent=2))
        elif args.command == "runs" and args.runs_command == "delete":
            await runtime.delete_run(args.run_id)
            print(json.dumps({"deleted": [args.run_id]}, indent=2))
        elif args.command == "runs" and args.runs_command == "prune":
            print(json.dumps(await runtime.prune(**_prune_payload(args)), indent=2))
        elif args.command == "runs":
            from .history import RunFilter

            query = _runs_query(args)
            f = RunFilter(status=query["status"], asset=query["asset"], tag=query["tag"], q=query.get("q"))
            page = await runtime.list_runs(f, before=query.get("before"), limit=query["limit"])
            print(json.dumps(page, indent=2))
        elif args.command == "run-show":
            print(json.dumps(await runtime.run_detail(args.run_id), indent=2))
        elif args.command == "logs":
            sys.stdout.write(
                (await runtime.state.attempt_log(args.run_id, args.attempt_id, args.tail)).decode()
            )
        elif args.command == "automations":
            if not args.action:
                autos = list(runtime.m.automations.values())
                print(json.dumps(autos, indent=2))
            else:
                if not args.name:
                    parser.error("automations enable|disable|run-now needs a name")
                if args.action == "run-now":
                    print(json.dumps(await runtime.run_automation(args.name), indent=2))
                else:
                    enabled = args.action == "enable"
                    print(json.dumps(await runtime.set_automation(args.name, enabled), indent=2))
        elif args.command == "commit":
            print(
                json.dumps(
                    await runtime.commit_source(args.source, **_commit_payload(args)),
                    indent=2,
                )
            )
    finally:
        await runtime.state.close()


if __name__ == "__main__":
    main()
