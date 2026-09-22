"""The cursus CLI. `CURSUS_SERVER_URL` selects a running server (token auth via
`CURSUS_API_TOKEN`); otherwise commands drive an in-process engine against
`CURSUS_STATE_URL`. `cursus manifest` always runs locally."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

TERMINAL = {"succeeded", "failed", "canceled"}


def _server_url():
    return os.getenv("CURSUS_SERVER_URL")


def _client():
    import httpx

    token = os.getenv("CURSUS_API_TOKEN")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.AsyncClient(base_url=_server_url(), headers=headers, timeout=60)


async def _remote_project(client):
    response = await client.get("/api/diagnostics")
    response.raise_for_status()
    return response.json()["project"]


async def _local_engine(args):
    """An in-process engine: re-registers --project when given, else serves the
    manifest persisted by the last registration."""

    from .engine import Engine
    from .placements.local import load_manifest
    from .state import State
    from .storage import SlateState

    slate = await SlateState.open(args.state_url, args.namespace)
    state = State(slate)
    project = getattr(args, "project", None)
    try:
        if project:
            manifest = await load_manifest(project)
        else:
            manifest = await state.manifest()
            async with state.transaction() as tx:
                project = await tx.get("sys/entrypoint")
            project = project or manifest["name"]
    except BaseException:
        await slate.close()
        raise
    runtime = Engine(state, manifest, project=project)
    try:
        await runtime.initialize()
    except BaseException:
        await slate.close()
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


def main():
    parser = argparse.ArgumentParser(description="Asset orchestration on object storage")
    parser.add_argument(
        "--state-url", default=os.getenv("CURSUS_STATE_URL", Path(".cursus").resolve().as_uri())
    )
    parser.add_argument("--namespace", default=os.getenv("CURSUS_NAMESPACE", "default"))
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--project", default=os.getenv("CURSUS_PROJECT"))
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="Serve the API and console", parents=[common])
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    serve.add_argument("--insecure", action="store_true", help="Loopback-only: disable token auth")

    commands.add_parser("manifest", help="Print the project manifest", parents=[common])

    run = commands.add_parser("run", help="Materialize assets", parents=[common])
    run.add_argument("targets", nargs="+")
    run.add_argument("--partition", action="append", default=[])
    run.add_argument("--partitions", choices=["latest", "all", "missing"])
    run.add_argument("--full", action="store_true", help="Full run: reset watermarks, no prior (§8)")
    run.add_argument("--upstream", action="store_true")
    run.add_argument("--config", default="{}", help="Run configuration as a JSON object")
    run.add_argument("--keys", action="append", default=[], help="EDGE=full or EDGE=k1,k2")

    commands.add_parser("runs", help="List runs", parents=[common])

    run_show = commands.add_parser("run-show", help="Show a run's tasks and attempts", parents=[common])
    run_show.add_argument("run_id")

    logs = commands.add_parser("logs", help="Print an attempt's log", parents=[common])
    logs.add_argument("attempt_id")

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
    commit.add_argument("--remove", action="append", default=[])

    worker = commands.add_parser("worker", help="Run a pool worker (§10)")
    worker_sub = worker.add_subparsers(dest="worker_command", required=True)
    pool = worker_sub.add_parser("pool")
    pool.add_argument("name")
    pool.add_argument("--server", default=_server_url())

    commands.add_parser("selftest", help="Check state and object storage connectivity")

    retention = commands.add_parser(
        "retention", help="Sweep retained history per asset/project policy (§5)", parents=[common]
    )
    retention.add_argument("action", choices=["sweep"])

    gc = commands.add_parser("gc", help="Run one SlateDB garbage-collection pass (§4.4)")
    gc.add_argument(
        "--min-age-ms",
        type=int,
        default=300_000,
        help="Only collect objects older than this (default: 5 minutes)",
    )
    gc.add_argument("--dry-run", action="store_true", help="Report what would be collected")

    args = parser.parse_args()

    if args.command == "serve":
        if args.insecure and args.host not in {"127.0.0.1", "localhost", "::1"}:
            parser.error(
                "--insecure is limited to loopback listeners; set CURSUS_API_TOKEN for remote access"
            )
        if os.getenv("CURSUS_SELFTEST") == "1":
            from .selftest import selftest

            print(json.dumps(asyncio.run(selftest(args.state_url)), indent=2), flush=True)
        import uvicorn

        from .api import create_app

        project = args.project or "cursus_server.demo:project"
        uvicorn.run(
            create_app(
                state_url=args.state_url,
                namespace=args.namespace,
                project=project,
                insecure=args.insecure,
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
        from .placements.local import load_manifest

        print(json.dumps(await load_manifest(args.project), indent=2))
        return

    if args.command == "worker":
        if not args.server:
            parser.error("cursus worker pool needs --server or CURSUS_SERVER_URL")
        from cursus_worker.worker import run_pool

        await run_pool(args.name, args.server.rstrip("/"), token=os.getenv("CURSUS_API_TOKEN"))
        return

    if args.command == "migrate":
        await _migrate(args, parser)
        return

    if args.command == "gc":
        # Always local: GC runs against the state store itself, no server needed.
        from .storage import gc_once

        await gc_once(args.state_url, args.namespace, min_age_ms=args.min_age_ms, dry_run=args.dry_run)
        print(json.dumps({"gc": "ok", "dry_run": args.dry_run}))
        return

    if args.command == "retention":
        # Local only: the sweep reads the manifest and drives state directly.
        if _server_url():
            parser.error("cursus retention runs locally (no remote endpoint)")
        runtime = await _local_engine(args)
        try:
            print(json.dumps(await runtime.retention_sweep(), indent=2))
        finally:
            await runtime.state.close()
        return

    if _server_url():
        await _remote(args, parser)
    else:
        await _local(args, parser)


async def _migrate(args, parser):
    """Apply declared migrations through the local harness path (§4): load the
    project, bind its stores to the namespace's object store, migrate."""

    import obstore
    from cursus_worker.worker import load_project

    from .state import State
    from .storage import SlateState

    slate = await SlateState.open(args.state_url, args.namespace)
    try:
        state = State(slate)
        entrypoint = args.project
        if not entrypoint:
            async with state.transaction() as tx:
                entrypoint = await tx.get("sys/entrypoint")
        if not entrypoint:
            parser.error("cursus migrate needs --project or a previously registered project")
        project = load_project(entrypoint)
        objects = obstore.store.from_url(state.objects_url)
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
            if hasattr(store, "bind_objects"):
                store.bind_objects(objects)
            applied = await store.migrate(output, output.migrations)
            for name in applied:
                print(f"{output.name}: applied {name}")
            if not applied:
                print(f"{output.name}: up to date")
    finally:
        await slate.close()


def _parse_keys(specs):
    if not specs:
        return None
    out = {}
    for spec in specs:
        edge, _, value = spec.partition("=")
        out[edge] = value if value == "full" else [k for k in value.split(",") if k]
    return out


def _commit_payload(args):
    keys = json.loads(args.keys) if args.keys else None
    upsert = json.loads(args.upsert) if args.upsert else None
    return {"version": args.version, "keys": keys, "upsert": upsert, "remove": args.remove}


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
        elif args.command == "runs":
            response = await client.get(f"{base}/runs")
            response.raise_for_status()
            print(json.dumps(response.json()["runs"], indent=2))
        elif args.command == "run-show":
            response = await client.get(f"{base}/runs/{args.run_id}")
            response.raise_for_status()
            print(json.dumps(response.json(), indent=2))
        elif args.command == "logs":
            response = await client.get(f"{base}/attempts/{args.attempt_id}/logs")
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
    runtime = await _local_engine(args)
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
            )
            if run is None:
                print(json.dumps({"status": "skipped-active"}))
                return
            detail = await runtime.run_until(run["id"])
            print(json.dumps(detail, indent=2))
            if detail["request"]["status"] != "succeeded":
                raise SystemExit(1)
        elif args.command == "runs":
            print(json.dumps(await runtime.list_runs(), indent=2))
        elif args.command == "run-show":
            print(json.dumps(await runtime.run_detail(args.run_id), indent=2))
        elif args.command == "logs":
            for key in await runtime.state.list_objects(f"logs/{args.attempt_id}/"):
                data = await runtime.state.get_object(key)
                if data:
                    sys.stdout.write(data.decode())
        elif args.command == "automations":
            if not args.action:
                autos = [a for _, a in await runtime.state.automations()]
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
