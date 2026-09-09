from __future__ import annotations

import argparse
import os
import sys

from .database import Database


def main() -> None:
    parser = argparse.ArgumentParser(prog="dorc", description="Asset-first data orchestration")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="Initialize or upgrade PostgreSQL metadata")
    register = commands.add_parser("register", help="Register a versioned definition manifest")
    register.add_argument("entrypoint", help="module:definitions")
    run = commands.add_parser("run", help="Submit a materialization request")
    run.add_argument("assets", nargs="+")
    run.add_argument("--partition", action="append", default=[])
    run.add_argument(
        "--mode", choices=["incremental", "recompute", "fill_missing"], default="incremental"
    )
    for command in ["serve", "dev", "worker"]:
        command_parser = commands.add_parser(command)
        command_parser.add_argument("--concurrency", type=int, default=4)
        if command == "dev":
            command_parser.add_argument("entrypoint")
        if command != "worker":
            command_parser.add_argument("--host", default="127.0.0.1")
            command_parser.add_argument("--port", type=int, default=8000)
            command_parser.add_argument("--no-worker", action="store_true")
            command_parser.add_argument("--allow-unauthenticated", action="store_true")
    execute = commands.add_parser("_execute", help=argparse.SUPPRESS)
    execute.add_argument("task_id")
    execute.add_argument("token")
    args = parser.parse_args()
    database = Database()
    # Entrypoints are explicitly registered by the operator, never supplied by API clients.
    sys.path.insert(0, os.getcwd())
    if args.command == "migrate":
        database.migrate()
    elif args.command == "register":
        print(database.register(args.entrypoint))
    elif args.command == "run":
        print(database.submit(args.assets, partitions=args.partition, mode=args.mode))
    elif args.command == "_execute":
        from .engine import Engine

        Engine(database).execute(args.task_id, args.token)
    elif args.command == "worker":
        from .worker import Worker

        Worker(database, concurrency=args.concurrency).run()
    else:
        import uvicorn

        from .api import create_app

        if (
            args.host not in {"127.0.0.1", "localhost", "::1"}
            and not os.getenv("DORC_API_TOKEN")
            and not args.allow_unauthenticated
        ):
            parser.error(
                "Set DORC_API_TOKEN before binding publicly, or explicitly pass --allow-unauthenticated for a trusted local deployment"
            )
        if args.command == "dev":
            database.migrate()
            database.register(args.entrypoint)
        uvicorn.run(
            create_app(database, with_worker=not args.no_worker, concurrency=args.concurrency),
            host=args.host,
            port=args.port,
        )


if __name__ == "__main__":
    main()
