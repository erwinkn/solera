from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Asset orchestration on object storage")
    parser.add_argument("command", choices=["serve", "manifest", "run", "selftest"])
    parser.add_argument("targets", nargs="*")
    parser.add_argument("--project", default=os.getenv("CURSUS_PROJECT", "cursus_server.demo:project"))
    parser.add_argument(
        "--state-url", default=os.getenv("CURSUS_STATE_URL", Path(".cursus").resolve().as_uri())
    )
    parser.add_argument("--namespace", default=os.getenv("CURSUS_NAMESPACE", "default"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    parser.add_argument("--insecure", action="store_true", help="Disable auth on a loopback listener only")
    parser.add_argument("--partition", action="append", default=[])
    parser.add_argument("--mode", default="incremental", choices=["incremental", "fill_missing", "recompute"])
    parser.add_argument("--config", default="{}", help="Request configuration as a JSON object")
    args = parser.parse_args()
    if args.insecure and args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("--insecure is limited to loopback listeners; set CURSUS_API_TOKEN for remote access")
    if args.command == "serve":
        if os.getenv("CURSUS_SELFTEST") == "1":
            from .selftest import selftest

            print(json.dumps(asyncio.run(selftest(args.state_url)), indent=2), flush=True)
        import uvicorn

        from .api import create_app

        uvicorn.run(
            create_app(
                state_url=args.state_url,
                namespace=args.namespace,
                project=args.project,
                insecure=args.insecure,
            ),
            host=args.host,
            port=args.port,
        )
    elif args.command == "selftest":
        from .selftest import selftest

        print(json.dumps(asyncio.run(selftest(args.state_url)), indent=2), flush=True)
    else:
        from .engine import Engine
        from .execution import LocalSubprocess
        from .storage import SlateState

        async def execute():
            backend = LocalSubprocess(args.project)
            manifest = await backend.manifest()
            if args.command == "manifest":
                print(json.dumps(manifest, indent=2))
                return
            state = await SlateState.open(args.state_url, args.namespace)
            try:
                runtime = Engine(state, manifest, backend)
                await runtime.initialize()
                config = json.loads(args.config)
                if not isinstance(config, dict):
                    parser.error("--config must be a JSON object")
                run = await runtime.submit(
                    args.targets, partitions=args.partition, mode=args.mode, config=config
                )
                while True:
                    await runtime.execute_next()
                    detail = await runtime.run_detail(run["id"])
                    if detail["request"]["status"] in {"succeeded", "failed", "canceled"}:
                        print(json.dumps(detail, indent=2))
                        if detail["request"]["status"] != "succeeded":
                            raise SystemExit(1)
                        return
                    await asyncio.sleep(0.1)
            finally:
                await state.close()

        asyncio.run(execute())


if __name__ == "__main__":
    main()
