"""Run the seeded Railway demo and verify its public HTTP routes after startup."""

from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.error
import urllib.request

import uvicorn

from data_orchestrator.api import create_app
from data_orchestrator.database import Database
from examples.seed_demo import main as seed


PUBLIC_ORIGIN = "https://orchestrator-production-bd89.up.railway.app"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError("Demo verification does not follow redirects")


def verify() -> None:
    token = os.environ["DORC_API_TOKEN"]
    opener = urllib.request.build_opener(NoRedirect())

    def fetch(path: str, payload=None, *, authenticated: bool = True):
        headers = {"Authorization": f"Bearer {token}"} if authenticated else {}
        body = None
        if payload is not None:
            headers.update({"Content-Type": "application/json", "Origin": PUBLIC_ORIGIN})
            body = json.dumps(payload).encode()
        req = urllib.request.Request(PUBLIC_ORIGIN + path, data=body, headers=headers)
        with opener.open(req, timeout=15) as response:
            return response.read()

    try:
        deadline = time.monotonic() + 150
        while True:
            try:
                assert json.loads(fetch("/healthz", authenticated=False))["status"] == "ok"
                break
            except (urllib.error.URLError, TimeoutError, AssertionError):
                if time.monotonic() >= deadline:
                    raise
                time.sleep(2)
        html = fetch("/", authenticated=False).decode()
        assert "Data Orchestrator" in html and "<script" in html
        bundles = re.findall(r'(?:src|href)="(/assets/[^\"]+)"', html)
        assert bundles
        for bundle in bundles:
            assert fetch(bundle, authenticated=False)
        try:
            fetch("/api/catalog", authenticated=False)
        except urllib.error.HTTPError as error:
            assert error.code == 401, error.code
        else:
            raise AssertionError("The API allowed an unauthenticated catalog read")
        catalog = json.loads(fetch("/api/catalog"))
        assert len(catalog["assets"]) == 7
        assert all(asset["status"] == "materialized" for asset in catalog["assets"])
        request = json.loads(
            fetch(
                "/api/runs",
                {
                    "targets": ["sample_quality"],
                    "idempotency_key": "demo:https-smoke:" + catalog["manifest_id"],
                },
            )
        )
        deadline = time.monotonic() + 90
        while True:
            result = json.loads(fetch("/api/runs/" + request["id"]))
            status = result["request"]["status"]
            if status == "succeeded":
                break
            if status in {"failed", "canceled"} or time.monotonic() >= deadline:
                raise AssertionError(f"Public API materialization did not succeed: {status}")
            time.sleep(0.5)
        automations = json.loads(fetch("/api/automations"))
        print(
            "PUBLIC_DEMO_SMOKE_OK "
            + json.dumps(
                {
                    "assets": len(catalog["assets"]),
                    "ui_bundles": len(bundles),
                    "unauthenticated_api_status": 401,
                    "https_origin_write": "accepted",
                    "run_id": request["id"],
                    "run_status": status,
                    "automations": len(automations),
                    "enabled_automations": sum(a["enabled"] for a in automations),
                }
            ),
            flush=True,
        )
    except Exception as error:
        print(f"PUBLIC_DEMO_SMOKE_FAILED {type(error).__name__}: {error}", flush=True)


def main() -> None:
    if not os.environ.get("DORC_API_TOKEN"):
        raise RuntimeError("A demo API token must be configured before public deployment")
    seed()
    threading.Thread(target=verify, daemon=True, name="public-demo-verification").start()
    uvicorn.run(
        create_app(Database(), with_worker=True, concurrency=2),
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        proxy_headers=True,
        forwarded_allow_ips="*",
    )


if __name__ == "__main__":
    main()
