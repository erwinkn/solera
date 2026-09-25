"""Server-side placements (§10): a placement is lifecycle only — start the
harness somewhere, report when it stopped. It never reads a spec or a result.

    Stage    = {"attempt": str, "objects": str}
    RunHandle = JSON dict, durable across engine restarts
    Exit     = {"code": int | None, "reason": str | None, "meta": dict}
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from .local import LocalPlacement, load_manifest
from .pool import PoolPlacement


@dataclass(frozen=True)
class PlacementContext:
    """What the engine hands every placement it builds."""

    state: Any
    objects_url: str
    project: str
    clock: Any


class ServerPlacement(Protocol):
    """launch/wait/cancel; `max_concurrent` caps in-flight attempts per env (§10)."""

    max_concurrent: int | None = None

    async def launch(self, stage: dict) -> dict: ...

    async def wait(self, run: dict, timeout: float) -> dict | None: ...

    async def cancel(self, run: dict) -> None: ...


class UnavailablePlacement(ServerPlacement):
    """A registered kind whose backing integration is not installed."""

    def __init__(self, kind: str, error: Exception):
        self.kind, self.error = kind, error

    async def launch(self, stage):
        raise RuntimeError(f"{self.kind} placement unavailable: {self.error}")

    async def wait(self, run, timeout):
        return None

    async def cancel(self, run):
        return None


def _remote(kind: str):
    """AWSECS / K8sJob / Modal: lazily resolved, unavailable without the SDK."""

    def build(environment, options, ctx):
        from . import remote

        try:
            env_cls = getattr(remote, kind)
            return env_cls(environment, options, ctx)
        except Exception as error:
            return UnavailablePlacement(kind, error)

    return build


class Registry:
    """Rebuilds executable placements from manifest `{kind, environment, placement}`.

    Built-ins plus any kinds registered for this engine (tests, or executor
    classes declared with `Project(executors=)` that the server can import).
    """

    def __init__(self, ctx: PlacementContext, extra: dict | None = None):
        self.ctx = ctx
        self.builders = {
            "Local": lambda env, opt, c: LocalPlacement(c),
            "Pool": lambda env, opt, c: PoolPlacement(c, env["name"]),
            "AWSECS": _remote("AWSECS"),
            "K8sJob": _remote("K8sJob"),
            "Modal": _remote("Modal"),
        }
        self.builders.update(extra or {})

    def register(self, kind: str, builder):
        self.builders[kind] = builder

    def build(self, spec: dict) -> ServerPlacement:
        kind = spec["kind"]
        if kind not in self.builders:
            raise ValueError(f"Unregistered placement kind: {kind!r}")
        return self.builders[kind](spec.get("environment") or {}, spec.get("placement") or {}, self.ctx)

    def env_key(self, spec: dict) -> str:
        """In-flight attempts are counted per environment against max_concurrent."""

        import json

        return json.dumps(
            {"kind": spec["kind"], "environment": spec.get("environment") or {}},
            sort_keys=True,
        )


__all__ = [
    "PlacementContext",
    "Registry",
    "ServerPlacement",
    "Stage",
    "UnavailablePlacement",
    "load_manifest",
]


@dataclass(frozen=True)
class Stage:
    attempt: str
    objects: str
