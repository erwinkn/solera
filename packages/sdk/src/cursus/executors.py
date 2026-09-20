"""Environments and placements (§10). Calling an environment returns a placement."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .sdk import RegistrationError

_BYTES = {
    "B": 1,
    "KB": 10**3,
    "MB": 10**6,
    "GB": 10**9,
    "TB": 10**12,
    "KIB": 2**10,
    "MIB": 2**20,
    "GIB": 2**30,
    "TIB": 2**40,
}


def parse_bytes(value: str | int | None) -> int | None:
    if value is None or isinstance(value, int):
        return value
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([A-Za-z]+)", str(value).strip())
    if not match or match.group(2).upper() not in _BYTES:
        raise RegistrationError(f"Invalid memory value: {value!r}")
    return int(float(match.group(1)) * _BYTES[match.group(2).upper()])


@dataclass(frozen=True)
class Placement:
    """A typed per-asset request built from an environment (§10)."""

    kind: str
    environment: dict
    options: dict

    def serialized(self) -> dict:
        return {"kind": self.kind, "environment": self.environment, "placement": self.options}


class Environment:
    kind = "?"
    allowed: frozenset[str] = frozenset()

    def __init__(self, **config: Any):
        self.config = config

    def _options(self, **options: Any) -> dict:
        return {k: v for k, v in options.items() if v is not None}

    def _check(self, options: dict, allowed: set[str]) -> dict:
        unknown = set(options) - allowed
        if unknown:
            raise RegistrationError(f"{self.kind} does not take options: {sorted(unknown)}")
        if "cpu" in options and options["cpu"] is not None and not isinstance(options["cpu"], int):
            raise RegistrationError(f"{self.kind}: cpu must be an int")
        if "memory" in options:
            options["memory"] = parse_bytes(options["memory"])
        return {k: v for k, v in options.items() if v is not None}

    def __call__(self, **options) -> Placement:
        return Placement(self.kind, dict(self.config), self._check(options, set(self.allowed)))


class Local(Environment):
    """In-process/subprocess on the engine host. No options."""

    kind = "Local"

    def __init__(self):
        super().__init__()

    def __call__(self, **options) -> Placement:
        return Placement(self.kind, dict(self.config), self._check(options, set()))


class AWSECS(Environment):
    kind = "AWSECS"

    def __init__(self, *, cluster: str, region: str):
        super().__init__(cluster=cluster, region=region)

    def __call__(self, *, cpu: int | None = None, memory=None, gpu=None, image=None) -> Placement:
        return Placement(
            self.kind,
            dict(self.config),
            self._check(
                {"cpu": cpu, "memory": memory, "gpu": gpu, "image": image},
                {"cpu", "memory", "gpu", "image"},
            ),
        )


class K8sJob(Environment):
    kind = "K8sJob"

    def __init__(self, *, cluster: str, namespace: str = "default"):
        super().__init__(cluster=cluster, namespace=namespace)

    def __call__(self, *, cpu: int | None = None, memory=None, image=None) -> Placement:
        return Placement(
            self.kind,
            dict(self.config),
            self._check({"cpu": cpu, "memory": memory, "image": image}, {"cpu", "memory", "image"}),
        )


class Modal(Environment):
    kind = "Modal"

    def __init__(self, *, app: str):
        super().__init__(app=app)

    def __call__(self, *, gpu=None) -> Placement:
        return Placement(self.kind, dict(self.config), self._check({"gpu": gpu}, {"gpu"}))


class Pool(Environment):
    """Pull path: external workers claim stages through the API (§10)."""

    kind = "Pool"

    def __init__(self, name: str):
        super().__init__(name=name)

    def __call__(self, *, cpu: int | None = None, memory=None, gpu=None) -> Placement:
        return Placement(
            self.kind,
            dict(self.config),
            self._check({"cpu": cpu, "memory": memory, "gpu": gpu}, {"cpu", "memory", "gpu"}),
        )


BUILTIN_KINDS = {k.kind: k for k in (Local, AWSECS, K8sJob, Modal, Pool)}
