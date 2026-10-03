"""Making and driving engines in tests: inline projects on the real
file:// object store."""

from solera_server.engine import Engine
from solera_server.executors.inline import InlinePlacement


def inline(project):
    return {"Local": lambda s, c: InlinePlacement(c, project)}


def make_engine(state, project, placements=None, **kw):
    kw.setdefault("eval_interval", 0.05)
    kw.setdefault("clock", state.clock)
    return Engine(state, project.manifest, placements=placements or inline(project), **kw)


async def drive(engine, run, timeout=30):
    return await engine.run_until(run["id"], timeout)


def status_of(detail):
    return detail["request"]["status"]


def task_statuses(detail):
    return {t["asset"]: t["status"] for t in detail["tasks"]}
