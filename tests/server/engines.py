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


def attempt_channel(engine, attempt):
    """A worker's channel to `engine`'s routes in this process, with the attempt's token."""

    from solera import lifecycle
    from solera_server.api import local_transport
    from solera_worker.channel import AttemptChannel

    transport = local_transport(engine, lifecycle.token(engine.secret, attempt))
    return AttemptChannel(transport, engine.manifest["name"], attempt)


def sensor_channel(engine):
    """The engine's own sensor host's channel, in this process, with its token."""

    from solera import lifecycle
    from solera_server.api import local_transport
    from solera_server.sensors import HOST_TOKEN
    from solera_worker.channel import SensorChannel

    return SensorChannel(
        local_transport(engine, lifecycle.token(engine.secret, HOST_TOKEN)), engine.manifest["name"]
    )
