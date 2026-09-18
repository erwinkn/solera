import inspect

import pytest
from data_orchestrator import AssetContext
from data_orchestrator.sdk import normalize_result
from dorc.engine import Engine
from dorc.storage import SlateState


class InlineBackend:
    """Test transformations without a process boundary; storage is always real SlateDB."""

    def __init__(self, project):
        self.project = project
        self.calls = []
        self.fail_when = None

    async def execute(self, spec):
        self.calls.append(spec)
        if self.fail_when and self.fail_when(spec):
            raise RuntimeError("Injected transform failure")
        producer = self.project.producers[spec["producer"]]
        args = dict(spec["inputs"])
        ctx = None
        if "ctx" in inspect.signature(producer.fn).parameters:
            ctx = AssetContext(**spec["context"])
            args["ctx"] = ctx
        args.update(
            {
                k: v
                for k, v in self.project.resources.items()
                if k in inspect.signature(producer.fn).parameters
            }
        )
        value = producer.fn(**args)
        if inspect.isawaitable(value):
            value = await value
        payload = normalize_result(value, list(producer.outputs))
        if ctx is not None:
            payload["log_entries"] = ctx._records
        return payload, "test output"


@pytest.fixture
async def state(tmp_path):
    store = await SlateState.open(tmp_path.as_uri(), flush_interval="1ms")
    yield store
    await store.close()


@pytest.fixture
def make_engine(state):
    async def make(project, **kwargs):
        backend = InlineBackend(project)
        engine = Engine(state, project.manifest, backend, retry_delay=0, **kwargs)
        await engine.initialize()
        return engine

    return make


async def finish(engine, run):
    for _ in range(100):
        await engine.execute_next()
        detail = await engine.run_detail(run["id"])
        if detail["request"]["status"] in {"succeeded", "failed", "canceled"}:
            return detail
    raise AssertionError("Run did not terminate")
