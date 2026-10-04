import pytest
from solera_server.state import State


@pytest.fixture
async def state(tmp_path):
    opened = await State.open(tmp_path.as_uri(), "test", flush_interval=0.001)
    yield opened
    await opened.close()
