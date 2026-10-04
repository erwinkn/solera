"""Shared harness for the A31 probes: the exact df9ac17 `bench/keys/fp/layers.py`
over an in-memory ObjectIO, with the small sizes of `test_layers.py`."""
import asyncio, random, sys
sys.path.insert(0, "/tmp/a31/src/bench/keys/fp")
import layers as L  # noqa: E402
L.BLOCK, L.SMALL, L.B0, L.Z, L.FILE_LIMIT = 128, 512, 64, 256, 2048

class MemIO:
    """ObjectIO's four calls over a dict, counting requests and bytes."""
    def __init__(self):
        self.objs: dict[str, bytes] = {}
        self.gets = 0
        self.bytes = 0
    async def write(self, path, data):
        self.objs[path] = bytes(data)
    async def read_whole(self, path, size=None):
        self.gets += 1
        d = self.objs[path]
        self.bytes += len(d)
        return d
    async def read(self, path, start, end, size=None):
        self.gets += 1
        d = self.objs[path][start:end]
        self.bytes += len(d)
        return d
    async def delete(self, paths):
        for p in paths:
            self.objs.pop(p, None)
    def head(self, path):
        return path in self.objs

def key(i): return b"k%04d" % i

async def load(ix, keys, gen=1):
    import pyarrow as pa
    await ix.load_base([pa.array(sorted(keys), pa.binary())], gen)

def run(c): return asyncio.run(c)
