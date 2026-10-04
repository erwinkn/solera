"""Read-only probes against A17 Python plus an existing pre-net native build.
Run with the existing thr_9kc32wdrrt .venv Python, PYTHONDONTWRITEBYTECODE=1.
"""
import importlib.util
import sys
from pathlib import Path
ROOT = Path('/home/exedev/.bb/plugins/environment-git-worktree/host-data/worktrees/thr_di2vgmzppa-1/data-orchestrator')
NATIVE = Path('/home/exedev/.bb/plugins/environment-git-worktree/host-data/worktrees/thr_9kc32wdrrt-1/data-orchestrator/python/solera/_native.abi3.so')
sys.path.insert(0, str(ROOT / 'python'))
sys.path.insert(1, str(ROOT))
spec = importlib.util.spec_from_file_location('solera._native', NATIVE)
native = importlib.util.module_from_spec(spec)
sys.modules['solera._native'] = native
spec.loader.exec_module(native)
import asyncio
from dataclasses import replace
from types import SimpleNamespace
from obstore.store import MemoryStore
from solera.keys.index import IndexState, KeyIndex, Options, Span, FileInfo
from solera.keys import SortedEntries, parse_tail, parse_footer, FOOTER_SIZE
from solera.keys.io import ObjectIO
from solera_server.upkeep import Upkeep
from solera_server.positions import reads

async def built(gens, options, endpoints):
    io, state = ObjectIO(MemoryStore()), IndexState(prefix='keys/x/_/', life='1')
    for c, g in enumerate(gens):
        k = KeyIndex(io, None, state, options)
        delta, _ = await k.resolve(SortedEntries.of([b'k']), commit_number=c, attempt=str(c), generation=g)
        state = state.committed(c, delta)
    k = KeyIndex(io, None, state, options)
    merged = await k.merge((0, len(gens)), endpoints)
    return io, state.merged(merged.inputs, merged.span)

async def pages():
    io, state = await built(range(1, 21), Options(block_size=16), set(range(1, 20)))
    k = KeyIndex(io, None, state)
    f = state.files[0]
    raw = await io.read_whole(state.path(f.name), f.size)
    tail = parse_tail(raw[-f.tail:], len(raw))
    print('pagination: versions=20, blocks=', len(tail['blocks']), 'lookup=', await k.lookup([b'k']))
    print('pagination page(limit=1):', await k.page(None, 1))
    print('pagination changes_page(1,19,limit=1):', await k.changes_page(1,19,None,1))
    print('pagination changes(keys=k):', [(p.keys,list(p.classes),p.generations) async for p in k.changes(1,19,keys=[b'k'])])
    # With an earlier cursor, it fails to advance instead of ending.
    print('pagination page(after=j,limit=1):', await k.page(b'j',1))

async def sentinel():
    gmax = 2**64-1
    io, state = await built([gmax-1, gmax], Options(), {1})
    k = KeyIndex(io,None,state)
    print('u64 endpoint lookup at=1 expected',gmax-1, 'actual', await k.lookup([b'k'],at=1))
    print('u64 endpoint page at=1:', await k.page(None,10,at=1))
    print('u64 changes 0..0:',await k.changes_page(0,0,None,10))

async def main():
    await pages()
    await sentinel()
    from solera_server.engine import Engine
    from solera_server.model import Model
    engine = Engine.__new__(Engine)
    engine.state = SimpleNamespace(model=Model())
    engine.m.heads[('x','')] = {'commit_number':1}
    engine.read_ahead_cap = 100
    position = {'output':'x','upstream_partition':'','next':2,'fingerprint':'f','patterns':['old/*']}
    _, plan, _ = engine._selection({'asset':'consumer'}, {'output':'x','patterns':['new/*']}, {}, '', 'f', {'id':'r'}, position, {'keys':['new/k']}, None, 1, None)
    print('actual pattern-selection plan=', plan)
    try:
        reads({'x':plan})
    except Exception as e:
        print('pattern-selection reads:',type(e).__name__,str(e))

if __name__ == '__main__':
    asyncio.run(main())
