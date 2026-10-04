from reproduce import *
from solera_server.state import State
from solera_server.model import Model
from solera_server.journal import Journal
from solera.keys.cache import EngineCache
from solera.sdk import Project, Source
from solera_server.engine import Engine
import tempfile

async def local_files():
    io, state = await built(range(1,21), Options(block_size=16,max_file_bytes=48),set(range(1,20)))
    with tempfile.TemporaryDirectory(prefix='a17-local-') as root:
        cache=EngineCache(root)
        await cache.fill(io,state)
        with cache.open(state) as local:
            print('local duplicate file range: files=',len(state.files),'cold lookup=', await KeyIndex(io,None,state).lookup([b'k']), 'local=',native.Snapshot(local.runs).get([b'k']))
            files,*_ = native.Snapshot(local.runs).resolve(SortedEntries.of([b'k']),replace=False,generation=21)
            from tests.sdk.keys_reference import iter_file
            print('cached write predecessor, expected 20:',list(iter_file(files[0])))

async def open_state(store):
    model = Model()
    journal = Journal(store,flush_interval=3600,min_checkpoint=10**9,clock=lambda:0)
    await journal.open(model.restore,model.apply,lambda:model.snapshot(copied=False))
    return State(store,url='memory:///',namespace='default',objects_url='memory:///',journal=journal,model=model,clock=lambda:0)

async def takeover():
    objects=MemoryStore()
    a=await open_state(objects)
    for c in range(2):
        idx=a.model.index('x','')
        delta,_=await KeyIndex(ObjectIO(objects),None,idx).resolve(SortedEntries.of([b'k']),commit_number=c,attempt=str(c),generation=c+1)
        a.record({'type':'SourceCommitted','source':'x','at':0,'head':{'ref':{'generation':c+1},'commit_number':c},'keys':{'commit_number':c,**delta.to_json()}})
    await a.durable()
    listed,resume=asyncio.Event(),asyncio.Event()
    original=a.list_objects
    async def stalled(prefix):
        listed.set()
        await resume.wait()
        return await original(prefix)
    a.list_objects=stalled
    collector=Upkeep(a,None,{},clock=lambda:0)
    task=asyncio.create_task(collector.collect_orphans())
    await listed.wait()
    b=await open_state(objects)  # CAS-fences a, but a has no pending journal writes to discover it.
    idx=b.model.index('x','')
    merged=await KeyIndex(ObjectIO(objects),None,idx).merge((0,2),set())
    b.record({'type':'IndexMerged','output':'x','partition':'','life':idx.life,'prefix':idx.prefix,**merged.to_json(),'at':0})
    await b.durable()
    path=b.model.index('x','').path(merged.span.files[0].name)
    print('takeover before old collector resumes: current merge object present=',await b.get_object(path) is not None,'old detected fence=',a.poisoned)
    resume.set()
    await task
    print('takeover after old collector: live object present=',await b.get_object(path) is not None,'still named=',merged.span.files[0].name in b.model.index('x','').referenced())
    await a.journal.close(checkpoint=False)
    await b.journal.close(checkpoint=False)

async def sources():
    state=await State.open('memory:///')
    engine=Engine(state,Project(sources=[Source('feed',key='id')]).manifest,resolve_cache=False)
    try:
        await engine.initialize()
        for c in range(70):
            await engine.commit_source('feed',upsert=['k'])
        print('source commits accepted with no upkeep:',len(state.model.index('feed','').spans),'configured backpressure threshold=',2*engine.key_options.fan_in)
    finally:
        await engine.stop()
        await state.close()

async def main():
    await local_files()
    await takeover()
    await sources()
    print('retry covering landing endpoint reads=',reads({'x':{'kind':'held','position':{'output':'x','upstream_partition':'','next':1,'ahead':[[2,'r','a']]},'head':2,'each':{'kind':'retry'}}}))

asyncio.run(main())
