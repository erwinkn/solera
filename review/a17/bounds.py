from reproduce import *
from solera_server.model import Model

async def retry_endpoint():
    io=ObjectIO(MemoryStore())
    state=IndexState(prefix='keys/x/_/',life='1')
    for c in range(3):
        d,_=await KeyIndex(io,None,state).resolve(SortedEntries.of([b'k']),commit_number=c,attempt=str(c),generation=c+1)
        state=state.committed(c,d)
    position={'output':'x','upstream_partition':'','next':1,'ahead':[[2,'r','a']]}
    plan={'kind':'held','position':position,'head':2,'each':{'kind':'retry'}}
    m=Model()
    m.partitions[('consumer','')]={'positions':{'x':position}}
    m.claims['retry']={'reads':reads({'x':plan})}
    for c in range(3,5):
        d,_=await KeyIndex(io,None,state).resolve(SortedEntries.of([b'k']),commit_number=c,attempt=str(c),generation=c+1)
        state=state.committed(c,d)
    ends=m.endpoints('x','')
    merged=await KeyIndex(io,None,state).merge((1,4),ends)
    state=state.merged(merged.inputs,merged.span)
    print('retry landing: reserved=',sorted(ends),'new position=3','generation(3)=',state.generation(3),'covers(3,4)=',state.covers(3,4))

async def attempts():
    # Exercise production maintain and _merge, with only the merge I/O failing.
    # Same input set and life on every "restart".
    f=FileInfo('delta',b'k',b'k',1,100,20,10)
    index=IndexState(prefix='keys/x/_/',life='1',spans=(Span(0,0,((0,1),),(f,)),Span(1,1,((1,2),),(replace(f,name='delta2'),))))
    m=SimpleNamespace(indexes={('x',''):index},endpoints=lambda *args:set())
    state=SimpleNamespace(model=m,objects=None)
    original=KeyIndex.merge
    calls=[]
    async def failure(self,plan,endpoints):
        calls.append(plan)
        raise RuntimeError('injected failure after upload')
    KeyIndex.merge=failure
    import logging
    logging.disable(logging.CRITICAL)
    try:
        for restart in range(2):
            u=Upkeep(state,None,{},clock=lambda:0)
            for _ in range(5):
                u.maintain()
                await asyncio.gather(*u.jobs.values())
            print('restart',restart,'cumulative attempts=',len(calls),'stopped=',u.stopped,'alarm=',u.failing)
    finally:
        KeyIndex.merge=original
        logging.disable(logging.NOTSET)

asyncio.run(retry_endpoint())
asyncio.run(attempts())
