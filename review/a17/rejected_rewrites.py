from reproduce import *

async def main():
    io=ObjectIO(MemoryStore())
    state=IndexState(prefix='keys/x/_/',life='1')
    for c,ks in enumerate(([f'b{i:04}'.encode() for i in range(1000)], [f'x{i:04}'.encode() for i in range(100)], [f'y{i:04}'.encode() for i in range(100)])):
        d,_=await KeyIndex(io,None,state).resolve(SortedEntries.of(ks),commit_number=c,attempt=str(c),generation=c+1)
        state=state.committed(c,d)
    merged=await KeyIndex(io,None,state).merge((1,2),{2})
    state=state.merged(merged.inputs,merged.span)
    print('tail segment counts, no keys duplicated:',state.spans[1].counts)
    for c in range(3,7):
        k=KeyIndex(io,None,state)
        plan=k.plan_merge(set())
        before=io.metrics.puts
        result=await k.merge(plan,set())
        print('same tail rewrite before commit',c,'plan=',plan,'PUTs=',io.metrics.puts-before,'published=',result is not None)
        d,_=await k.resolve(SortedEntries.of([f'z{c}'.encode()]),commit_number=c,attempt=str(c),generation=c+1)
        state=state.committed(c,d)

asyncio.run(main())
