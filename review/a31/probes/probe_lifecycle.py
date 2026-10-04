"""Lifecycle probes beyond test_layers.py:
A. a merge that crashes between upload and publication: the attempt stays
   counted, the uploaded files are orphans, a successor epoch collects them
   and the retry succeeds.
B. the window moves the cut past an unfolded reader: the prototype's set_cut
   takes max(oldest P, head - W) and the reader fails with CutError (the
   design says the engine must fold first; nothing in the index enforces the order).
C. attempts/stopped keys ignore the life (ids repeat across lives).
D. the pin seq rule: garbage replaced at the pin's own seq is deletable."""
from common import *

async def a_crash():
    io = MemIO(); ix = L.Layers(io, "t/", epoch=1)
    await load(ix, [key(i) for i in range(50)])
    for c in range(1, 5):
        await ix.commit(c, c + 1, [key(200 + c)], [])
    n_before = len(io.objs)
    real_write = io.write
    calls = {"n": 0}
    async def crashy(path, data):
        calls["n"] += 1
        await real_write(path, data)
        if calls["n"] == 1:
            raise RuntimeError("crash after the first upload")
    io.write = crashy
    try:
        await ix.merge(1, 4)
    except RuntimeError:
        pass
    io.write = real_write
    k = ix._key(ix.s.layers[1:5])
    orphan = [p for p in io.objs if p not in {q for x in ix.s.layers for q in x.paths()}]
    succ = L.Layers(io, "t/", epoch=2, state=L.State.from_json(ix.s.to_json()))
    dead = await succ.collect_orphans(list(io.objs))
    out = await succ.merge(1, 4)
    print(f"A crash between upload and publication: attempts[{k}]={ix.s.attempts.get(k)} before restart; orphans {len(orphan)}, collected by epoch 2: {len(dead)}; "
          f"retry published: {out is not None}, attempts now {succ.s.attempts.get(k)} (cleared on publication)")

async def b_window():
    io = MemIO(); ix = L.Layers(io, "t/", window=10)
    await load(ix, [key(i) for i in range(50)])
    for c in range(1, 31):
        await ix.commit(c, c + 1, [key(200 + c)], [])
    await ix.upkeep(oldest_p=5)   # a live reader at 5, not yet folded
    try:
        await L.Reader(io, ix.s).page(5, None, 10)
        print("B window: reader at 5 served (unexpected)")
    except L.CutError as e:
        print(f"B window: the cut moved to {ix.s.cut} over a live P = 5 -> {e!r}")

def c_life():
    io = MemIO(); ix = L.Layers(io, "t/")
    k = ix._key([L.Layer("D1", 1, 1, L.Part([])), L.Layer("D2", 2, 2, L.Part([]))])
    print(f"C attempt key for inputs D1,D2 = {k!r}: no life in it (state.life={ix.s.life!r}); a reset that keeps the State's attempts/stopped would carry old-life failures into the new life's same ids")

async def d_pin():
    io = MemIO(); ix = L.Layers(io, "t/")
    await load(ix, [key(i) for i in range(50)])
    for c in range(1, 5):
        await ix.commit(c, c + 1, [key(200 + c)], [])
    await ix.merge(1, 2)               # seq 1: D1, D2 replaced
    pinned = ix.pin("b")               # pin at seq 1: its manifest does not name D1, D2
    await ix.merge(1, 2)               # seq 2: L1-2, D3 replaced; the pin names them
    dead = ix.deletable()
    names = [p.rsplit("/", 1)[1][:6] for p in dead]
    print(f"D pin at seq {ix.s.pins['b']}: deletable = garbage replaced at seq <= pin: {sorted(set(names))}; garbage at seq 2 kept: {all(s <= 1 for p, s in ix.s.garbage if p in dead)}")

if __name__ == "__main__":
    run(a_crash()); run(b_window()); c_life(); run(d_pin())
