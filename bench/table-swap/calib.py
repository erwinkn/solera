import random, sys
from tests.tracking import fullrun
from tests.tracking.__main__ import shrink, trace
rules = sys.argv[1].split(",")
n = int(sys.argv[2]) if len(sys.argv) > 2 else 1500
for rule in rules:
    off = frozenset({rule})
    first, failed = None, 0
    for s in range(n):
        ev = fullrun.history(random.Random(s), 60)
        if fullrun.run(ev, "clear", "table", off):
            failed += 1
            first = first or ev
    print(f"== {rule}: {failed} of {n} fail", flush=True)
    if first:
        ev = shrink("fullrun", first, "clear", "table", off)
        print(f"    ('fullrun', 'clear', 'table', {rule!r}): {ev!r},")
        print("    " + trace("fullrun", ev, "clear", "table", off).replace("\n", "\n    "), flush=True)
