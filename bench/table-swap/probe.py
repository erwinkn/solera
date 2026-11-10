from tests.tracking import fullrun, oracle
from tests.tracking.histories import CALIBRATIONS
for key, ev in CALIBRATIONS.items():
    if key[2] == "table" and key[3] in fullrun.ADDED["clear"][4:]:
        r = fullrun.run(ev, "clear", "table", frozenset({key[3]}))
        print(key[3], "->", r and r[1])
K4 = (("k1", True), ("k2", True), ("k3", True), ("k4", True))
w = oracle.replay_fullrun([("items", K4), ("step", "copy", 4), ("switch", "copy", True), ("step", "archive", 4),
    ("items", (("k3", False), ("k5", True))), ("full", "copy"), ("step", "copy", 2)], kind="table")
o = w.copy.output
v = lambda rows: {k: r.version for k, r in sorted(rows.items())}
print("mid: solera", v(o.rows), "outside", v(o.outside()), "held", w.archive.held())
w.apply(("step", "copy", 2)); print("done: solera", v(o.rows), "outside", v(o.outside()), w.owed)
w.apply(("switch", "copy", True)); print("switched: outside", v(o.outside()), "hidden", o.sides[1 - o.live], w.check())
