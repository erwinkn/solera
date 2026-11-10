"""Run the matrix one configuration at a time, each in a fresh process, appending JSON lines."""

import itertools
import subprocess
import sys

OUT = sys.argv[1]
REPEATS = 3

configs = []
for shape, store, n, v in itertools.product(["q1", "q2", "q3"], ["file", "s3", "s3lat"], [1, 50], ["A", "B"]):
    configs.append([v, store, shape, str(n)])
for shape in ["q2", "q3"]:
    configs.append(["Athread", "s3lat", shape, "50"])
for store, v in itertools.product(["file", "s3", "s3lat"], ["A", "B"]):
    configs.append([v, store, "q3", "50", "--decode-us", "1000"])
for store, v in itertools.product(["s3", "s3lat"], ["A", "Acache", "B"]):
    configs.append([v, store, "sf", "50"])

with open(OUT, "a") as f:
    for rep in range(REPEATS):
        for c in configs:
            args = [sys.executable, "driver.py", *c, "--seed", str(rep)]
            r = subprocess.run(args, capture_output=True, text=True, timeout=600)
            if r.returncode != 0:
                print("FAILED", c, r.stderr[-2000:], flush=True)
                continue
            f.write(r.stdout)
            f.flush()
            print(rep, " ".join(c), r.stdout.strip()[:160], flush=True)
