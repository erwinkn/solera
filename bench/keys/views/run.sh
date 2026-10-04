#!/bin/sh
# One viewbench run, niced and capped at 2 threads, logged under runs/.
# Usage: run.sh NAME ARGS...
cd "$(dirname "$0")/../../.."
name=$1; shift
RAYON_NUM_THREADS=2 nice -n 10 .venv/bin/python bench/keys/views/viewbench.py "$@" > "bench/keys/views/runs/$name.log" 2>&1
echo "exit $?" >> "bench/keys/views/runs/$name.log"
