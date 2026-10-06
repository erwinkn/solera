#!/bin/bash
# T44: every number in results.md, from one script.
#   bench/layer-format/all.sh            # ~1.5 h on the 96-core box, at most 2 runs at once
# Results land in bench/layer-format/results/; then `python3 report.py results/final-*.jsonl`.
set -e
cd "$(dirname "$0")"
source ./configs.sh
run() { ./run.sh "$@"; }
for keys in 10000000 100000000; do
  tag=$((keys / 1000000))m
  for w in daily-scattered daily-clustered; do
    run final-$w-$tag workload=$w keys=$keys $OURS $PARQUET $COMMON threads=8 &
  done
  wait
  for w in hot rewrite; do
    run final-$w-$tag workload=$w keys=$keys $OURS $PARQUET $COMMON threads=8 &
  done
  wait
done
# DuckDB over each format, from the 10M daily layers.
dump=${TMPDIR:-/tmp}/layer-format/dump-10m
run dump-10m workload=daily-scattered keys=10000000 $OURS $PARQUET $COMMON queries=none merges=0 dump=$dump
(cd ../.. && uv run python bench/layer-format/duckdb_read.py "$dump") > results/duckdb-10m.jsonl
# Parquet's dependency cost in the worker's native module.
./depcost.sh > results/depcost.txt
