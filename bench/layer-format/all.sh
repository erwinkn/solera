#!/bin/bash
# T44: every number in results.md, from one script.
#   bench/layer-format/all.sh            # ~1.5 h on the 96-core box, at most 2 runs at once
# Results land in bench/layer-format/results/; then `python3 report.py results/final-*.jsonl`.
set -e
cd "$(dirname "$0")"
source ./configs.sh
export CPU=1200 MEM=110G  # each run's scope: 12 cores, 110 GB (a 100M run peaks near 40 GB)
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
# The variants at 100M (daily-scattered) and the Parquet reader's ablations at 10M.
run variant-pq-rg16k-100m workload=daily-scattered keys=100000000 $OURS rg_rows=16384 page_bytes=32768 page_rows=4096 unit_pages=4 level=1 $COMMON threads=8 merges=0 &
run variant-untuned-100m workload=daily-scattered keys=100000000 $OURS $PARQUET coalesce=1048576 one_get=1000000000000 unit_pages=1000000 threads=8 merges=0 &
wait
for ab in "units unit_pages=1000000" "batch pq_batch=rowgroup" "coalesce256k coalesce=262144" "coalesce1m coalesce=1048576"; do
  set -- $ab
  run ablation-$1-10m workload=daily-scattered keys=10000000 $OURS $PARQUET $COMMON $2 threads=4 merges=0
done
python3 headline.py results/final-*-100m.jsonl > /dev/null && python3 report.py results/final-*.jsonl > results-tables.md
