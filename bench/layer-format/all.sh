#!/bin/bash
# T44: every number in results.md, from one script.
#   bench/layer-format/all.sh      # ~2 h on the 96-core machine, at most 2 runs at once
# Results land in $RESULTS (results/); `python3 report.py`, `headline.py`, `variants.py`
# and `ablations.py` make the tables. SCALES, VKEYS and AKEYS shrink it for a smoke run:
#   RESULTS=/tmp/lf-smoke SCALES=1000000 VKEYS=1000000 AKEYS=1000000 bench/layer-format/all.sh
set -e
cd "$(dirname "$0")"
source ./configs.sh
export CPU=1200 MEM=110G  # each run's scope: 12 cores, 110 GB (a 100M run peaks near 40 GB)
export RESULTS=${RESULTS:-results}
SCALES=${SCALES:-"10000000 100000000"}
VKEYS=${VKEYS:-100000000}  # the variants' scale
AKEYS=${AKEYS:-10000000}   # the ablations' and DuckDB's scale
tag() { echo $(($1 / 1000000))m; }
run() { ./run.sh "$@"; }
for keys in $SCALES; do
  for pair in "daily-scattered daily-clustered" "hot rewrite"; do
    for w in $pair; do run final-$w-$(tag $keys) workload=$w keys=$keys $OURS $PARQUET $COMMON threads=8 & done
    wait
  done
done
# Parquet's row groups, and a request-frugal read policy, at 100M (daily-scattered).
run variant-pq-rg16k-$(tag $VKEYS) workload=daily-scattered keys=$VKEYS $OURS rg_rows=16384 page_bytes=32768 page_rows=4096 unit_pages=4 level=1 $COMMON threads=8 merges=0 &
run variant-untuned-$(tag $VKEYS) workload=daily-scattered keys=$VKEYS $OURS $PARQUET coalesce=1048576 one_get=1000000000000 unit_pages=1000000 threads=8 merges=0 &
wait
# The Parquet reader's choices, each undone alone.
for ab in "units unit_pages=1000000" "batch pq_batch=rowgroup" "coalesce256k coalesce=262144" "coalesce1m coalesce=1048576"; do
  set -- $ab
  run ablation-$1-$(tag $AKEYS) workload=daily-scattered keys=$AKEYS $OURS $PARQUET $COMMON $2 threads=4 merges=0
done
# DuckDB over each format.
dump=${TMPDIR:-/tmp}/layer-format/dump-$(tag $AKEYS)
run dump-$(tag $AKEYS) workload=daily-scattered keys=$AKEYS $OURS $PARQUET $COMMON queries=none merges=0 threads=4 dump=$dump
(cd ../.. && uv run python bench/layer-format/duckdb_read.py "$dump") > $RESULTS/duckdb-$(tag $AKEYS).jsonl
# Parquet's dependency cost in the worker's native module.
./depcost.sh > $RESULTS/depcost.txt
python3 report.py $RESULTS/final-*.jsonl > $RESULTS/tables.md
