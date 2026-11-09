#!/bin/bash
# T44 ranges: our format with range records and run-length columns, against plain ours.
#   bench/layer-format/ranges.sh     # 10M, every workload, every variant (~5 min)
#   SCALES=100000000 WORKLOADS='rewrite daily-scattered' FMTS=ours,ours+ranges THREADS=2 bench/layer-format/ranges.sh
#                                    # 100M: two variants at a time fit in 16 GB
# Results land in $RESULTS (results/) as ranges-<workload>-<scale>.jsonl;
# `python3 ranges_report.py results/ranges-*.jsonl` makes the tables.
# Runs one at a time, without the systemd scope of run.sh (a 4-core, 16 GB machine).
set -e
cd "$(dirname "$0")"
source ./configs.sh
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/layer-format-target}
cargo build --release -q
RESULTS=${RESULTS:-results}
mkdir -p "$RESULTS"
FMTS=${FMTS:-ours,ours+ranges,ours+rle,ours+ranges+rle}
run() {
  local name=$1; shift
  /usr/bin/time -v "$CARGO_TARGET_DIR/release/layer-format" "$@" fmts=$FMTS $OURS $COMMON threads=${THREADS:-4} \
    tmp="${TMPDIR:-/tmp}/layer-format/$name" > "$RESULTS/$name.jsonl" 2> "$RESULTS/$name.log"
  echo "$name: $(grep -c '"ok":true' "$RESULTS/$name.jsonl") checked, $(grep -c '"ok":false' "$RESULTS/$name.jsonl") mismatched"
}
for keys in ${SCALES:-10000000}; do
  for w in ${WORKLOADS:-daily-scattered daily-clustered hot rewrite}; do
    run ranges-$w-$((keys / 1000000))m workload=$w keys=$keys
  done
done
