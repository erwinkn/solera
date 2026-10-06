#!/bin/bash
# T44: build the bench and run one configuration, confined to a systemd scope
# (CPU and memory capped, niced) so other work on the machine keeps its share.
#   run.sh NAME ARGS...   ->  results/NAME.jsonl (one JSON line per measurement), results/NAME.log
# Example: run.sh daily-10m workload=daily-scattered keys=10000000
set -e
cd "$(dirname "$0")"
source ~/.cargo/env 2>/dev/null || true
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/layer-format-target}
cargo build --release -q
name=$1; shift
mkdir -p results "${TMPDIR:-/tmp}/layer-format"
systemd-run --user --scope -q --unit="lf-$name-$$" -p CPUQuota=${CPU:-800}% -p MemoryMax=${MEM:-96G} \
  nice -n 10 "$CARGO_TARGET_DIR/release/layer-format" "$@" tmp="${TMPDIR:-/tmp}/layer-format/$name" \
  > "results/$name.jsonl" 2> "results/$name.log"
echo "$name: $(grep -c '"ok":true' results/$name.jsonl) checked, $(grep -c '"ok":false' results/$name.jsonl) mismatched"
