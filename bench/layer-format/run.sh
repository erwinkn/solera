#!/bin/bash
# T44: build the bench and run one configuration, confined to a systemd scope
# (CPU and memory capped, niced) so other work on the machine keeps its share.
#   run.sh NAME ARGS...   ->  $RESULTS/NAME.jsonl (one JSON line per measurement), $RESULTS/NAME.log
#   ($RESULTS: results/ by default)
# Example: run.sh daily-10m workload=daily-scattered keys=10000000
set -e
cd "$(dirname "$0")"
source ~/.cargo/env 2>/dev/null || true
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/layer-format-target}
cargo build --release -q
name=$1; shift
out=${RESULTS:-results}
mkdir -p "$out" "${TMPDIR:-/tmp}/layer-format"
systemd-run --user --scope -q --unit="lf-$name-$$" -p CPUQuota=${CPU:-800}% -p MemoryMax=${MEM:-96G} \
  nice -n 10 "$CARGO_TARGET_DIR/release/layer-format" "$@" tmp="${TMPDIR:-/tmp}/layer-format/$name" \
  > "$out/$name.jsonl" 2> "$out/$name.log"
echo "$name: $(grep -c '"ok":true' "$out/$name.jsonl") checked, $(grep -c '"ok":false' "$out/$name.jsonl") mismatched"
