#!/bin/bash
# Build the change-index bench and run one configuration.
#   run.sh NAME ARGS...   ->  $RESULTS/NAME.jsonl (one JSON line per measurement), $RESULTS/NAME.log
# Example: run.sh daily-scattered-10m workload=daily-scattered keys=10000000
# Layer files go to $DATA/NAME (default /tmp/change-index) and are removed after.
set -e
cd "$(dirname "$0")"
cargo build --release -q
name=$1; shift
out=${RESULTS:-results}
data=${DATA:-/tmp/change-index}/$name
mkdir -p "$out"
./target/release/change-index-bench "$@" dir="$data" > "$out/$name.jsonl" 2> "$out/$name.log"
rm -rf "$data"
echo "$name: $(grep -c '"ok":true' "$out/$name.jsonl") checked, $(grep -c '"ok":false' "$out/$name.jsonl") mismatched"
