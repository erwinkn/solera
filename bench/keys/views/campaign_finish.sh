#!/bin/sh
# After the layers builds: the forced base merge at 100M, then the quiet
# timing pass (every build's cold readers, one build at a time, alone).
cd "$(dirname "$0")/../../.."
while pgrep -f queue-layers.sh >/dev/null || pgrep -f "viewbench.py --index" >/dev/null; do sleep 60; done
echo "$(date +%T) builds done"; grep -h "^exit" bench/keys/views/runs/*layers*.log
bench/keys/views/final_reads.sh
echo "$(date +%T) timing pass done"
