#!/bin/sh
# The timing pass: every build's cold readers again, one build at a time,
# nothing else of ours running (timings from the builds overlapped an
# overloaded machine). Results: runs-final/<build dir name>.json.
cd "$(dirname "$0")/../../.."
mkdir -p bench/keys/views/runs-final
for d in /tmp/viewbench/*/; do
  name=$(basename "$d")
  [ -f "$d/built.json" ] || continue
  [ -f "bench/keys/views/runs-final/$name.json" ] && continue
  echo "$(date +%T) $name $(uptime | sed 's/.*averages: //')"
  SCANS_ALL=1 RAYON_NUM_THREADS=2 .venv/bin/python bench/keys/views/viewbench.py --reads "$d" | tail -1 > "bench/keys/views/runs-final/$name.json"
done
