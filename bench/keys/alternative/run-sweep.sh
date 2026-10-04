#!/usr/bin/env bash
set -euo pipefail
binary=${1:-/tmp/solera-a20-snapshot}
for page in 16 64 256 1024; do
  "$binary" 1000000 "$page" 1000 1000 uniform
 done
for batch in 16 100000; do
  "$binary" 1000000 256 256 "$batch" uniform
 done
"$binary" 1000000 256 1000 1000 hot
"$binary" 1000000 64 12000 1000 history
