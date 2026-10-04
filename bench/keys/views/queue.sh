#!/bin/sh
# Run queue.txt's lines through run.sh, at most 3 viewbench processes at once.
cd "$(dirname "$0")"
while IFS= read -r line; do
  [ -z "$line" ] && continue
  name=${line%% *}; args=${line#* }
  [ -f "runs/$name.log" ] && continue
  while [ "$(pgrep -f 'viewbench.py --index' | wc -l)" -ge 3 ]; do sleep 30; done
  ./run.sh "$name" $args &
  sleep 5
done < queue.txt
wait
