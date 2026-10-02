#!/bin/sh
# The benchmark run (bench/railway, MODE=bench): the network, then the key
# index benches against the bucket with nothing injected. SIZES picks them.
set -u
cd /home/app
URL="$(python bench/railway/url.py)"
echo "== network =="
python bench/railway/net.py
for n in $(echo "${SIZES:-1e6,1e7}" | tr ',' ' '); do
  echo "== bench.py $n =="
  python bench/keys/bench.py --s3 "$URL" --prefix "bench-$n/" --latency 0 --bandwidth 0 --sizes "$n" 2>&1 | grep -v "^{" | tail -80
  echo "== warm.py $n: resolves, engine-served reads, recount =="
  python bench/keys/warm.py --s3 "$URL" --prefix "warm-$n/" --latency 0 --bandwidth 0 --sizes "$n" --reads --recount 2>&1 | grep -v "^{" | tail -40
done
echo "== done =="
sleep 86400
