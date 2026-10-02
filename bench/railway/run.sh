#!/bin/sh
# The probe service's entrypoint (bench/railway): capability checks, then if
# create-if-absent holds, the S3-gated suites against the bucket. MODE=bench
# runs the benchmarks instead (bench.sh). Credentials come from the bucket's
# reference variables (ENDPOINT, BUCKET, REGION, ACCESS_KEY_ID,
# SECRET_ACCESS_KEY: ${{bucket.…}}) and are never printed. To deploy it, the
# root railway.toml on the deploying branch names bench/railway/Dockerfile
# and `sh bench/railway/run.sh` — Railway takes no other config path.
set -u
cd /home/app
echo "== probe =="
python bench/railway/probe.py
ok=$?
if [ "${MODE:-probe}" = "bench" ]; then
  exec sh bench/railway/bench.sh
fi
if [ $ok -eq 0 ]; then
  export SOLERA_TEST_S3="$(python bench/railway/url.py)"
  echo "== store conformance, journal fencing, S3-gated tests =="
  python -m pytest -q -rs -p no:cacheprovider tests/sdk/test_store_conformance.py tests/server/test_journal.py \
    tests/sdk/test_filestore.py \
    "tests/worker/test_worker.py::test_a_local_worker_reaches_state_on_a_private_object_store" 2>&1 | tail -80
else
  echo "create-if-absent does not hold: stopping"
fi
echo "== done =="
sleep 86400
