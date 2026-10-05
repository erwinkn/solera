# Railway validation

Use a dedicated project and a **private Storage Bucket**, not a Railway disk volume. A volume and an S3-compatible bucket have different APIs and failure semantics.

## Configuration

Deploy `erwinkn/solera` from `main`. `railway.toml` selects the `Dockerfile` builder: its first stage installs a Rust toolchain and runs `uv sync --locked --no-dev --no-editable`, which builds the `solera._native` extension; the image keeps only the resulting environment and starts `solera serve --host 0.0.0.0` (the CLI reads `$PORT`). The console bundle is committed under `python/solera_server/web`, so no Node runtime, database service, or persistent application volume is needed for S3 mode.

**Build identity.** The deploy includes a build identity
(`per-key-processing.md` §13). Without one, each host hashes the modules
the project runs as it imported them, and a host that imports another
copy of the code (a checkout against the image) computes another deploy.
The `Dockerfile` declares `ARG RAILWAY_GIT_COMMIT_SHA`, which Railway
fills at build time, and sets `SOLERA_BUILD` from it (an explicit
`--build-arg SOLERA_BUILD=…` wins). Workers and sensor workers that run
elsewhere must set the same `SOLERA_BUILD`; when one reports a deploy
computed by another method, the engine logs a warning saying so.

Set these environment variables with Railway's reference picker (verify the actual bucket reference names; do not invent them):

| App variable | Value |
|---|---|
| `SOLERA_STATE_URL` | `s3://<bucket BUCKET reference>/orchestrator` |
| `SOLERA_NAMESPACE` | `demo` |
| `SOLERA_DATA_URL` | `s3://<bucket BUCKET reference>/data`: the demo's outputs; without it they sit on the container's disk and a redeploy loses them |
| `AWS_ACCESS_KEY_ID` | Bucket `ACCESS_KEY_ID` reference |
| `AWS_SECRET_ACCESS_KEY` | Bucket `SECRET_ACCESS_KEY` reference |
| `AWS_REGION` | Bucket `REGION` reference |
| `AWS_ENDPOINT` | Bucket `ENDPOINT` reference; include `https://` |
| `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | `false` if the provider expects path-style requests |
| `AWS_CONDITIONAL_PUT` | `etag` |
| `SOLERA_API_TOKEN` | Strong generated secret, not a committed value |
| `SOLERA_SELFTEST` | `1` for validation; `0` after testing |

Keep exactly one replica. Overlapping deployment startup intentionally replaces the previous writer. Do not scale this coordinator horizontally or attach multiple write processes to the same namespace. Services should fail closed rather than run repeated competing takeovers.

`solera serve` runs `solera selftest` before starting the API when `SOLERA_SELFTEST=1`; a failed probe exits non-zero before anything listens, so `/healthz` never answers and the deployment fails (`tests/server/test_selftest.py`). It prints only synthetic test results, namespace, and elapsed time, never credentials. If the endpoint ignores preconditions, transactions fail, or the old writer can publish after takeover, startup fails. Health is exposed at `/healthz`; the UI is `/` and requires the API token for data/actions.

A second deployment runs the probe under a new namespace. For application persistence verification, materialize an asset in `demo`, redeploy with the same bucket/namespace and no local volume, then inspect the prior run and output in the console. Do not infer persistence merely from a green container healthcheck.

## Cleanup and caveats

The probe writes under new `probe-*` namespaces and retains them for inspection. Stop the probe and remove only its namespace prefixes to clean up. Removing the real workspace prefix destroys its durable state. Keep automations disabled on an idle demo to avoid unnecessary runs.

Real-provider results must be reported separately from the local filesystem and Moto emulator tests. A bucket creation action returning `staged` or `applying` is not proof that the bucket exists or its credentials resolve. The deployment must complete authenticated reads/writes against the provider before the experiment is described as validated.

## Attempted validation — September 11, 2026

The dedicated `data-orchestrator-s3-test` Railway project was created and the application built, but bucket provisioning did not complete: the environment returned no buckets and bucket references resolved without a bucket name. The application correctly failed before S3 tests. **No real Railway S3 validation or public demo is claimed.** Local filesystem and HTTP S3-emulator results are separate.

The failed `orchestrator-s3-test` service was deleted on September 26, 2026; no bucket was ever provisioned. Its removal had sat staged since this attempt: Railway requires two-factor verification to commit staged destructive changes, which API tokens cannot provide. Delete services directly through the API instead of staging their removal.

For a future new Railway bucket, inspect its actual addressing mode: new buckets may use virtual-hosted style, requiring `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true`. Do not assume path-style compatibility or substitute a filesystem deployment for a provider test.

## Validation — October 5, 2026

**Result: pass.** `solera selftest` passed against a real Railway Storage Bucket (Tigris-backed, `https://t3.storageapi.dev`, region `auto`, created in `sjc`). All four checks passed:

- create-if-absent writes are enforced (a second `If-None-Match: *` create is refused);
- key index files write and read back by range;
- manifest registration and control-plane initialization;
- state restores from the object store, and a new writer fences the old one (the old writer logs `journal stopped: another engine wrote the journal`).

The run took 3.7 s. The deployment then served normally (`solera serve` under uvicorn, sensor requests answered 200).

**How it was run.** The scratch project `solera-cas-probe-2026-10-05` held one bucket and one service built from `erwinkn/solera` `main` through `railway.toml`. The service got its variables as in the table above, with the credentials as Railway reference variables (`${{probe-bucket.ACCESS_KEY_ID}}`, `${{probe-bucket.SECRET_ACCESS_KEY}}`) and `SOLERA_SELFTEST=1`. The probe ran inside Railway: `solera serve` runs `solera selftest` before it starts the API.

**Lesson: run the probe inside Railway.** Bucket credentials are redacted for OAuth apps. `get_bucket_credentials` returns the endpoint, bucket name, region and URL style but `valuesRedacted: true`, with no access key or secret, so the probe can't run from outside with them. `reset_bucket_credentials` needs interactive user approval. The reference variables resolve at runtime on a service in the same project, so no secret leaves Railway.

**Addressing style.** The bucket reports `urlStyle: virtual-host`, so `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true` is required. With it, the engine's object store expects the bucket in the endpoint host, not as a path segment. With `AWS_ENDPOINT=https://t3.storageapi.dev` the first deployment's probe failed: the request went to `https://t3.storageapi.dev/<prefix>/...`, and Tigris read the first path segment as the bucket and answered `404 NoSuchBucket`. That was a configuration error, not a provider fault. With `AWS_ENDPOINT=https://<bucket name>.t3.storageapi.dev` the probe passed. Use the bucket's own host as the endpoint. The `ENDPOINT` reference alone isn't enough: it gives the bare `t3.storageapi.dev`, so build the host from the `BUCKET` reference. Path-style was not tried.

**Caveats.**
- `solera selftest` reports checks, not raw HTTP statuses. A pass shows the provider enforces `If-None-Match: *` and that compare-and-swap fenced a stale writer. It does not print the 412 status itself. `bench/railway/probe.py` reports statuses, but it was not run here.
- Bucket creation was `APPLYING` at first and `live` within a poll or two (seconds). `describe_environment` was the signal to trust, not the create call's response.
- The first deployment, whose probe failed, still showed `SUCCESS` in `list_deployments`; read the deploy logs, not the status. Solera's side checks out: the same failure (a 404 `NoSuchBucket`) makes `solera serve` exit 1 within a second, before it listens. The status is Railway's.
- Cleanup: the service and the bucket (with its `probe-*` objects) were deleted through the API, and `describe_environment` then listed no services, buckets or volumes. The gateway has no project delete, so the empty project must be deleted by hand in the Railway UI.
