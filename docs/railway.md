# Railway validation

Use a dedicated project and a **private Storage Bucket**, not a Railway disk volume. A volume and an S3-compatible bucket have different APIs and failure semantics.

## Configuration

Deploy `erwinkn/solera` from `feat/s3-state-backend`. `railway.toml` selects the Nixpacks builder: it detects `uv.lock`, runs `uv sync --no-dev --frozen`, and starts `solera serve --host 0.0.0.0` (the CLI reads `$PORT`). The console bundle is committed under `apps/server/src/solera_server/web`, so no Node runtime, Dockerfile, database service, or persistent application volume is needed for S3 mode.

Set these environment variables with Railway's reference picker (verify the actual bucket reference names; do not invent them):

| App variable | Value |
|---|---|
| `SOLERA_STATE_URL` | `s3://<bucket BUCKET reference>/orchestrator` |
| `SOLERA_NAMESPACE` | `demo` |
| `AWS_ACCESS_KEY_ID` | Bucket `ACCESS_KEY_ID` reference |
| `AWS_SECRET_ACCESS_KEY` | Bucket `SECRET_ACCESS_KEY` reference |
| `AWS_REGION` | Bucket `REGION` reference |
| `AWS_ENDPOINT` | Bucket `ENDPOINT` reference; include `https://` |
| `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | `false` if the provider expects path-style requests |
| `AWS_CONDITIONAL_PUT` | `etag` |
| `SOLERA_API_TOKEN` | Strong generated secret, not a committed value |
| `SOLERA_SELFTEST` | `1` for validation; `0` after testing |

Keep exactly one replica. Overlapping deployment startup intentionally replaces the previous writer. Do not scale this coordinator horizontally or attach multiple write processes to the same namespace. Services should fail closed rather than run repeated competing takeovers.

`solera serve` runs `solera selftest` before starting the API when `SOLERA_SELFTEST=1`; a failed probe exits non-zero so the deployment fails. It prints only synthetic test results, namespace, and elapsed time, never credentials. If the endpoint ignores preconditions, transactions fail, or the old writer can publish after takeover, startup fails. Health is exposed at `/healthz`; the UI is `/` and requires the API token for data/actions.

A second deployment runs the probe under a new namespace. For application persistence verification, materialize an asset in `demo`, redeploy with the same bucket/namespace and no local volume, then inspect the prior run and output in the console. Do not infer persistence merely from a green container healthcheck.

## Cleanup and caveats

The probe writes under new `probe-*` namespaces and retains them for inspection. Stop the probe and remove only its namespace prefixes to clean up. Removing the real workspace prefix destroys its durable state. Keep automations disabled on an idle demo to avoid unnecessary runs.

Real-provider results must be reported separately from the local filesystem and Moto emulator tests. A bucket creation action returning `staged` or `applying` is not proof that the bucket exists or its credentials resolve. The deployment must complete authenticated reads/writes against the provider before the experiment is described as validated.

## Attempted validation — September 11, 2026

The dedicated `solera-s3-test` Railway project was created and the application built, but bucket provisioning did not complete: the environment returned no buckets and bucket references resolved without a bucket name. The application correctly failed before S3 tests. **No real Railway S3 validation or public demo is claimed.** Local filesystem and HTTP S3-emulator results are separate.

Cleanup of the failed `solera-s3-test` service is staged but requires two-factor approval in the Railway dashboard. The API cannot complete it. Project: `87381f11-c1f0-4cac-a0d5-e872be0b8ada`; environment: `production`. The user must approve that staged removal; the service is not reported as deleted. No bucket was provisioned.

For a future new Railway bucket, inspect its actual addressing mode: new buckets may use virtual-hosted style, requiring `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true`. Do not assume path-style compatibility or substitute a filesystem deployment for a provider test.
