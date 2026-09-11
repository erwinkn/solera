# Railway validation

Use a dedicated project and a **private Storage Bucket**, not a Railway disk volume. A volume and an S3-compatible bucket have different APIs and failure semantics.

## Configuration

Deploy `erwinkn/data-orchestrator` from `feat/s3-state-backend`. The included Dockerfile serves the console from the Python wheel. No Node runtime, database service, or persistent application volume is needed for S3 mode.

Set these environment variables with Railway's reference picker (verify the actual bucket reference names; do not invent them):

| App variable | Value |
|---|---|
| `DORC_STATE_URL` | `s3://<bucket BUCKET reference>/orchestrator` |
| `DORC_NAMESPACE` | `demo` |
| `AWS_ACCESS_KEY_ID` | Bucket `ACCESS_KEY_ID` reference |
| `AWS_SECRET_ACCESS_KEY` | Bucket `SECRET_ACCESS_KEY` reference |
| `AWS_REGION` | Bucket `REGION` reference |
| `AWS_ENDPOINT` | Bucket `ENDPOINT` reference; include `https://` |
| `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` | `false` if the provider expects path-style requests |
| `AWS_CONDITIONAL_PUT` | `etag` |
| `DORC_API_TOKEN` | Strong generated secret, not a committed value |
| `DORC_SELFTEST` | `1` for validation; `0` after testing |

Keep exactly one replica. Overlapping deployment startup intentionally replaces the previous writer. Do not scale this coordinator horizontally or attach multiple write processes to the same namespace. Services should fail closed rather than run repeated competing takeovers.

The container waits for `dorc selftest` to pass before starting the API when `DORC_SELFTEST=1`. It prints only synthetic test results, namespace, and elapsed time, never credentials. If the endpoint ignores preconditions, transactions fail, or the old writer can publish after takeover, startup fails. Health is exposed at `/healthz`; the UI is `/` and requires the API token for data/actions.

A second deployment runs the probe under a new namespace. For application persistence verification, materialize an asset in `demo`, redeploy with the same bucket/namespace and no local volume, then inspect the prior run and output in the console. Do not infer persistence merely from a green container healthcheck.

## Cleanup and caveats

The probe writes under new `probe-*` namespaces and retains them for inspection. Stop the probe and remove only its namespace prefixes to clean up. Removing the real workspace prefix destroys its durable state. Keep automations disabled on an idle demo to avoid unnecessary runs.

Real-provider results must be reported separately from the local filesystem and Moto emulator tests. A bucket creation action returning `staged` or `applying` is not proof that the bucket exists or its credentials resolve. The deployment must complete authenticated reads/writes against the provider before the experiment is described as validated.
