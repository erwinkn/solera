# Contributing

Use a feature branch and a draft pull request. Keep the public programming model small: assets, stores, partitions, automations, and ordinary resource bindings. Runs and execution tasks are engine records, not user-defined workflows.

## Development

Python 3.12+, uv 0.12.10+, Node 24, and PostgreSQL 17 are the tested environment. Run `uv sync --locked`, then `cd ui && npm ci && npm run build`. Start the example with `uv run dorc dev examples.lab:definitions`. The full setup is in the README.

Before submitting a change:

```sh
uv run ruff check src tests examples
uv run ruff format --check src tests examples
uv run mypy src/data_orchestrator
TEST_DATABASE_URL=postgresql:///orchestrator uv run pytest -q
(cd ui && npm run format:check && npm run build && npm test)
```

Integration tests use isolated PostgreSQL schemas and drop only their own fixtures. Without `TEST_DATABASE_URL`, they skip; that is not sufficient validation for an engine change. Browser tests exercise a real API and subprocess worker against `DORC_DATABASE_URL`. Use a disposable database, not a live workspace.

## Reliability rules

Do not advance a checkpoint outside the output-publication transaction. Do not bypass write-scope ownership or the attempt token when publishing. Preserve pinned inputs on retry. Test recovery after staging, after a committed batch, during cancellation, and with a late worker. An incomplete source inventory must never infer deletions.

New stores must preserve immutable versions. A mutable-destination adapter needs a separate receipt/reconciliation design; implementing the existing `Store` protocol by overwriting current data is incorrect. Backend adapters must not take over planning or checkpoint semantics.

A dependency or helper-code change that affects data must change the asset's explicit `version`; the framework does not hash the entire Python dependency closure. Update both dependency lockfiles when changing runtime requirements.

Use Ruff for Python and Prettier for the frontend. UI actions must reach the live API, show errors, and remain keyboard accessible. Avoid fake operational data and external-font/CDN dependencies.
