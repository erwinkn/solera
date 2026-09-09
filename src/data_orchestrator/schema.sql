CREATE TABLE IF NOT EXISTS schema_version (version integer PRIMARY KEY);
CREATE TABLE IF NOT EXISTS manifests (
  id text PRIMARY KEY, entrypoint text NOT NULL, definition jsonb NOT NULL,
  registered_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS workspace (
  singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
  manifest_id text NOT NULL REFERENCES manifests(id)
);
CREATE TABLE IF NOT EXISTS requests (
  id uuid PRIMARY KEY, manifest_id text NOT NULL REFERENCES manifests(id),
  targets jsonb NOT NULL, partitions jsonb NOT NULL DEFAULT '[]',
  mode text NOT NULL CHECK(mode IN ('incremental','recompute','fill_missing')),
  reason text NOT NULL, include_upstream boolean NOT NULL DEFAULT true, status text NOT NULL DEFAULT 'queued'
    CHECK(status IN ('queued','running','succeeded','failed','canceled')),
  paused boolean NOT NULL DEFAULT false, parent_id uuid REFERENCES requests(id),
  idempotency_key text UNIQUE, created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);
CREATE INDEX IF NOT EXISTS requests_created ON requests(created_at DESC);
CREATE TABLE IF NOT EXISTS tasks (
  id uuid PRIMARY KEY, request_id uuid NOT NULL REFERENCES requests(id),
  producer text NOT NULL, partition_key text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT 'waiting'
    CHECK(status IN ('waiting','queued','running','succeeded','skipped','failed','blocked','canceled')),
  attempt integer NOT NULL DEFAULT 0, max_attempts integer NOT NULL,
  owner uuid, lease_expires timestamptz, retry_at timestamptz NOT NULL DEFAULT now(),
  input_refs jsonb, output_commit uuid, signature text,
  started_at timestamptz, finished_at timestamptz, error text,
  UNIQUE(request_id, producer, partition_key)
);
CREATE INDEX IF NOT EXISTS tasks_queue ON tasks(retry_at) WHERE status='queued';
CREATE INDEX IF NOT EXISTS tasks_request ON tasks(request_id);
CREATE TABLE IF NOT EXISTS dependencies (
  task_id uuid NOT NULL REFERENCES tasks(id), upstream_id uuid NOT NULL REFERENCES tasks(id),
  PRIMARY KEY(task_id,upstream_id)
);
CREATE TABLE IF NOT EXISTS attempts (
  token uuid PRIMARY KEY, task_id uuid NOT NULL REFERENCES tasks(id),
  number integer NOT NULL, status text NOT NULL DEFAULT 'running',
  started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz, error text,
  UNIQUE(task_id,number)
);
CREATE TABLE IF NOT EXISTS scope_locks (
  asset_key text NOT NULL, partition_key text NOT NULL DEFAULT '',
  owner uuid, lease_expires timestamptz, PRIMARY KEY(asset_key,partition_key)
);
CREATE TABLE IF NOT EXISTS objects (
  id text PRIMARY KEY, value jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS commits (
  id uuid PRIMARY KEY, task_id uuid NOT NULL REFERENCES tasks(id),
  producer text NOT NULL, partition_key text NOT NULL, definition_version text NOT NULL,
  signature text NOT NULL, input_refs jsonb NOT NULL, outputs jsonb NOT NULL,
  metadata jsonb NOT NULL, checkpoint_generation bigint,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS commits_task ON commits(task_id,created_at DESC);
CREATE TABLE IF NOT EXISTS asset_heads (
  asset_key text NOT NULL, partition_key text NOT NULL DEFAULT '',
  commit_id uuid NOT NULL REFERENCES commits(id), version text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(asset_key,partition_key)
);
CREATE TABLE IF NOT EXISTS checkpoints (
  producer text NOT NULL, partition_key text NOT NULL DEFAULT '',
  generation bigint NOT NULL DEFAULT 0, cursor_value jsonb, state_version text,
  updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(producer,partition_key)
);
CREATE TABLE IF NOT EXISTS item_state (
  producer text NOT NULL, partition_key text NOT NULL DEFAULT '', item_key text NOT NULL,
  revision text NOT NULL, transform_version text NOT NULL, last_task_id uuid,
  PRIMARY KEY(producer,partition_key,item_key)
);
CREATE TABLE IF NOT EXISTS append_receipts (
  asset_key text NOT NULL, partition_key text NOT NULL DEFAULT '', batch_id text NOT NULL,
  payload_hash text NOT NULL, commit_id uuid NOT NULL REFERENCES commits(id),
  PRIMARY KEY(asset_key,partition_key,batch_id)
);
CREATE TABLE IF NOT EXISTS events (
  id bigserial PRIMARY KEY, request_id uuid REFERENCES requests(id),
  task_id uuid REFERENCES tasks(id), kind text NOT NULL, message text NOT NULL,
  detail jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_request ON events(request_id,id);
CREATE TABLE IF NOT EXISTS automations (
  name text PRIMARY KEY, manifest_id text NOT NULL REFERENCES manifests(id),
  definition jsonb NOT NULL, enabled boolean NOT NULL DEFAULT false,
  next_tick timestamptz NOT NULL DEFAULT now(), last_tick timestamptz,
  last_request uuid REFERENCES requests(id)
);
CREATE TABLE IF NOT EXISTS outbox (
  id uuid PRIMARY KEY, automation_name text NOT NULL REFERENCES automations(name),
  commit_id uuid NOT NULL REFERENCES commits(id), delivered_at timestamptz,
  UNIQUE(automation_name,commit_id)
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(automation_name) WHERE delivered_at IS NULL;
INSERT INTO schema_version(version) VALUES (1) ON CONFLICT DO NOTHING;
