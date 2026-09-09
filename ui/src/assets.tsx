import { useEffect, useRef, useState } from "react";
import { useQuery } from "./api";
import {
  Badge,
  DataPreview,
  Dialog,
  Empty,
  ErrorNotice,
  Icon,
  Loading,
  go,
  time,
} from "./components";
import type { Asset, AssetDetail, Catalog } from "./types";

function Graph({ assets }: { assets: Asset[] }) {
  const byKey = new Map(assets.map((asset) => [asset.key, asset]));
  const levels = new Map<string, number>();
  function depth(key: string): number {
    if (levels.has(key)) return levels.get(key)!;
    const value = Math.max(
      0,
      ...(byKey
        .get(key)
        ?.inputs.filter((input) => byKey.has(input))
        .map((input) => depth(input) + 1) || []),
    );
    levels.set(key, value);
    return value;
  }
  assets.forEach((asset) => depth(asset.key));
  const columns = new Map<number, Asset[]>();
  assets.forEach((asset) => {
    const level = depth(asset.key);
    columns.set(level, [...(columns.get(level) || []), asset]);
  });
  const height = Math.max(
    440,
    ...Array.from(columns.values()).map((items) => items.length * 124 + 80),
  );
  const width = Math.max(760, (Math.max(0, ...levels.values()) + 1) * 284 + 32);
  const positions = new Map<string, { x: number; y: number }>();
  columns.forEach((items, level) =>
    items.forEach((asset, index) =>
      positions.set(asset.key, {
        x: 32 + level * 284,
        y: (height - items.length * 124) / 2 + index * 124,
      }),
    ),
  );
  return (
    <div className="graph-scroll" aria-label="Asset lineage graph">
      <div className="graph-canvas" style={{ width, height }}>
        <svg
          className="graph-edges"
          width={width}
          height={height}
          aria-hidden="true"
        >
          <defs>
            <marker
              id="edge-arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="5"
              markerHeight="5"
              orient="auto"
            >
              <path d="M0 0 10 5 0 10z" />
            </marker>
          </defs>
          {assets.flatMap((asset) =>
            asset.inputs
              .filter((input) => positions.has(input))
              .map((input) => {
                const from = positions.get(input)!;
                const to = positions.get(asset.key)!;
                const x = from.x + 216;
                const y = from.y + 44;
                return (
                  <path
                    key={`${input}:${asset.key}`}
                    d={`M${x},${y} C${x + 34},${y} ${to.x - 34},${to.y + 44} ${to.x},${to.y + 44}`}
                    markerEnd="url(#edge-arrow)"
                  />
                );
              }),
          )}
        </svg>
        {assets.map((asset) => (
          <button
            className={`graph-node ${asset.status}`}
            key={asset.key}
            style={{
              left: positions.get(asset.key)!.x,
              top: positions.get(asset.key)!.y,
            }}
            onClick={() => go(`/assets/${encodeURIComponent(asset.key)}`)}
            aria-label={`Inspect ${asset.key}`}
          >
            <span className="node-label">
              <Icon name="assets" />
              <strong title={asset.key}>{asset.key}</strong>
            </span>
            <span className="node-meta">
              <span>
                {asset.incremental
                  ? "Incremental"
                  : asset.partitions
                    ? "Partitioned"
                    : "Snapshot"}
              </span>
              <span className={`node-dot ${asset.status}`} />
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}

export function Assets({
  catalog,
  materialize,
}: {
  catalog: Catalog;
  materialize: (targets: string[]) => void;
}) {
  const [search, setSearch] = useState("");
  const [group, setGroup] = useState("all");
  const [status, setStatus] = useState("all");
  const [view, setView] = useState<"table" | "graph">("table");
  const [selected, setSelected] = useState<string[]>([]);
  const input = useRef<HTMLInputElement>(null);
  const assets = catalog.assets.filter(
    (asset) =>
      `${asset.key} ${asset.description}`
        .toLowerCase()
        .includes(search.toLowerCase()) &&
      (group === "all" || asset.group === group) &&
      (status === "all" || asset.status === status),
  );
  useEffect(() => {
    function shortcut(event: KeyboardEvent) {
      if (
        event.key === "/" &&
        !(
          event.target instanceof HTMLInputElement ||
          event.target instanceof HTMLTextAreaElement
        )
      ) {
        event.preventDefault();
        input.current?.focus();
      }
    }
    window.addEventListener("keydown", shortcut);
    return () => window.removeEventListener("keydown", shortcut);
  }, []);
  function toggle(key: string) {
    setSelected((current) =>
      current.includes(key)
        ? current.filter((value) => value !== key)
        : [...current, key],
    );
  }
  return (
    <section>
      <div className="page-heading">
        <div>
          <div className="eyebrow">Workspace</div>
          <h1>Asset catalog</h1>
          <p>Data products, their dependencies, and what needs to run.</p>
        </div>
        <button
          className="button primary"
          onClick={() =>
            materialize(
              selected.length
                ? selected
                : catalog.assets.map((asset) => asset.key),
            )
          }
        >
          <Icon name="runs" />
          Materialize{selected.length ? ` ${selected.length} selected` : ""}
        </button>
      </div>
      <div className="catalog-summary">
        <strong>{catalog.assets.length} assets</strong>
        <span>
          {new Set(catalog.assets.map((asset) => asset.group)).size} groups
        </span>
        <span>
          {catalog.assets.filter((asset) => asset.incremental).length}{" "}
          incremental outputs
        </span>
        <span className="summary-right">
          {catalog.queue.running
            ? `${catalog.queue.running} running`
            : "No active executions"}
        </span>
      </div>
      <div className="toolbar">
        <div className="search-input">
          <Icon name="search" />
          <input
            ref={input}
            aria-label="Filter assets"
            placeholder="Filter assets…"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
          <kbd>/</kbd>
        </div>
        <select
          aria-label="Filter by group"
          value={group}
          onChange={(event) => setGroup(event.target.value)}
        >
          <option value="all">All groups</option>
          {Array.from(new Set(catalog.assets.map((asset) => asset.group))).map(
            (value) => (
              <option key={value}>{value}</option>
            ),
          )}
        </select>
        <select
          aria-label="Filter by status"
          value={status}
          onChange={(event) => setStatus(event.target.value)}
        >
          <option value="all">All statuses</option>
          <option value="materialized">Materialized</option>
          <option value="missing">Not materialized</option>
          <option value="stale">Stale</option>
          <option value="running">Running</option>
          <option value="failed">Failed</option>
        </select>
        <div className="segmented" aria-label="Asset view">
          <button
            aria-pressed={view === "table"}
            onClick={() => setView("table")}
          >
            <Icon name="table" />
            Table
          </button>
          <button
            aria-pressed={view === "graph"}
            onClick={() => setView("graph")}
          >
            <Icon name="graph" />
            Graph
          </button>
        </div>
      </div>
      {assets.length === 0 ? (
        <Empty title="No matching assets">
          Change the filters to see other assets in this workspace.
        </Empty>
      ) : view === "graph" ? (
        <Graph assets={assets} />
      ) : (
        <div className="table-scroll">
          <table className="asset-table">
            <thead>
              <tr>
                <th className="checkbox-column">
                  <input
                    type="checkbox"
                    aria-label="Select visible assets"
                    checked={
                      assets.length > 0 &&
                      assets.every((asset) => selected.includes(asset.key))
                    }
                    onChange={(event) =>
                      setSelected(
                        event.target.checked
                          ? Array.from(
                              new Set([
                                ...selected,
                                ...assets.map((asset) => asset.key),
                              ]),
                            )
                          : selected.filter(
                              (key) =>
                                !assets.some((asset) => asset.key === key),
                            ),
                      )
                    }
                  />
                </th>
                <th>Asset</th>
                <th>Status</th>
                <th>Update strategy</th>
                <th>Last materialization</th>
                <th className="numeric">Rows</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {assets.map((asset) => (
                <tr key={asset.key}>
                  <td>
                    <input
                      type="checkbox"
                      aria-label={`Select ${asset.key}`}
                      checked={selected.includes(asset.key)}
                      onChange={() => toggle(asset.key)}
                    />
                  </td>
                  <td>
                    <button
                      className="asset-name"
                      onClick={() =>
                        go(`/assets/${encodeURIComponent(asset.key)}`)
                      }
                    >
                      <span className="asset-glyph">
                        <Icon name="assets" />
                      </span>
                      <span>
                        <strong>{asset.key}</strong>
                        <small>{asset.group}</small>
                      </span>
                    </button>
                  </td>
                  <td>
                    <Badge status={asset.status} />
                  </td>
                  <td>
                    <span className="strategy">
                      {asset.incremental?.kind === "key"
                        ? "Keyed incremental"
                        : asset.incremental?.kind === "cursor"
                          ? "Cursor incremental"
                          : asset.partitions
                            ? "Daily partitions"
                            : "Snapshot"}
                    </span>
                  </td>
                  <td
                    className="muted"
                    title={asset.last_materialized || undefined}
                  >
                    {time(asset.last_materialized)}
                  </td>
                  <td className="numeric mono">
                    {asset.rows === null ? "—" : asset.rows.toLocaleString()}
                  </td>
                  <td>
                    <button
                      className="icon-button row-action"
                      aria-label={`Materialize ${asset.key}`}
                      title="Materialize asset"
                      onClick={() => materialize([asset.key])}
                    >
                      <Icon name="runs" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="table-footer">
        <span>
          {assets.length} of {catalog.assets.length} assets
        </span>
        <span>
          Definition <code>{catalog.manifest_id.slice(0, 10)}</code>
        </span>
      </div>
    </section>
  );
}

export function AssetDrawer({
  assetKey,
  catalog,
  materialize,
}: {
  assetKey: string;
  catalog: Catalog;
  materialize: (targets: string[]) => void;
}) {
  const [tab, setTab] = useState("overview");
  const [partition, setPartition] = useState("");
  useEffect(() => {
    setPartition("");
    setTab("overview");
  }, [assetKey]);
  const query = useQuery<AssetDetail>(
    `/assets/${encodeURIComponent(assetKey)}${partition ? `?partition=${encodeURIComponent(partition)}` : ""}`,
  );
  const data = query.data?.definition.key === assetKey ? query.data : null;
  const close = () => go("/assets");
  return (
    <Dialog title={assetKey} drawer close={close}>
      {query.error && <ErrorNotice message={query.error.message} />}
      {!data ? (
        <Loading />
      ) : (
        <>
          <div className="drawer-intro">
            <div className="spread">
              <Badge status={data.definition.status} />
              <button
                className="button small"
                onClick={() => {
                  close();
                  materialize([assetKey]);
                }}
              >
                <Icon name="runs" />
                Materialize
              </button>
            </div>
            <p>
              {data.definition.description ||
                "No description provided in the asset definition."}
            </p>
          </div>
          <div className="tabs" role="tablist" aria-label="Asset details">
            {["overview", "data", "commits"].map((value) => (
              <button
                key={value}
                role="tab"
                aria-selected={tab === value}
                onClick={() => setTab(value)}
              >
                {value[0].toUpperCase() + value.slice(1)}
              </button>
            ))}
          </div>
          <div className="drawer-content">
            {data.definition.partitions && (
              <label className="field">
                Partition
                <select
                  value={partition}
                  onChange={(event) => setPartition(event.target.value)}
                >
                  <option value="">Latest materialization</option>
                  {data.partitions.map((scope) => (
                    <option key={scope.partition_key}>
                      {scope.partition_key}
                    </option>
                  ))}
                </select>
              </label>
            )}
            {tab === "overview" && (
              <>
                <h3>Definition</h3>
                <dl className="properties">
                  <dt>Producer</dt>
                  <dd>
                    <code>{data.definition.producer}</code>
                  </dd>
                  <dt>Store</dt>
                  <dd>
                    <code>{data.definition.store}</code>
                  </dd>
                  <dt>Update strategy</dt>
                  <dd>
                    {data.definition.incremental?.kind === "key"
                      ? "Keyed inventory"
                      : data.definition.incremental?.kind === "cursor"
                        ? "Cursor"
                        : "Snapshot"}
                  </dd>
                  <dt>Outputs</dt>
                  <dd>{data.definition.outputs.join(", ")}</dd>
                  <dt>Data version</dt>
                  <dd>
                    <code>
                      {data.definition.version?.slice(0, 16) ||
                        "Not materialized"}
                    </code>
                  </dd>
                </dl>
                {data.definition.incremental?.kind === "key" && (
                  <div className="inset-note">
                    Tracks <code>{data.definition.incremental.key}</code> by{" "}
                    <code>{data.definition.incremental.revision}</code>,
                    committing up to {data.definition.incremental.batch_size}{" "}
                    source changes per batch.
                  </div>
                )}
                <h3>Dependencies</h3>
                <div className="dependency-list">
                  {data.definition.inputs.length ? (
                    data.definition.inputs.map((key) => (
                      <button
                        key={key}
                        className="dependency"
                        onClick={() => go(`/assets/${encodeURIComponent(key)}`)}
                      >
                        <Icon name="assets" />
                        <code>{key}</code>
                        <Icon name="arrow" />
                      </button>
                    ))
                  ) : (
                    <p className="muted">
                      Source asset · no upstream dependencies
                    </p>
                  )}
                </div>
                <h3>Downstream</h3>
                <div className="dependency-list">
                  {catalog.assets
                    .filter((asset) => asset.inputs.includes(assetKey))
                    .map((asset) => (
                      <button
                        className="dependency"
                        key={asset.key}
                        onClick={() =>
                          go(`/assets/${encodeURIComponent(asset.key)}`)
                        }
                      >
                        <Icon name="assets" />
                        <code>{asset.key}</code>
                        <Icon name="arrow" />
                      </button>
                    ))}
                  {!catalog.assets.some((asset) =>
                    asset.inputs.includes(assetKey),
                  ) && <p className="muted">No downstream assets</p>}
                </div>
                {data.checkpoint && (
                  <>
                    <h3>Incremental checkpoint</h3>
                    <dl className="properties">
                      <dt>Generation</dt>
                      <dd>{data.checkpoint.generation}</dd>
                      <dt>Tracked items</dt>
                      <dd>{data.checkpoint.tracked_items.toLocaleString()}</dd>
                      <dt>Updated</dt>
                      <dd>{time(data.checkpoint.updated_at)}</dd>
                      {data.definition.incremental?.kind === "cursor" && (
                        <>
                          <dt>Cursor</dt>
                          <dd>
                            <code>
                              {JSON.stringify(data.checkpoint.cursor_value)}
                            </code>
                          </dd>
                        </>
                      )}
                    </dl>
                  </>
                )}
                {data.definition.partitions && (
                  <>
                    <h3>
                      Materialized partitions{" "}
                      <span className="muted">{data.partitions.length}</span>
                    </h3>
                    <div className="partition-grid">
                      {data.partitions.map((scope) => (
                        <button
                          key={scope.partition_key}
                          title={`${scope.partition_key} · ${time(scope.updated_at)}`}
                          aria-label={`Inspect partition ${scope.partition_key}`}
                          onClick={() => setPartition(scope.partition_key)}
                        >
                          {scope.partition_key.slice(5)}
                        </button>
                      ))}
                    </div>
                  </>
                )}
              </>
            )}
            {tab === "data" &&
              (data.commits.length ? (
                <DataPreview value={data.preview} />
              ) : (
                <Empty title="No committed data">
                  Materialize this asset to inspect its output.
                </Empty>
              ))}
            {tab === "commits" && (
              <>
                {data.commits.length === 0 ? (
                  <Empty title="No materializations yet">
                    Committed outputs will appear here after an execution.
                  </Empty>
                ) : (
                  data.commits.map((commit) => (
                    <details className="commit-item" key={commit.id}>
                      <summary>
                        <span>
                          <code>{commit.id.slice(0, 8)}</code>
                          <small>{time(commit.created_at)}</small>
                        </span>
                        <span className="muted">
                          {commit.checkpoint_generation === null
                            ? "Snapshot"
                            : `Checkpoint ${commit.checkpoint_generation}`}
                        </span>
                      </summary>
                      <dl className="properties">
                        <dt>Rows</dt>
                        <dd>{commit.outputs[assetKey]?.rows ?? "—"}</dd>
                        <dt>Partition</dt>
                        <dd>{commit.partition_key || "Whole asset"}</dd>
                        <dt>Outputs</dt>
                        <dd>{Object.keys(commit.outputs).join(", ")}</dd>
                      </dl>
                      <pre className="code-block">
                        {JSON.stringify(
                          {
                            inputs: Object.fromEntries(
                              Object.entries(commit.input_refs).map(
                                ([key, value]) => [key, value.version],
                              ),
                            ),
                            metadata: commit.metadata,
                          },
                          null,
                          2,
                        )}
                      </pre>
                    </details>
                  ))
                )}
                <p className="caption">
                  Latest 50 commits. Outputs and checkpoints share one
                  publication transaction.
                </p>
              </>
            )}
          </div>
        </>
      )}
    </Dialog>
  );
}
