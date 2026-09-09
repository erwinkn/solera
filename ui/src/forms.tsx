import { useState } from "react";
import { request, useAction } from "./api";
import { Dialog, ErrorNotice, Icon, go } from "./components";
import type { Catalog } from "./types";

export function MaterializeForm({
  catalog,
  targets,
  close,
}: {
  catalog: Catalog;
  targets: string[];
  close: () => void;
}) {
  const [selected, setSelected] = useState(targets);
  const [mode, setMode] = useState("incremental");
  const [partition, setPartition] = useState(
    new Date().toISOString().slice(0, 10),
  );
  const [upstream, setUpstream] = useState(true);
  const [key, setKey] = useState(crypto.randomUUID());
  const action = useAction();
  const partitioned = catalog.assets.some(
    (asset) => selected.includes(asset.key) && asset.partitions,
  );
  const renew = () => setKey(crypto.randomUUID());
  return (
    <Dialog title="Materialize assets" close={close}>
      <form
        onSubmit={async (event) => {
          event.preventDefault();
          const result = await action.run(() =>
            request<{ id: string }>("/runs", {
              targets: selected,
              mode,
              partitions: partitioned ? [partition] : [],
              include_upstream: upstream,
              idempotency_key: key,
            }),
          );
          if (result) {
            close();
            go(`/runs/${result.id}`);
          }
        }}
      >
        <div className="form-body">
          <p className="muted">
            Submit a durable request. Dependencies are planned automatically;
            each producer publishes its outputs together.
          </p>
          <fieldset className="asset-picker">
            <legend>
              Selected assets <span className="muted">{selected.length}</span>
            </legend>
            {catalog.assets.map((asset) => (
              <label key={asset.key}>
                <input
                  type="checkbox"
                  checked={selected.includes(asset.key)}
                  onChange={(event) => {
                    renew();
                    setSelected((current) =>
                      event.target.checked
                        ? [...current, asset.key]
                        : current.filter((value) => value !== asset.key),
                    );
                  }}
                />
                <code>{asset.key}</code>
                <small>{asset.group}</small>
              </label>
            ))}
          </fieldset>
          <label className="field">
            Update mode
            <select
              value={mode}
              onChange={(event) => {
                renew();
                setMode(event.target.value);
              }}
            >
              <option value="incremental">Incremental · process changes</option>
              <option value="fill_missing">
                Fill missing · retain valid outputs
              </option>
              <option value="recompute">
                Recompute · rerun selected scopes
              </option>
            </select>
          </label>
          {partitioned && (
            <label className="field">
              Partition date <span className="muted">UTC</span>
              <input
                required
                type="date"
                value={partition}
                onChange={(event) => {
                  renew();
                  setPartition(event.target.value);
                }}
              />
            </label>
          )}
          <label className="checkbox-label">
            <input
              type="checkbox"
              checked={upstream}
              onChange={(event) => {
                renew();
                setUpstream(event.target.checked);
              }}
            />
            Include upstream assets
          </label>
          <p className="caption">
            Without upstream execution, the request reads existing committed
            input versions. Recompute never resets a live cursor.
          </p>
          {action.error && <ErrorNotice message={action.error} />}
        </div>
        <div className="dialog-footer">
          <button type="button" className="button" onClick={close}>
            Cancel
          </button>
          <button
            className="button primary"
            disabled={action.pending || selected.length === 0}
          >
            <Icon name="runs" />
            {action.pending ? "Submitting…" : "Launch materialization"}
          </button>
        </div>
      </form>
    </Dialog>
  );
}

export function BackfillForm({
  catalog,
  close,
}: {
  catalog: Catalog;
  close: () => void;
}) {
  const partitioned = catalog.assets.filter((asset) => asset.partitions);
  const [asset, setAsset] = useState(
    partitioned[partitioned.length - 1]?.key || "",
  );
  const [start, setStart] = useState(
    new Date(Date.now() - 6 * 86400000).toISOString().slice(0, 10),
  );
  const [end, setEnd] = useState(new Date().toISOString().slice(0, 10));
  const [mode, setMode] = useState("fill_missing");
  const action = useAction();
  const count =
    Math.floor(
      (new Date(end).getTime() - new Date(start).getTime()) / 86400000,
    ) + 1;
  return (
    <Dialog title="Create backfill" close={close}>
      <form
        onSubmit={async (event) => {
          event.preventDefault();
          const result = await action.run(() =>
            request<{ id: string }>("/backfills", {
              asset,
              start,
              end,
              mode,
              include_upstream: true,
            }),
          );
          if (result) {
            close();
            go(`/runs/${result.id}`);
          }
        }}
      >
        <div className="form-body">
          <p className="muted">
            A bounded materialization request over daily partitions. Running
            batches commit independently, so completed work survives
            interruption.
          </p>
          <label className="field">
            Target asset
            <select
              aria-label="Target asset"
              required
              value={asset}
              onChange={(event) => setAsset(event.target.value)}
            >
              {partitioned.map((value) => (
                <option key={value.key}>{value.key}</option>
              ))}
            </select>
          </label>
          <div className="form-columns">
            <label className="field">
              Start date
              <input
                aria-label="Start date"
                required
                type="date"
                min={
                  partitioned.find((value) => value.key === asset)?.partitions
                    ?.start
                }
                value={start}
                onChange={(event) => setStart(event.target.value)}
              />
            </label>
            <label className="field">
              End date
              <input
                aria-label="End date"
                required
                type="date"
                min={start}
                value={end}
                onChange={(event) => setEnd(event.target.value)}
              />
            </label>
          </div>
          <label className="field">
            Update mode
            <select
              value={mode}
              onChange={(event) => setMode(event.target.value)}
            >
              <option value="fill_missing">Fill missing partitions</option>
              <option value="recompute">
                Recompute all selected partitions
              </option>
              <option value="incremental">Process incremental changes</option>
            </select>
          </label>
          <div className="inset-note">
            {Number.isFinite(count) && count > 0
              ? `${count} daily partitions`
              : "Select a valid date range"}{" "}
            · includes upstream dependencies
            <br />
            Ranges are inclusive, in UTC. Maximum 366 partitions per request.
          </div>
          {action.error && <ErrorNotice message={action.error} />}
        </div>
        <div className="dialog-footer">
          <button type="button" className="button" onClick={close}>
            Cancel
          </button>
          <button
            className="button primary"
            disabled={action.pending || !asset || count < 1 || count > 366}
          >
            <Icon name="backfills" />
            {action.pending ? "Submitting…" : "Launch backfill"}
          </button>
        </div>
      </form>
    </Dialog>
  );
}
