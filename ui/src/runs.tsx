import { useState } from "react";
import { request, useAction, useQuery } from "./api";
import {
  Badge,
  Empty,
  ErrorNotice,
  Icon,
  Loading,
  duration,
  go,
  time,
} from "./components";
import type { Automation, Run, RunDetail, Task } from "./types";

function Progress({ counts }: { counts: Record<string, number> }) {
  const total = Object.values(counts).reduce((sum, value) => sum + value, 0);
  const done = (counts.succeeded || 0) + (counts.skipped || 0);
  return (
    <div className="progress-cell">
      <div
        className="progress-track"
        role="progressbar"
        aria-label="Completed execution scopes"
        aria-valuenow={done}
        aria-valuemax={total || 1}
        aria-valuemin={0}
      >
        <span style={{ width: `${total ? (done / total) * 100 : 0}%` }} />
      </div>
      <span className="mono">
        {done}/{total}
      </span>
    </div>
  );
}

export function Runs({
  backfills,
  createBackfill,
}: {
  backfills: boolean;
  createBackfill: () => void;
}) {
  const [offset, setOffset] = useState(0);
  const query = useQuery<Run[]>(
    `/runs?limit=50&offset=${offset}&backfills=${backfills}`,
  );
  return (
    <section>
      <div className="page-heading">
        <div>
          <div className="eyebrow">Execution</div>
          <h1>{backfills ? "Backfills" : "Runs"}</h1>
          <p>
            {backfills
              ? "Bounded reprocessing, with progress that survives interruption."
              : "Requests, attempts, and the commits they produced."}
          </p>
        </div>
        {backfills ? (
          <button className="button primary" onClick={createBackfill}>
            <Icon name="backfills" />
            Create backfill
          </button>
        ) : (
          <button className="button" onClick={query.refresh}>
            <Icon name="refresh" />
            Refresh
          </button>
        )}
      </div>
      {query.error && <ErrorNotice message={query.error.message} />}
      {query.data === null ? (
        <Loading />
      ) : query.data.length === 0 ? (
        <Empty
          title={backfills ? "No backfills yet" : "No runs yet"}
          action={
            backfills ? (
              <button className="button" onClick={createBackfill}>
                Create a backfill
              </button>
            ) : (
              <button className="button" onClick={() => go("/assets")}>
                Browse assets
              </button>
            )
          }
        >
          {backfills
            ? "Select a partitioned asset and a date range to process."
            : "Materialize an asset or enable an automation to start recording execution history."}
        </Empty>
      ) : (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Run</th>
                <th>Status</th>
                <th>Selection</th>
                <th>Progress</th>
                <th>Requested</th>
                <th>Duration</th>
              </tr>
            </thead>
            <tbody>
              {query.data.map((run) => (
                <tr key={run.id}>
                  <td>
                    <button
                      className="link mono"
                      onClick={() => go(`/runs/${run.id}`)}
                    >
                      {run.id.slice(0, 8)}
                      <Icon name="arrow" size={13} />
                    </button>
                    <small className="cell-note">{run.reason}</small>
                  </td>
                  <td>
                    {run.paused &&
                    !["succeeded", "failed", "canceled"].includes(
                      run.status,
                    ) ? (
                      <span className="paused-label">
                        <Icon name="pause" />
                        Paused
                      </span>
                    ) : (
                      <Badge status={run.status} />
                    )}
                  </td>
                  <td>
                    <span
                      className="selection-label"
                      title={run.targets.join(", ")}
                    >
                      {run.targets.length === 1
                        ? run.targets[0]
                        : `${run.targets.length} assets`}
                    </span>
                    <small className="cell-note">
                      {run.partitions.length
                        ? `${run.partitions.length} partition${run.partitions.length === 1 ? "" : "s"}`
                        : run.mode.replace("_", " ")}
                    </small>
                  </td>
                  <td>
                    <Progress counts={run.counts || {}} />
                  </td>
                  <td className="muted">{time(run.created_at)}</td>
                  <td className="mono muted">
                    {duration(run.created_at, run.finished_at)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="table-footer">
        <span>
          Showing {offset + 1}–{offset + (query.data?.length || 0)}
        </span>
        <div className="inline">
          <button
            className="button small"
            disabled={offset === 0}
            onClick={() => setOffset((value) => Math.max(0, value - 50))}
          >
            Previous
          </button>
          <button
            className="button small"
            disabled={(query.data?.length || 0) < 50}
            onClick={() => setOffset((value) => value + 50)}
          >
            Next
          </button>
        </div>
      </div>
    </section>
  );
}

function TaskRow({ task, detail }: { task: Task; detail: RunDetail }) {
  return (
    <details className="task-item">
      <summary>
        <span className="task-identity">
          <Icon name="chevron" size={13} />
          <code>{task.producer}</code>
          {task.partition_key && <small>{task.partition_key}</small>}
        </span>
        <span className="task-right">
          <span className="muted">
            {task.attempt ? `Attempt ${task.attempt}` : "Not started"}
          </span>
          <span className="mono muted">
            {duration(task.started_at, task.finished_at)}
          </span>
          <Badge status={task.status} />
        </span>
      </summary>
      <div className="task-expanded">
        {task.error && <pre className="error-trace">{task.error}</pre>}
        <dl className="properties">
          <dt>Task</dt>
          <dd>
            <code>{task.id}</code>
          </dd>
          <dt>Committed output</dt>
          <dd>
            <code>{task.output_commit || "None"}</code>
          </dd>
          <dt>Attempt budget</dt>
          <dd>{task.max_attempts}</dd>
        </dl>
        <h4>Pinned inputs</h4>
        <pre className="code-block">
          {JSON.stringify(
            Object.fromEntries(
              Object.entries(task.input_refs || {}).map(([key, value]) => [
                key,
                {
                  asset: value.asset,
                  commit: value.commit_id,
                  version: value.version,
                },
              ]),
            ),
            null,
            2,
          )}
        </pre>
        {detail.attempts
          .filter((attempt) => attempt.task_id === task.id)
          .map((attempt) => (
            <div className="attempt" key={attempt.token}>
              <span>
                Attempt {attempt.number} · {attempt.status}
              </span>
              <span className="muted">
                {duration(attempt.started_at, attempt.finished_at)}
              </span>
              {attempt.error && (
                <pre className="error-trace">{attempt.error}</pre>
              )}
            </div>
          ))}
      </div>
    </details>
  );
}

export function RunPage({ id }: { id: string }) {
  const query = useQuery<RunDetail>(`/runs/${id}`);
  const action = useAction();
  const detail = query.data?.request.id === id ? query.data : null;
  async function control(operation: string, body: unknown = {}) {
    await action.run(() => request(`/runs/${id}/${operation}`, body));
    query.refresh();
  }
  if (!detail)
    return (
      <>
        {query.error && <ErrorNotice message={query.error.message} />}
        <Loading />
      </>
    );
  const run = detail.request;
  const active = ["running", "queued"].includes(run.status);
  const counts: Record<string, number> = {};
  detail.tasks.forEach((task) => {
    counts[task.status] = (counts[task.status] || 0) + 1;
  });
  return (
    <section>
      <button
        className="back-link"
        onClick={() => go(run.reason === "backfill" ? "/backfills" : "/runs")}
      >
        ← {run.reason === "backfill" ? "Backfills" : "All runs"}
      </button>
      <div className="page-heading">
        <div>
          <div className="eyebrow">
            {run.reason === "backfill"
              ? "Backfill request"
              : "Materialization request"}
          </div>
          <h1 className="run-heading">
            Run <span className="mono">{id.slice(0, 8)}</span>
            <Badge status={run.status} />
          </h1>
          <p>{run.targets.join(", ")}</p>
        </div>
        <div className="inline">
          {active && (
            <>
              <button
                className="button"
                disabled={action.pending}
                onClick={() => {
                  void control("pause", { paused: !run.paused });
                }}
              >
                <Icon name={run.paused ? "runs" : "pause"} />
                {run.paused ? "Resume" : "Pause"}
              </button>
              <button
                className="button danger-outline"
                disabled={action.pending}
                onClick={() => {
                  if (
                    window.confirm(
                      "Cancel unpublished work? Already committed outputs will be retained.",
                    )
                  )
                    void control("cancel");
                }}
              >
                Cancel run
              </button>
            </>
          )}
          {["failed", "canceled"].includes(run.status) && (
            <button
              className="button primary"
              disabled={action.pending}
              onClick={async () => {
                const result = await action.run(() =>
                  request<{ id: string }>(`/runs/${id}/repair`, {}),
                );
                if (result) go(`/runs/${result.id}`);
              }}
            >
              <Icon name="refresh" />
              Repair run
            </button>
          )}
        </div>
      </div>
      {action.error && <ErrorNotice message={action.error} />}
      {query.error && <ErrorNotice message={query.error.message} />}
      {run.paused && active && (
        <div className="inset-note">
          This request is paused. Running tasks may finish; new tasks will not
          start until it is resumed.
        </div>
      )}
      <div className="run-facts">
        <div>
          <small>Requested</small>
          <span>{time(run.created_at)}</span>
        </div>
        <div>
          <small>Mode</small>
          <span>{run.mode.replace("_", " ")}</span>
        </div>
        <div>
          <small>Cause</small>
          <span>{run.reason}</span>
        </div>
        <div>
          <small>Duration</small>
          <span className="mono">
            {duration(run.created_at, run.finished_at)}
          </span>
        </div>
        <div>
          <small>Scope progress</small>
          <Progress counts={counts} />
        </div>
      </div>
      {run.parent_id && (
        <p className="caption">
          Repair of{" "}
          <button
            className="link mono"
            onClick={() => go(`/runs/${run.parent_id}`)}
          >
            {run.parent_id.slice(0, 8)}
          </button>
          . Successful upstream commits are retained.
        </p>
      )}
      <div className="section-title">
        <h2>Execution scopes</h2>
        <span className="muted">{detail.tasks.length} tasks</span>
      </div>
      <div className="task-list">
        {detail.tasks.map((task) => (
          <TaskRow task={task} detail={detail} key={task.id} />
        ))}
      </div>
      <div className="section-title">
        <h2>Event log</h2>
        <span className="muted">Latest 250 events · live</span>
      </div>
      <div className="event-log">
        {detail.events.map((event) => (
          <div className={`event ${event.kind}`} key={event.id}>
            <time>{time(event.created_at)}</time>
            <span className="event-kind">
              {event.kind.replaceAll("_", " ")}
            </span>
            <div>
              <pre>{event.message}</pre>
              {event.kind === "committed" && (
                <code className="event-detail">
                  {JSON.stringify(event.detail)}
                </code>
              )}
            </div>
          </div>
        ))}
        {!detail.events.length && (
          <p className="muted">Waiting for execution events…</p>
        )}
      </div>
    </section>
  );
}

function triggerLabel(automation: Automation) {
  const trigger = automation.definition.trigger;
  return trigger.kind === "interval"
    ? `Every ${trigger.seconds! >= 60 && trigger.seconds! % 60 === 0 ? `${trigger.seconds! / 60} minutes` : `${trigger.seconds} seconds`}`
    : trigger.kind === "cron"
      ? `${trigger.expression} · ${trigger.timezone}`
      : `After ${(trigger.assets || []).join(", ")}`;
}
export function Automations() {
  const query = useQuery<Automation[]>("/automations");
  const action = useAction();
  return (
    <section>
      <div className="page-heading">
        <div>
          <div className="eyebrow">Scheduling</div>
          <h1>Automations</h1>
          <p>Time and commit triggers, one materialization engine.</p>
        </div>
        <span className="code-managed">
          <Icon name="code" />
          Defined in code
        </span>
      </div>
      {action.error && <ErrorNotice message={action.error} />}
      {query.error && <ErrorNotice message={query.error.message} />}
      {query.data === null ? (
        <Loading />
      ) : query.data.length === 0 ? (
        <Empty title="No automations defined">
          Add an Automation to your Definitions and register the updated
          manifest.
        </Empty>
      ) : (
        <div className="automation-list">
          {query.data.map((automation) => (
            <article className="automation" key={automation.name}>
              <div className="automation-title">
                <span className="automation-glyph">
                  <Icon
                    name={
                      automation.definition.trigger.kind === "commit"
                        ? "graph"
                        : "clock"
                    }
                    size={20}
                  />
                </span>
                <div>
                  <h2>{automation.name}</h2>
                  <p>{triggerLabel(automation)}</p>
                </div>
                <button
                  className="switch"
                  role="switch"
                  aria-checked={automation.enabled}
                  aria-label={`Enable ${automation.name}`}
                  disabled={action.pending}
                  onClick={async () => {
                    await action.run(() =>
                      request(
                        `/automations/${encodeURIComponent(automation.name)}`,
                        { enabled: !automation.enabled },
                      ),
                    );
                    query.refresh();
                  }}
                >
                  <span />
                </button>
              </div>
              <div className="automation-details">
                <div>
                  <small>Targets</small>
                  <span>
                    {automation.definition.targets.map((target) => (
                      <button
                        className="link mono"
                        key={target}
                        onClick={() =>
                          go(`/assets/${encodeURIComponent(target)}`)
                        }
                      >
                        {target}
                      </button>
                    ))}
                  </span>
                </div>
                <div>
                  <small>Last requested</small>
                  <span>{time(automation.last_tick)}</span>
                </div>
                <div>
                  <small>
                    {automation.definition.trigger.kind === "commit"
                      ? "Trigger state"
                      : "Next scheduled"}
                  </small>
                  <span>
                    {!automation.enabled
                      ? "Disabled"
                      : automation.definition.trigger.kind === "commit"
                        ? "Waiting for new commits"
                        : time(automation.next_tick)}
                  </span>
                </div>
                <button
                  className="button small"
                  disabled={action.pending}
                  onClick={async () => {
                    const result = await action.run(() =>
                      request<{ id: string }>(
                        `/automations/${encodeURIComponent(automation.name)}/run`,
                        {},
                      ),
                    );
                    if (result) go(`/runs/${result.id}`);
                  }}
                >
                  <Icon name="runs" />
                  Run now
                </button>
              </div>
            </article>
          ))}
        </div>
      )}
      <div className="inset-note">
        Schedule state and materialization requests are persisted together.
        Missed timer ticks are coalesced; commit triggers only react to changed
        output versions.
      </div>
    </section>
  );
}
