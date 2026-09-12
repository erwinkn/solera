import { Pause, Play, RefreshCw, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { request, useAction, useQuery } from "@/lib/api";
import { duration, time } from "@/lib/format";
import type { RunDetail, TaskRecord } from "@/lib/types";
import { useWorkspace } from "@/lib/workspace";
import {
  ErrorNotice,
  JsonBlock,
  Loading,
  Properties,
  StatusBadge,
} from "./common";

export function RunSheet() {
  const { selection, select } = useWorkspace();
  const open = selection?.kind === "run";
  return (
    <Sheet
      open={open}
      modal={false}
      onOpenChange={(next) => {
        if (!next) select(null);
      }}
    >
      <SheetContent
        side="right"
        hideOverlay
        className="w-full overflow-y-auto sm:max-w-2xl"
      >
        {open && <RunDetailView key={selection.id} id={selection.id} />}
      </SheetContent>
    </Sheet>
  );
}

function RunDetailView({ id }: { id: string }) {
  const { refresh } = useWorkspace();
  const query = useQuery<RunDetail>(`/runs/${id}`);
  const action = useAction();
  const detail = query.data?.request.id === id ? query.data : null;

  async function control(operation: string) {
    await action.run(() => request(`/runs/${id}/${operation}`));
    query.refresh();
    refresh();
  }

  return (
    <>
      <SheetHeader>
        <SheetTitle>
          Run <span className="font-mono">{id.slice(0, 8)}</span>
        </SheetTitle>
      </SheetHeader>
      <div className="flex flex-col gap-4 px-4 pb-6">
        {query.error && <ErrorNotice message={query.error.message} />}
        {action.error && <ErrorNotice message={action.error} />}
        {!detail ? (
          <Loading label="Loading run…" />
        ) : (
          <RunContent
            detail={detail}
            control={control}
            pending={action.pending}
          />
        )}
      </div>
    </>
  );
}

function RunContent({
  detail,
  control,
  pending,
}: {
  detail: RunDetail;
  control: (operation: string) => Promise<void>;
  pending: boolean;
}) {
  const run = detail.request;
  const active = ["running", "queued", "paused"].includes(run.status);
  return (
    <>
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge status={run.paused && active ? "paused" : run.status} />
        <span className="text-sm text-muted-foreground">
          {run.targets.join(", ")} · {time(run.created_at)}
        </span>
      </div>
      <div className="flex flex-wrap gap-2">
        {active && (
          <>
            <Button
              variant="outline"
              size="sm"
              disabled={pending}
              onClick={() => control(run.paused ? "resume" : "pause")}
            >
              {run.paused ? <Play /> : <Pause />}
              {run.paused ? "Resume" : "Pause"}
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="text-destructive"
              disabled={pending}
              onClick={() => {
                if (
                  window.confirm(
                    "Cancel unpublished work? Already committed outputs are retained.",
                  )
                )
                  void control("cancel");
              }}
            >
              <X />
              Cancel request
            </Button>
          </>
        )}
        {run.status === "failed" && (
          <Button size="sm" disabled={pending} onClick={() => control("retry")}>
            <RefreshCw />
            Retry failed work
          </Button>
        )}
      </div>
      {run.paused && active && (
        <p className="rounded-lg bg-muted/50 px-3 py-2 text-xs text-muted-foreground">
          This request is paused. Running tasks may finish; new tasks will not
          start until it is resumed.
        </p>
      )}
      <Properties
        entries={[
          ["Cause", run.cause.replaceAll("_", " ")],
          ["Mode", run.mode.replaceAll("_", " ")],
          ["Requested", time(run.created_at)],
          ["Duration", duration(run.created_at, run.updated_at)],
          [
            "Partitions",
            run.partitions.length
              ? `${run.partitions.length} (${run.partitions[0]} … ${run.partitions[run.partitions.length - 1]})`
              : "Whole scope",
          ],
        ]}
      />
      <section>
        <h3 className="mb-2 text-sm font-medium">
          Tasks{" "}
          <span className="font-normal text-muted-foreground">
            {detail.tasks.length}
          </span>
        </h3>
        <div className="flex flex-col gap-2">
          {detail.tasks.map((task) => (
            <TaskRow key={task.id} task={task} detail={detail} />
          ))}
        </div>
      </section>
      <section>
        <h3 className="mb-2 text-sm font-medium">
          Events{" "}
          <span className="font-normal text-muted-foreground">
            {detail.events.length} · live
          </span>
        </h3>
        <div className="flex flex-col gap-2">
          {detail.events.map((event) => (
            <div
              key={event.id}
              className="rounded-lg border px-3 py-2 text-sm"
              data-event-kind={event.kind}
            >
              <div className="flex items-center gap-2">
                <time className="text-xs text-muted-foreground tabular-nums">
                  {time(event.at)}
                </time>
                <span className="rounded-full bg-muted px-2 py-0.5 text-xs">
                  {event.kind.replaceAll("_", " ")}
                </span>
              </div>
              <p className="mt-1">{event.message}</p>
              {event.data != null && (
                <div className="mt-1">
                  <JsonBlock value={event.data} />
                </div>
              )}
            </div>
          ))}
          {!detail.events.length && (
            <p className="text-sm text-muted-foreground">
              Waiting for execution events…
            </p>
          )}
        </div>
      </section>
    </>
  );
}

function TaskRow({ task, detail }: { task: TaskRecord; detail: RunDetail }) {
  const attempts = detail.attempts[task.id] ?? [];
  return (
    <details className="group rounded-lg border">
      <summary className="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm">
        <code className="min-w-0 flex-1 truncate font-mono text-xs font-medium">
          {task.producer}
        </code>
        {task.partition && (
          <span className="text-xs text-muted-foreground">
            {task.partition}
          </span>
        )}
        <span className="text-xs text-muted-foreground">
          gen {task.generation}
        </span>
        <StatusBadge status={task.status} />
      </summary>
      <div className="flex flex-col gap-3 border-t px-3 py-3">
        {task.error && (
          <pre className="overflow-x-auto rounded-lg border border-red-600/30 bg-red-500/10 p-3 text-xs text-red-700 whitespace-pre-wrap">
            {task.error}
          </pre>
        )}
        {task.pinned_inputs && Object.keys(task.pinned_inputs).length > 0 && (
          <div>
            <h4 className="mb-1 text-xs font-medium text-muted-foreground">
              Pinned inputs
            </h4>
            <JsonBlock
              value={Object.fromEntries(
                Object.entries(task.pinned_inputs).map(([arg, dep]) => [
                  arg,
                  `${dep.asset}${dep.partition ? ` @ ${dep.partition}` : ""}`,
                ]),
              )}
            />
          </div>
        )}
        {attempts.map((attempt) => (
          <div key={attempt.generation} className="flex flex-col gap-2">
            <div className="flex items-center gap-2 text-xs">
              <span className="font-medium">
                Attempt {attempt.generation + 1}
              </span>
              <span className="text-muted-foreground">{attempt.status}</span>
              {attempt.commit_id && (
                <code className="font-mono text-muted-foreground">
                  {attempt.commit_id.slice(0, 10)}
                </code>
              )}
            </div>
            {(attempt.log_entries ?? []).map((entry, index) => (
              <div
                key={index}
                className="rounded-lg border px-3 py-2 text-sm"
                data-log-entry
              >
                <div className="flex items-center gap-2">
                  <time className="text-xs text-muted-foreground tabular-nums">
                    {time(entry.at)}
                  </time>
                  <span className="rounded-full bg-muted px-2 py-0.5 text-xs">
                    log
                  </span>
                </div>
                <p className="mt-1">{entry.message}</p>
                {entry.fields && Object.keys(entry.fields).length > 0 && (
                  <div className="mt-1">
                    <JsonBlock value={entry.fields} />
                  </div>
                )}
              </div>
            ))}
            {attempt.error && (
              <pre className="overflow-x-auto rounded-lg border border-red-600/30 bg-red-500/10 p-3 text-xs text-red-700 whitespace-pre-wrap">
                {attempt.error}
              </pre>
            )}
            {attempt.logs && (
              <pre className="max-h-64 overflow-x-auto rounded-lg bg-muted/40 p-3 font-mono text-xs whitespace-pre-wrap">
                {attempt.logs}
              </pre>
            )}
          </div>
        ))}
        {!attempts.length && (
          <p className="text-xs text-muted-foreground">No attempts yet.</p>
        )}
      </div>
    </details>
  );
}
