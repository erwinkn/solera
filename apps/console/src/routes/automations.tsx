import { createFileRoute } from "@tanstack/react-router";
import { Clock, Code2, Play, Zap } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Empty, ErrorNotice } from "@/components/common";
import { request, useAction } from "@/lib/api";
import { describeInterval, time } from "@/lib/format";
import type { AutomationRecord } from "@/lib/types";
import { useWorkspace } from "@/lib/workspace";

export const Route = createFileRoute("/automations")({
  component: AutomationsPage,
});

function triggerLabel(automation: AutomationRecord) {
  const trigger = automation.trigger;
  if (trigger.kind === "cron")
    return `Cron ${trigger.expression} · ${trigger.timezone}`;
  if (trigger.kind === "commit")
    return `On commit of ${(trigger.assets ?? []).join(", ")}`;
  return `Every ${describeInterval(trigger.seconds ?? 0)}`;
}

function AutomationsPage() {
  const { state, refresh, select } = useWorkspace();
  const action = useAction();
  if (!state) return null;
  const automations = state.automations;
  return (
    <section className="flex flex-col gap-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
            Scheduling
          </div>
          <h1 className="font-heading text-xl font-medium">Automations</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Time and commit triggers, one materialization engine.
          </p>
        </div>
        <span className="flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs text-muted-foreground">
          <Code2 className="size-3.5" />
          Defined in code
        </span>
      </div>
      {action.error && <ErrorNotice message={action.error} />}
      {!automations.length ? (
        <Empty title="No automations defined">
          Add an Automation to your project definitions and restart the writer
          to register the updated manifest.
        </Empty>
      ) : (
        <div className="flex flex-col gap-3">
          {automations.map((automation) => (
            <article
              className="rounded-xl border bg-card p-4"
              key={automation.name}
              data-automation={automation.name}
            >
              <div className="flex items-center gap-3">
                <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-muted">
                  {automation.trigger.kind === "commit" ? (
                    <Zap className="size-4" />
                  ) : (
                    <Clock className="size-4" />
                  )}
                </span>
                <div className="min-w-0 flex-1">
                  <h2 className="flex items-center gap-2 font-heading text-sm font-medium">
                    {automation.name}
                    {automation.pending && (
                      <Badge variant="outline" className="text-amber-700">
                        Pending
                      </Badge>
                    )}
                  </h2>
                  <p className="text-xs text-muted-foreground">
                    {triggerLabel(automation)}
                  </p>
                </div>
                <Switch
                  checked={automation.enabled}
                  disabled={action.pending}
                  aria-label={`Enable ${automation.name}`}
                  onCheckedChange={async (checked) => {
                    await action.run(() =>
                      request(
                        `/automations/${encodeURIComponent(automation.name)}`,
                        { body: { enabled: checked === true } },
                      ),
                    );
                    refresh();
                  }}
                />
              </div>
              <div className="mt-3 grid grid-cols-2 items-end gap-3 sm:grid-cols-4">
                <div>
                  <div className="text-xs text-muted-foreground">Targets</div>
                  <div className="mt-0.5 flex flex-wrap gap-1">
                    {automation.targets.map((target) => (
                      <button
                        key={target}
                        className="font-mono text-xs text-primary hover:underline"
                        onClick={() => select({ kind: "asset", name: target })}
                      >
                        {target}
                      </button>
                    ))}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">
                    Last requested
                  </div>
                  <div className="mt-0.5 text-sm">
                    {time(automation.last_at)}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">
                    {automation.trigger.kind === "commit"
                      ? "Trigger state"
                      : "Next scheduled"}
                  </div>
                  <div className="mt-0.5 text-sm">
                    {!automation.enabled
                      ? "Disabled"
                      : automation.trigger.kind === "commit"
                        ? automation.pending
                          ? "Commit pending delivery"
                          : "Waiting for new commits"
                        : time(automation.next_at)}
                  </div>
                </div>
                <div className="flex justify-end">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={action.pending}
                    onClick={async () => {
                      const run = await action.run(() =>
                        request<{ id: string }>(
                          `/automations/${encodeURIComponent(automation.name)}/run`,
                          { body: {} },
                        ),
                      );
                      if (run) {
                        refresh();
                        select({ kind: "run", id: run.id });
                      }
                    }}
                  >
                    <Play />
                    Run now
                  </Button>
                </div>
              </div>
            </article>
          ))}
        </div>
      )}
      <p className="rounded-lg bg-muted/50 px-3 py-2 text-xs text-muted-foreground">
        Missed interval and cron ticks are coalesced. Commit automations run
        their targets against committed inputs once watched assets publish new
        output.
      </p>
    </section>
  );
}
