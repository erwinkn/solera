import { createFileRoute } from "@tanstack/react-router";
import { Clock, Code2, Play, Zap } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Empty, ErrorNotice } from "@/components/common";
import { request, useAction, useQuery } from "@/lib/api";
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
  if (trigger.kind === "onchange")
    return trigger.outputs?.length
      ? `On change of ${trigger.outputs.join(", ")}`
      : "On change of inputs";
  if (trigger.kind === "ondeploy") return "On deploy";
  return `Every ${describeInterval(trigger.seconds ?? 0)}`;
}

function AutomationsPage() {
  const { base, refresh, select, diagnostics } = useWorkspace();
  const query = useQuery<{ automations: AutomationRecord[] }>(
    base ? `${base}/automations` : null,
    2000,
  );
  const action = useAction();
  if (!diagnostics) return null;
  const automations = query.data?.automations ?? [];
  return (
    <section className="flex flex-col gap-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
            Scheduling
          </div>
          <h1 className="font-heading text-xl font-medium">Automations</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Time and change triggers, one materialization engine.
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
              <div className="flex flex-wrap items-center gap-3">
                <span className="flex size-9 items-center justify-center rounded-lg bg-muted">
                  {automation.trigger.kind === "onchange" ? (
                    <Zap className="size-4" />
                  ) : (
                    <Clock className="size-4" />
                  )}
                </span>
                <div className="min-w-0 flex-1">
                  <div className="font-mono text-sm font-medium">
                    {automation.name}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {triggerLabel(automation)} → {automation.targets.join(", ")}
                    {automation.partitions
                      ? ` · ${Array.isArray(automation.partitions) ? automation.partitions.join(", ") : automation.partitions}`
                      : ""}
                    {automation.mode !== "incremental"
                      ? ` · ${automation.mode}`
                      : ""}
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Badge variant="outline">
                    {automation.last_at
                      ? `fired ${time(automation.last_at)}`
                      : "never fired"}
                  </Badge>
                  {automation.trigger.kind === "ondeploy" &&
                    automation.last_revision && (
                      <Badge variant="outline" className="font-mono">
                        rev {automation.last_revision.slice(0, 8)}
                      </Badge>
                    )}
                  {automation.trigger.kind === "onchange" && (
                    <Badge variant="outline" className="font-mono">
                      {automation.pending.length} pending
                    </Badge>
                  )}
                  <Switch
                    aria-label={`Enable ${automation.name}`}
                    checked={automation.enabled}
                    onCheckedChange={(enabled) =>
                      action.run(async () => {
                        await request(
                          `${base}/automations/${automation.name}/${enabled ? "enable" : "disable"}`,
                          { body: {} },
                        );
                        query.refresh();
                      })
                    }
                  />
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      action.run(async () => {
                        await request(
                          `${base}/automations/${automation.name}/run-now`,
                          { body: {} },
                        );
                        query.refresh();
                        refresh();
                      })
                    }
                  >
                    <Play /> Run now
                  </Button>
                  {automation.last_run && (
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() =>
                        select({ kind: "run", id: automation.last_run! })
                      }
                    >
                      Last run
                    </Button>
                  )}
                </div>
              </div>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
