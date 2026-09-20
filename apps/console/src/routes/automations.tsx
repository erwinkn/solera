import { createFileRoute } from "@tanstack/react-router";
import {
  ArrowRight,
  Clock,
  Code2,
  Play,
  RefreshCw,
  RocketIcon,
  Zap,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Empty, ErrorNotice, PageHeader } from "@/components/common";
import { request, useAction, useQuery } from "@/lib/api";
import { describeInterval, time } from "@/lib/format";
import type { AutomationRecord } from "@/lib/types";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";

export const Route = createFileRoute("/automations")({
  component: AutomationsPage,
});

function triggerIcon(kind: string) {
  if (kind === "onchange") return Zap;
  if (kind === "ondeploy") return RocketIcon;
  return Clock;
}

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

function runFields(automation: AutomationRecord) {
  const parts: string[] = [];
  const p = automation.partitions;
  if (p) parts.push(Array.isArray(p) ? `${p.length} scopes` : p);
  if (automation.mode !== "incremental") parts.push(automation.mode);
  if (automation.upstream) parts.push("+upstream");
  return parts.join(" · ");
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
      <PageHeader
        eyebrow="Scheduling"
        title="Automations"
        description="Time and change triggers, one materialization engine."
        aside={
          <span className="flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs text-muted-foreground">
            <Code2 className="size-3.5" />
            Defined in code
          </span>
        }
      />
      {action.error && <ErrorNotice message={action.error} />}
      {!automations.length ? (
        <Empty title="No automations defined">
          Add an Automation to your project definitions and restart the writer
          to register the updated manifest.
        </Empty>
      ) : (
        <div className="flex flex-col gap-2.5">
          {automations.map((automation) => {
            const Icon = triggerIcon(automation.trigger.kind);
            const fields = runFields(automation);
            return (
              <article
                className="flex flex-wrap items-center gap-3 rounded-xl border bg-card p-4"
                key={automation.name}
                data-automation={automation.name}
              >
                <span
                  className={cn(
                    "flex size-9 shrink-0 items-center justify-center rounded-lg",
                    automation.enabled
                      ? "bg-primary/10 text-primary"
                      : "bg-muted text-muted-foreground",
                  )}
                >
                  <Icon className="size-4.5" />
                </span>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <span className="truncate font-mono text-sm font-medium">
                      {automation.name}
                    </span>
                    {!!automation.pending.length && (
                      <span className="rounded-full bg-primary/10 px-2 py-0.5 text-[0.7rem] font-semibold text-primary tabular-nums">
                        {automation.pending.length} pending
                      </span>
                    )}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {triggerLabel(automation)}
                    <ArrowRight className="mx-1 inline size-3 align-[-1px]" />
                    <span className="font-mono">
                      {automation.targets.join(", ")}
                    </span>
                    {fields && ` · ${fields}`}
                  </div>
                </div>
                <div className="flex flex-col items-end gap-0.5 text-xs text-muted-foreground">
                  <span>
                    {automation.last_at
                      ? `fired ${time(automation.last_at)}`
                      : "never fired"}
                  </span>
                  {automation.trigger.kind === "ondeploy" &&
                    automation.last_revision && (
                      <span className="font-mono">
                        rev {automation.last_revision.slice(0, 8)}
                      </span>
                    )}
                </div>
                <div className="flex items-center gap-2 border-l pl-3">
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
                      size="icon-sm"
                      aria-label="Open last run"
                      title="Open last run"
                      onClick={() =>
                        select({ kind: "run", id: automation.last_run! })
                      }
                    >
                      <RefreshCw />
                    </Button>
                  )}
                </div>
              </article>
            );
          })}
        </div>
      )}
    </section>
  );
}
