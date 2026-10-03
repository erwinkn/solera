import { useSuspenseQuery } from "@tanstack/react-query";
import { getRouteApi, Link } from "@tanstack/react-router";
import { Zap } from "lucide-react";
import { q, useProject } from "@/api/queries";
import { useAutomationToggle, useRunAutomation } from "@/api/mutations";
import type { Automation } from "@/api/types";
import { describeTrigger } from "@/features/triggers";
import { useNow } from "@/lib/clock";
import { cn } from "@/lib/cn";
import { plural, shortId, until } from "@/lib/format";
import { Button } from "@/ui/button";
import { Empty, Time } from "@/ui/data";
import { SearchInput, Switch } from "@/ui/form";
import { Card, Page, PageHeader } from "@/ui/layout";
import { Tooltip } from "@/ui/overlay";
import { Table, TableScroll, Td, Th, Tr } from "@/ui/table";

const route = getRouteApi("/automations");

const KIND_ORDER = { every: 0, cron: 1, onchange: 2, ondeploy: 3 } as const;

export function Automations() {
  const { q: text = "" } = route.useSearch();
  const navigate = route.useNavigate();
  const project = useProject();
  const { data } = useSuspenseQuery(q.automations(project));
  const rows = data
    .filter((a) => !text || `${a.name} ${a.targets.join(" ")}`.toLowerCase().includes(text.toLowerCase()))
    .sort((a, b) => KIND_ORDER[a.trigger.kind] - KIND_ORDER[b.trigger.kind] || a.name.localeCompare(b.name));
  const disabled = data.filter((a) => !a.enabled).length;
  return (
    <Page>
      <PageHeader
        title="Automations"
        description="A trigger says when; the automation says what run to submit. Schedules skip partitions still running."
        meta={
          <>
            <span>{plural(data.length, "automation")}</span>
            {disabled > 0 && <span className="text-warn-fg">{disabled} disabled</span>}
          </>
        }
        actions={
          <SearchInput
            aria-label="Filter automations"
            placeholder="Filter by name or target"
            className="w-64"
            value={text}
            onChange={(e) =>
              navigate({
                search: { q: e.target.value || undefined },
                replace: true,
              })
            }
          />
        }
      />
      <Card>
        {rows.length === 0 ? (
          <Empty title={data.length ? "No automation matches" : "No automations"}>
            {data.length ? undefined : "Attach `automations=` to an asset, or declare one on the project."}
          </Empty>
        ) : (
          <TableScroll>
            <Table>
              <thead>
                <tr>
                  <Th>Automation</Th>
                  <Th>Trigger</Th>
                  <Th>Submits</Th>
                  <Th>Last fired</Th>
                  <Th>Next</Th>
                  <Th className="text-right">Enabled</Th>
                  <Th />
                </tr>
              </thead>
              <tbody>
                {rows.map((a) => (
                  <Row key={a.name} a={a} />
                ))}
              </tbody>
            </Table>
          </TableScroll>
        )}
      </Card>
    </Page>
  );
}

function Row({ a }: { a: Automation }) {
  const toggle = useAutomationToggle();
  const fire = useRunAutomation();
  const now = useNow();
  return (
    <Tr className={cn(!a.enabled && "text-fg-subtle")}>
      <Td className="py-2">
        <span className="font-mono text-xs text-fg">{a.name}</span>
        <span className="mt-0.5 flex flex-wrap gap-1">
          {a.targets.map((t) => (
            <Link
              key={t}
              to="/assets/$asset"
              params={{ asset: t }}
              className="text-xs text-link hover:underline"
            >
              {t}
            </Link>
          ))}
        </span>
      </Td>
      <Td className="text-fg-muted">{describeTrigger(a.trigger, a.watched)}</Td>
      <Td className="text-xs text-fg-muted">
        {typeof a.partitions === "string"
          ? a.partitions
          : Array.isArray(a.partitions)
            ? plural(a.partitions.length, "partition")
            : a.trigger.kind === "onchange"
              ? "changed partitions"
              : "latest"}
        {a.mode === "full" && " · full"}
        {a.upstream && " · upstream"}
        {a.skip_missing_inputs && " · skips missing inputs"}
        {Object.entries(a.tags).map(([k, v]) => ` · ${k}=${v}`)}
      </Td>
      <Td className="text-fg-muted">
        {a.last_fired ? (
          <span className="flex items-center gap-2">
            <Time at={a.last_fired} />
            {a.last_run && (
              <Link
                to="/runs/$run"
                params={{ run: a.last_run }}
                className="font-mono text-xs text-link hover:underline"
              >
                {shortId(a.last_run)}
              </Link>
            )}
          </span>
        ) : (
          "never"
        )}
      </Td>
      <Td className="text-fg-muted tabular">
        {!a.enabled ? (
          "—"
        ) : a.next_at != null ? (
          <Tooltip content={new Date(a.next_at * 1000).toLocaleString()}>
            <span>{until(a.next_at, now)}</span>
          </Tooltip>
        ) : a.trigger.kind === "onchange" ? (
          a.pending.length ? (
            <span className="text-wait-fg">{plural(a.pending.length, "change")} pending</span>
          ) : (
            "on the next change"
          )
        ) : (
          "on the next deploy"
        )}
      </Td>
      <Td className="text-right">
        <Switch
          checked={a.enabled}
          label={`${a.enabled ? "Disable" : "Enable"} ${a.name}`}
          onCheckedChange={(enabled) => toggle.mutate({ name: a.name, enabled })}
        />
      </Td>
      <Td className="text-right">
        <Button
          size="sm"
          variant="ghost"
          icon={<Zap />}
          onClick={() => fire.mutate(a.name)}
          disabled={fire.isPending && fire.variables === a.name}
        >
          Run now
        </Button>
      </Td>
    </Tr>
  );
}
