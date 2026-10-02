import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ArrowDownToLine, Pause } from "lucide-react";
import { ACTIVE_ATTEMPT, q, useProject } from "@/api/queries";
import type { Attempt, Json, LogLine } from "@/api/types";
import { cn } from "@/lib/cn";
import { clock, count } from "@/lib/format";
import { Button } from "@/ui/button";
import { Empty, ErrorNote, Skeleton } from "@/ui/data";
import { SearchInput, Segmented } from "@/ui/form";

const LEVELS = ["all", "info", "warning", "error"] as const;
type Level = (typeof LEVELS)[number];
const RANK: Record<string, number> = {
  debug: 0,
  info: 1,
  warning: 2,
  warn: 2,
  error: 3,
  critical: 4,
};

const levelClass: Record<string, string> = {
  debug: "text-fg-subtle",
  info: "text-fg-subtle",
  warning: "text-warn-fg bg-warn-soft",
  warn: "text-warn-fg bg-warn-soft",
  error: "text-fail-fg bg-fail-soft",
  critical: "text-fail-fg bg-fail-soft",
};

/**
 * An attempt's log. A running attempt tails: the last lines, polled every
 * second, following the bottom unless you pause it. A finished one is read
 * once. Filtering is by level and text; `key=` fields from per-key calls
 * are shown as tags.
 */
export function Logs({
  run,
  attempt,
  level,
  text,
  onFilter,
}: {
  run: string;
  attempt: Attempt;
  level: Level;
  text: string;
  onFilter: (patch: { level?: Level; text?: string }) => void;
}) {
  const project = useProject();
  const live = ACTIVE_ATTEMPT.has(attempt.status);
  const [all, setAll] = useState(false);
  const [follow, setFollow] = useState(true);
  const tail = all ? null : 2000;
  const { data, error, isPending } = useQuery(q.attemptLogs(project, run, attempt, tail));

  const lines = (data ?? []).filter(
    (line) =>
      (level === "all" || (RANK[line.level] ?? 1) >= RANK[level]!) &&
      (!text ||
        `${line.message} ${JSON.stringify(line.fields ?? {})}`.toLowerCase().includes(text.toLowerCase())),
  );
  const start = data?.find((l) => l.at)?.at ?? attempt.started_at ?? 0;

  return (
    <div className="flex flex-col">
      <div className="flex flex-wrap items-center gap-2 border-b border-line px-4 py-2.5">
        <Segmented
          size="sm"
          label="Log level"
          value={level}
          onChange={(v) => onFilter({ level: v })}
          options={LEVELS.map((l) => ({
            value: l,
            label: l === "all" ? "All" : l === "warning" ? "Warn+" : l === "error" ? "Errors" : "Info+",
          }))}
        />
        <SearchInput
          aria-label="Search the log"
          placeholder="Search the log"
          value={text}
          onChange={(e) => onFilter({ text: e.target.value })}
          className="w-56"
        />
        <span className="ml-auto flex items-center gap-2 text-xs text-fg-subtle tabular">
          {data &&
            `${count(lines.length)}${lines.length !== data.length ? ` of ${count(data.length)}` : ""} lines`}
          {live && (
            <Button
              size="sm"
              variant="ghost"
              icon={follow ? <Pause /> : <ArrowDownToLine />}
              onClick={() => setFollow(!follow)}
            >
              {follow ? "Pause" : "Follow"}
            </Button>
          )}
          {!all && data && data.length >= 2000 && (
            <Button size="sm" variant="ghost" onClick={() => setAll(true)}>
              Load everything
            </Button>
          )}
        </span>
      </div>
      {error ? (
        <div className="p-4">
          <ErrorNote error={error} title="Couldn't read the log" />
        </div>
      ) : isPending ? (
        <div className="flex flex-col gap-1.5 p-4">
          {[0, 1, 2, 3, 4].map((i) => (
            <Skeleton key={i} className="h-4" />
          ))}
        </div>
      ) : lines.length === 0 ? (
        <Empty
          compact
          title={
            data?.length ? "No line matches" : live ? "Waiting for output…" : "This attempt logged nothing"
          }
        />
      ) : (
        <div
          role="log"
          aria-live={live && follow ? "polite" : "off"}
          className="max-h-[32rem] overflow-auto bg-sunken py-1.5 font-mono text-xs leading-5"
        >
          {lines.map((line, i) => (
            <Line key={i} line={line} start={start} />
          ))}
          {/* Remounts with every new line; when following, its ref keeps the bottom in view. */}
          <div
            key={data?.length}
            ref={live && follow ? (el) => el?.scrollIntoView({ block: "nearest" }) : undefined}
          />
        </div>
      )}
    </div>
  );
}

function Line({ line, start }: { line: LogLine; start: number }) {
  const fields = Object.entries(line.fields ?? {});
  const key = fields.find(([k]) => k === "key")?.[1];
  return (
    <div className="grid grid-cols-[4.5rem_3.25rem_minmax(0,1fr)] gap-x-3 px-4 hover:bg-surface-2/60">
      <span className="text-fg-subtle tabular" title={line.at ? clock(line.at) : undefined}>
        {line.at ? `+${(line.at - start).toFixed(2)}s` : ""}
      </span>
      <span
        className={cn(
          "h-5 self-start rounded-xs px-1 text-center text-2xs leading-5 uppercase",
          levelClass[line.level] ?? "text-fg-subtle",
        )}
      >
        {line.level === "warning" ? "warn" : line.level}
      </span>
      <span className="break-words whitespace-pre-wrap text-fg">
        {key !== undefined && (
          <span className="mr-2 rounded-xs bg-accent-soft px-1 text-fg-muted">{String(key)}</span>
        )}
        {line.message}
        {fields
          .filter(([k]) => k !== "key")
          .map(([k, v]) => (
            <span key={k} className="ml-2 text-fg-subtle">
              {k}=<span className="text-fg-muted">{show(v)}</span>
            </span>
          ))}
      </span>
    </div>
  );
}

const show = (v: Json) => (typeof v === "string" ? v : JSON.stringify(v));

export type { Level as LogLevel };
