import { useInfiniteQuery } from "@tanstack/react-query";
import { q, useProject } from "@/api/queries";
import { staleKey } from "@/api/read";
import { cn } from "@/lib/cn";
import { plural } from "@/lib/format";
import { staleReason } from "@/lib/status";
import { Button } from "@/ui/button";
import { Card, CardHeader } from "@/ui/layout";
import { Tooltip } from "@/ui/overlay";

/**
 * Why a partition or key is stale (glossary, "stale"), one chip per reason,
 * each explained on hover. Staleness is computed on demand: these are the
 * engine's answer now, not a stored state.
 */
export function StaleReasons({ reasons, className }: { reasons: string[] | undefined; className?: string }) {
  if (!reasons?.length) return null;
  return (
    <span className={cn("inline-flex flex-wrap items-center gap-1", className)}>
      {reasons.map((r) => {
        const reason = staleReason(r);
        return (
          <Tooltip key={r} content={reason.means}>
            <span className="inline-flex h-5 items-center rounded-full border-theme border-warn/40 px-2 text-xs whitespace-nowrap text-warn-fg">
              {reason.label}
            </span>
          </Tooltip>
        );
      })}
    </span>
  );
}

/** The same reasons as plain words, for tooltips and one-line summaries. */
export const reasonsText = (reasons: string[] | undefined) =>
  (reasons ?? []).map((r) => staleReason(r).label).join(", ");

/**
 * A stale partition's keys and why each is stale. For an `each` asset they
 * are its own keys, traced one to one to their upstream keys; a keyed asset
 * that isn't `each` has all its keys stale together. A default run of the
 * partition loads exactly these.
 */
export function StaleKeysCard({
  name,
  partition,
  className,
}: {
  name: string;
  partition: string;
  className?: string;
}) {
  const project = useProject();
  const pages = useInfiniteQuery(q.staleKeys(project, name, partition));
  const first = pages.data?.pages[0];
  // Nothing to show for an unkeyed asset, or a partition that isn't stale.
  if (pages.isError || !first || !first.tracked || first.reasons.length === 0) return null;
  const keys = pages.data?.pages.flatMap((page) => page.keys.map((k) => staleKey(k, page.reasons))) ?? [];
  return (
    <Card className={className}>
      <CardHeader
        title="Stale keys"
        description={
          <>
            {keys.length
              ? `${plural(keys.length, "key")}${pages.hasNextPage ? " so far" : ""}${partition ? ` in ${partition}` : ""}. A run of the ${partition ? "partition" : "asset"} loads exactly these.`
              : "No single key is stale: the partition is, as a whole."}{" "}
            <StaleReasons reasons={first.reasons} className="ml-1 align-middle" />
          </>
        }
      />
      {keys.length > 0 && (
        <ul className="flex max-h-80 flex-col divide-y divide-line overflow-y-auto border-t border-line">
          {keys.map((k) => (
            <li
              key={k.key}
              className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1 px-4 py-1.5"
            >
              <span className="truncate font-mono text-xs text-fg">{k.key}</span>
              <StaleReasons reasons={k.reasons} />
            </li>
          ))}
        </ul>
      )}
      {pages.hasNextPage && (
        <div className="flex justify-end border-t border-line px-4 py-2">
          <Button size="sm" onClick={() => pages.fetchNextPage()} disabled={pages.isFetchingNextPage}>
            Load more
          </Button>
        </div>
      )}
    </Card>
  );
}
