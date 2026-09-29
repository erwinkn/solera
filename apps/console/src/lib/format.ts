export function time(value: number | null | undefined) {
  return value
    ? new Intl.DateTimeFormat(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }).format(new Date(value * 1000))
    : "—";
}

export function duration(
  start: number | null | undefined,
  end?: number | null,
) {
  if (!start) return "—";
  const ms = Math.max(0, (end ?? Date.now() / 1000) * 1000 - start * 1000);
  return ms < 1000
    ? `${Math.round(ms)} ms`
    : ms < 60000
      ? `${(ms / 1000).toFixed(1)} s`
      : `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

export function describeInterval(seconds: number) {
  if (seconds >= 3600 && seconds % 3600 === 0)
    return `${seconds / 3600} hour${seconds === 3600 ? "" : "s"}`;
  if (seconds >= 60 && seconds % 60 === 0)
    return `${seconds / 60} minute${seconds === 60 ? "" : "s"}`;
  return `${seconds} second${seconds === 1 ? "" : "s"}`;
}

/** A length of time in seconds, at the precision that matters for it. */
export function seconds(value: number | null | undefined) {
  if (value == null) return "—";
  if (value < 1) return `${Math.round(value * 1000)} ms`;
  if (value < 60) return `${value.toFixed(value < 10 ? 1 : 0)} s`;
  if (value < 3600)
    return `${Math.floor(value / 60)}m ${Math.round(value % 60)}s`;
  return `${Math.floor(value / 3600)}h ${Math.round((value % 3600) / 60)}m`;
}

export function count(value: number | null | undefined) {
  if (value == null) return "—";
  return new Intl.NumberFormat(undefined, {
    notation: value >= 10000 ? "compact" : "standard",
    maximumFractionDigits: 1,
  }).format(value);
}

/** A bucket start as a label: the time of day for buckets under a day, the
    date otherwise. */
export function bucketLabel(t: number, bucket: number) {
  return new Intl.DateTimeFormat(
    undefined,
    bucket < 86400
      ? { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }
      : { month: "short", day: "numeric" },
  ).format(new Date(t * 1000));
}

/** A time window, sharing what its ends share: `Sep 29, 3:00 – 6:00 PM`, or
    by day `Sep 22 – 28` (the last day included, not the midnight after it). */
export function windowLabel(since: number, until: number) {
  const days = until - since >= 86400;
  return new Intl.DateTimeFormat(
    undefined,
    days
      ? { month: "short", day: "numeric" }
      : { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" },
  ).formatRange(
    new Date(since * 1000),
    new Date((until - (days ? 1 : 0)) * 1000),
  );
}

/** Failures out of a total: `0`, or `3 · 12%` — never a misleading `0%`. */
export function failures(failed: number, total: number) {
  if (!failed) return "0";
  const pct = (failed / total) * 100;
  return `${count(failed)} · ${pct < 1 ? "<1" : Math.round(pct)}%`;
}
