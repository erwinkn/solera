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
