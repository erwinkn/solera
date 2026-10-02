/** Formatting for times, sizes and identifiers. Times are epoch seconds. */

const numberFormat = new Intl.NumberFormat("en-US");
const compactFormat = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

export const count = (n: number) => numberFormat.format(n);
export const compact = (n: number) =>
  Math.abs(n) < 10_000 ? numberFormat.format(n) : compactFormat.format(n);

export function plural(n: number, one: string, many = `${one}s`) {
  return `${count(n)} ${n === 1 ? one : many}`;
}

/** 0.3s · 4.2s · 1m 05s · 2h 03m · 3d 4h */
export function duration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds)) return "—";
  const s = Math.max(0, seconds);
  if (s < 0.01) return "0s";
  if (s < 10) return `${s.toFixed(s < 1 ? 2 : 1).replace(/\.?0+$/, "")}s`;
  if (s < 60) return `${Math.round(s)}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${String(Math.round(s % 60)).padStart(2, "0")}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${String(m % 60).padStart(2, "0")}m`;
  const d = Math.floor(h / 24);
  return `${d}d ${h % 24}h`;
}

/** A coarse duration for "4m ago" / "in 4m". */
function span(seconds: number): string {
  const s = Math.abs(seconds);
  if (s < 60) return `${Math.max(1, Math.round(s))}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 86400 * 2) return `${Math.round(s / 3600)}h`;
  return `${Math.round(s / 86400)}d`;
}

export function ago(at: number | null | undefined, now: number): string {
  if (at == null) return "never";
  const delta = now - at;
  if (delta < 3) return "just now";
  if (delta < 0) return `in ${span(delta)}`;
  if (delta > 86400 * 30) return date(at);
  return `${span(delta)} ago`;
}

export function until(at: number | null | undefined, now: number): string {
  if (at == null) return "—";
  const delta = at - now;
  if (delta <= 1) return "due now";
  return `in ${span(delta)}`;
}

const timeFormat = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});
const dateFormat = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
});
const fullFormat = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  timeZoneName: "short",
});

export const clock = (at: number) => timeFormat.format(at * 1000);
export const date = (at: number) => dateFormat.format(at * 1000);
export const stamp = (at: number) => fullFormat.format(at * 1000);
export function dateTime(at: number, now: number) {
  return now - at < 86400 * 0.75 ? clock(at) : `${date(at)}, ${clock(at)}`;
}

/** ULIDs share their time prefix with every neighbour: show the random tail. */
export const shortId = (id: string) => (id.length > 12 ? id.slice(-7) : id);
export const shortHash = (hex: string | null | undefined) => (hex ? hex.slice(0, 8) : "—");

export function bytes(n: number | null | undefined): string {
  if (n == null) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v < 10 && i > 0 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
}

export function percent(part: number, whole: number) {
  return whole > 0 ? `${Math.round((100 * part) / whole)}%` : "—";
}

/** Seconds as a compact interval: "every 10s", "every 5m". */
export function interval(seconds: number): string {
  if (seconds % 86400 === 0) return `${seconds / 86400}d`;
  if (seconds % 3600 === 0) return `${seconds / 3600}h`;
  if (seconds % 60 === 0) return `${seconds / 60}m`;
  return `${seconds}s`;
}

export function firstLine(text: string | null | undefined): string {
  if (!text) return "";
  const line =
    text
      .trim()
      .split("\n")
      .find((l) => l.trim()) ?? "";
  return line.length > 240 ? `${line.slice(0, 239)}…` : line;
}
