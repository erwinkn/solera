import { useEffect, useId, useRef } from "react";
import type { ReactNode } from "react";
import type { Json, Status } from "./types";

const paths = {
  assets: "M3 3h7v7H3zM14 3h7v7h-7zM3 14h7v7H3zM14 14h7v7h-7z",
  runs: "m8 4 12 8-12 8z",
  backfills: "M3 11a9 9 0 1 1 2 7M3 4v7h7M12 7v5l3 2",
  automations: "m13 2-9 12h7l-1 8 10-12h-7z",
  search: "M21 21l-5-5M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0",
  graph: "M4 4h6v6H4zM14 14h6v6h-6zM10 7h7v7M7 10v7h7",
  table: "M3 4h18v16H3zM3 9h18M3 14h18M9 4v16",
  close: "m6 6 12 12M6 18 18 6",
  arrow: "M5 12h14m-5-5 5 5-5 5",
  chevron: "m9 5 7 7-7 7",
  check: "m5 12 4 4L19 6",
  refresh: "M20 8a8 8 0 0 0-14-3L3 8m0-5v5h5M4 16a8 8 0 0 0 14 3l3-3m0 5v-5h-5",
  pause: "M8 5v14M16 5v14",
  clock: "M12 7v5l3 2M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0",
  code: "m8 6-6 6 6 6m8-12 6 6-6 6m-3-14-2 16",
  logout: "M10 4H4v16h6m4-12 4 4-4 4M8 12h10",
} as const;
export type IconName = keyof typeof paths;
export function Icon({ name, size = 16 }: { name: IconName; size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d={paths[name]} />
    </svg>
  );
}

const labels: Record<Status, string> = {
  missing: "Not materialized",
  materialized: "Materialized",
  stale: "Stale",
  queued: "Queued",
  waiting: "Waiting",
  running: "Running",
  succeeded: "Succeeded",
  skipped: "Unchanged",
  failed: "Failed",
  blocked: "Blocked",
  canceled: "Canceled",
};
export function Badge({ status }: { status: Status }) {
  return (
    <span className={`badge ${status}`}>
      <span className="status-dot" />
      {labels[status]}
    </span>
  );
}
export function time(value: string | null) {
  return value
    ? new Intl.DateTimeFormat(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }).format(new Date(value))
    : "—";
}
export function duration(start: string | null, end: string | null) {
  if (!start) return "—";
  const ms = Math.max(
    0,
    new Date(end || Date.now()).getTime() - new Date(start).getTime(),
  );
  return ms < 1000
    ? `${ms} ms`
    : ms < 60000
      ? `${(ms / 1000).toFixed(1)} s`
      : `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}
export function go(path: string) {
  window.location.hash = path;
}
export function Empty({
  title,
  children,
  action,
}: {
  title: string;
  children: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className="empty">
      <Icon name="assets" size={25} />
      <h3>{title}</h3>
      <p>{children}</p>
      {action}
    </div>
  );
}
export function ErrorNotice({ message }: { message: string }) {
  return (
    <div className="error-notice" role="alert">
      {message}
    </div>
  );
}
export function Loading() {
  return (
    <div className="loading" role="status">
      <span className="loader" />
      Loading workspace data…
    </div>
  );
}

export function Dialog({
  title,
  children,
  close,
  drawer = false,
}: {
  title: string;
  children: ReactNode;
  close: () => void;
  drawer?: boolean;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const id = useId();
  useEffect(() => {
    if (ref.current && !ref.current.open) ref.current.showModal();
  }, []);
  return (
    <dialog
      ref={ref}
      className={drawer ? "dialog drawer" : "dialog"}
      aria-labelledby={id}
      onClose={close}
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          const r = event.currentTarget.getBoundingClientRect();
          if (
            event.clientX < r.left ||
            event.clientX > r.right ||
            event.clientY < r.top ||
            event.clientY > r.bottom
          )
            close();
        }
      }}
    >
      <div className="dialog-header">
        <h2 id={id}>{title}</h2>
        <button
          className="icon-button"
          aria-label="Close dialog"
          onClick={close}
        >
          <Icon name="close" />
        </button>
      </div>
      {children}
    </dialog>
  );
}

function display(value: Json | undefined) {
  return value === undefined || value === null
    ? "—"
    : typeof value === "object"
      ? JSON.stringify(value)
      : String(value);
}
export function DataPreview({ value }: { value: Json }) {
  if (
    Array.isArray(value) &&
    value.every(
      (row) => row !== null && typeof row === "object" && !Array.isArray(row),
    )
  ) {
    if (!value.length)
      return (
        <Empty title="Empty dataset">
          This materialization committed zero rows.
        </Empty>
      );
    const rows = value as Record<string, Json>[];
    const columns = Array.from(
      new Set(rows.flatMap((row) => Object.keys(row))),
    ).slice(0, 8);
    return (
      <>
        <div className="table-scroll">
          <table className="data-table">
            <thead>
              <tr>
                {columns.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={index}>
                  {columns.map((column) => (
                    <td key={column} title={display(row[column])}>
                      {display(row[column])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="caption">
          Committed snapshot preview · up to 20 rows and 8 columns
        </p>
      </>
    );
  }
  return <pre className="code-block">{JSON.stringify(value, null, 2)}</pre>;
}
