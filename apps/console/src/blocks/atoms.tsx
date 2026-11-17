import { AlertTriangle, CircleAlert, Info } from "lucide-react"
import type { ReactNode } from "react"
import { Link, useLocation, useSearch } from "wouter"
import { ago, shortId, when } from "../model/format"
import type { Status, Tone } from "../model/status"
import { MarkIcon, TONE_MARK } from "../ui/status"

/* Small pieces every page uses. */

export function StatusPill({ status, quiet }: { status: Status; quiet?: boolean }) {
  return (
    <span className="status" data-quiet={quiet ? "" : undefined}>
      <MarkIcon mark={TONE_MARK[status.tone]} />
      {status.label}
    </span>
  )
}

export function Dot({ tone }: { tone: Tone }) {
  return <MarkIcon mark={TONE_MARK[tone]} />
}

export const AssetLink = ({ name }: { name: string }) => (
  <Link href={`/assets/${encodeURIComponent(name)}`} className="name">
    {name}
  </Link>
)

export const SourceLink = ({ name }: { name: string }) => (
  <Link href={`/sources/${encodeURIComponent(name)}`} className="name">
    {name}
  </Link>
)

export const RunLink = ({ id, at }: { id: string; at?: string }) => (
  <Link href={`/runs/${id}${at ? `?at=${encodeURIComponent(at)}` : ""}`} className="ref">
    {shortId(id)}
  </Link>
)

/** A moment, as "3m ago", with the exact time on hover. */
export function Ago({ at, now }: { at: number | null | undefined; now: number }) {
  if (at === null || at === undefined) return <span className="faint">never</span>
  return (
    <time className="num" dateTime={new Date(at * 1000).toISOString()} title={when(at, now)}>
      {ago(at, now)}
    </time>
  )
}

export function Notice({ tone, children, action }: { tone: Tone; children: ReactNode; action?: ReactNode }) {
  const Icon = tone === "failed" ? CircleAlert : tone === "stale" ? AlertTriangle : Info
  return (
    <div className="notice" data-tone={tone} role={tone === "failed" ? "alert" : "status"}>
      <Icon size={15} aria-hidden="true" />
      <div className="grow">{children}</div>
      {action}
    </div>
  )
}

export function Empty({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="empty">
      <strong>{title}</strong>
      {children}
    </div>
  )
}

/** A section of a page: a title with what goes beside it, then the content. `flush` content runs edge to edge, as a table does. */
export function Panel({ title, count, actions, children, flush, className }: { title?: ReactNode; count?: ReactNode; actions?: ReactNode; children: ReactNode; flush?: boolean; className?: string }) {
  return (
    <section className={`panel ${className ?? ""}`}>
      {(title || actions) && (
        <header className="panel-head">
          <h2 className="panel-title truncate">
            {title}
            {count !== undefined && count !== null && <span className="panel-count num">{count}</span>}
          </h2>
          <span className="grow" />
          {actions}
        </header>
      )}
      <div className={flush ? undefined : "panel-body"}>{children}</div>
    </section>
  )
}

export function Facts({ rows }: { rows: [string, ReactNode][] }) {
  return (
    <dl className="facts">
      {rows
        .filter(([, value]) => value !== null && value !== undefined && value !== "")
        .map(([name, value]) => (
          <div key={name} style={{ display: "contents" }}>
            <dt>{name}</dt>
            <dd>{value}</dd>
          </div>
        ))}
    </dl>
  )
}

export function Loading({ what = "Loading" }: { what?: string }) {
  return <div className="empty faint">{what}…</div>
}

export function Failure({ error }: { error: unknown }) {
  return (
    <div className="failure">
      <Notice tone="failed">{(error as Error)?.message ?? "Something went wrong."}</Notice>
    </div>
  )
}

/** One value kept in the address bar, so a view can be linked to and survives a reload. */
export function useParam(name: string, fallback = ""): [string, (value: string | null) => void] {
  const search = useSearch()
  const [location, navigate] = useLocation()
  const value = new URLSearchParams(search).get(name) ?? fallback
  const set = (next: string | null) => {
    const params = new URLSearchParams(window.location.search)
    if (next === null || next === fallback) params.delete(name)
    else params.set(name, next)
    const text = params.toString()
    navigate(text ? `${location}?${text}` : location, { replace: true })
  }
  return [value, set]
}
