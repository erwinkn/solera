import type { ComponentProps, ReactNode } from "react";
import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/cn";

/** The page's content column. */
export function Page({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      className={cn("mx-auto flex w-full max-w-[1400px] flex-col gap-6 px-4 py-6 sm:px-8 sm:py-8", className)}
      {...props}
    />
  );
}

export function PageHeader({
  title,
  ident,
  badge,
  eyebrow,
  description,
  actions,
  meta,
}: {
  title: ReactNode;
  /** The title is a name (an asset, a run's targets): never case-transformed by a theme. */
  ident?: boolean;
  /** Shown beside the title, outside its decoration: a status, a kind. */
  badge?: ReactNode;
  eyebrow?: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  meta?: ReactNode;
}) {
  return (
    <header className="flex flex-col gap-3">
      {eyebrow && (
        <nav aria-label="Breadcrumb" className="flex items-center gap-1 text-xs text-fg-subtle">
          {eyebrow}
        </nav>
      )}
      <div className="flex flex-wrap items-end justify-between gap-x-6 gap-y-3">
        <div className="flex min-w-0 flex-col gap-1.5">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
            <h1
              className={cn(
                "title-mark w-fit font-display text-2xl break-all text-fg",
                ident && "normal-case",
              )}
            >
              {title}
            </h1>
            {badge}
          </div>
          {description && <p className="max-w-3xl text-sm text-fg-muted">{description}</p>}
        </div>
        {actions && <div className="flex flex-wrap items-center gap-2">{actions}</div>}
      </div>
      {meta && (
        <div className="flex flex-wrap items-center gap-x-5 gap-y-1.5 text-xs text-fg-muted">{meta}</div>
      )}
    </header>
  );
}

export function Crumb({ children, last }: { children: ReactNode; last?: boolean }) {
  return (
    <>
      <span className={cn(last && "text-fg-muted")}>{children}</span>
      {!last && <ChevronRight aria-hidden className="size-3" />}
    </>
  );
}

/** A labelled fact in a page header's meta line. */
export function Meta({ label, children }: { label: string; children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className="text-fg-subtle">{label}</span>
      <span className="text-fg">{children}</span>
    </span>
  );
}

export function Card({ className, ...props }: ComponentProps<"section">) {
  return (
    <section
      className={cn("min-w-0 rounded-lg border-theme border-line-strong bg-surface shadow-2", className)}
      {...props}
    />
  );
}

export function CardHeader({
  title,
  ident,
  description,
  actions,
  className,
  id,
}: {
  title: ReactNode;
  /** The title is a name: never case-transformed by a theme. */
  ident?: boolean;
  description?: ReactNode;
  actions?: ReactNode;
  className?: string;
  id?: string;
}) {
  return (
    <div
      className={cn(
        "flex flex-wrap items-center justify-between gap-x-4 gap-y-2 px-4 pt-3.5 pb-3",
        className,
      )}
    >
      <div className="flex min-w-0 flex-col gap-0.5">
        <h2 id={id} className={cn("font-display text-base text-fg", ident && "normal-case")}>
          {title}
        </h2>
        {description && <p className="text-xs text-fg-subtle">{description}</p>}
      </div>
      {actions && <div className="flex items-center gap-2">{actions}</div>}
    </div>
  );
}

/** A small fact block: label over a value. */
export function Fact({
  label,
  children,
  className,
}: {
  label: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("flex min-w-0 flex-col gap-0.5", className)}>
      <dt className="text-2xs font-medium tracking-wide text-fg-subtle uppercase">{label}</dt>
      <dd className="min-w-0 text-sm text-fg">{children}</dd>
    </div>
  );
}

export function Facts({ className, ...props }: ComponentProps<"dl">) {
  return (
    <dl
      className={cn("grid grid-cols-[repeat(auto-fill,minmax(150px,1fr))] gap-x-6 gap-y-4", className)}
      {...props}
    />
  );
}
