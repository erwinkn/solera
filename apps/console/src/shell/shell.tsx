import { Suspense, useState, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, Outlet, useRouter } from "@tanstack/react-router";
import { Dialog as BaseDialog } from "@base-ui/react/dialog";
import {
  Activity,
  Boxes,
  CalendarClock,
  Cpu,
  Database,
  HeartPulse,
  LayoutDashboard,
  LogOut,
  Menu as MenuIcon,
  Radar,
  Search,
  X,
} from "lucide-react";
import { q } from "@/api/queries";
import { cn } from "@/lib/cn";
import { connect, disconnect, useSession } from "@/lib/session";
import { setTheme, THEME_NAMES, THEMES, useTheme, type Theme } from "@/lib/theme";
import { toneSoft, type Tone } from "@/lib/status";
import { Button, IconButton } from "@/ui/button";
import { Input, Select } from "@/ui/form";
import { TooltipProvider } from "@/ui/overlay";
import { StatusDot } from "@/ui/status";
import { Toasts } from "./toasts";
import { Palette } from "./palette";
import { palette } from "@/lib/palette";
import { Kbd } from "@/ui/data";

export function Shell() {
  const { locked } = useSession();
  return (
    <TooltipProvider delay={350}>
      <a
        href="#main"
        className="sr-only z-50 rounded-sm bg-surface px-3 py-2 focus:not-sr-only focus:fixed focus:top-2 focus:left-2"
      >
        Skip to content
      </a>
      {locked ? (
        <Connect />
      ) : (
        <div className="min-h-dvh lg:grid lg:grid-cols-[232px_minmax(0,1fr)]">
          <div data-chrome className="hidden border-r border-line bg-[var(--chrome-bg)] lg:block">
            <aside className="sticky top-0 flex h-dvh flex-col">
              <Sidebar />
            </aside>
          </div>
          <MobileBar />
          <main id="main" className="min-w-0">
            <Outlet />
          </main>
        </div>
      )}
      {!locked && <Palette />}
      <Toasts />
    </TooltipProvider>
  );
}

function Sidebar({ onNavigate }: { onNavigate?: () => void }) {
  return (
    <div className="flex h-full flex-col gap-6 px-3 py-4">
      <Brand />
      <button
        type="button"
        onClick={() => {
          onNavigate?.();
          palette.set(true);
        }}
        className="flex h-8 items-center gap-2 rounded-sm border-theme border-line bg-surface px-2.5 text-sm text-fg-subtle motion-1 transition-colors hover:text-fg [&>svg]:size-3.5"
      >
        <Search aria-hidden />
        <span className="flex-1 text-left">Jump to…</span>
        <Kbd>⌘K</Kbd>
      </button>
      <Nav onNavigate={onNavigate} />
      <div className="mt-auto flex flex-col gap-3">
        <EngineStatus />
        <ThemeSwitch />
        <SessionControl />
      </div>
    </div>
  );
}

function Brand() {
  const { data } = useQuery(q.diagnostics());
  return (
    <Link to="/" className="flex items-center gap-2.5 rounded-sm px-2 py-1">
      <Mark />
      <span className="flex min-w-0 flex-col leading-tight">
        <span className="font-display text-lg text-fg">solera</span>
        <span className="truncate font-mono text-2xs text-fg-subtle">
          {data ? `${data.project} · ${data.namespace}` : " "}
        </span>
      </span>
    </Link>
  );
}

function Mark() {
  return (
    <svg viewBox="0 0 32 32" aria-hidden className="size-7 shrink-0">
      <rect width="32" height="32" rx="8" fill="var(--fg)" />
      <g fill="var(--warn)" stroke="var(--fg)" strokeWidth="1.4">
        <circle cx="11.5" cy="12" r="4.6" />
        <circle cx="20.5" cy="12" r="4.6" />
        <circle cx="7" cy="20.5" r="4.6" />
        <circle cx="16" cy="20.5" r="4.6" />
        <circle cx="25" cy="20.5" r="4.6" />
      </g>
    </svg>
  );
}

function useAttention() {
  const diagnostics = useQuery(q.diagnostics()).data;
  const project = diagnostics?.project;
  const holds = useQuery({
    ...q.holds(project ?? ""),
    enabled: !!project,
  }).data;
  const status = useQuery({
    ...q.assetStatus(project ?? ""),
    enabled: !!project,
  }).data;
  const failing = status
    ? Object.values(status).filter(
        (a) => a.partitions.failed > 0 || Object.values(a.failures ?? {}).some((n) => (n ?? 0) > 0),
      ).length
    : 0;
  const leftover = holds ? holds.unsettled.length + holds.discards.length : 0;
  return { running: diagnostics?.active_runs ?? 0, failing, leftover };
}

function Nav({ onNavigate }: { onNavigate?: () => void }) {
  const { running, failing, leftover } = useAttention();
  const items: {
    to: string;
    label: string;
    icon: ReactNode;
    badge?: number;
    tone?: Tone;
    exact?: boolean;
  }[] = [
    { to: "/", label: "Overview", icon: <LayoutDashboard />, exact: true },
    {
      to: "/assets",
      label: "Assets",
      icon: <Boxes />,
      badge: failing,
      tone: "fail",
    },
    {
      to: "/runs",
      label: "Runs",
      icon: <Activity />,
      badge: running,
      tone: "run",
    },
    { to: "/automations", label: "Automations", icon: <CalendarClock /> },
    { to: "/sensors", label: "Sensors", icon: <Radar /> },
    { to: "/sources", label: "Sources", icon: <Database /> },
    { to: "/executors", label: "Executors", icon: <Cpu /> },
    {
      to: "/health",
      label: "Health",
      icon: <HeartPulse />,
      badge: leftover,
      tone: "warn",
    },
  ];
  return (
    <nav aria-label="Main" className="flex flex-col gap-0.5">
      {items.map((item) => (
        <Link
          key={item.to}
          to={item.to}
          onClick={onNavigate}
          activeOptions={{ exact: item.exact ?? false }}
          className={cn(
            "group flex h-8 items-center gap-2.5 rounded-sm px-2.5 text-sm text-fg-muted motion-1 transition-colors",
            "hover:bg-accent-soft hover:text-fg [&_svg]:size-4 [&_svg]:shrink-0",
            "data-[status=active]:bg-accent-soft data-[status=active]:font-medium data-[status=active]:text-[var(--nav-active-fg)]",
            "data-[status=active]:shadow-[inset_2px_0_0_var(--nav-bar)]",
            "border-b border-[var(--nav-divider)]",
          )}
        >
          {item.icon}
          <span className="flex-1">{item.label}</span>
          {!!item.badge && (
            <span
              className={cn(
                "min-w-5 rounded-full px-1.5 text-center text-2xs leading-5 font-semibold tabular",
                toneSoft[item.tone ?? "idle"],
              )}
              aria-label={`${item.badge} need attention`}
            >
              {item.badge}
            </span>
          )}
        </Link>
      ))}
    </nav>
  );
}

function EngineStatus() {
  const { data: health, isError } = useQuery(q.health());
  const diagnostics = useQuery(q.diagnostics()).data;
  // A failed check is an answer too: the engine is unreachable, whatever it said before.
  const healthy = !isError && (health?.ok ?? true);
  const tone: Tone = isError
    ? "fail"
    : !health
      ? "idle"
      : !healthy
        ? "fail"
        : diagnostics?.last_error
          ? "warn"
          : "ok";
  const text =
    !health && !isError
      ? "Connecting…"
      : !healthy
        ? "Engine unavailable"
        : diagnostics?.last_error
          ? "Engine degraded"
          : "Engine healthy";
  return (
    <Link
      to="/health"
      className="flex items-center gap-2 rounded-sm px-2.5 py-1 text-xs text-fg-muted hover:text-fg"
    >
      <StatusDot tone={tone} pulse={tone === "ok"} />
      <span className="flex-1">{text}</span>
      {diagnostics && (
        <span className="font-mono text-2xs text-fg-subtle">{diagnostics.revision.slice(0, 7)}</span>
      )}
    </Link>
  );
}

function ThemeSwitch() {
  const theme = useTheme();
  return (
    <label className="flex items-center justify-between gap-2 px-2.5">
      <span className="text-xs text-fg-subtle">Theme</span>
      <Select
        aria-label="Theme"
        className="h-7 w-36 text-xs"
        value={theme}
        onChange={(e) => setTheme(e.target.value as Theme)}
      >
        {THEMES.map((t) => (
          <option key={t} value={t}>
            {THEME_NAMES[t]}
          </option>
        ))}
      </Select>
    </label>
  );
}

function SessionControl() {
  const { token } = useSession();
  const client = useQueryClient();
  if (!token) return null;
  return (
    <button
      type="button"
      onClick={() => {
        disconnect();
        client.clear();
      }}
      className="flex items-center gap-2 rounded-sm px-2.5 py-1 text-xs text-fg-subtle hover:text-fg [&_svg]:size-3.5"
    >
      <LogOut aria-hidden /> Forget token
    </button>
  );
}

function MobileBar() {
  const [open, setOpen] = useState(false);
  return (
    <div
      data-chrome
      className="sticky top-0 z-30 flex h-12 items-center justify-between border-b border-line bg-[var(--chrome-bg)] px-3 backdrop-blur lg:hidden"
    >
      <Brand />
      <BaseDialog.Root open={open} onOpenChange={setOpen}>
        <BaseDialog.Trigger render={<IconButton label="Open navigation" />}>
          <MenuIcon />
        </BaseDialog.Trigger>
        <BaseDialog.Portal>
          <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-overlay transition-opacity motion-2 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
          <BaseDialog.Popup
            data-chrome
            aria-label="Navigation"
            className="fixed inset-y-0 left-0 z-50 w-[min(280px,85vw)] border-r border-line-strong bg-surface shadow-3 transition-transform motion-2 data-[ending-style]:-translate-x-full data-[starting-style]:-translate-x-full"
          >
            <BaseDialog.Close
              render={<IconButton label="Close navigation" className="absolute top-3 right-3" />}
            >
              <X />
            </BaseDialog.Close>
            <Suspense>
              <Sidebar onNavigate={() => setOpen(false)} />
            </Suspense>
          </BaseDialog.Popup>
        </BaseDialog.Portal>
      </BaseDialog.Root>
    </div>
  );
}

/** Shown when the server asks for a token: nothing else renders until it has one. */
function Connect() {
  const client = useQueryClient();
  const router = useRouter();
  const { token } = useSession();
  return (
    <main id="main" className="grid min-h-dvh place-items-center px-4">
      <form
        className="flex w-full max-w-sm flex-col gap-5 rounded-lg border-theme border-line-strong bg-surface p-6 shadow-3"
        onSubmit={(event) => {
          event.preventDefault();
          const value = new FormData(event.currentTarget).get("token");
          if (typeof value !== "string" || !value.trim()) return;
          connect(value.trim());
          client.clear();
          void router.invalidate();
        }}
      >
        <div className="flex items-center gap-3">
          <Mark />
          <div>
            <h1 className="font-display text-xl">Connect to Solera</h1>
            <p className="text-sm text-fg-muted">
              {token ? "That token was refused. Try another." : "This server needs its API token."}
            </p>
          </div>
        </div>
        <label className="flex flex-col gap-1.5">
          <span className="text-xs font-medium text-fg-muted">API token</span>
          <Input
            name="token"
            type="password"
            autoComplete="off"
            autoFocus
            aria-label="API token"
            placeholder="SOLERA_API_TOKEN"
          />
        </label>
        <Button type="submit" variant="primary">
          Connect
        </Button>
        <p className="text-xs text-fg-subtle">Kept for this browser tab only.</p>
      </form>
    </main>
  );
}
