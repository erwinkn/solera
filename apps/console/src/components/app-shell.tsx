import { useState } from "react";
import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import {
  Cpu,
  Database,
  Inbox,
  LayoutGrid,
  LogOut,
  Moon,
  Play,
  Sun,
  Workflow,
  Zap,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ApiError, request, useAction } from "@/lib/api";
import { useTheme } from "@/lib/theme";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import { ErrorNotice, Segmented } from "./common";

const navigation = [
  { to: "/assets", label: "Assets", icon: LayoutGrid },
  { to: "/runs", label: "Runs", icon: Play },
  { to: "/automations", label: "Automations", icon: Zap },
  { to: "/sources", label: "Sources", icon: Inbox },
  { to: "/executors", label: "Executors", icon: Cpu },
  { to: "/storage", label: "Storage", icon: Database },
] as const;

function Brand({ project }: { project: string }) {
  return (
    <Link
      to="/assets"
      aria-label="Solera home"
      className="flex items-center gap-2.5"
    >
      <span className="flex size-8 items-center justify-center rounded-lg bg-gradient-to-br from-indigo-500 to-violet-600 text-white shadow-sm">
        <Workflow className="size-4" />
      </span>
      <span className="leading-tight font-heading font-semibold">
        Solera
        <span className="block text-[0.6rem] font-medium tracking-[0.12em] text-muted-foreground uppercase">
          {project}
        </span>
      </span>
    </Link>
  );
}

function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  return (
    <Segmented
      ariaLabel="Theme"
      value={theme}
      onChange={setTheme}
      options={[
        { value: "light", label: "Light", icon: <Sun className="size-3.5" /> },
        { value: "dark", label: "Dark", icon: <Moon className="size-3.5" /> },
      ]}
    />
  );
}

export function Login() {
  const [value, setValue] = useState("");
  const action = useAction();
  const workspace = useWorkspace();
  return (
    <main className="flex min-h-dvh items-center justify-center bg-muted/30 p-4">
      <Card className="w-full max-w-sm">
        <form
          onSubmit={async (event) => {
            event.preventDefault();
            sessionStorage.setItem("solera-token", value);
            const result = await action.run(async () => {
              try {
                return await request("/diagnostics");
              } catch (failure) {
                sessionStorage.removeItem("solera-token");
                throw failure;
              }
            });
            if (result) workspace.refresh();
          }}
        >
          <CardHeader>
            <div className="mb-2 flex size-9 items-center justify-center rounded-lg bg-gradient-to-br from-indigo-500 to-violet-600 text-white">
              <Workflow className="size-5" />
            </div>
            <CardTitle className="text-lg">Connect to your workspace</CardTitle>
            <CardDescription>
              Enter the API token configured for this Solera instance.
            </CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            <Label htmlFor="token">API token</Label>
            <Input
              id="token"
              autoFocus
              required
              type="password"
              autoComplete="off"
              value={value}
              onChange={(event) => setValue(event.target.value)}
            />
            {action.error && <ErrorNotice message={action.error} />}
          </CardContent>
          <CardFooter className="flex-col items-stretch gap-3">
            <Button type="submit" disabled={action.pending}>
              {action.pending ? "Connecting…" : "Connect"}
            </Button>
            <p className="text-center text-xs text-muted-foreground">
              Stored for this browser tab only. No cloud account is required.
            </p>
          </CardFooter>
        </form>
      </Card>
    </main>
  );
}

function ConnectionDot({ offline }: { offline: boolean }) {
  return (
    <span className="relative flex size-2">
      {!offline && (
        <span className="absolute inline-flex size-full animate-ping rounded-full bg-emerald-500/60" />
      )}
      <span
        className={cn(
          "relative inline-flex size-2 rounded-full",
          offline ? "bg-red-500" : "bg-emerald-500",
        )}
      />
    </span>
  );
}

export function AppShell({ children }: { children: ReactNode }) {
  const { diagnostics, assets, error, refresh, checked, openMaterialize } =
    useWorkspace();
  const unauthorized = error instanceof ApiError && error.status === 401;
  if (unauthorized || (!diagnostics && !sessionStorage.getItem("solera-token")))
    return <Login />;
  const activeRuns = diagnostics?.active_runs ?? 0;
  return (
    <div className="flex h-dvh flex-col md:flex-row">
      <aside className="flex shrink-0 flex-col gap-4 border-b bg-sidebar px-4 py-3 text-sidebar-foreground md:w-60 md:border-r md:border-b-0 md:py-5">
        <div className="flex items-center justify-between gap-3 md:block">
          <Brand project={diagnostics?.project ?? "…"} />
          <div className="flex items-center gap-2 text-xs text-muted-foreground md:hidden">
            <ConnectionDot offline={!!error} />
            {error ? "Offline" : "Live"}
          </div>
        </div>
        <div className="hidden md:block">
          <div className="text-[0.6rem] font-semibold tracking-[0.12em] text-muted-foreground uppercase">
            Workspace
          </div>
          <div className="mt-0.5 font-mono text-xs text-foreground">
            {diagnostics?.namespace ?? "default"}
          </div>
        </div>
        <nav
          aria-label="Main navigation"
          className="flex gap-1 overflow-x-auto md:flex-col"
        >
          {navigation.map((item) => (
            <Link
              key={item.to}
              to={item.to}
              activeOptions={{ exact: true }}
              activeProps={{ "data-active": true }}
              className="flex items-center gap-2.5 rounded-lg px-2.5 py-1.5 text-sm whitespace-nowrap text-muted-foreground transition-colors hover:bg-sidebar-accent/60 hover:text-foreground data-active:bg-sidebar-accent data-active:font-medium data-active:text-sidebar-accent-foreground"
            >
              <item.icon className="size-4 shrink-0" />
              <span>{item.label}</span>
              {item.to === "/assets" && !!assets.length && (
                <span className="ml-auto text-xs text-muted-foreground tabular-nums">
                  {assets.length}
                </span>
              )}
              {item.to === "/runs" && !!activeRuns && (
                <span className="ml-auto rounded-full bg-sky-500/15 px-1.5 text-xs font-medium text-sky-700 tabular-nums dark:text-sky-300">
                  {activeRuns}
                </span>
              )}
            </Link>
          ))}
        </nav>
        <div className="mt-auto hidden flex-col gap-3 border-t pt-3 text-xs text-muted-foreground md:flex">
          <ThemeToggle />
          <div className="flex items-center gap-2">
            <ConnectionDot offline={!!error} />
            <span className="min-w-0 truncate">
              {error
                ? "Connection interrupted"
                : diagnostics
                  ? `Connected · ${new URL(diagnostics.state).protocol.replace(":", "")}`
                  : "Connecting"}
            </span>
            {sessionStorage.getItem("solera-token") && (
              <Button
                variant="ghost"
                size="icon-sm"
                aria-label="Disconnect"
                className="ml-auto"
                onClick={() => {
                  sessionStorage.removeItem("solera-token");
                  refresh();
                }}
              >
                <LogOut />
              </Button>
            )}
          </div>
        </div>
      </aside>
      <div className="flex min-w-0 flex-1 flex-col overflow-y-auto">
        <header className="sticky top-0 z-10 flex items-center gap-2 border-b bg-background/80 px-4 py-2 backdrop-blur md:px-8">
          <div className="flex items-center gap-2 md:hidden">
            <ThemeToggle />
          </div>
          <Button
            size="sm"
            className="ml-auto"
            onClick={() =>
              openMaterialize(checked.length ? checked : undefined)
            }
          >
            <Play />
            Materialize{checked.length ? ` ${checked.length} selected` : ""}
          </Button>
        </header>
        <main className="mx-auto w-full max-w-6xl flex-1 px-4 py-6 md:px-8">
          {error && !unauthorized && (
            <div className="mb-4">
              <ErrorNotice
                message={`${error.message}${diagnostics ? " · Displaying last received state." : ""}`}
              />
            </div>
          )}
          {children}
        </main>
      </div>
    </div>
  );
}
