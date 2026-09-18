import { useState } from "react";
import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import {
  Database,
  LayoutGrid,
  LogOut,
  Play,
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
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import { ErrorNotice } from "./common";

const navigation = [
  { to: "/assets", label: "Assets", icon: LayoutGrid },
  { to: "/runs", label: "Runs", icon: Play },
  { to: "/automations", label: "Automations", icon: Zap },
  { to: "/storage", label: "Storage", icon: Database },
] as const;

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
            sessionStorage.setItem("dorc-token", value);
            const result = await action.run(async () => {
              try {
                return await request("/state");
              } catch (failure) {
                sessionStorage.removeItem("dorc-token");
                throw failure;
              }
            });
            if (result) workspace.refresh();
          }}
        >
          <CardHeader>
            <div className="mb-2 flex size-9 items-center justify-center rounded-lg bg-primary text-primary-foreground">
              <Workflow className="size-5" />
            </div>
            <CardTitle className="text-lg">Connect to your workspace</CardTitle>
            <CardDescription>
              Enter the API token configured for this Data Orchestrator
              instance.
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
    <span
      className={cn(
        "size-2 rounded-full",
        offline ? "bg-red-500" : "bg-emerald-500",
      )}
    />
  );
}

export function AppShell({ children }: { children: ReactNode }) {
  const { state, error, refresh, checked, openMaterialize } = useWorkspace();
  const unauthorized = error instanceof ApiError && error.status === 401;
  if (unauthorized || (!state && !sessionStorage.getItem("dorc-token")))
    return <Login />;
  return (
    <div className="flex h-dvh flex-col md:flex-row">
      <aside className="flex shrink-0 flex-col gap-3 border-b bg-sidebar px-4 py-3 md:w-60 md:border-r md:border-b-0 md:py-5">
        <div className="flex items-center justify-between gap-3 md:block">
          <Link
            to="/assets"
            aria-label="Data Orchestrator home"
            className="flex items-center gap-2.5 font-heading font-medium"
          >
            <span className="flex size-8 items-center justify-center rounded-lg bg-primary text-primary-foreground">
              <Workflow className="size-4" />
            </span>
            <span className="leading-tight">
              Data Orchestrator
              <span className="block text-[0.65rem] font-normal tracking-wider text-muted-foreground uppercase">
                Experimental
              </span>
            </span>
          </Link>
          <div className="flex items-center gap-2 text-xs text-muted-foreground md:hidden">
            <ConnectionDot offline={!!error} />
            {error ? "Offline" : "Live"}
          </div>
        </div>
        <div className="hidden text-[0.65rem] font-medium tracking-wider text-muted-foreground md:block">
          WORKSPACE
          <div className="mt-1 font-mono text-xs font-normal tracking-normal text-foreground">
            {state?.storage.namespace ?? "default"}
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
              className="flex items-center gap-2 rounded-lg px-2.5 py-1.5 text-sm whitespace-nowrap text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-foreground data-active:bg-sidebar-accent data-active:font-medium data-active:text-foreground"
            >
              <item.icon className="size-4" />
              <span>{item.label}</span>
              {item.to === "/assets" && state && (
                <span className="ml-auto text-xs text-muted-foreground">
                  {state.assets.length}
                </span>
              )}
              {item.to === "/runs" &&
                !!state?.runs.filter((run) =>
                  ["running", "queued", "paused"].includes(run.status),
                ).length && (
                  <span className="ml-auto rounded-full bg-sky-500/15 px-1.5 text-xs font-medium text-sky-700">
                    {
                      state.runs.filter((run) =>
                        ["running", "queued", "paused"].includes(run.status),
                      ).length
                    }
                  </span>
                )}
            </Link>
          ))}
        </nav>
        <div className="mt-auto hidden gap-2 border-t pt-3 text-xs text-muted-foreground md:flex md:flex-col">
          <div className="flex items-center gap-2">
            <ConnectionDot offline={!!error} />
            <span>
              {error
                ? "Connection interrupted"
                : state
                  ? `Connected · ${state.storage.scheme}`
                  : "Connecting"}
            </span>
            {sessionStorage.getItem("dorc-token") && (
              <Button
                variant="ghost"
                size="icon-sm"
                aria-label="Disconnect"
                className="ml-auto"
                onClick={() => {
                  sessionStorage.removeItem("dorc-token");
                  refresh();
                }}
              >
                <LogOut />
              </Button>
            )}
          </div>
          <p>Self-hosted · Apache-2.0 · No cloud dependency</p>
        </div>
      </aside>
      <div className="flex min-w-0 flex-1 flex-col overflow-y-auto">
        <header className="sticky top-0 z-10 flex items-center justify-end gap-2 border-b bg-background/80 px-4 py-2 backdrop-blur md:px-8">
          <Button
            size="sm"
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
                message={`${error.message}${state ? " · Displaying last received state." : ""}`}
              />
            </div>
          )}
          {children}
        </main>
      </div>
    </div>
  );
}
