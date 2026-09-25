import type { ReactNode } from "react";
import {
  HeadContent,
  Outlet,
  Scripts,
  createRootRoute,
} from "@tanstack/react-router";
import { AppShell } from "@/components/app-shell";
import { AssetSheet } from "@/components/asset-sheet";
import { MaterializeDialog } from "@/components/materialize-dialog";
import { RunSheet } from "@/components/run-sheet";
import { ThemeProvider, themeBootScript } from "@/lib/theme";
import { WorkspaceProvider } from "@/lib/workspace";
import appCss from "../styles.css?url";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { name: "theme-color", content: "#f8f9fa" },
      { title: "Solera" },
    ],
    links: [
      { rel: "stylesheet", href: appCss },
      { rel: "icon", type: "image/svg+xml", href: "/static/favicon.svg" },
    ],
    // Apply the stored/system theme before first paint to avoid a flash.
    scripts: [{ children: themeBootScript }],
  }),
  component: RootComponent,
});

function RootComponent() {
  return (
    <RootDocument>
      <WorkspaceProvider>
        <Shell />
      </WorkspaceProvider>
    </RootDocument>
  );
}

function Shell() {
  // The SPA shell prerenders without a DOM; ship a minimal frame there.
  if (typeof window === "undefined") return null;
  return (
    <ThemeProvider>
      <AppShell>
        <Outlet />
      </AppShell>
      <MaterializeDialog />
      <AssetSheet />
      <RunSheet />
    </ThemeProvider>
  );
}

function RootDocument({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  );
}
