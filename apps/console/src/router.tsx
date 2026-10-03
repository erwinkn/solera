import type { QueryClient } from "@tanstack/react-query";
import {
  createRootRouteWithContext,
  createRoute,
  createRouter,
  lazyRouteComponent,
  Outlet,
} from "@tanstack/react-router";
import { Shell } from "@/shell/shell";
import { PageError, PageLoading, NotFound } from "@/shell/states";

/**
 * Every route, with its search params validated: the URL is the source of
 * truth for navigation, filters and selection. Multi-valued filters are
 * comma-separated strings (`?status=failed,canceled`), split where read.
 */

const str = (v: unknown): string | undefined =>
  v === undefined || v === null || v === "" ? undefined : String(v);
const num = (v: unknown): number | undefined => {
  const n = typeof v === "number" ? v : typeof v === "string" && v !== "" ? Number(v) : NaN;
  return Number.isFinite(n) ? n : undefined;
};
const oneOf =
  <T extends string>(...values: T[]) =>
  (v: unknown): T | undefined =>
    values.includes(v as T) ? (v as T) : undefined;

/** Every search param is optional: a link without `search` is a valid link. */
const optional = <T extends object>(o: T) => o as { [K in keyof T]?: Exclude<T[K], undefined> };

export const list = (v: string | undefined): string[] => (v ? v.split(",").filter(Boolean) : []);
export const join = (values: string[]): string | undefined => (values.length ? values.join(",") : undefined);

const root = createRootRouteWithContext<{ queryClient: QueryClient }>()({
  component: Shell,
  notFoundComponent: NotFound,
});

const overview = createRoute({
  getParentRoute: () => root,
  path: "/",
  validateSearch: (s: Record<string, unknown>) =>
    optional({ activity: oneOf("6h", "24h", "7d")(s.activity) }),
  component: lazyRouteComponent(() => import("@/routes/overview"), "Overview"),
});

// -- assets ---------------------------------------------------------------------

const assets = createRoute({
  getParentRoute: () => root,
  path: "/assets",
  validateSearch: (s: Record<string, unknown>) =>
    optional({
      view: oneOf("graph", "list")(s.view),
      q: str(s.q),
    }),
  component: lazyRouteComponent(() => import("@/routes/assets"), "Assets"),
});

const asset = createRoute({
  getParentRoute: () => root,
  path: "/assets/$asset",
  validateSearch: (s: Record<string, unknown>) => optional({ partition: str(s.partition) }),
  component: lazyRouteComponent(() => import("@/routes/asset"), "AssetLayout"),
});

const assetOverview = createRoute({
  getParentRoute: () => asset,
  path: "/",
  component: lazyRouteComponent(() => import("@/routes/asset"), "AssetOverview"),
});

const assetPartitions = createRoute({
  getParentRoute: () => asset,
  path: "/partitions",
  component: lazyRouteComponent(() => import("@/routes/asset-partitions"), "AssetPartitions"),
});

const assetKeys = createRoute({
  getParentRoute: () => asset,
  path: "/keys",
  validateSearch: (s: Record<string, unknown>) =>
    optional({
      outcome: str(s.outcome),
      key: str(s.key),
      edge: str(s.edge),
      q: str(s.q),
      output: str(s.output),
    }),
  component: lazyRouteComponent(() => import("@/routes/asset-keys"), "AssetKeys"),
});

const assetEdges = createRoute({
  getParentRoute: () => asset,
  path: "/edges",
  component: lazyRouteComponent(() => import("@/routes/asset-edges"), "AssetEdges"),
});

const assetHistory = createRoute({
  getParentRoute: () => asset,
  path: "/history",
  validateSearch: (s: Record<string, unknown>) =>
    // `output` and the asset's `partition` filter the list; `generation` with `vout` and
    // `vpartition` names the version whose lineage shows, wherever it is in the list.
    optional({
      output: str(s.output),
      generation: str(s.generation),
      vout: str(s.vout),
      vpartition: str(s.vpartition),
    }),
  component: lazyRouteComponent(() => import("@/routes/asset-history"), "AssetHistory"),
});

const assetRuns = createRoute({
  getParentRoute: () => asset,
  path: "/runs",
  component: lazyRouteComponent(() => import("@/routes/asset"), "AssetRuns"),
});

// -- runs -----------------------------------------------------------------------

export const RANGES = ["1h", "6h", "24h", "7d", "30d"] as const;
export type Range = (typeof RANGES)[number];

export const runSearch = (s: Record<string, unknown>) =>
  optional({
    status: str(s.status),
    trigger: str(s.trigger),
    automation: str(s.automation),
    asset: str(s.asset),
    tag: str(s.tag),
    q: str(s.q),
    range: oneOf(...RANGES)(s.range),
    since: num(s.since),
    until: num(s.until),
  });
export type RunSearch = ReturnType<typeof runSearch>;

const runs = createRoute({
  getParentRoute: () => root,
  path: "/runs",
  validateSearch: runSearch,
  component: lazyRouteComponent(() => import("@/routes/runs"), "Runs"),
});

const run = createRoute({
  getParentRoute: () => root,
  path: "/runs/$run",
  validateSearch: (s: Record<string, unknown>) =>
    optional({
      task: str(s.task),
      attempt: str(s.attempt),
      tab: oneOf("logs", "result", "spec", "events")(s.tab),
      level: oneOf("all", "info", "warning", "error")(s.level),
      lq: str(s.lq),
    }),
  component: lazyRouteComponent(() => import("@/routes/run"), "Run"),
});

// -- everything else -------------------------------------------------------------

const automations = createRoute({
  getParentRoute: () => root,
  path: "/automations",
  validateSearch: (s: Record<string, unknown>) => optional({ q: str(s.q) }),
  component: lazyRouteComponent(() => import("@/routes/automations"), "Automations"),
});

const sensors = createRoute({
  getParentRoute: () => root,
  path: "/sensors",
  component: lazyRouteComponent(() => import("@/routes/sensors"), "Sensors"),
});

const sensor = createRoute({
  getParentRoute: () => root,
  path: "/sensors/$sensor",
  validateSearch: (s: Record<string, unknown>) =>
    optional({ all: s.all === true || s.all === "true" ? true : undefined }),
  component: lazyRouteComponent(() => import("@/routes/sensors"), "Sensor"),
});

const sources = createRoute({
  getParentRoute: () => root,
  path: "/sources",
  component: lazyRouteComponent(() => import("@/routes/sources"), "Sources"),
});

const source = createRoute({
  getParentRoute: () => root,
  path: "/sources/$source",
  component: lazyRouteComponent(() => import("@/routes/sources"), "Source"),
});

const executors = createRoute({
  getParentRoute: () => root,
  path: "/executors",
  component: lazyRouteComponent(() => import("@/routes/executors"), "Executors"),
});

const health = createRoute({
  getParentRoute: () => root,
  path: "/health",
  component: lazyRouteComponent(() => import("@/routes/health"), "Health"),
});

const tree = root.addChildren([
  overview,
  assets,
  asset.addChildren([assetOverview, assetPartitions, assetKeys, assetEdges, assetHistory, assetRuns]),
  runs,
  run,
  automations,
  sensors,
  sensor,
  sources,
  source,
  executors,
  health,
]);

export function makeRouter(queryClient: QueryClient) {
  return createRouter({
    routeTree: tree,
    context: { queryClient },
    defaultPreload: "intent",
    defaultPreloadStaleTime: 0,
    defaultPendingComponent: PageLoading,
    defaultPendingMs: 150,
    defaultErrorComponent: PageError,
    scrollRestoration: true,
  });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof makeRouter>;
  }
}

export { Outlet };
