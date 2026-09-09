import { useEffect, useState } from "react";
import { ApiError, request, useAction, useQuery } from "./api";
import { AssetDrawer, Assets } from "./assets";
import { ErrorNotice, Icon, Loading, go } from "./components";
import type { IconName } from "./components";
import { BackfillForm, MaterializeForm } from "./forms";
import { Automations, RunPage, Runs } from "./runs";
import type { Catalog } from "./types";

const navigation: { key: string; label: string; icon: IconName }[] = [
  { key: "assets", label: "Assets", icon: "assets" },
  { key: "runs", label: "Runs", icon: "runs" },
  { key: "backfills", label: "Backfills", icon: "backfills" },
  { key: "automations", label: "Automations", icon: "automations" },
];
function useRoute() {
  const [route, setRoute] = useState(
    window.location.hash.slice(1) || "/assets",
  );
  useEffect(() => {
    const update = () => setRoute(window.location.hash.slice(1) || "/assets");
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);
  return route;
}

function Login({ connected }: { connected: () => void }) {
  const [token, setToken] = useState("");
  const action = useAction();
  return (
    <main className="login-page">
      <form
        className="login"
        onSubmit={async (event) => {
          event.preventDefault();
          sessionStorage.setItem("dorc-token", token);
          const result = await action.run(() => request<Catalog>("/catalog"));
          if (result) connected();
          else sessionStorage.removeItem("dorc-token");
        }}
      >
        <div className="brand-mark">
          <Icon name="graph" size={23} />
        </div>
        <h1>Connect to your workspace</h1>
        <p>
          Enter the API token configured for this Data Orchestrator instance.
        </p>
        <label className="field">
          API token
          <input
            autoFocus
            required
            type="password"
            autoComplete="off"
            value={token}
            onChange={(event) => setToken(event.target.value)}
          />
        </label>
        {action.error && <ErrorNotice message={action.error} />}
        <button className="button primary" disabled={action.pending}>
          {action.pending ? "Connecting…" : "Connect"}
        </button>
        <p className="caption">
          Stored for this browser tab only. No cloud account is required.
        </p>
      </form>
    </main>
  );
}

export default function App() {
  const route = useRoute();
  const [, page = "assets", rawDetail] = route.split("/");
  const detail = rawDetail ? decodeURIComponent(rawDetail) : undefined;
  const catalog = useQuery<Catalog>("/catalog");
  const [targets, setTargets] = useState<string[] | null>(null);
  const [backfill, setBackfill] = useState(false);
  if (catalog.error instanceof ApiError && catalog.error.status === 401)
    return <Login connected={catalog.refresh} />;
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a
          className="brand"
          href="#/assets"
          aria-label="Data Orchestrator home"
        >
          <span className="brand-mark">
            <Icon name="graph" size={21} />
          </span>
          <span>
            Data Orchestrator<small>Asset-first orchestration</small>
          </span>
        </a>
        <div className="workspace-label">WORKSPACE</div>
        <nav aria-label="Main navigation">
          {navigation.map((item) => (
            <a
              href={`#/${item.key}`}
              key={item.key}
              className={page === item.key ? "nav-item active" : "nav-item"}
              aria-current={page === item.key ? "page" : undefined}
              aria-label={item.label}
            >
              <Icon name={item.icon} />
              <span>{item.label}</span>
              {item.key === "assets" && catalog.data && (
                <small>{catalog.data.assets.length}</small>
              )}
              {item.key === "runs" && !!catalog.data?.queue.running && (
                <small className="running-count">
                  {catalog.data.queue.running}
                </small>
              )}
            </a>
          ))}
        </nav>
        <div className="sidebar-footer">
          <div className="version-label">
            <span className="status-dot" />
            <span>
              Self-hosted <code>0.1 alpha</code>
            </span>
          </div>
          <p>Apache-2.0 · No cloud dependency</p>
        </div>
      </aside>
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumbs">
            <span>Workspace</span>
            <span className="muted">/</span>
            <strong>{catalog.data?.name || "Data Orchestrator"}</strong>
          </div>
          <div className="connection">
            <span
              className={
                catalog.error ? "connection-dot offline" : "connection-dot"
              }
            />
            {catalog.error
              ? "Connection interrupted"
              : catalog.data
                ? "Live updates"
                : "Connecting"}
            {sessionStorage.getItem("dorc-token") && (
              <button
                className="icon-button"
                aria-label="Disconnect"
                onClick={() => {
                  sessionStorage.removeItem("dorc-token");
                  catalog.refresh();
                }}
              >
                <Icon name="logout" />
              </button>
            )}
          </div>
        </header>
        <main id="main-content" className="content">
          {catalog.error && (
            <ErrorNotice
              message={`${catalog.error.message}${catalog.data ? " · Displaying last received state." : ""}`}
            />
          )}
          {!catalog.data ? (
            <>
              <Loading />
              {catalog.error && (
                <button className="button" onClick={catalog.refresh}>
                  Retry connection
                </button>
              )}
            </>
          ) : page === "assets" ? (
            <Assets catalog={catalog.data} materialize={setTargets} />
          ) : page === "runs" && detail ? (
            <RunPage key={detail} id={detail} />
          ) : page === "runs" || page === "backfills" ? (
            <Runs
              key={page}
              backfills={page === "backfills"}
              createBackfill={() => setBackfill(true)}
            />
          ) : page === "automations" ? (
            <Automations />
          ) : (
            <div className="empty">
              <h1>Page not found</h1>
              <button className="button" onClick={() => go("/assets")}>
                Return to assets
              </button>
            </div>
          )}
        </main>
      </div>
      {catalog.data && page === "assets" && detail && (
        <AssetDrawer
          assetKey={detail}
          catalog={catalog.data}
          materialize={setTargets}
        />
      )}
      {catalog.data && targets && (
        <MaterializeForm
          catalog={catalog.data}
          targets={targets}
          close={() => setTargets(null)}
        />
      )}
      {catalog.data && backfill && (
        <BackfillForm catalog={catalog.data} close={() => setBackfill(false)} />
      )}
    </div>
  );
}
