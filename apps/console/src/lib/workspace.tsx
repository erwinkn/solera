import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";
import type { ReactNode } from "react";
import { useQuery } from "./api";
import type { CatalogAsset, Diagnostics } from "./types";

export type Selection =
  { kind: "asset"; name: string } | { kind: "run"; id: string } | null;

interface Workspace {
  diagnostics: Diagnostics | null;
  /** `/projects/{name}` — the API prefix for every project-scoped call. */
  base: string | null;
  assets: CatalogAsset[];
  error: Error | null;
  refresh: () => void;
  selection: Selection;
  select: (next: Selection) => void;
  checked: string[];
  setChecked: (next: string[] | ((current: string[]) => string[])) => void;
  materializeTargets: string[] | null;
  materializeScopes: string[];
  openMaterialize: (targets?: string[], scopes?: string[]) => void;
  closeMaterialize: () => void;
}

const WorkspaceContext = createContext<Workspace | null>(null);

export function WorkspaceProvider({ children }: { children: ReactNode }) {
  const diagnostics = useQuery<Diagnostics>("/diagnostics", 3000);
  const base = diagnostics.data
    ? `/projects/${diagnostics.data.project}`
    : null;
  const catalog = useQuery<{ assets: CatalogAsset[] }>(
    base ? `${base}/assets` : null,
    2000,
  );
  const [selection, setSelection] = useState<Selection>(null);
  const [checked, setChecked] = useState<string[]>([]);
  const [materializeTargets, setMaterializeTargets] = useState<string[] | null>(
    null,
  );
  const [materializeScopes, setMaterializeScopes] = useState<string[]>([]);
  const select = useCallback((next: Selection) => setSelection(next), []);
  const openMaterialize = useCallback(
    (targets?: string[], scopes?: string[]) => {
      setSelection(null);
      setMaterializeTargets(targets ?? []);
      setMaterializeScopes(scopes ?? []);
    },
    [],
  );
  const closeMaterialize = useCallback(() => setMaterializeTargets(null), []);
  const refresh = useCallback(() => {
    diagnostics.refresh();
    catalog.refresh();
  }, [diagnostics.refresh, catalog.refresh]);
  const value = useMemo<Workspace>(
    () => ({
      diagnostics: diagnostics.data ?? null,
      base,
      assets: catalog.data?.assets ?? [],
      error: diagnostics.error ?? catalog.error,
      refresh,
      selection,
      select,
      checked,
      setChecked,
      materializeTargets,
      materializeScopes,
      openMaterialize,
      closeMaterialize,
    }),
    [
      diagnostics.data,
      diagnostics.error,
      base,
      catalog.data,
      catalog.error,
      refresh,
      selection,
      select,
      checked,
      materializeTargets,
      materializeScopes,
      openMaterialize,
      closeMaterialize,
    ],
  );
  return (
    <WorkspaceContext.Provider value={value}>
      {children}
    </WorkspaceContext.Provider>
  );
}

export function useWorkspace() {
  const context = useContext(WorkspaceContext);
  if (!context) throw new Error("useWorkspace outside WorkspaceProvider");
  return context;
}
