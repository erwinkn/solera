import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";
import type { ReactNode } from "react";
import { useQuery } from "./api";
import type { StateResponse } from "./types";

export type Selection =
  { kind: "asset"; name: string } | { kind: "run"; id: string } | null;

interface Workspace {
  state: StateResponse | null;
  error: Error | null;
  refresh: () => void;
  selection: Selection;
  select: (next: Selection) => void;
  checked: string[];
  setChecked: (next: string[] | ((current: string[]) => string[])) => void;
  materializeTargets: string[] | null;
  openMaterialize: (targets?: string[]) => void;
  closeMaterialize: () => void;
}

const WorkspaceContext = createContext<Workspace | null>(null);

export function WorkspaceProvider({ children }: { children: ReactNode }) {
  const query = useQuery<StateResponse>("/state", 2000);
  const [selection, setSelection] = useState<Selection>(null);
  const [checked, setChecked] = useState<string[]>([]);
  const [materializeTargets, setMaterializeTargets] = useState<string[] | null>(
    null,
  );
  const select = useCallback((next: Selection) => setSelection(next), []);
  const openMaterialize = useCallback((targets?: string[]) => {
    setSelection(null);
    setMaterializeTargets(targets ?? []);
  }, []);
  const closeMaterialize = useCallback(() => setMaterializeTargets(null), []);
  const value = useMemo<Workspace>(
    () => ({
      state: query.data,
      error: query.error,
      refresh: query.refresh,
      selection,
      select,
      checked,
      setChecked,
      materializeTargets,
      openMaterialize,
      closeMaterialize,
    }),
    [
      query.data,
      query.error,
      query.refresh,
      selection,
      select,
      checked,
      materializeTargets,
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
