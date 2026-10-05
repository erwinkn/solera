import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { complain, notify } from "@/lib/toasts";
import { shortId } from "@/lib/format";
import { api } from "./client";
import { useProject } from "./queries";
import type { Automation, RunRequest } from "./types";

/**
 * Every write the console makes. Each invalidates what it changes, from the
 * resource down (see queries.ts), and reports failure as a toast.
 */

const enc = encodeURIComponent;

export interface RunInput {
  targets: string[];
  partitions: "latest" | "missing" | "all" | string[];
  mode: "incremental" | "full";
  upstream: boolean;
  config?: Record<string, unknown>;
  /** Per keyed incremental input (by upstream output): a list of keys, or "all" to read every
   * key again without a reset. Not with mode "full", which starts over instead. */
  keys?: Record<string, string[] | "all"> | null;
  tags?: Record<string, string>;
}

export function useSubmitRun() {
  const project = useProject();
  const client = useQueryClient();
  const navigate = useNavigate();
  return useMutation({
    mutationFn: (input: RunInput) =>
      api<RunRequest | { status: "skipped-active" }>(`/projects/${enc(project)}/runs`, {
        body: { ...input, by: "console" },
        headers: { "Idempotency-Key": crypto.randomUUID() },
      }),
    onSuccess: (run) => {
      client.invalidateQueries({ queryKey: ["runs"] });
      client.invalidateQueries({ queryKey: ["assets"] });
      if ("id" in run) {
        notify("Run submitted", `${run.targets.join(", ")} · ${shortId(run.id)}`);
        navigate({ to: "/runs/$run", params: { run: run.id } });
      } else notify("Nothing submitted", "Every target partition is already running.");
    },
    onError: (error) => complain("Couldn't submit the run", error),
  });
}

type RunAction = "cancel" | "pause" | "resume";
const PAST: Record<RunAction, string> = {
  cancel: "Cancel requested",
  pause: "Run paused",
  resume: "Run resumed",
};

export function useRunAction(run: string) {
  const project = useProject();
  const client = useQueryClient();
  return useMutation({
    mutationFn: (action: RunAction) =>
      api(`/projects/${enc(project)}/runs/${enc(run)}/${action}`, {
        method: "POST",
      }),
    onSuccess: (_, action) => {
      notify(PAST[action], shortId(run));
      client.invalidateQueries({ queryKey: ["runs"] });
    },
    onError: (error, action) => complain(`Couldn't ${action} the run`, error),
  });
}

/** A finished run never changes: retrying it submits its failed, canceled and blocked work as a new run. */
export function useRetryRun(run: string) {
  const project = useProject();
  const client = useQueryClient();
  const navigate = useNavigate();
  return useMutation({
    mutationFn: () => api<RunRequest>(`/projects/${enc(project)}/runs/${enc(run)}/retry`, { method: "POST" }),
    onSuccess: (retry) => {
      notify("Retry submitted", `run ${shortId(retry.id)} retries ${shortId(run)}`);
      client.invalidateQueries({ queryKey: ["runs"] });
      navigate({ to: "/runs/$run", params: { run: retry.id } });
    },
    onError: (error) => complain("Couldn't retry the run", error),
  });
}

export function useDeleteRun() {
  const project = useProject();
  const client = useQueryClient();
  const navigate = useNavigate();
  return useMutation({
    mutationFn: (run: string) => api(`/projects/${enc(project)}/runs/${enc(run)}`, { method: "DELETE" }),
    onSuccess: (_, run) => {
      notify("Run deleted", shortId(run));
      client.removeQueries({ queryKey: ["runs", run] });
      client.invalidateQueries({ queryKey: ["runs"] });
      navigate({ to: "/runs" });
    },
    onError: (error) => complain("Couldn't delete the run", error),
  });
}

export function useAutomationToggle() {
  const project = useProject();
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      api<Automation>(
        `/projects/${enc(project)}/automations/${enc(name)}/${enabled ? "enable" : "disable"}`,
        {
          method: "POST",
        },
      ),
    // Optimistic: the switch moves at once, and moves back if the server refuses.
    onMutate: async ({ name, enabled }) => {
      await client.cancelQueries({ queryKey: ["automations"] });
      const before = client.getQueryData<Automation[]>(["automations"]);
      client.setQueryData<Automation[]>(["automations"], (list) =>
        list?.map((a) => (a.name === name ? { ...a, enabled } : a)),
      );
      return { before };
    },
    onError: (error, { name }, context) => {
      client.setQueryData(["automations"], context?.before);
      complain(`Couldn't change ${name}`, error);
    },
    onSettled: () => {
      client.invalidateQueries({ queryKey: ["automations"] });
      client.invalidateQueries({ queryKey: ["assets"] });
    },
  });
}

export function useRunAutomation() {
  const project = useProject();
  const client = useQueryClient();
  return useMutation({
    mutationFn: (name: string) =>
      api<Automation>(`/projects/${enc(project)}/automations/${enc(name)}/run-now`, { method: "POST" }),
    onSuccess: (auto) => {
      notify(`Fired ${auto.name}`, auto.last_run ? `run ${shortId(auto.last_run)}` : "nothing to run");
      client.invalidateQueries({ queryKey: ["automations"] });
      client.invalidateQueries({ queryKey: ["runs"] });
    },
    onError: (error, name) => complain(`Couldn't fire ${name}`, error),
  });
}

export interface CommitInput {
  source: string;
  version?: string;
  upsert?: string[] | Record<string, string | null>;
  remove?: string[];
}

export function useCommitSource() {
  const project = useProject();
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ source, ...body }: CommitInput) =>
      api(`/projects/${enc(project)}/sources/${enc(source)}/commit`, {
        body: { ...body, by: "console" },
      }),
    onSuccess: (_, { source }) => {
      notify("Committed", source);
      client.invalidateQueries({ queryKey: ["outputs", source] });
      client.invalidateQueries({ queryKey: ["runs"] });
      client.invalidateQueries({ queryKey: ["assets"] });
    },
    onError: (error, { source }) => complain(`Couldn't commit to ${source}`, error),
  });
}

export function useClearCleanups() {
  const project = useProject();
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ output, partition }: { output: string; partition: string }) =>
      api(`/projects/${enc(project)}/cleanups:clear`, {
        body: { output, partition, by: "console" },
      }),
    onSuccess: (_, { output, partition }) => {
      notify("Stuck cleanups cleared", partition ? `${output} · ${partition}` : output);
      client.invalidateQueries({ queryKey: ["cleanups"] });
      client.invalidateQueries({ queryKey: ["diagnostics"] });
      client.invalidateQueries({ queryKey: ["outputs", output] });
    },
    onError: (error) => complain("Couldn't clear the cleanups", error),
  });
}

export function useRetryKeys(asset: string) {
  const project = useProject();
  const client = useQueryClient();
  return useMutation({
    mutationFn: ({ classes, partition }: { classes: string[]; partition?: string }) =>
      api<{ classes: string[]; partitions: string[] }>(
        `/projects/${enc(project)}/assets/${enc(asset)}/keys:retry`,
        {
          body: { classes, partition, by: "console" },
        },
      ),
    onSuccess: (found) => {
      notify(
        found.partitions.length ? "Retry requested" : "Nothing to retry",
        found.partitions.length
          ? `${found.classes.join(", ")} keys in ${found.partitions.length} partition(s)`
          : undefined,
      );
      client.invalidateQueries({ queryKey: ["assets", asset] });
      client.invalidateQueries({ queryKey: ["runs"] });
    },
    onError: (error) => complain("Couldn't request the retry", error),
  });
}
