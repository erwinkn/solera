/**
 * What each state means to an operator, as one of six tones. Every status
 * shown anywhere — runs, tasks, attempts, partitions, keys, ticks, inputs —
 * maps here, so "red" means the same thing on every screen.
 *
 *   ok    done and current            run   working right now
 *   wait  queued, due, waiting        warn  needs a look, not broken
 *   fail  broken: someone should act  idle  inert: skipped, canceled, gone
 */
export type Tone = "ok" | "run" | "wait" | "warn" | "fail" | "idle";

const TONES: Record<string, Tone> = {
  // runs, tasks, attempts
  succeeded: "ok",
  committed: "ok",
  materialized: "ok",
  stale: "warn",
  running: "run",
  launched: "run",
  claimed: "run",
  preparing: "run",
  provisioning: "run",
  queued: "wait",
  waiting: "wait",
  paused: "wait",
  ready: "wait",
  failed: "fail",
  lost: "fail",
  timed_out: "fail",
  timeout: "fail",
  blocked: "warn",
  canceled: "idle",
  aborted: "idle",
  skipped: "idle",
  // partitions
  missing: "idle",
  // keys (and removed partitions)
  ok: "ok",
  rejected: "warn",
  retrying: "wait",
  removed: "idle",
  unmatched: "idle",
  // ticks
  advanced: "run",
  requested: "ok",
  refused: "warn",
  // explain's verdicts (pending: written upstream, not processed yet)
  pending: "wait",
  failing: "fail",
  excluded: "idle",
  not_matched: "idle",
  absent: "idle",
};

export function tone(status: string | null | undefined): Tone {
  return (status && TONES[status]) || "idle";
}

const LABELS: Record<string, string> = {
  timed_out: "timed out",
  not_matched: "not matched",
  onchange: "on change",
  ondeploy: "on deploy",
};

/** Why something is stale, in the glossary's words; the API has spelled them with spaces and with underscores. */
export const STALE_REASONS = {
  input_changed: {
    label: "input changed",
    means: "An input it read changed since: new upstream data, or new patterns on the input.",
  },
  upstream_stale: {
    label: "upstream stale",
    means: "A partition it reads is itself stale: run upstream first, or run with upstream.",
  },
  definition_changed: {
    label: "definition changed",
    means: "Its asset changed since it was written: version, configuration, patterns, rename or reset.",
  },
} as const;

export function staleReason(reason: string): { label: string; means: string } {
  const known = STALE_REASONS[reason.replaceAll(" ", "_") as keyof typeof STALE_REASONS];
  return known ?? { label: reason.replaceAll("_", " "), means: reason };
}

export function label(status: string | null | undefined): string {
  if (!status) return "—";
  return LABELS[status] ?? status.replaceAll("_", " ");
}

/** Text, soft fill and solid mark classes per tone. Literal, so Tailwind sees them. */
export const toneText: Record<Tone, string> = {
  ok: "text-ok-fg",
  run: "text-run-fg",
  wait: "text-wait-fg",
  warn: "text-warn-fg",
  fail: "text-fail-fg",
  idle: "text-idle-fg",
};
export const toneSoft: Record<Tone, string> = {
  ok: "bg-ok-soft text-ok-fg",
  run: "bg-run-soft text-run-fg",
  wait: "bg-wait-soft text-wait-fg",
  warn: "bg-warn-soft text-warn-fg",
  fail: "bg-fail-soft text-fail-fg",
  idle: "bg-idle-soft text-idle-fg",
};
export const toneSolid: Record<Tone, string> = {
  ok: "bg-ok",
  run: "bg-run",
  wait: "bg-wait",
  warn: "bg-warn",
  fail: "bg-fail",
  idle: "bg-idle",
};
export const toneStroke: Record<Tone, string> = {
  ok: "stroke-ok",
  run: "stroke-run",
  wait: "stroke-wait",
  warn: "stroke-warn",
  fail: "stroke-fail",
  idle: "stroke-idle",
};
export const toneFill: Record<Tone, string> = {
  ok: "fill-ok",
  run: "fill-run",
  wait: "fill-wait",
  warn: "fill-warn",
  fail: "fill-fail",
  idle: "fill-idle",
};

/**
 * A mark in a chart: like toneSolid, but "ok" takes the theme's quieter
 * --viz-ok, because success is the common case and shouldn't shout.
 */
export const markSolid: Record<Tone, string> = { ...toneSolid, ok: "bg-viz-ok" };

/** Worst first: what a rollup of several states shows. */
export const SEVERITY: Tone[] = ["fail", "warn", "run", "wait", "ok", "idle"];

export function worst(tones: Iterable<Tone>): Tone {
  let best = SEVERITY.length - 1;
  for (const t of tones) best = Math.min(best, SEVERITY.indexOf(t));
  return SEVERITY[best] ?? "idle";
}
