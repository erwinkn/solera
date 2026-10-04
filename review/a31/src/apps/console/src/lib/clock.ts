import { useSyncExternalStore } from "react";

/**
 * One shared clock for relative times and live durations. It ticks only while
 * something reads it, once a second, in step for every reader.
 */
let now = Date.now() / 1000;
const listeners = new Set<() => void>();
let timer: ReturnType<typeof setInterval> | undefined;

function subscribe(listener: () => void) {
  listeners.add(listener);
  if (timer === undefined) {
    now = Date.now() / 1000;
    timer = setInterval(() => {
      now = Date.now() / 1000;
      for (const l of listeners) l();
    }, 1000);
  }
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0 && timer !== undefined) {
      clearInterval(timer);
      timer = undefined;
    }
  };
}

/** Seconds since the epoch, updated every second while mounted. */
export function useNow(): number {
  return useSyncExternalStore(subscribe, () => now);
}
