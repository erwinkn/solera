import type { Cleanup, KeyPage, StaleKey, StaleKeys } from "./types";

/**
 * Readers for the few response fields whose shape the API is still settling
 * (the observed-set rebuild lands them field by field): each accepts every
 * shape the server has sent, so a view reads one.
 */

/** A key's entry in a key page: the generation that last wrote it and, for a source, the version it was served at. */
export function keyEntry(value: KeyPage["keys"][string]): { generation: number; version: string | null } {
  return typeof value === "number"
    ? { generation: value, version: null }
    : { generation: value.generation, version: value.version ?? null };
}

/** A stale key with its own reasons, else the partition's. */
export function staleKey(entry: StaleKeys["keys"][number], partition: string[]): Required<StaleKey> {
  return typeof entry === "string"
    ? { key: entry, reasons: partition }
    : { key: entry.key, reasons: entry.reasons ?? partition };
}

/** Whether a cleanup needs an operator: a store cleanup with stuck entries, or a cleanup task that gave up. */
export const cleanupStuck = (c: Cleanup): boolean => ("due" in c ? !!c.stuck : c.stuck.length > 0);
