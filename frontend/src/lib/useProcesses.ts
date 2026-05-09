/**
 * useProcesses — polling hook for /system/processes (#1076 / #1064).
 *
 * Cadence:
 *   - 5s while ANY row is `running` (operator wants near-live progress).
 *   - 30s otherwise (cheap background poll).
 *
 * The cadence flip is derived from `data.rows.some(r => r.status === "running")`.
 * On a status transition (running → ok, etc.) the interval resets to the
 * new cadence on the next render — same ref-based pattern as
 * `AdminPage.tsx` so a flip mid-cycle does not double-fire or drop ticks.
 *
 * Stale-while-revalidate via `useAsync({ preserveOnRefetch: true })` —
 * polling does not flicker the table to a skeleton.
 */

import { useEffect, useRef } from "react";

import { fetchProcesses } from "@/api/processes";
import type { AsyncState } from "@/lib/useAsync";
import { useAsync } from "@/lib/useAsync";
import type { ProcessListResponse } from "@/api/types";

export const POLL_INTERVAL_RUNNING_MS = 5_000;
export const POLL_INTERVAL_IDLE_MS = 30_000;

export type UseProcessesResult = AsyncState<ProcessListResponse>;

export function useProcesses(): UseProcessesResult {
  const state = useAsync(fetchProcesses, [], { preserveOnRefetch: true });

  const anyRunning =
    state.data?.rows.some((r) => r.status === "running") ?? false;
  const interval = anyRunning
    ? POLL_INTERVAL_RUNNING_MS
    : POLL_INTERVAL_IDLE_MS;

  // Keep `refetch` in a ref so the timer does not re-arm on every
  // render — only when the cadence itself changes (running ↔ idle
  // transition). Mirrors AdminPage.tsx's ref-based interval pattern.
  const refetchRef = useRef(state.refetch);
  useEffect(() => {
    refetchRef.current = state.refetch;
  }, [state.refetch]);

  useEffect(() => {
    const id = window.setInterval(() => refetchRef.current(), interval);
    return () => window.clearInterval(id);
  }, [interval]);

  return state;
}
