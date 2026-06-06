/**
 * useProcesses — polling hook for /system/processes (#1076 / #1064).
 *
 * Cadence:
 *   - 5s while ANY row is `running` (operator wants near-live progress).
 *   - 60s otherwise (cheap background poll).
 *   - ×3 on a degraded link (see SLOW_LINK_BACKOFF) so a high-latency
 *     RDP / VPN session does not queue overlapping polls.
 *   - paused entirely while the tab/window is hidden; resumed with an
 *     immediate catch-up refetch on return (#1480).
 *
 * The cadence flip is derived from `data.rows.some(r => r.status === "running")`.
 * On a status transition (running → ok, etc.) the interval resets to the
 * new cadence on the next render — same ref-based pattern as
 * `AdminPage.tsx` so a flip mid-cycle does not double-fire or drop ticks.
 *
 * Stale-while-revalidate via `useAsync({ preserveOnRefetch: true })` —
 * polling does not flicker the table to a skeleton. Row-level repaint is
 * gated separately by `React.memo` on a content signature (#1480) so an
 * unchanged poll is a no-op at the DOM layer.
 */

import { useCallback, useEffect, useRef, useState } from "react";

import { fetchProcesses } from "@/api/processes";
import type { AsyncState } from "@/lib/useAsync";
import { useAsync } from "@/lib/useAsync";
import type { ProcessListResponse } from "@/api/types";

export const POLL_INTERVAL_RUNNING_MS = 5_000;
export const POLL_INTERVAL_IDLE_MS = 60_000;

// A round-trip slower than this marks the link as degraded; the poll
// cadence is multiplied by SLOW_LINK_BACKOFF until a fast fetch clears
// the flag. Stops a high-latency session from arming a 5s timer it can
// never keep up with (#1480).
export const SLOW_LINK_THRESHOLD_MS = 2_000;
export const SLOW_LINK_BACKOFF = 3;

export type UseProcessesResult = AsyncState<ProcessListResponse>;

function isDocumentHidden(): boolean {
  return typeof document !== "undefined" && document.hidden;
}

export function useProcesses(): UseProcessesResult {
  const [slowLink, setSlowLink] = useState(false);

  // Measure round-trip latency around each fetch. `setSlowLink` is a
  // no-op when the boolean is unchanged (React bails on identical
  // state), so this only triggers a re-render when the link crosses the
  // threshold — not on every poll.
  //
  // `useAsync` does not abort an in-flight request when a newer refetch
  // fires (it only ignores the stale *data* via its cancelled flag), so
  // requests can overlap. Guard the latency side effect with a sequence
  // counter — only the most-recently-STARTED request may update the
  // flag, so a slow stale request finishing after a fast newer one
  // cannot back off the cadence on obsolete timing (Codex ckpt-2).
  const fetchSeqRef = useRef(0);
  const measuredFetch = useCallback(async () => {
    const seq = ++fetchSeqRef.current;
    const startedAt = performance.now();
    try {
      return await fetchProcesses();
    } finally {
      if (seq === fetchSeqRef.current) {
        setSlowLink(performance.now() - startedAt > SLOW_LINK_THRESHOLD_MS);
      }
    }
  }, []);

  const state = useAsync(measuredFetch, [], { preserveOnRefetch: true });

  const anyRunning =
    state.data?.rows.some((r) => r.status === "running") ?? false;
  const baseInterval = anyRunning
    ? POLL_INTERVAL_RUNNING_MS
    : POLL_INTERVAL_IDLE_MS;
  const interval = slowLink ? baseInterval * SLOW_LINK_BACKOFF : baseInterval;

  // Keep `refetch` in a ref so the timer + visibility listener do not
  // re-arm on every render — only when the cadence itself changes
  // (running ↔ idle, slow ↔ fast, hidden ↔ visible).
  const refetchRef = useRef(state.refetch);
  useEffect(() => {
    refetchRef.current = state.refetch;
  }, [state.refetch]);

  // Pause polling while the tab/window is hidden; resume + catch up on
  // return. No point spending fetches (or repainting) a surface the
  // operator cannot see, and the catch-up refetch means the first thing
  // they see on return is fresh, not a stale frame (#1480).
  const [visible, setVisible] = useState(() => !isDocumentHidden());
  useEffect(() => {
    function onVisibilityChange() {
      const nowVisible = !isDocumentHidden();
      setVisible(nowVisible);
      if (nowVisible) refetchRef.current();
    }
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () =>
      document.removeEventListener("visibilitychange", onVisibilityChange);
  }, []);

  useEffect(() => {
    if (!visible) return; // hidden — do not arm a timer
    const id = window.setInterval(() => refetchRef.current(), interval);
    return () => window.clearInterval(id);
  }, [interval, visible]);

  return state;
}
