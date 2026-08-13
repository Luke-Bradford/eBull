/**
 * LiveQuoteProvider — page-level shared SSE for quote ticks (#501).
 *
 * Why page-level (not per-cell):
 *   The browser caps SSE connections to ~6 per origin. A portfolio
 *   table with 7+ rows that each opened its own EventSource would
 *   silently queue connections beyond the cap. The provider opens
 *   ONE EventSource per page carrying the union of every visible
 *   instrument id; consumer cells subscribe to per-id ticks via
 *   React context.
 *
 * Per the spec
 * (docs/superpowers/specs/2026-04-25-visibility-driven-live-prices-spec.md
 * Invariants 2 + 5):
 *   - One stream per page.
 *   - Same id rendered twice on a page consumes from the same stream.
 *   - Stream re-opens only on canonical-set change (dedup + numeric
 *     sort + join), so harmless re-renders or row reorders don't
 *     churn the SSE connection.
 *
 * Backend integration:
 *   The SSE endpoint at GET /sse/quotes?ids=<csv> ref-counts the
 *   ids on stream open and decrements on close. The subscriber
 *   sends Subscribe / Unsubscribe frames to eToro accordingly.
 *   This file is the only consumer the operator's UI needs to
 *   wire prices everywhere.
 */
import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useReducer,
  useRef,
  type ReactNode,
} from "react";

import type { LiveTickPayload } from "@/lib/useLiveQuote";

const REOPEN_DEBOUNCE_MS = 300;

interface LiveQuoteContextValue {
  /** Latest tick by instrument_id; undefined while waiting for the
   *  first tick (or when no tick will arrive — halted / illiquid
   *  instruments may never produce a snapshot). */
  ticks: ReadonlyMap<number, LiveTickPayload>;
  /** True once the SSE connection has opened. */
  connected: boolean;
  /** True if the backend returned 503 or the connection errored
   *  permanently. UI falls back to its REST snapshot. */
  unavailable: boolean;
}

const LiveQuoteContext = createContext<LiveQuoteContextValue>({
  ticks: new Map(),
  connected: false,
  unavailable: false,
});

interface State {
  ticks: Map<number, LiveTickPayload>;
  connected: boolean;
  unavailable: boolean;
}

type Action =
  | { type: "tick"; payload: LiveTickPayload }
  | { type: "open" }
  | { type: "error" }
  | { type: "reset" };

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case "tick": {
      // ``Map`` is mutable but React relies on identity; clone so
      // consumers that select via ``ticks.get(id)`` actually re-
      // render. Cost is one map allocation per tick — negligible at
      // typical eToro tick rates.
      const next = new Map(state.ticks);
      next.set(action.payload.instrument_id, action.payload);
      return { ...state, ticks: next };
    }
    case "open":
      return { ...state, connected: true, unavailable: false };
    case "error":
      return { ...state, connected: false, unavailable: true };
    case "reset":
      return { ticks: new Map(), connected: false, unavailable: false };
  }
}

/**
 * Canonical set representation for the visible-id list — dedup +
 * numeric sort + comma-join. Two arrays with the same membership
 * (regardless of order or duplicates) produce the same string, so
 * the EventSource doesn't churn on re-renders that merely change
 * row order. Pinned in the spec (Codex round 3 finding 1).
 */
function canonicaliseIds(ids: readonly number[]): string {
  const unique = Array.from(new Set(ids)).filter((n) => Number.isFinite(n));
  unique.sort((a, b) => a - b);
  return unique.join(",");
}

interface LiveQuoteProviderProps {
  /** Instrument ids the page wants live ticks for. Order, duplicates,
   *  and per-render identity are all ignored — only the canonical
   *  set membership matters. */
  instrumentIds: readonly number[];
  children: ReactNode;
}

export function LiveQuoteProvider({
  instrumentIds,
  children,
}: LiveQuoteProviderProps) {
  const [state, dispatch] = useReducer(reducer, undefined, () => ({
    ticks: new Map(),
    connected: false,
    unavailable: false,
  }));

  const canonical = useMemo(() => canonicaliseIds(instrumentIds), [instrumentIds]);
  const sourceRef = useRef<EventSource | null>(null);
  const reopenTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (typeof EventSource === "undefined") return;
    if (canonical === "") {
      // No ids → no stream. Close any prior connection cleanly so
      // pages that briefly drop to zero rows don't leave a dangling
      // refcount on the backend.
      const prior = sourceRef.current;
      if (prior !== null) {
        prior.close();
        sourceRef.current = null;
        dispatch({ type: "reset" });
      }
      return;
    }

    // Debounce reopen so a burst of state changes resolves into one
    // SSE handshake. ``canonical`` already filters out re-renders
    // that don't change the set; the debounce guards rapid genuine
    // changes (e.g. rows arriving in waves from a slow REST fetch).
    if (reopenTimerRef.current !== null) {
      clearTimeout(reopenTimerRef.current);
    }
    reopenTimerRef.current = setTimeout(() => {
      const prior = sourceRef.current;
      if (prior !== null) {
        prior.close();
      }
      // Reset state on each (re)connect so stale ticks from a
      // previous canonical set don't bleed into the new view.
      dispatch({ type: "reset" });

      // Route through ``/api/*`` so the Vite dev proxy strips the
      // prefix and forwards to the backend's /sse/quotes route.
      const url = `/api/sse/quotes?ids=${encodeURIComponent(canonical)}`;
      const source = new EventSource(url, { withCredentials: true });
      sourceRef.current = source;

      const isActive = (): boolean => sourceRef.current === source;

      source.onopen = () => {
        if (!isActive()) return;
        dispatch({ type: "open" });
      };

      source.onmessage = (ev: MessageEvent) => {
        if (!isActive()) return;
        try {
          const payload = JSON.parse(ev.data) as LiveTickPayload;
          if (typeof payload.instrument_id === "number") {
            dispatch({ type: "tick", payload });
          }
        } catch {
          // Malformed JSON — drop the frame; the connection stays
          // open for the next one.
        }
      };

      source.onerror = () => {
        if (!isActive()) return;
        // EventSource auto-reconnects on transient drops; we only
        // mark unavailable once the connection is definitively
        // closed.
        if (source.readyState === EventSource.CLOSED) {
          dispatch({ type: "error" });
        }
      };
    }, REOPEN_DEBOUNCE_MS);

    return () => {
      if (reopenTimerRef.current !== null) {
        clearTimeout(reopenTimerRef.current);
        reopenTimerRef.current = null;
      }
      const source = sourceRef.current;
      if (source !== null) {
        source.close();
        sourceRef.current = null;
      }
    };
  }, [canonical]);

  const value = useMemo<LiveQuoteContextValue>(
    () => ({
      ticks: state.ticks,
      connected: state.connected,
      unavailable: state.unavailable,
    }),
    [state.ticks, state.connected, state.unavailable],
  );

  return (
    <LiveQuoteContext.Provider value={value}>
      {children}
    </LiveQuoteContext.Provider>
  );
}

/**
 * Consumer hook — returns the latest tick for the given instrument
 * id, or ``null`` while waiting for the first tick. A null value is
 * not "broken" — it just means eToro hasn't pushed a rate for this
 * instrument yet (quiet book, market closed, halted instrument).
 * Callers should fall back to whatever REST snapshot they have on
 * hand for the initial paint.
 */
export function useLiveTick(
  instrumentId: number | null | undefined,
): LiveTickPayload | null {
  const ctx = useContext(LiveQuoteContext);
  if (instrumentId === null || instrumentId === undefined) return null;
  return ctx.ticks.get(instrumentId) ?? null;
}

export function useLiveQuoteConnection(): {
  connected: boolean;
  unavailable: boolean;
} {
  const ctx = useContext(LiveQuoteContext);
  return { connected: ctx.connected, unavailable: ctx.unavailable };
}
