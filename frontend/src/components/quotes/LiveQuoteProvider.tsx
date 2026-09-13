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

import {
  isConnected,
  isUnavailable,
  statusOnStreamError,
  tickIsAuthoritative,
  type LiveConnectionStatus,
} from "@/lib/liveQuoteConnection";
import type { LiveTickPayload } from "@/lib/useLiveQuote";

const REOPEN_DEBOUNCE_MS = 300;

interface LiveQuoteContextValue {
  /** Latest tick by instrument_id; undefined while waiting for the
   *  first tick (or when no tick will arrive — halted / illiquid
   *  instruments may never produce a snapshot). */
  ticks: ReadonlyMap<number, LiveTickPayload>;
  /** Transport state (#2944). The single source for badge copy. */
  status: LiveConnectionStatus;
  /** Ids whose tick arrived on the connection that is currently open. A
   *  reopen clears this: a retained tick predates the new connection and
   *  must not be re-blessed as live without a fresh frame. */
  freshIds: ReadonlySet<number>;
  /** True once the SSE connection has opened. ⚠ Does NOT mean the retained
   *  tick is live — gate a live badge on ``tickIsAuthoritative``. */
  connected: boolean;
  /** True if the backend returned 503 or the connection errored
   *  permanently. UI falls back to its REST snapshot. */
  unavailable: boolean;
}

const LiveQuoteContext = createContext<LiveQuoteContextValue>({
  ticks: new Map(),
  status: "idle",
  freshIds: new Set(),
  connected: false,
  unavailable: false,
});

interface State {
  ticks: Map<number, LiveTickPayload>;
  status: LiveConnectionStatus;
  freshIds: Set<number>;
}

type Action =
  | { type: "tick"; payload: LiveTickPayload }
  | { type: "open" }
  | { type: "error"; status: LiveConnectionStatus }
  /** Canonical set changed: the old source is already closed, so nothing is
   *  live any more. Ticks are deliberately KEPT (the debounce exists to avoid
   *  flicker) but every one of them is now a marked cache. */
  | { type: "suspend" }
  | { type: "reset" };

const EMPTY_STATE: State = { ticks: new Map(), status: "idle", freshIds: new Set() };

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case "tick": {
      // ``Map`` is mutable but React relies on identity; clone so
      // consumers that select via ``ticks.get(id)`` actually re-
      // render. Cost is one map allocation per tick — negligible at
      // typical eToro tick rates.
      const next = new Map(state.ticks);
      next.set(action.payload.instrument_id, action.payload);
      const fresh = new Set(state.freshIds);
      fresh.add(action.payload.instrument_id);
      return { ...state, ticks: next, freshIds: fresh };
    }
    case "open":
      // Clearing freshIds on open is the point: a reconnect that delivers no
      // frame must leave every retained tick marked stale (Codex ckpt-1).
      return { ...state, status: "live", freshIds: new Set() };
    case "error":
      return { ...state, status: action.status, freshIds: new Set() };
    case "suspend":
      return { ...state, status: "connecting", freshIds: new Set() };
    case "reset":
      return { ticks: new Map(), status: "idle", freshIds: new Set() };
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
    ticks: new Map(EMPTY_STATE.ticks),
    status: EMPTY_STATE.status,
    freshIds: new Set(EMPTY_STATE.freshIds),
  }));

  const canonical = useMemo(() => canonicaliseIds(instrumentIds), [instrumentIds]);
  const subscribedIds = useMemo(
    () => new Set(canonical === "" ? [] : canonical.split(",").map(Number)),
    [canonical],
  );
  const sourceRef = useRef<EventSource | null>(null);
  const reopenTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    // The old source is closed by cleanup before this body runs, so nothing is
    // live from here until the new one opens. Leaving ``status`` at "live"
    // through the 300ms debounce was the provider's copy of #2944's badge lie.
    dispatch({ type: "suspend" });

    if (typeof EventSource === "undefined") {
      // Reset unconditionally: the guard used to sit ABOVE the empty-set
      // branch, so losing EventSource support left prior ticks and status
      // standing.
      dispatch({ type: "reset" });
      return;
    }
    if (canonical === "") {
      // No ids → no stream. Close any prior connection cleanly so
      // pages that briefly drop to zero rows don't leave a dangling
      // refcount on the backend.
      const prior = sourceRef.current;
      if (prior !== null) {
        prior.close();
        sourceRef.current = null;
      }
      // Reset regardless of whether a source existed: a page that mounts with
      // zero ids must also land in "idle", not in the initial state by luck.
      dispatch({ type: "reset" });
      return;
    }
    let hasOpened = false;

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
        hasOpened = true;
        dispatch({ type: "open" });
      };

      source.onmessage = (ev: MessageEvent) => {
        if (!isActive()) return;
        try {
          const payload = JSON.parse(ev.data) as LiveTickPayload;
          // Membership check, matching useLiveQuote's own defensive filter: a
          // frame for an id this stream did not subscribe to is a server-side
          // filter bug, not data for this page.
          if (typeof payload.instrument_id === "number" && subscribedIds.has(payload.instrument_id)) {
            dispatch({ type: "tick", payload });
          }
        } catch {
          // Malformed JSON — drop the frame; the connection stays
          // open for the next one.
        }
      };

      source.onerror = () => {
        if (!isActive()) return;
        // The browser fires onerror on every reconnect attempt too. The old
        // code handled only CLOSED, so ``connected`` stayed true through an
        // entire outage (#2944).
        dispatch({ type: "error", status: statusOnStreamError(source.readyState, hasOpened) });
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
      status: state.status,
      freshIds: state.freshIds,
      connected: isConnected(state.status),
      unavailable: isUnavailable(state.status),
    }),
    [state.ticks, state.status, state.freshIds],
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

/**
 * Per-instrument tick PLUS whether it is the price of right now (#2944).
 *
 * ``authoritative`` false does not mean hide the tick — it means render it as
 * the cache it is, per `.claude/skills/frontend/safety-state-ui.md`.
 */
export function useLiveTickFreshness(instrumentId: number | null | undefined): {
  tick: LiveTickPayload | null;
  status: LiveConnectionStatus;
  authoritative: boolean;
} {
  const ctx = useContext(LiveQuoteContext);
  if (instrumentId === null || instrumentId === undefined) {
    return { tick: null, status: ctx.status, authoritative: false };
  }
  const tick = ctx.ticks.get(instrumentId) ?? null;
  return {
    tick,
    status: ctx.status,
    authoritative: tick !== null && tickIsAuthoritative(ctx.status, ctx.freshIds.has(instrumentId)),
  };
}

export function useLiveQuoteConnection(): {
  status: LiveConnectionStatus;
  connected: boolean;
  unavailable: boolean;
} {
  const ctx = useContext(LiveQuoteContext);
  return { status: ctx.status, connected: ctx.connected, unavailable: ctx.unavailable };
}
