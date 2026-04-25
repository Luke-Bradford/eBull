/**
 * useLiveQuote — open an SSE stream for one instrument and return
 * the latest tick (#488, backend #487).
 *
 * The backend endpoint (GET /sse/quotes?ids=<id>) now triggers a
 * live eToro Subscribe frame for the instrument on stream open
 * and Unsubscribe on close — so opening the hook on any page adds
 * the instrument to the WS topic set, and unmounting removes it.
 *
 * Payload shape (from app/api/sse_quotes.py _format_tick):
 *   {
 *     instrument_id: number,
 *     native_currency: string | null,
 *     bid: string,       // Decimal-preserved string
 *     ask: string,
 *     last: string | null,
 *     quoted_at: string, // ISO 8601
 *     display: null | { currency, bid, ask, last }
 *   }
 *
 * Clients should prefer the display block when present (matches the
 * operator's runtime_config.display_currency); native is the fallback.
 *
 * Fallback: the hook doesn't drive REST polling itself. The page's
 * existing InstrumentSummary fetch loads the initial snapshot; this
 * hook overlays live ticks on top of it when they arrive. A 503 or
 * connection failure leaves the snapshot in place — the page degrades
 * silently to whatever the last-fetched quote was.
 */

import { useEffect, useRef, useState } from "react";

export interface LiveTickPayload {
  instrument_id: number;
  native_currency: string | null;
  bid: string;
  ask: string;
  last: string | null;
  quoted_at: string;
  display: null | {
    currency: string;
    bid: string;
    ask: string;
    last: string | null;
  };
}

export interface LiveQuoteState {
  /** Latest tick received on this connection, or null before the first
   *  tick arrives. Null is not a "broken" state — it just means eToro
   *  hasn't pushed a rate for this instrument yet (quiet book). */
  tick: LiveTickPayload | null;
  /** True once the SSE connection has opened. Useful for a
   *  "LIVE" badge UI. */
  connected: boolean;
  /** True if the backend returned 503 (no quote bus available) or
   *  the connection errored in a non-recoverable way. The UI should
   *  fall back to its REST snapshot. */
  unavailable: boolean;
}

export function useLiveQuote(instrumentId: number | null | undefined): LiveQuoteState {
  const [tick, setTick] = useState<LiveTickPayload | null>(null);
  const [connected, setConnected] = useState(false);
  const [unavailable, setUnavailable] = useState(false);
  // Ref holds the active EventSource across renders so React's strict-
  // mode double-invocation cleanup doesn't close a live stream while a
  // new one is being set up.
  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (instrumentId === null || instrumentId === undefined) return;
    // Defensive: EventSource is a browser API. Test environments
    // without a jsdom polyfill hit this branch — no-op and let the
    // page fall back to its REST snapshot. Also covers SSR if we
    // ever render this component server-side.
    if (typeof EventSource === "undefined") return;

    // Reset state when the subscribed id changes so the caller
    // doesn't see stale ticks from a previous instrument.
    setTick(null);
    setConnected(false);
    setUnavailable(false);

    // Route through ``/api/*`` so the Vite dev proxy (see
    // frontend/vite.config.ts) strips the prefix and forwards to
    // the backend's ``/sse/quotes`` route. In prod the same prefix
    // lands on the reverse proxy's catch-all.
    const url = `/api/sse/quotes?ids=${encodeURIComponent(String(instrumentId))}`;
    const source = new EventSource(url, { withCredentials: true });
    sourceRef.current = source;

    // Guard: after cleanup or id change, the OLD EventSource can
    // still deliver a queued message/open/error event before the
    // browser unbinds its handlers. Without a per-effect "active"
    // check these handlers would mutate state with stale data and
    // briefly display the previous instrument's price. We compare
    // against ``sourceRef.current`` (which cleanup nulls before
    // the new effect runs) so late events from a closed source are
    // no-ops.
    const isActive = (): boolean => sourceRef.current === source;

    source.onopen = () => {
      if (!isActive()) return;
      setConnected(true);
    };

    source.onmessage = (ev: MessageEvent) => {
      if (!isActive()) return;
      try {
        const payload = JSON.parse(ev.data) as LiveTickPayload;
        // Only accept ticks for the currently-subscribed id — defensive
        // against a server-side filter bug leaking foreign ticks.
        if (payload.instrument_id === instrumentId) {
          setTick(payload);
        }
      } catch {
        // Malformed JSON — ignore the frame; the connection stays
        // open for the next one.
      }
    };

    source.onerror = () => {
      if (!isActive()) return;
      // EventSource's built-in auto-reconnect handles transient drops.
      // We only set ``unavailable`` once the connection is definitively
      // closed (readyState CLOSED = 2). Browsers fire onerror on every
      // reconnect attempt too, which should NOT flip unavailable.
      if (source.readyState === EventSource.CLOSED) {
        setUnavailable(true);
        setConnected(false);
      }
    };

    return () => {
      source.close();
      // Null the ref BEFORE any subsequent effect body runs so the
      // stale-event guard above sees ``sourceRef.current !== source``
      // and drops any event that fires after close().
      if (sourceRef.current === source) {
        sourceRef.current = null;
      }
      setConnected(false);
    };
  }, [instrumentId]);

  return { tick, connected, unavailable };
}

/**
 * Pick the best "current price" string from a live tick, preferring
 * display currency when available and falling back to native. Returns
 * null when no useful value exists.
 */
export function liveTickDisplayPrice(tick: LiveTickPayload | null): {
  value: string;
  currency: string | null;
} | null {
  if (tick === null) return null;
  if (tick.display !== null) {
    const v = tick.display.last ?? tick.display.bid;
    return { value: v, currency: tick.display.currency };
  }
  const v = tick.last ?? tick.bid;
  return { value: v, currency: tick.native_currency };
}
