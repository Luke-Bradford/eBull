/**
 * liveQuoteConnection — the ONE connection-state policy shared by the
 * single-instrument hook (`useLiveQuote`) and the page-level provider
 * (`LiveQuoteProvider`) (#2944).
 *
 * Both implementations previously handled only `readyState === CLOSED` in
 * `onerror`. EventSource fires `onerror` on every automatic reconnect attempt
 * too, with `readyState === CONNECTING` — and neither touched `connected` on
 * that branch, so a dropped transport left a pulsing LIVE badge over a frozen
 * price.
 *
 * The discriminator is the HTML spec's own `readyState`
 * (https://html.spec.whatwg.org/multipage/server-sent-events.html —
 * CONNECTING = 0, OPEN = 1, CLOSED = 2). Nothing here is inferred from
 * timing or heuristics.
 *
 * ⚠ Scope: this describes the TRANSPORT. An open stream whose upstream eToro
 * subscriber has gone away still heartbeats (`app/api/sse_quotes.py`), so
 * `live` means "the pipe is up", not "the venue is quoting". Detecting a
 * frozen upstream needs a tick-age or backend feed-health signal and is not
 * this module's job.
 */

export type LiveConnectionStatus =
  /** No subscription: no instrument id, an empty id set, or no EventSource. */
  | "idle"
  /** Stream constructed; no successful open yet. */
  | "connecting"
  /** Open. */
  | "live"
  /** Errored with readyState CONNECTING AFTER a successful open — the
   *  transport dropped and the browser is retrying by itself. */
  | "reconnecting"
  /** Errored with readyState CLOSED. Terminal for this EventSource: the
   *  browser will not retry, so recovery needs a new stream. */
  | "unavailable";

/**
 * Next status after an EventSource `error` event.
 *
 * `hasOpened` separates "never got up" from "fell over": `readyState` is
 * CONNECTING in both cases and cannot tell them apart on its own.
 */
export function statusOnStreamError(
  readyState: number,
  hasOpened: boolean,
): LiveConnectionStatus {
  // EventSource.CLOSED is 2; compare the literal so this stays callable in a
  // test environment whose fake source does not expose the static constants.
  if (readyState === 2) return "unavailable";
  return hasOpened ? "reconnecting" : "connecting";
}

/** Legacy boolean, unchanged in meaning, derived rather than tracked. */
export function isConnected(status: LiveConnectionStatus): boolean {
  return status === "live";
}

/** Legacy boolean, unchanged in meaning, derived rather than tracked. */
export function isUnavailable(status: LiveConnectionStatus): boolean {
  return status === "unavailable";
}

/**
 * Is the retained tick the price of RIGHT NOW, or a cache?
 *
 * BOTH halves are required. `status === "live"` alone is not enough: a stream
 * that drops at 09:00 and reopens at 09:05 without delivering a frame (quiet
 * book, halted name, market closed) would otherwise silently re-bless the
 * 09:00 price as live and pulse forever. Callers set the second argument false
 * on `open` as well as on `error`, and true only when a frame actually lands.
 *
 * A `false` here does NOT mean hide the price — per
 * `.claude/skills/frontend/safety-state-ui.md`, a retained value is kept and
 * MARKED, because "a silent cache that looks live is worse than no cache at
 * all".
 */
export function tickIsAuthoritative(
  status: LiveConnectionStatus,
  tickArrivedOnThisConnection: boolean,
): boolean {
  return status === "live" && tickArrivedOnThisConnection;
}

/**
 * Operator-facing reason a displayed price is not live. Null when it is.
 * Shared so the instrument header and the table cells cannot drift apart.
 */
export function staleQuoteReason(status: LiveConnectionStatus): string | null {
  switch (status) {
    case "live":
      // Live transport but no frame on this connection yet.
      return "Waiting for a live price — showing last received price";
    case "reconnecting":
      return "Price stream reconnecting — showing last received price";
    case "unavailable":
      return "Price stream unavailable — showing last received price";
    case "connecting":
      return "Connecting to the price stream — showing last received price";
    case "idle":
      return "Price stream not running — showing last received price";
  }
}
