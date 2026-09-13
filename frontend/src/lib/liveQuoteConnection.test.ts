/**
 * Tests for the shared live-quote connection policy (#2944).
 *
 * These are pure-function tests on purpose: the policy is the one thing both
 * `useLiveQuote` and `LiveQuoteProvider` must agree on, and a divergence
 * between them is exactly how the same bug shipped twice.
 */
import { describe, expect, it } from "vitest";

import {
  isConnected,
  isUnavailable,
  staleQuoteReason,
  statusOnStreamError,
  tickIsAuthoritative,
} from "./liveQuoteConnection";

// WHATWG EventSource readyState values.
const CONNECTING = 0;
const OPEN = 1;
const CLOSED = 2;

describe("statusOnStreamError", () => {
  it("reports reconnecting when a previously-open stream drops", () => {
    // THE #2944 BUG: both implementations handled only CLOSED, so this case
    // left `connected` true and the UI pulsed LIVE over a dead transport.
    expect(statusOnStreamError(CONNECTING, true)).toBe("reconnecting");
  });

  it("reports connecting when the stream never opened", () => {
    // readyState alone cannot tell "never got up" from "fell over".
    expect(statusOnStreamError(CONNECTING, false)).toBe("connecting");
  });

  it("reports unavailable only on a definitive close", () => {
    expect(statusOnStreamError(CLOSED, true)).toBe("unavailable");
    expect(statusOnStreamError(CLOSED, false)).toBe("unavailable");
  });

  it("does not treat an OPEN readyState as an outage", () => {
    expect(statusOnStreamError(OPEN, true)).toBe("reconnecting");
  });
});

describe("isConnected / isUnavailable", () => {
  it("keeps the legacy booleans exact", () => {
    expect(isConnected("live")).toBe(true);
    expect(["idle", "connecting", "reconnecting", "unavailable"].map(isConnected as never)).toEqual([
      false,
      false,
      false,
      false,
    ]);
    expect(isUnavailable("unavailable")).toBe(true);
    expect(["idle", "connecting", "live", "reconnecting"].map(isUnavailable as never)).toEqual([
      false,
      false,
      false,
      false,
    ]);
  });
});

describe("tickIsAuthoritative", () => {
  it("requires BOTH a live stream and a tick from this connection", () => {
    expect(tickIsAuthoritative("live", true)).toBe(true);
    // The reconnect-resurrection case: the stream is up again but has not
    // delivered a frame, so the retained tick is still a cache.
    expect(tickIsAuthoritative("live", false)).toBe(false);
    expect(tickIsAuthoritative("reconnecting", true)).toBe(false);
    expect(tickIsAuthoritative("unavailable", true)).toBe(false);
    expect(tickIsAuthoritative("idle", true)).toBe(false);
  });
});

describe("staleQuoteReason", () => {
  it("names every non-authoritative state, including an open-but-quiet stream", () => {
    expect(staleQuoteReason("live")).toMatch(/waiting for a live price/i);
    expect(staleQuoteReason("reconnecting")).toMatch(/reconnecting/i);
    expect(staleQuoteReason("unavailable")).toMatch(/unavailable/i);
    expect(staleQuoteReason("connecting")).toMatch(/connecting/i);
    expect(staleQuoteReason("idle")).toMatch(/not running/i);
  });
});
