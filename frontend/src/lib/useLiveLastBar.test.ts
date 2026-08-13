/**
 * Tests for the live-tick aggregator pure function (#602).
 *
 * The hook itself can't run in jsdom (EventSource missing) — the
 * `useLiveQuote` integration is covered by its own test file. Here
 * we pin the bucket-aware aggregation logic so a tick that lands in
 * the in-progress bar updates H/L/C, a tick that crosses the
 * boundary appends a new bar with O=H=L=C=tick, and a stale tick
 * before the historical anchor is dropped.
 */
import { act, renderHook } from "@testing-library/react";
import { createRef, type MutableRefObject } from "react";
import type { ISeriesApi } from "lightweight-charts";
import { describe, expect, it, vi } from "vitest";

import { aggregateTick, useLiveLastBar } from "@/lib/useLiveLastBar";
import type { LiveTickPayload } from "@/lib/useLiveQuote";

// Mock the LiveQuoteProvider hooks so the hook-under-test runs in
// jsdom without the SSE EventSource. `useLiveTick` returns whatever
// the test pushes via the mutable ref; `useLiveQuoteConnection`
// reports a healthy stream.
const mockTickRef: { current: LiveTickPayload | null } = { current: null };
vi.mock("@/components/quotes/LiveQuoteProvider", () => ({
  useLiveTick: () => mockTickRef.current,
  useLiveQuoteConnection: () => ({ connected: true, unavailable: false }),
}));

const T_BAR_OPEN = Math.floor(Date.UTC(2026, 3, 27, 14, 30) / 1000); // 14:30
const T_INSIDE = T_BAR_OPEN + 30; // 14:30:30
const T_NEXT_BAR = T_BAR_OPEN + 60; // 14:31

describe("aggregateTick — within current bucket", () => {
  it("first tick into the historical bar PRESERVES the historical open", () => {
    // Codex pre-push fix (#602): rewriting the open from a tick
    // mid-bar visibly distorts the candle. The aggregator must
    // carry the historical OHLC into the live bar.
    const result = aggregateTick({
      prev: null,
      histLastBar: { time: T_BAR_OPEN, open: 99, high: 102, low: 98 },
      bucketSeconds: 60,
      tickEpochSeconds: T_INSIDE,
      tickPrice: 100,
    });
    expect(result.verdict).toBe("update");
    if (result.verdict === "skip") return;
    expect(result.next.open).toBe(99);
    expect(result.next.high).toBe(102); // historical 102 wins over tick 100
    expect(result.next.low).toBe(98); // historical 98 wins over tick 100
    expect(result.next.close).toBe(100);
  });

  it("first tick when there is no historical bar opens at the tick price", () => {
    const result = aggregateTick({
      prev: null,
      histLastBar: null,
      bucketSeconds: 60,
      tickEpochSeconds: T_INSIDE,
      tickPrice: 100,
    });
    expect(result.verdict).toBe("append");
    if (result.verdict === "skip") return;
    expect(result.next).toEqual({
      time: T_BAR_OPEN,
      open: 100,
      high: 100,
      low: 100,
      close: 100,
    });
  });

  it("subsequent tick raises high if higher (live-bar already in this bucket)", () => {
    const prev = { time: T_BAR_OPEN, open: 100, high: 102, low: 99, close: 101 };
    const result = aggregateTick({
      prev,
      histLastBar: { time: T_BAR_OPEN, open: 99, high: 102, low: 98 },
      bucketSeconds: 60,
      tickEpochSeconds: T_INSIDE,
      tickPrice: 105,
    });
    expect(result.verdict).toBe("update");
    if (result.verdict === "skip") return;
    // Live bar's open wins over historical when both anchor in-bucket.
    expect(result.next.open).toBe(100);
    expect(result.next.high).toBe(105);
    expect(result.next.low).toBe(99);
    expect(result.next.close).toBe(105);
  });

  it("subsequent tick lowers low if lower", () => {
    const prev = { time: T_BAR_OPEN, open: 100, high: 102, low: 99, close: 101 };
    const result = aggregateTick({
      prev,
      histLastBar: { time: T_BAR_OPEN, open: 99, high: 102, low: 98 },
      bucketSeconds: 60,
      tickEpochSeconds: T_INSIDE,
      tickPrice: 95,
    });
    if (result.verdict === "skip") return;
    expect(result.next.low).toBe(95);
    expect(result.next.close).toBe(95);
  });

  it("preserves live-bar open across many ticks in the same bucket", () => {
    const prev = { time: T_BAR_OPEN, open: 100, high: 105, low: 95, close: 95 };
    const result = aggregateTick({
      prev,
      histLastBar: { time: T_BAR_OPEN, open: 99, high: 102, low: 98 },
      bucketSeconds: 60,
      tickEpochSeconds: T_INSIDE,
      tickPrice: 102,
    });
    if (result.verdict === "skip") return;
    expect(result.next.open).toBe(100);
    expect(result.next.close).toBe(102);
  });
});

describe("aggregateTick — bucket boundary", () => {
  it("tick in the next bucket emits an append verdict and opens a fresh bar", () => {
    const prev = { time: T_BAR_OPEN, open: 100, high: 102, low: 99, close: 101 };
    const result = aggregateTick({
      prev,
      histLastBar: { time: T_BAR_OPEN, open: 99, high: 102, low: 98 },
      bucketSeconds: 60,
      tickEpochSeconds: T_NEXT_BAR + 5,
      tickPrice: 103,
    });
    expect(result.verdict).toBe("append");
    if (result.verdict === "skip") return;
    expect(result.next).toEqual({
      time: T_NEXT_BAR,
      open: 103,
      high: 103,
      low: 103,
      close: 103,
    });
  });

  it("first-ever tick after history with no prior live bar appends if newer", () => {
    const result = aggregateTick({
      prev: null,
      histLastBar: { time: T_BAR_OPEN, open: 99, high: 102, low: 98 },
      bucketSeconds: 60,
      tickEpochSeconds: T_NEXT_BAR + 5,
      tickPrice: 100,
    });
    expect(result.verdict).toBe("append");
  });
});

describe("useLiveLastBar — session-filter dedupe (PR #610 round 3)", () => {
  // 04:00 ET = 08:00 UTC during EDT — first minute of pre-market.
  const PM_TICK_AT = "2026-04-28T08:00:00Z";

  function makePmTick(quotedAt: string = PM_TICK_AT): LiveTickPayload {
    return {
      instrument_id: 1699,
      native_currency: "USD",
      bid: "25.00",
      ask: "25.05",
      last: "25.00",
      quoted_at: quotedAt,
      display: null,
    };
  }

  function makeRefs(): {
    candle: MutableRefObject<ISeriesApi<"Candlestick"> | null>;
    line: null;
    area: null;
    update: ReturnType<typeof vi.fn>;
  } {
    const update = vi.fn();
    const candle = createRef<ISeriesApi<"Candlestick"> | null>() as MutableRefObject<
      ISeriesApi<"Candlestick"> | null
    >;
    candle.current = { update } as unknown as ISeriesApi<"Candlestick">;
    return { candle, line: null, area: null, update };
  }

  it("rejects PM tick when acceptPre=false AND does not retroactively apply when acceptPre flips to true", () => {
    mockTickRef.current = makePmTick();
    const refs = makeRefs();

    const { result, rerender } = renderHook(
      ({ acceptPre }: { acceptPre: boolean }) =>
        useLiveLastBar({
          instrumentId: 1699,
          bucketSeconds: 60,
          historicalLastBar: null,
          refs,
          acceptPre,
          acceptAh: true,
        }),
      { initialProps: { acceptPre: false } },
    );

    // First render: filter is OFF, tick should be dropped + dedupe key
    // recorded so a future flip can't replay it.
    expect(result.current.appliedTicks).toBe(0);
    expect(refs.update).not.toHaveBeenCalled();

    // Flip filter ON, same tick still in-flight (no new tick arrived).
    act(() => {
      rerender({ acceptPre: true });
    });

    // Stale tick must NOT be retroactively applied — dedupe key was
    // recorded on the rejected first pass.
    expect(result.current.appliedTicks).toBe(0);
    expect(refs.update).not.toHaveBeenCalled();
  });

  it("accepts a NEW PM tick after filter flips ON", () => {
    mockTickRef.current = makePmTick();
    const refs = makeRefs();

    const { rerender, result } = renderHook(
      ({ acceptPre }: { acceptPre: boolean }) =>
        useLiveLastBar({
          instrumentId: 1699,
          bucketSeconds: 60,
          historicalLastBar: null,
          refs,
          acceptPre,
          acceptAh: true,
        }),
      { initialProps: { acceptPre: false } },
    );
    expect(result.current.appliedTicks).toBe(0);

    // Now a fresh tick arrives at a later quoted_at AFTER filter flips.
    act(() => {
      mockTickRef.current = makePmTick("2026-04-28T08:01:00Z");
      rerender({ acceptPre: true });
    });

    expect(result.current.appliedTicks).toBe(1);
    expect(refs.update).toHaveBeenCalledTimes(1);
  });
});

describe("aggregateTick — stale tick", () => {
  it("tick before the historical anchor is skipped", () => {
    const result = aggregateTick({
      prev: null,
      histLastBar: { time: T_BAR_OPEN, open: 99, high: 102, low: 98 },
      bucketSeconds: 60,
      tickEpochSeconds: T_BAR_OPEN - 120,
      tickPrice: 100,
    });
    expect(result.verdict).toBe("skip");
  });

  it("works when there is no historical anchor (chart hasn't loaded)", () => {
    const result = aggregateTick({
      prev: null,
      histLastBar: null,
      bucketSeconds: 60,
      tickEpochSeconds: T_INSIDE,
      tickPrice: 100,
    });
    expect(result.verdict).toBe("append");
  });
});
