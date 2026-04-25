/**
 * Tests for useLiveQuote (#488).
 *
 * EventSource is not a pure function — we stub it with a minimal
 * implementation that lets the test trigger onopen/onmessage/onerror
 * synchronously. Covers: tick delivery, instrument-id filter,
 * connection status flag, unavailable flag on definitive close.
 */
import { describe, expect, it, beforeEach, afterEach, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";

import { liveTickDisplayPrice, useLiveQuote, type LiveTickPayload } from "@/lib/useLiveQuote";

class FakeEventSource {
  static instances: FakeEventSource[] = [];
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 2;

  url: string;
  readyState = FakeEventSource.CONNECTING;
  onopen: ((ev: Event) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;

  constructor(url: string, _init?: EventSourceInit) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  close(): void {
    this.readyState = FakeEventSource.CLOSED;
  }

  // Helpers for tests.
  fireOpen(): void {
    this.readyState = FakeEventSource.OPEN;
    this.onopen?.(new Event("open"));
  }
  fireMessage(data: string): void {
    this.onmessage?.(new MessageEvent("message", { data }));
  }
  fireError(finalClose = false): void {
    if (finalClose) {
      this.readyState = FakeEventSource.CLOSED;
    }
    this.onerror?.(new Event("error"));
  }
}

const makeTick = (overrides: Partial<LiveTickPayload> = {}): LiveTickPayload => ({
  instrument_id: 1001,
  native_currency: "USD",
  bid: "100",
  ask: "101",
  last: "100.5",
  quoted_at: "2026-04-25T12:00:00+00:00",
  display: null,
  ...overrides,
});

describe("useLiveQuote", () => {
  beforeEach(() => {
    FakeEventSource.instances = [];
    vi.stubGlobal("EventSource", FakeEventSource);
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("opens a stream for the given instrument id and yields ticks", () => {
    const { result } = renderHook(() => useLiveQuote(1001));
    expect(FakeEventSource.instances).toHaveLength(1);
    expect(FakeEventSource.instances[0]!.url).toBe("/sse/quotes?ids=1001");

    act(() => {
      FakeEventSource.instances[0]!.fireOpen();
    });
    expect(result.current.connected).toBe(true);

    act(() => {
      FakeEventSource.instances[0]!.fireMessage(JSON.stringify(makeTick()));
    });
    expect(result.current.tick?.instrument_id).toBe(1001);
    expect(result.current.tick?.bid).toBe("100");
  });

  it("ignores ticks for foreign instrument ids (defensive)", () => {
    const { result } = renderHook(() => useLiveQuote(1001));
    act(() => {
      FakeEventSource.instances[0]!.fireMessage(
        JSON.stringify(makeTick({ instrument_id: 9999 })),
      );
    });
    expect(result.current.tick).toBeNull();
  });

  it("ignores malformed JSON frames without flipping unavailable", () => {
    const { result } = renderHook(() => useLiveQuote(1001));
    act(() => {
      FakeEventSource.instances[0]!.fireMessage("not json");
    });
    expect(result.current.tick).toBeNull();
    expect(result.current.unavailable).toBe(false);
  });

  it("sets unavailable only on definitive CLOSED, not on transient errors", () => {
    const { result } = renderHook(() => useLiveQuote(1001));

    // Transient error (reconnect attempt) — must NOT flip unavailable.
    act(() => {
      FakeEventSource.instances[0]!.fireError(false);
    });
    expect(result.current.unavailable).toBe(false);

    // Definitive close — now flip.
    act(() => {
      FakeEventSource.instances[0]!.fireError(true);
    });
    expect(result.current.unavailable).toBe(true);
  });

  it("opens a new stream when the instrument id changes", () => {
    const { rerender } = renderHook(({ id }: { id: number }) => useLiveQuote(id), {
      initialProps: { id: 1001 },
    });
    expect(FakeEventSource.instances).toHaveLength(1);

    rerender({ id: 2002 });
    expect(FakeEventSource.instances).toHaveLength(2);
    expect(FakeEventSource.instances[1]!.url).toBe("/sse/quotes?ids=2002");
    // First stream was closed.
    expect(FakeEventSource.instances[0]!.readyState).toBe(FakeEventSource.CLOSED);
  });

  it("does nothing when id is null or undefined", () => {
    renderHook(() => useLiveQuote(null));
    expect(FakeEventSource.instances).toHaveLength(0);
  });

  it("closes the stream on unmount", () => {
    const { unmount } = renderHook(() => useLiveQuote(1001));
    const source = FakeEventSource.instances[0]!;
    unmount();
    expect(source.readyState).toBe(FakeEventSource.CLOSED);
  });

  it("ignores late events from the OLD source after id change (stale-event guard)", () => {
    // Regression for Codex review high finding on #488: a queued
    // message or error on the previous EventSource can fire after
    // cleanup has swapped in the new source; without a guard the
    // stale handler calls setTick with the old instrument's data.
    const { result, rerender } = renderHook(({ id }: { id: number }) => useLiveQuote(id), {
      initialProps: { id: 1001 },
    });
    const oldSource = FakeEventSource.instances[0]!;

    // Switch ids. Cleanup closes oldSource; new source opens.
    rerender({ id: 2002 });
    const newSource = FakeEventSource.instances[1]!;

    // Stale frame from old source AFTER cleanup — must be ignored.
    act(() => {
      oldSource.fireMessage(
        JSON.stringify(makeTick({ instrument_id: 1001, bid: "STALE" })),
      );
      oldSource.fireError(true);
    });
    expect(result.current.tick).toBeNull();
    expect(result.current.unavailable).toBe(false);

    // Fresh frame from new source — still gets through.
    act(() => {
      newSource.fireOpen();
      newSource.fireMessage(
        JSON.stringify(makeTick({ instrument_id: 2002, bid: "FRESH" })),
      );
    });
    expect(result.current.tick?.bid).toBe("FRESH");
    expect(result.current.connected).toBe(true);
  });

  it("no-ops when EventSource is undefined (SSR / non-browser env)", () => {
    // Simulate an environment without EventSource (e.g. Node SSR).
    // The hook must NOT throw, NOT construct anything, and must
    // leave tick/connected/unavailable at their initial values so
    // the page falls back to its REST snapshot.
    vi.unstubAllGlobals();
    vi.stubGlobal("EventSource", undefined);
    const { result } = renderHook(() => useLiveQuote(1001));
    expect(FakeEventSource.instances).toHaveLength(0);
    expect(result.current.tick).toBeNull();
    expect(result.current.connected).toBe(false);
    expect(result.current.unavailable).toBe(false);
  });

  it("resets state when the subscribed id changes", () => {
    const { result, rerender } = renderHook(({ id }: { id: number }) => useLiveQuote(id), {
      initialProps: { id: 1001 },
    });
    act(() => {
      FakeEventSource.instances[0]!.fireOpen();
      FakeEventSource.instances[0]!.fireMessage(JSON.stringify(makeTick()));
    });
    expect(result.current.tick?.instrument_id).toBe(1001);

    rerender({ id: 2002 });
    expect(result.current.tick).toBeNull();
    expect(result.current.connected).toBe(false);
  });
});

describe("liveTickDisplayPrice", () => {
  it("prefers display block when present", () => {
    const out = liveTickDisplayPrice(
      makeTick({
        display: { currency: "GBP", bid: "75", ask: "76", last: "75.5" },
      }),
    );
    expect(out).toEqual({ value: "75.5", currency: "GBP" });
  });

  it("falls back to display.bid when last is null", () => {
    const out = liveTickDisplayPrice(
      makeTick({
        display: { currency: "GBP", bid: "75", ask: "76", last: null },
      }),
    );
    expect(out?.value).toBe("75");
  });

  it("uses native triple when display is null", () => {
    const out = liveTickDisplayPrice(makeTick({ display: null, last: "100.5" }));
    expect(out).toEqual({ value: "100.5", currency: "USD" });
  });

  it("returns null for a null tick", () => {
    expect(liveTickDisplayPrice(null)).toBeNull();
  });
});
