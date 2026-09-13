/**
 * Tests for LiveQuoteProvider — page-level shared SSE for live quotes (#501).
 *
 * Verifies:
 *   - One EventSource per page (not per cell).
 *   - Same id rendered twice on a page consumes from the same stream
 *     and both consumers see the same tick.
 *   - Canonical-set equality: prop changes that don't change the
 *     unique sorted membership do NOT churn the EventSource.
 *   - Cleanup closes the stream on unmount + on canonical-set change.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, act, cleanup } from "@testing-library/react";

import {
  LiveQuoteProvider,
  useLiveTick,
  useLiveTickFreshness,
} from "./LiveQuoteProvider";

interface FakeEventSource {
  url: string;
  withCredentials: boolean;
  readyState: number;
  close: () => void;
  onopen: ((this: EventSource, ev: Event) => unknown) | null;
  onmessage: ((this: EventSource, ev: MessageEvent) => unknown) | null;
  onerror: ((this: EventSource, ev: Event) => unknown) | null;
}

let openedSources: FakeEventSource[] = [];

beforeEach(() => {
  openedSources = [];
  vi.useFakeTimers();
  // jsdom doesn't ship EventSource — install a controllable fake.
  // We capture every constructed instance so tests can assert on
  // count, url, and dispatch synthetic ticks.
  // @ts-expect-error — jsdom global lacks the type
  globalThis.EventSource = class {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSED = 2;
    url: string;
    withCredentials: boolean;
    readyState = 0;
    onopen: ((this: EventSource, ev: Event) => unknown) | null = null;
    onmessage: ((this: EventSource, ev: MessageEvent) => unknown) | null = null;
    onerror: ((this: EventSource, ev: Event) => unknown) | null = null;
    constructor(url: string, init?: { withCredentials?: boolean }) {
      this.url = url;
      this.withCredentials = init?.withCredentials ?? false;
      openedSources.push(this as unknown as FakeEventSource);
    }
    close() {
      this.readyState = 2;
    }
  };
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  // @ts-expect-error — restore
  delete globalThis.EventSource;
});

function PriceConsumer({ id, label }: { id: number; label: string }) {
  const tick = useLiveTick(id);
  return (
    <div data-testid={`consumer-${label}`}>
      {tick === null ? "—" : tick.bid}
    </div>
  );
}

/** Renders both the price and the freshness verdict for one id (#2944). */
function StatusConsumer({ id }: { id: number }) {
  const { tick, status, authoritative } = useLiveTickFreshness(id);
  return (
    <>
      <div data-testid="consumer-a">{tick === null ? "—" : tick.bid}</div>
      <div data-testid="status">{`${status}/${authoritative ? "authoritative" : "stale"}`}</div>
    </>
  );
}

function dispatchTick(idx: number, payload: Record<string, unknown>): void {
  const src = openedSources[idx];
  if (src === undefined || src.onmessage === null) return;
  src.onmessage.call(src as unknown as EventSource, new MessageEvent("message", {
    data: JSON.stringify(payload),
  }));
}

function fireOpen(idx: number): void {
  const src = openedSources[idx];
  if (src === undefined || src.onopen === null) return;
  src.readyState = 1;
  src.onopen.call(src as unknown as EventSource, new Event("open"));
}

function fireError(idx: number, finalClose: boolean): void {
  const src = openedSources[idx];
  if (src === undefined || src.onerror === null) return;
  // finalClose=false leaves readyState CONNECTING, which is what the browser
  // reports while it retries by itself — the case both implementations used
  // to ignore entirely.
  src.readyState = finalClose ? 2 : 0;
  src.onerror.call(src as unknown as EventSource, new Event("error"));
}

describe("LiveQuoteProvider", () => {
  it("opens exactly one EventSource for N consumers on the same page", async () => {
    render(
      <LiveQuoteProvider instrumentIds={[1, 2, 3]}>
        <PriceConsumer id={1} label="a" />
        <PriceConsumer id={2} label="b" />
        <PriceConsumer id={3} label="c" />
      </LiveQuoteProvider>,
    );
    // Debounce timer: advance past it.
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    expect(openedSources).toHaveLength(1);
    expect(openedSources[0]?.url).toContain("ids=1%2C2%2C3");
  });

  it("delivers ticks to every consumer subscribed to the same id", async () => {
    const view = render(
      <LiveQuoteProvider instrumentIds={[42]}>
        <PriceConsumer id={42} label="a" />
        <PriceConsumer id={42} label="b" />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });

    await act(async () => {
      dispatchTick(0, {
        instrument_id: 42,
        bid: "100.50",
        ask: "100.60",
        last: "100.55",
        quoted_at: "2026-04-25T14:30:00Z",
        native_currency: "USD",
        display: null,
      });
    });

    expect(view.getByTestId("consumer-a").textContent).toBe("100.50");
    expect(view.getByTestId("consumer-b").textContent).toBe("100.50");
  });

  it("does NOT reopen the stream when the prop array changes order/identity but membership is the same", async () => {
    const view = render(
      <LiveQuoteProvider instrumentIds={[1, 2, 3]}>
        <PriceConsumer id={1} label="a" />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    expect(openedSources).toHaveLength(1);

    // Reorder + duplicate same membership (Codex round 3 finding 1).
    view.rerender(
      <LiveQuoteProvider instrumentIds={[3, 1, 1, 2]}>
        <PriceConsumer id={1} label="a" />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });

    expect(openedSources).toHaveLength(1);
  });

  it("reopens the stream when the canonical-set membership changes", async () => {
    const view = render(
      <LiveQuoteProvider instrumentIds={[1, 2]}>
        <PriceConsumer id={1} label="a" />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    expect(openedSources).toHaveLength(1);

    view.rerender(
      <LiveQuoteProvider instrumentIds={[1, 2, 3]}>
        <PriceConsumer id={1} label="a" />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });

    expect(openedSources).toHaveLength(2);
    // Prior connection closed.
    expect(openedSources[0]?.readyState).toBe(2);
  });

  it("opens no stream when the id list is empty", async () => {
    render(
      <LiveQuoteProvider instrumentIds={[]}>
        <PriceConsumer id={1} label="a" />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    expect(openedSources).toHaveLength(0);
  });

  it("closes the stream on unmount", async () => {
    const view = render(
      <LiveQuoteProvider instrumentIds={[1]}>
        <PriceConsumer id={1} label="a" />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    const source = openedSources[0];
    expect(source).toBeDefined();
    view.unmount();
    expect(source?.readyState).toBe(2);
  });

  // ---------------------------------------------------------------
  // #2944 — the provider carried the same connection-state defect as
  // useLiveQuote: onerror handled only CLOSED, so a dropped transport
  // left every cell reading as live.
  // ---------------------------------------------------------------

  it("leaves live on a transient error without going unavailable", async () => {
    const view = render(
      <LiveQuoteProvider instrumentIds={[7]}>
        <StatusConsumer id={7} />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    await act(async () => {
      fireOpen(0);
      dispatchTick(0, { instrument_id: 7, bid: "100" });
    });
    expect(view.getByTestId("status").textContent).toBe("live/authoritative");

    await act(async () => {
      fireError(0, /* finalClose */ false);
    });
    expect(view.getByTestId("status").textContent).toBe("reconnecting/stale");
    // The price is kept — a retained tick is marked, not hidden.
    expect(view.getByTestId("consumer-a").textContent).toBe("100");

    await act(async () => {
      fireError(0, /* finalClose */ true);
    });
    expect(view.getByTestId("status").textContent).toBe("unavailable/stale");
  });

  it("does not re-bless a retained tick when the stream reopens with no frame", async () => {
    const view = render(
      <LiveQuoteProvider instrumentIds={[7]}>
        <StatusConsumer id={7} />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    await act(async () => {
      fireOpen(0);
      dispatchTick(0, { instrument_id: 7, bid: "100" });
      fireError(0, false);
      fireOpen(0);
    });
    // Transport back up, but no frame on THIS connection.
    expect(view.getByTestId("status").textContent).toBe("live/stale");

    await act(async () => {
      dispatchTick(0, { instrument_id: 7, bid: "101" });
    });
    expect(view.getByTestId("status").textContent).toBe("live/authoritative");
  });

  it("an open alone never blesses a tick from before it", async () => {
    // Pins the invariant AT THE HANDLER. A real browser always fires `error`
    // before reconnecting, and that clears freshness too — so this sequence
    // (open with no preceding error) cannot occur in practice and the guard
    // it covers is redundant defence, deliberately kept. Asserted so a future
    // edit cannot quietly make `open` the thing that re-blesses a cache.
    const view = render(
      <LiveQuoteProvider instrumentIds={[7]}>
        <StatusConsumer id={7} />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    await act(async () => {
      fireOpen(0);
      dispatchTick(0, { instrument_id: 7, bid: "100" });
      fireOpen(0);
    });
    expect(view.getByTestId("status").textContent).toBe("live/stale");
  });

  it("stops claiming live the moment the canonical set changes, before the debounce fires", async () => {
    const view = render(
      <LiveQuoteProvider instrumentIds={[7]}>
        <StatusConsumer id={7} />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    await act(async () => {
      fireOpen(0);
      dispatchTick(0, { instrument_id: 7, bid: "100" });
    });
    expect(view.getByTestId("status").textContent).toBe("live/authoritative");

    // Cleanup closes the old source immediately; the replacement only opens
    // when the 300ms timer fires. Nothing may read as live in that gap.
    view.rerender(
      <LiveQuoteProvider instrumentIds={[7, 8]}>
        <StatusConsumer id={7} />
      </LiveQuoteProvider>,
    );
    expect(view.getByTestId("status").textContent).toBe("connecting/stale");
  });

  it("drops a frame for an id outside the subscribed set", async () => {
    const view = render(
      <LiveQuoteProvider instrumentIds={[7]}>
        <PriceConsumer id={9} label="a" />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
      fireOpen(0);
      dispatchTick(0, { instrument_id: 9, bid: "999" });
    });
    expect(view.getByTestId("consumer-a").textContent).toBe("—");
  });

  it("returns to idle with no ticks when the id list empties", async () => {
    const view = render(
      <LiveQuoteProvider instrumentIds={[7]}>
        <StatusConsumer id={7} />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
      fireOpen(0);
      dispatchTick(0, { instrument_id: 7, bid: "100" });
    });
    expect(view.getByTestId("consumer-a").textContent).toBe("100");

    view.rerender(
      <LiveQuoteProvider instrumentIds={[]}>
        <StatusConsumer id={7} />
      </LiveQuoteProvider>,
    );
    await act(async () => {
      vi.advanceTimersByTime(400);
    });
    expect(view.getByTestId("status").textContent).toBe("idle/stale");
    expect(view.getByTestId("consumer-a").textContent).toBe("—");
  });
});
