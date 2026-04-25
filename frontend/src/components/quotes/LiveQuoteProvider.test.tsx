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

function dispatchTick(idx: number, payload: Record<string, unknown>): void {
  const src = openedSources[idx];
  if (src === undefined || src.onmessage === null) return;
  src.onmessage.call(src as unknown as EventSource, new MessageEvent("message", {
    data: JSON.stringify(payload),
  }));
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
});
