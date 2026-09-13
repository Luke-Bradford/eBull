/**
 * Tests for LivePriceCell (#2133).
 *
 * The cell renders ONE currency — the one its row is denominated in —
 * on both the REST-fallback and the live-tick path. The bug this file
 * guards is a row that agrees with itself until a tick lands and then
 * flips its Price to the display currency while every other money cell
 * stays native.
 *
 * ``useLiveTickFreshness`` is mocked rather than driven through a fake
 * EventSource: the SSE plumbing already has its own coverage in
 * LiveQuoteProvider.test.tsx, and what matters here is only which tick
 * block reaches the DOM and whether it is marked as a cache (#2944).
 */
import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, cleanup } from "@testing-library/react";

import type { LiveConnectionStatus } from "@/lib/liveQuoteConnection";
import type { LiveTickPayload } from "@/lib/useLiveQuote";
import { LivePriceCell } from "./LivePriceCell";

interface Freshness {
  tick: LiveTickPayload | null;
  status: LiveConnectionStatus;
  authoritative: boolean;
}

const useLiveTickMock = vi.fn<(id: number | null | undefined) => Freshness>();

vi.mock("./LiveQuoteProvider", () => ({
  useLiveTickFreshness: (id: number | null | undefined) => useLiveTickMock(id),
}));

/** Default the legacy cases to a healthy live stream. */
function live(tick: LiveTickPayload | null): Freshness {
  return { tick, status: tick === null ? "connecting" : "live", authoritative: tick !== null };
}

/** A tick that converted cleanly: native USD 100.50, display GBP 75.50. */
const convertedTick: LiveTickPayload = {
  instrument_id: 1001,
  native_currency: "USD",
  bid: "100",
  ask: "101",
  last: "100.5",
  quoted_at: "2026-07-29T12:00:00+00:00",
  display: { currency: "GBP", bid: "75", ask: "76", last: "75.5" },
};

beforeEach(() => {
  cleanup();
  useLiveTickMock.mockReset();
});

describe("LivePriceCell", () => {
  it("renders the display figure for a normal (converted) row", () => {
    useLiveTickMock.mockReturnValue(live(convertedTick));
    render(<LivePriceCell instrumentId={1001} fallback={75.4} currency="GBP" />);
    expect(screen.getByText("£75.50")).toBeInTheDocument();
  });

  it("stays in native currency on a live tick when the row is FX-degraded", () => {
    // The row's money is USD because the FX rate was missing (#2129). The tick
    // itself converted fine, so the pre-#2133 cell rendered £75.50 here while
    // Invested / Value / P&L on the same row read $.
    useLiveTickMock.mockReturnValue(live(convertedTick));
    render(<LivePriceCell instrumentId={1001} fallback={100.4} currency="USD" />);
    expect(screen.getByText("US$100.50")).toBeInTheDocument();
    expect(screen.queryByText("£75.50")).not.toBeInTheDocument();
  });

  it("keeps the REST fallback when no tick block is in the row's currency", () => {
    // Row is in GBP but the tick carries no display block, so nothing on it is
    // denominated in GBP. A stale-but-true £ number beats a live $ number
    // wearing a £ sign.
    useLiveTickMock.mockReturnValue(live({ ...convertedTick, display: null }));
    render(<LivePriceCell instrumentId={1001} fallback={75.4} currency="GBP" />);
    expect(screen.getByText("£75.40")).toBeInTheDocument();
  });

  it("renders the fallback in the row currency before any tick arrives", () => {
    useLiveTickMock.mockReturnValue(live(null));
    render(<LivePriceCell instrumentId={1001} fallback={100.4} currency="USD" />);
    expect(screen.getByText("US$100.40")).toBeInTheDocument();
  });

  it("renders an em dash when there is neither a tick nor a fallback", () => {
    useLiveTickMock.mockReturnValue(live(null));
    render(<LivePriceCell instrumentId={1001} fallback={null} currency="USD" />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  // #2944 — a tick retained across a dropped or reopened stream is a cache.
  // It keeps the number (it is still the most recent price anyone has) but
  // must stop reading as live, per
  // `.claude/skills/frontend/safety-state-ui.md`.
  it.each([
    ["reconnecting", "Price stream reconnecting — showing last received price"],
    ["unavailable", "Price stream unavailable — showing last received price"],
    ["live", "Waiting for a live price — showing last received price"],
  ] as const)("marks a retained tick as stale while %s", (status, reason) => {
    useLiveTickMock.mockReturnValue({ tick: convertedTick, status, authoritative: false });
    render(<LivePriceCell instrumentId={1001} fallback={75.4} currency="GBP" />);

    const cell = screen.getByTestId("stale-live-price");
    // The number is still the tick's, not the fallback's — hiding a recent
    // price is its own harm.
    expect(cell).toHaveTextContent("£75.50");
    expect(cell).toHaveAttribute("title", reason);
    expect(screen.queryByTestId("authoritative-live-price")).not.toBeInTheDocument();
  });

  it("renders an authoritative tick unmarked", () => {
    useLiveTickMock.mockReturnValue({
      tick: convertedTick,
      status: "live",
      authoritative: true,
    });
    render(<LivePriceCell instrumentId={1001} fallback={75.4} currency="GBP" />);
    expect(screen.getByTestId("authoritative-live-price")).toHaveTextContent("£75.50");
    expect(screen.queryByTestId("stale-live-price")).not.toBeInTheDocument();
  });
});
