/**
 * Tests for LivePriceCell (#2133).
 *
 * The cell renders ONE currency — the one its row is denominated in —
 * on both the REST-fallback and the live-tick path. The bug this file
 * guards is a row that agrees with itself until a tick lands and then
 * flips its Price to the display currency while every other money cell
 * stays native.
 *
 * ``useLiveTick`` is mocked rather than driven through a fake
 * EventSource: the SSE plumbing already has its own coverage in
 * LiveQuoteProvider.test.tsx, and what matters here is only which tick
 * block reaches the DOM.
 */
import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, cleanup } from "@testing-library/react";

import type { LiveTickPayload } from "@/lib/useLiveQuote";
import { LivePriceCell } from "./LivePriceCell";

const useLiveTickMock = vi.fn<(id: number | null | undefined) => LiveTickPayload | null>();

vi.mock("./LiveQuoteProvider", () => ({
  useLiveTick: (id: number | null | undefined) => useLiveTickMock(id),
}));

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
    useLiveTickMock.mockReturnValue(convertedTick);
    render(<LivePriceCell instrumentId={1001} fallback={75.4} currency="GBP" />);
    expect(screen.getByText("£75.50")).toBeInTheDocument();
  });

  it("stays in native currency on a live tick when the row is FX-degraded", () => {
    // The row's money is USD because the FX rate was missing (#2129). The tick
    // itself converted fine, so the pre-#2133 cell rendered £75.50 here while
    // Invested / Value / P&L on the same row read $.
    useLiveTickMock.mockReturnValue(convertedTick);
    render(<LivePriceCell instrumentId={1001} fallback={100.4} currency="USD" />);
    expect(screen.getByText("US$100.50")).toBeInTheDocument();
    expect(screen.queryByText("£75.50")).not.toBeInTheDocument();
  });

  it("keeps the REST fallback when no tick block is in the row's currency", () => {
    // Row is in GBP but the tick carries no display block, so nothing on it is
    // denominated in GBP. A stale-but-true £ number beats a live $ number
    // wearing a £ sign.
    useLiveTickMock.mockReturnValue({ ...convertedTick, display: null });
    render(<LivePriceCell instrumentId={1001} fallback={75.4} currency="GBP" />);
    expect(screen.getByText("£75.40")).toBeInTheDocument();
  });

  it("renders the fallback in the row currency before any tick arrives", () => {
    useLiveTickMock.mockReturnValue(null);
    render(<LivePriceCell instrumentId={1001} fallback={100.4} currency="USD" />);
    expect(screen.getByText("US$100.40")).toBeInTheDocument();
  });

  it("renders an em dash when there is neither a tick nor a fallback", () => {
    useLiveTickMock.mockReturnValue(null);
    render(<LivePriceCell instrumentId={1001} fallback={null} currency="USD" />);
    expect(screen.getByText("—")).toBeInTheDocument();
  });
});
