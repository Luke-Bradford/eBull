/**
 * Tests for ChartPage (#576 Phase 1).
 *
 * lightweight-charts cannot render in jsdom (Canvas API absent), so we stub
 * ChartCanvas to a simple div. What we pin here is the page's contract:
 *
 *   - Symbol + back-link render
 *   - Default range is 1Y
 *   - Clicking a range button updates the URL param to ?range=<id>
 *   - Empty state when data has no valid rows
 *   - Error state propagates a retry button
 *   - Fetch calls with the active range
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";

// Stub ChartCanvas so lightweight-charts is never touched in jsdom.
vi.mock("@/components/instrument/PriceChart", () => ({
  ChartCanvas: ({ symbol }: { symbol: string }) => (
    <div data-testid="chart-canvas-stub" data-symbol={symbol} />
  ),
}));

vi.mock("@/api/instruments", () => ({
  fetchInstrumentSummary: vi.fn(),
  fetchInstrumentCandles: vi.fn(),
}));

import { fetchInstrumentSummary, fetchInstrumentCandles } from "@/api/instruments";
import type { InstrumentCandles, InstrumentSummary } from "@/api/types";
import { ChartPage } from "./ChartPage";

const mockSummary = vi.mocked(fetchInstrumentSummary);
const mockCandles = vi.mocked(fetchInstrumentCandles);

function makeCandles(
  range: InstrumentCandles["range"],
  rows: InstrumentCandles["rows"] = [],
): InstrumentCandles {
  return { symbol: "AAPL", range, days: 365, rows };
}

function makeSummary(): InstrumentSummary {
  return {
    instrument_id: 1,
    is_tradable: true,
    coverage_tier: 1,
    identity: { symbol: "AAPL", display_name: "Apple Inc.", market_cap: null, sector: null },
    price: { current: "189.50", day_change: null, day_change_pct: null, week_52_high: null, week_52_low: null, currency: "USD" },
    key_stats: null,
    source: {},
    has_sec_cik: true,
    has_filings_coverage: true,
    capabilities: {},
  } as InstrumentSummary;
}

function twoValidRows(): InstrumentCandles["rows"] {
  return [
    { date: "2026-01-10", open: "180", high: "182", low: "179", close: "181", volume: "1000" },
    { date: "2026-01-11", open: "181", high: "184", low: "180", close: "183", volume: "1200" },
  ];
}

function renderPage(path = "/instrument/AAPL/chart") {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route path="instrument/:symbol/chart" element={<ChartPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  mockSummary.mockResolvedValue(makeSummary());
  mockCandles.mockResolvedValue(makeCandles("1y", twoValidRows()));
});

afterEach(() => vi.clearAllMocks());

describe("ChartPage — header", () => {
  it("renders the symbol heading and back-link", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByRole("heading", { name: "AAPL" })).toBeInTheDocument();
    });
    expect(
      screen.getByRole("link", { name: /back to overview/i }),
    ).toHaveAttribute("href", "/instrument/AAPL");
  });

  it("renders the display name from summary data", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByText("Apple Inc.")).toBeInTheDocument();
    });
  });

  it("renders the price from summary data", async () => {
    renderPage();
    await waitFor(() => {
      // price formatted as "USD 189.5"
      expect(screen.getByText(/189/)).toBeInTheDocument();
    });
  });
});

describe("ChartPage — range picker", () => {
  it("renders all seven range buttons", async () => {
    renderPage();
    for (const r of ["1w", "1m", "3m", "6m", "1y", "5y", "max"]) {
      expect(screen.getByTestId(`chart-range-${r}`)).toBeInTheDocument();
    }
  });

  it("defaults to 1y range and fetches with it", async () => {
    renderPage();
    await waitFor(() => {
      expect(mockCandles).toHaveBeenCalledWith("AAPL", "1y");
    });
  });

  it("clicking a range button updates the URL and refetches with new range", async () => {
    mockCandles.mockImplementation((_, range) =>
      Promise.resolve(makeCandles(range, twoValidRows())),
    );
    const user = userEvent.setup();
    renderPage();
    await waitFor(() => {
      expect(mockCandles).toHaveBeenCalledWith("AAPL", "1y");
    });
    await user.click(screen.getByTestId("chart-range-3m"));
    await waitFor(() => {
      expect(mockCandles).toHaveBeenCalledWith("AAPL", "3m");
    });
  });

  it("honours a pre-set ?range= query param from the URL", async () => {
    mockCandles.mockImplementation((_, range) =>
      Promise.resolve(makeCandles(range, twoValidRows())),
    );
    renderPage("/instrument/AAPL/chart?range=5y");
    await waitFor(() => {
      expect(mockCandles).toHaveBeenCalledWith("AAPL", "5y");
    });
  });

  it("falls back to default range for an unrecognised ?range= value", async () => {
    renderPage("/instrument/AAPL/chart?range=garbage");
    await waitFor(() => {
      expect(mockCandles).toHaveBeenCalledWith("AAPL", "1y");
    });
  });
});

describe("ChartPage — chart body", () => {
  it("renders the chart canvas when data has >= 2 valid rows", async () => {
    renderPage();
    await waitFor(() => {
      expect(screen.getByTestId("chart-canvas-stub")).toBeInTheDocument();
    });
  });

  it("shows empty state when rows array is empty", async () => {
    mockCandles.mockResolvedValue(makeCandles("1y", []));
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/No price data/i)).toBeInTheDocument();
    });
    expect(screen.queryByTestId("chart-canvas-stub")).not.toBeInTheDocument();
  });

  it("shows empty state when rows have no valid OHLC", async () => {
    mockCandles.mockResolvedValue(
      makeCandles("1y", [
        { date: "2026-01-10", open: null, high: null, low: null, close: "181", volume: "1000" },
        { date: "2026-01-11", open: null, high: null, low: null, close: "183", volume: "1200" },
      ]),
    );
    renderPage();
    await waitFor(() => {
      expect(screen.getByText(/No price data/i)).toBeInTheDocument();
    });
    expect(screen.queryByTestId("chart-canvas-stub")).not.toBeInTheDocument();
  });

  it("shows retry button on fetch error", async () => {
    mockCandles.mockRejectedValue(new Error("network down"));
    renderPage();
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument();
    });
    expect(screen.queryByTestId("chart-canvas-stub")).not.toBeInTheDocument();
  });
});
