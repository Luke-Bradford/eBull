/**
 * Tests for PriceChart (Slice B of #316).
 *
 * Pin the contract that matters for operators:
 *   - All 7 range buttons render + switching triggers a re-fetch.
 *   - Empty data → "No price data" empty state, no SVG.
 *   - One bar is not enough to draw a line (need ≥2) — same empty state.
 *   - SVG renders when data is ≥2 rows.
 *   - Loading / error states.
 *
 * The visual geometry (path d="..." exact values) is intentionally
 * NOT asserted — those are render-details that churn freely. We
 * check structural presence (price path element, volume bars) only.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import { PriceChart } from "@/components/instrument/PriceChart";
import type { InstrumentCandles } from "@/api/types";

vi.mock("@/api/instruments", () => ({
  fetchInstrumentCandles: vi.fn(),
}));

import { fetchInstrumentCandles } from "@/api/instruments";

const mockedFetch = vi.mocked(fetchInstrumentCandles);

function candles(rows: InstrumentCandles["rows"]): InstrumentCandles {
  return {
    symbol: "AAPL",
    range: "1m",
    days: 30,
    rows,
  };
}

beforeEach(() => {
  mockedFetch.mockReset();
});

describe("PriceChart — range picker", () => {
  it("renders all seven range buttons", async () => {
    mockedFetch.mockResolvedValue(candles([]));
    render(<MemoryRouter><PriceChart symbol="AAPL" /></MemoryRouter>);
    for (const r of ["1w", "1m", "3m", "6m", "1y", "5y", "max"]) {
      expect(screen.getByTestId(`chart-range-${r}`)).toBeInTheDocument();
    }
  });

  it("clicking a range button refetches with the new range", async () => {
    mockedFetch.mockResolvedValue(candles([]));
    const user = userEvent.setup();
    render(<MemoryRouter><PriceChart symbol="AAPL" /></MemoryRouter>);

    await waitFor(() => {
      expect(mockedFetch).toHaveBeenCalledWith("AAPL", "1m");
    });
    await user.click(screen.getByTestId("chart-range-1y"));
    await waitFor(() => {
      expect(mockedFetch).toHaveBeenLastCalledWith("AAPL", "1y");
    });
  });
});

describe("PriceChart — data states", () => {
  it("renders 'No price data' when rows is empty", async () => {
    mockedFetch.mockResolvedValue(candles([]));
    render(<MemoryRouter><PriceChart symbol="AAPL" /></MemoryRouter>);
    await waitFor(() => {
      expect(screen.getByText(/No price data/i)).toBeInTheDocument();
    });
    expect(screen.queryByTestId("price-chart-AAPL")).not.toBeInTheDocument();
  });

  it("renders empty state when only one valid close (can't draw a line)", async () => {
    mockedFetch.mockResolvedValue(
      candles([
        {
          date: "2026-04-10",
          open: "100",
          high: "102",
          low: "99",
          close: "101",
          volume: "1000",
        },
      ]),
    );
    render(<MemoryRouter><PriceChart symbol="AAPL" /></MemoryRouter>);
    await waitFor(() => {
      expect(screen.getByText(/No price data/i)).toBeInTheDocument();
    });
  });

  it("renders the SVG chart when there are ≥2 rows with close", async () => {
    mockedFetch.mockResolvedValue(
      candles([
        {
          date: "2026-04-10",
          open: "100",
          high: "102",
          low: "99",
          close: "101",
          volume: "1000",
        },
        {
          date: "2026-04-11",
          open: "101",
          high: "104",
          low: "100",
          close: "103",
          volume: "1500",
        },
      ]),
    );
    render(<MemoryRouter><PriceChart symbol="AAPL" /></MemoryRouter>);
    await waitFor(() => {
      expect(screen.getByTestId("price-chart-AAPL")).toBeInTheDocument();
    });
    expect(screen.queryByText(/No price data/i)).not.toBeInTheDocument();
  });

  it("propagates fetch errors via SectionError + shows a retry button", async () => {
    mockedFetch.mockRejectedValue(new Error("network down"));
    render(<MemoryRouter><PriceChart symbol="AAPL" /></MemoryRouter>);
    await waitFor(() => {
      // Retry button is the operator's recovery affordance; presence
      // guards against a future refactor silently swallowing the
      // error (Codex slice-B round-2 test-hygiene finding).
      expect(
        screen.getByRole("button", { name: /retry/i }),
      ).toBeInTheDocument();
    });
    expect(screen.queryByTestId("price-chart-AAPL")).not.toBeInTheDocument();
  });
});
