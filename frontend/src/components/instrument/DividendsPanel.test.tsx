import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { InstrumentDividends } from "@/api/instruments";

import { DividendsPanel } from "./DividendsPanel";

vi.mock("@/api/instruments", () => ({
  fetchInstrumentDividends: vi.fn(),
}));

import { fetchInstrumentDividends } from "@/api/instruments";
const mockFetch = vi.mocked(fetchInstrumentDividends);


function paid(): InstrumentDividends {
  return {
    symbol: "AAPL",
    summary: {
      has_dividend: true,
      ttm_dps: "1.0000",
      ttm_dividends_paid: "15000000000.0000",
      ttm_yield_pct: "0.52",
      latest_dps: "0.2500",
      latest_dividend_at: "2025-12-28",
      dividend_streak_q: 40,
      dividend_currency: "USD",
    },
    history: [
      {
        period_end_date: "2025-12-28",
        period_type: "Q4",
        fiscal_year: 2025,
        fiscal_quarter: 4,
        dps_declared: "0.2500",
        dividends_paid: "4000000000.0000",
        reported_currency: "USD",
      },
      {
        period_end_date: "2025-09-28",
        period_type: "Q3",
        fiscal_year: 2025,
        fiscal_quarter: 3,
        dps_declared: "0.2500",
        dividends_paid: "3900000000.0000",
        reported_currency: "USD",
      },
    ],
  };
}


function notPaid(): InstrumentDividends {
  return {
    symbol: "GOOG",
    summary: {
      has_dividend: false,
      ttm_dps: null,
      ttm_dividends_paid: null,
      ttm_yield_pct: null,
      latest_dps: null,
      latest_dividend_at: null,
      dividend_streak_q: 0,
      dividend_currency: null,
    },
    history: [],
  };
}


afterEach(() => vi.clearAllMocks());


describe("DividendsPanel", () => {
  it("renders summary + per-quarter history for a paying instrument", async () => {
    mockFetch.mockResolvedValue(paid());
    render(<DividendsPanel symbol="AAPL" />);

    await waitFor(() => {
      expect(screen.getByText(/TTM yield/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/0.52%/)).toBeInTheDocument();
    expect(screen.getByText(/FY2025 Q4/)).toBeInTheDocument();
    expect(screen.getByText(/FY2025 Q3/)).toBeInTheDocument();
    expect(screen.getByText(/Consecutive quarters/i)).toBeInTheDocument();
    expect(screen.getByText("40")).toBeInTheDocument();
  });

  it("renders empty state when never-paid", async () => {
    mockFetch.mockResolvedValue(notPaid());
    render(<DividendsPanel symbol="GOOG" />);

    await waitFor(() => {
      expect(screen.getByText(/No dividend history on file/i)).toBeInTheDocument();
    });
    expect(screen.queryByText(/TTM yield/i)).not.toBeInTheDocument();
  });

  it("renders error state + retry on fetch failure", async () => {
    mockFetch.mockRejectedValue(new Error("boom"));
    render(<DividendsPanel symbol="AAPL" />);

    await waitFor(() => {
      expect(screen.getByText(/Failed to load/i)).toBeInTheDocument();
    });
  });
});
