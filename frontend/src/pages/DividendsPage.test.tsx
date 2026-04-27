import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi, afterEach } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import type { InstrumentDividends } from "@/api/instruments";
import { DividendsPage } from "./DividendsPage";

vi.mock("@/api/instruments", () => ({
  fetchInstrumentDividends: vi.fn(),
}));

import { fetchInstrumentDividends } from "@/api/instruments";
const mockFetch = vi.mocked(fetchInstrumentDividends);

afterEach(() => vi.clearAllMocks());

function makePeriod(
  fy: number,
  qt: string,
  date: string,
  dps: string,
): InstrumentDividends["history"][number] {
  return {
    period_end_date: date,
    period_type: qt,
    fiscal_year: fy,
    fiscal_quarter: null,
    dps_declared: dps,
    dividends_paid: null,
    reported_currency: "USD",
  };
}

function makeSummary(): InstrumentDividends["summary"] {
  return {
    has_dividend: true,
    ttm_dps: "1.0000",
    ttm_dividends_paid: "15000000000.0000",
    ttm_yield_pct: "0.52",
    latest_dps: "0.2500",
    latest_dividend_at: "2025-12-28",
    dividend_streak_q: 40,
    dividend_currency: "USD",
  };
}

/** Render DividendsPage under the correct route structure so useParams works. */
function renderPage(symbol: string) {
  return render(
    <MemoryRouter initialEntries={[`/instrument/${symbol}/dividends`]}>
      <Routes>
        <Route
          path="instrument/:symbol/dividends"
          element={<DividendsPage />}
        />
      </Routes>
    </MemoryRouter>,
  );
}

describe("DividendsPage", () => {
  it("renders full history (12 rows) when API returns 12 history entries", async () => {
    const history: InstrumentDividends["history"] = Array.from(
      { length: 12 },
      (_, i) => {
        const fy = 2025 - Math.floor(i / 4);
        const qNum = 4 - (i % 4);
        return makePeriod(
          fy,
          `Q${qNum}`,
          `${fy}-${String(qNum * 3).padStart(2, "0")}-28`,
          "0.25",
        );
      },
    );
    mockFetch.mockResolvedValue({
      symbol: "AAPL",
      summary: makeSummary(),
      history,
      upcoming: [],
    });

    renderPage("AAPL");

    await waitFor(() =>
      expect(screen.getByText(/Per-quarter history/i)).toBeInTheDocument(),
    );

    // All 12 progressbars should be present.
    const bars = screen.getAllByRole("progressbar");
    expect(bars).toHaveLength(12);
  });

  it("renders Per-FY totals section with summed DPS per fiscal year", async () => {
    mockFetch.mockResolvedValue({
      symbol: "AAPL",
      summary: makeSummary(),
      history: [
        makePeriod(2025, "Q4", "2025-12-28", "0.25"),
        makePeriod(2025, "Q3", "2025-09-28", "0.25"),
        makePeriod(2024, "Q4", "2024-12-28", "0.23"),
        makePeriod(2024, "Q3", "2024-09-28", "0.23"),
      ],
      upcoming: [],
    });

    renderPage("AAPL");

    await waitFor(() =>
      expect(screen.getByText(/Per-FY totals/i)).toBeInTheDocument(),
    );

    expect(screen.getByText("FY2025")).toBeInTheDocument();
    expect(screen.getByText("FY2024")).toBeInTheDocument();
    // FY2025 total = 0.50
    expect(screen.getByText("USD 0.5")).toBeInTheDocument();
    // FY2024 total = 0.46
    expect(screen.getByText("USD 0.46")).toBeInTheDocument();
  });

  it("renders empty state when both history and upcoming are empty", async () => {
    mockFetch.mockResolvedValue({
      symbol: "GME",
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
      upcoming: [],
    });

    renderPage("GME");

    await waitFor(() =>
      expect(screen.getByText(/No dividend data/i)).toBeInTheDocument(),
    );
    expect(screen.queryByText(/Per-quarter history/i)).toBeNull();
  });

  it("back link points to /instrument/:symbol", async () => {
    mockFetch.mockResolvedValue({
      symbol: "AAPL",
      summary: makeSummary(),
      history: [makePeriod(2025, "Q4", "2025-12-28", "0.25")],
      upcoming: [],
    });

    renderPage("AAPL");

    await waitFor(() =>
      expect(screen.getByText(/Back to AAPL/i)).toBeInTheDocument(),
    );

    const backLinks = screen.getAllByRole("link", { name: /Back to AAPL/i });
    // Header back link href must point to instrument overview.
    expect(backLinks[0]).toHaveAttribute("href", "/instrument/AAPL");
  });
});
