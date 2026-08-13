import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi, afterEach } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import type { InstrumentDividends } from "@/api/instruments";
import type {
  InstrumentFinancials,
  InstrumentPositionDetail,
  InstrumentSummary,
} from "@/api/types";
import { DividendsPage } from "./DividendsPage";

vi.mock("@/api/instruments", async () => {
  const actual = await vi.importActual<typeof import("@/api/instruments")>(
    "@/api/instruments",
  );
  return {
    ...actual,
    fetchInstrumentDividends: vi.fn(),
    fetchInstrumentSummary: vi.fn(),
    fetchInstrumentFinancials: vi.fn(),
  };
});

vi.mock("@/api/portfolio", () => ({
  fetchInstrumentPositions: vi.fn(),
}));

// Mock chart subcomponents — recharts' ResponsiveContainer needs a
// real layout pipeline that jsdom doesn't simulate, and the metric
// helpers are exercised in `dividendsMetrics.test.ts`. The page test
// focuses on data wiring, position-gated rendering, and empty-state
// branching.
vi.mock("@/components/dividends/dividendsCharts", () => ({
  DpsLineChart: () => <div data-testid="mock-dps-line">dps-line</div>,
  CumulativeDpsChart: () => (
    <div data-testid="mock-cumulative">cumulative</div>
  ),
  PayoutRatioChart: ({
    cashflowRows,
  }: {
    cashflowRows: ReadonlyArray<unknown>;
  }) => (
    <div data-testid="mock-payout">payout {cashflowRows.length}</div>
  ),
  YieldOnCostChart: ({ avgEntry }: { avgEntry: number | null }) => (
    <div data-testid="mock-yoc">yoc avg={avgEntry ?? "null"}</div>
  ),
}));

import {
  fetchInstrumentDividends,
  fetchInstrumentFinancials,
  fetchInstrumentSummary,
} from "@/api/instruments";
import { fetchInstrumentPositions } from "@/api/portfolio";

const mockDividends = vi.mocked(fetchInstrumentDividends);
const mockSummary = vi.mocked(fetchInstrumentSummary);
const mockFinancials = vi.mocked(fetchInstrumentFinancials);
const mockPositions = vi.mocked(fetchInstrumentPositions);

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

function makeInstrumentSummary(instrumentId: number = 1001): InstrumentSummary {
  // Minimal shape — DividendsPage only uses `instrument_id`. Cast
  // anything else through `as unknown as` so the test fixture
  // doesn't have to track every field on the canonical type.
  return {
    instrument_id: instrumentId,
    is_tradable: true,
    coverage_tier: 1,
    identity: {
      symbol: "AAPL",
      display_name: "Apple Inc.",
      sector: null,
      industry: null,
      exchange: "NASDAQ",
      country: "US",
      currency: "USD",
      market_cap: null,
    },
    price: null,
    key_stats: null,
    source: {},
    has_sec_cik: true,
    has_filings_coverage: true,
    capabilities: {},
  } as unknown as InstrumentSummary;
}

function makeFinancials(
  rows: InstrumentFinancials["rows"] = [],
): InstrumentFinancials {
  return {
    symbol: "AAPL",
    statement: "cashflow",
    period: "annual",
    currency: "USD",
    source: rows.length > 0 ? "financial_periods" : "unavailable",
    rows,
  };
}

function makePosition(
  partial: Partial<InstrumentPositionDetail> = {},
): InstrumentPositionDetail {
  return {
    instrument_id: 1001,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    currency: "USD",
    current_price: 200,
    total_units: 10,
    avg_entry: 150,
    total_invested: 1500,
    total_value: 2000,
    total_pnl: 500,
    ...partial,
  } as unknown as InstrumentPositionDetail;
}

function mockHappyPath(
  options: { held?: boolean } = {},
): void {
  mockDividends.mockResolvedValue({
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
  mockSummary.mockResolvedValue(makeInstrumentSummary());
  mockFinancials.mockResolvedValue(
    makeFinancials([
      {
        period_end: "2025-12-31",
        period_type: "FY",
        values: { operating_cf: "1000", capex: "200", dividends_paid: "100" },
      },
    ]),
  );
  if (options.held === true) {
    mockPositions.mockResolvedValue(makePosition({ avg_entry: 150 }));
  } else {
    mockPositions.mockResolvedValue(makePosition({ total_units: 0, avg_entry: null }));
  }
}

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
  it("renders the full set of analytical panes (DPS, cumulative, payout) when data lands", async () => {
    mockHappyPath();
    renderPage("AAPL");

    expect(await screen.findByTestId("mock-dps-line")).toBeInTheDocument();
    expect(screen.getByTestId("mock-cumulative")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByTestId("mock-payout")).toHaveTextContent("payout 1");
    });
  });

  it("renders the yield-on-cost pane only when the operator holds the instrument", async () => {
    mockHappyPath({ held: true });
    renderPage("AAPL");
    expect(await screen.findByTestId("mock-yoc")).toHaveTextContent(
      "avg=150",
    );
  });

  it("hides the yield-on-cost pane entirely when not held (zero units)", async () => {
    mockHappyPath({ held: false });
    renderPage("AAPL");
    await screen.findByTestId("mock-dps-line");
    expect(screen.queryByTestId("mock-yoc")).not.toBeInTheDocument();
  });

  it("hides the yield-on-cost pane when the position endpoint returns 404", async () => {
    mockHappyPath({ held: true });
    const { ApiError } = await import("@/api/client");
    mockPositions.mockRejectedValue(
      new ApiError(404, "Instrument not in portfolio"),
    );
    renderPage("AAPL");
    await screen.findByTestId("mock-dps-line");
    expect(screen.queryByTestId("mock-yoc")).not.toBeInTheDocument();
  });

  it("hides the yield-on-cost pane when avg_entry is non-positive (math undefined)", async () => {
    mockHappyPath({ held: true });
    mockPositions.mockResolvedValue(
      makePosition({ total_units: 10, avg_entry: 0 }),
    );
    renderPage("AAPL");
    await screen.findByTestId("mock-dps-line");
    expect(screen.queryByTestId("mock-yoc")).not.toBeInTheDocument();
  });

  it("renders payout-pane retry hint when the cashflow endpoint errors, leaving the rest of the page intact", async () => {
    mockHappyPath();
    mockFinancials.mockRejectedValue(new Error("cashflow down"));
    renderPage("AAPL");
    await screen.findByTestId("mock-dps-line");
    expect(screen.getByText(/Cash-flow data unavailable/i)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /Retry/i }),
    ).toBeInTheDocument();
    // Other panes still render
    expect(screen.getByTestId("mock-cumulative")).toBeInTheDocument();
  });

  it("passes an empty cashflow array to the payout pane on source='unavailable'", async () => {
    mockHappyPath();
    mockFinancials.mockResolvedValue(makeFinancials([]));
    renderPage("AAPL");
    expect(await screen.findByTestId("mock-payout")).toHaveTextContent(
      "payout 0",
    );
  });

  it("renders Per-FY totals section with summed DPS per fiscal year", async () => {
    mockHappyPath();
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
    mockHappyPath();
    mockDividends.mockResolvedValue({
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
    expect(screen.queryByTestId("mock-dps-line")).not.toBeInTheDocument();
  });

  it("back link points to /instrument/:symbol", async () => {
    mockHappyPath();
    renderPage("AAPL");

    await waitFor(() =>
      expect(screen.getByText(/Back to AAPL/i)).toBeInTheDocument(),
    );

    const backLinks = screen.getAllByRole("link", { name: /Back to AAPL/i });
    expect(backLinks[0]).toHaveAttribute("href", "/instrument/AAPL");
  });
});
