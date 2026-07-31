import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { FundamentalsPage } from "@/pages/FundamentalsPage";
import * as api from "@/api/instruments";

// Mock the chart subtree — recharts' ResponsiveContainer needs real
// layout, and the metric helpers are exercised in
// `fundamentalsMetrics.test.ts`. The page test focuses on routing,
// fetch coordination, period toggling, and empty-state branching.
vi.mock("@/components/fundamentals/fundamentalsCharts", () => {
  function makeStub(
    label: string,
  ): (props: { periods?: ReadonlyArray<unknown>; period?: unknown }) => JSX.Element {
    return ({ periods, period }) => (
      <div data-testid={`mock-${label}`}>
        {label} {Array.isArray(periods) ? periods.length : period ? "1" : "0"}
      </div>
    );
  }
  return {
    PnlStackedChart: makeStub("pnl"),
    MarginTrendsChart: makeStub("margins"),
    YoyGrowthChart: makeStub("yoy"),
    CashflowWaterfallChart: makeStub("waterfall"),
    BalanceStructureChart: makeStub("balance"),
    DebtStructureChart: makeStub("debt"),
    DupontChart: makeStub("dupont"),
    RoicChart: makeStub("roic"),
    FcfChart: makeStub("fcf"),
  };
});

const SAMPLE_INCOME = {
  symbol: "GME",
  statement: "income" as const,
  period: "quarterly" as const,
  currency: "USD",
  source: "financial_periods" as const,
  rows: [
    {
      period_end: "2026-03-31",
      period_type: "quarterly",
      values: { revenue: "100", net_income: "10" },
    },
  ],
};
const SAMPLE_BALANCE = {
  symbol: "GME",
  statement: "balance" as const,
  period: "quarterly" as const,
  currency: "USD",
  source: "financial_periods" as const,
  rows: [
    {
      period_end: "2026-03-31",
      period_type: "quarterly",
      values: { total_assets: "1000", shareholders_equity: "500" },
    },
  ],
};
const SAMPLE_CASHFLOW = {
  symbol: "GME",
  statement: "cashflow" as const,
  period: "quarterly" as const,
  currency: "USD",
  source: "financial_periods" as const,
  rows: [
    {
      period_end: "2026-03-31",
      period_type: "quarterly",
      values: { operating_cf: "50", capex: "10" },
    },
  ],
};

function mockHappyPath(): void {
  vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
    ((
      _symbol: string,
      query: { statement: "income" | "balance" | "cashflow" },
    ) => {
      if (query.statement === "income") return Promise.resolve(SAMPLE_INCOME);
      if (query.statement === "balance") return Promise.resolve(SAMPLE_BALANCE);
      return Promise.resolve(SAMPLE_CASHFLOW);
    }) as never,
  );
}

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <Routes>
        <Route
          path="/instrument/:symbol/fundamentals"
          element={<FundamentalsPage />}
        />
      </Routes>
    </MemoryRouter>,
  );
}

describe("FundamentalsPage", () => {
  it("renders all nine panes when the three statement endpoints succeed", async () => {
    mockHappyPath();
    renderAt("/instrument/GME/fundamentals");

    expect(await screen.findByTestId("mock-pnl")).toBeInTheDocument();
    expect(screen.getByTestId("mock-margins")).toBeInTheDocument();
    expect(screen.getByTestId("mock-yoy")).toBeInTheDocument();
    expect(screen.getByTestId("mock-waterfall")).toBeInTheDocument();
    expect(screen.getByTestId("mock-balance")).toBeInTheDocument();
    expect(screen.getByTestId("mock-debt")).toBeInTheDocument();
    expect(screen.getByTestId("mock-dupont")).toBeInTheDocument();
    expect(screen.getByTestId("mock-roic")).toBeInTheDocument();
    expect(screen.getByTestId("mock-fcf")).toBeInTheDocument();
  });

  it("toggles the period via ?period= search param", async () => {
    const stub = vi
      .spyOn(api, "fetchInstrumentFinancials")
      .mockImplementation(((
        _symbol: string,
        query: { period: "quarterly" | "annual" },
      ) => {
        const which = query.period === "annual" ? "annual" : "quarterly";
        return Promise.resolve({
          ...SAMPLE_INCOME,
          period: which,
        });
      }) as never);
    renderAt("/instrument/GME/fundamentals");
    await screen.findByTestId("mock-pnl");
    fireEvent.click(screen.getByTestId("fundamentals-period-annual"));
    await waitFor(() => {
      const calls = stub.mock.calls.map(
        (c) => (c as unknown as [string, { period: string }])[1].period,
      );
      expect(calls).toContain("annual");
    });
  });

  it("shows a 'no SEC XBRL coverage' empty state when every statement reports source='unavailable'", async () => {
    // The real /financials contract: 200 OK with source='unavailable'
    // and rows=[] for non-SEC instruments. A 404 only fires when the
    // route param names no instrument at all — a separate empty state.
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: "income" | "balance" | "cashflow" }) =>
        Promise.resolve({
          symbol: "GME",
          statement: query.statement,
          period: "quarterly",
          currency: null,
          source: "unavailable",
          rows: [],
        })) as never,
    );
    renderAt("/instrument/GME/fundamentals");
    expect(
      await screen.findByText(/No SEC XBRL coverage/i),
    ).toBeInTheDocument();
  });

  it("falls through to a generic SectionError when any statement throws", async () => {
    const { ApiError } = await import("@/api/client");
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      ((_symbol: string, query: { statement: "income" | "balance" | "cashflow" }) => {
        if (query.statement === "income") {
          return Promise.reject(new ApiError(500, "boom"));
        }
        return Promise.resolve({
          symbol: "GME",
          statement: query.statement,
          period: "quarterly",
          currency: "USD",
          source: "financial_periods",
          rows: [],
        });
      }) as never,
    );
    renderAt("/instrument/GME/fundamentals");
    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: /Retry/i }),
      ).toBeInTheDocument();
    });
  });

  it("links to the L3 raw statements view from the page header", async () => {
    mockHappyPath();
    renderAt("/instrument/GME/fundamentals");
    const link = await screen.findByRole("link", { name: /Raw statements/i });
    expect(link).toHaveAttribute("href", "/instrument/GME?tab=financials");
  });

  // ---- #2184 -------------------------------------------------------------

  it("renders an EmptyState (not the red error banner) on a 404", async () => {
    const { ApiError } = await import("@/api/client");
    vi.spyOn(api, "fetchInstrumentFinancials").mockImplementation(
      (() => Promise.reject(new ApiError(404, "Instrument ZZZZ not found"))) as never,
    );
    renderAt("/instrument/ZZZZ/fundamentals");

    expect(await screen.findByText(/Instrument not found/i)).toBeInTheDocument();
    expect(screen.getByText(/No instrument matches "ZZZZ"/i)).toBeInTheDocument();
    // The red "Failed to load. Check the browser console" banner and its
    // Retry affordance must NOT appear — nothing failed.
    expect(screen.queryByRole("button", { name: /Retry/i })).toBeNull();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("shows the RESOLVED symbol in the heading when the route param is a numeric id", async () => {
    mockHappyPath();
    renderAt("/instrument/1699/fundamentals");

    // Payload echoes symbol='GME'; the heading must not read "1699".
    expect(
      await screen.findByRole("heading", { name: /Fundamentals — GME/i }),
    ).toBeInTheDocument();
    expect(screen.queryByText(/Fundamentals — 1699/)).toBeNull();
    // Sibling links follow the resolved symbol too, so the operator does
    // not carry the id form into the next page.
    expect(
      screen.getByRole("link", { name: /Raw statements/i }),
    ).toHaveAttribute("href", "/instrument/GME?tab=financials");
  });
});
