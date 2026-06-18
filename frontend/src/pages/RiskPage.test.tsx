import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import type { InstrumentRiskMetrics, RiskWindowMetrics } from "@/api/types";
import { RiskPage } from "./RiskPage";

vi.mock("@/api/instruments", async () => {
  const actual =
    await vi.importActual<typeof import("@/api/instruments")>(
      "@/api/instruments",
    );
  return { ...actual, fetchInstrumentRiskMetrics: vi.fn() };
});

// Recharts needs a real layout pipeline jsdom lacks; the chart internals are
// covered structurally elsewhere. The page test focuses on data wiring,
// range slicing, and per-card status branching.
vi.mock("@/components/risk/riskCharts", () => ({
  UnderwaterChart: ({ points }: { points: ReadonlyArray<unknown> }) => (
    <div data-testid="mock-underwater">dd {points.length}</div>
  ),
  RollingVolChart: ({ points }: { points: ReadonlyArray<unknown> }) => (
    <div data-testid="mock-vol">vol {points.length}</div>
  ),
  ReturnsHistogram: ({ bins }: { bins: ReadonlyArray<unknown> }) => (
    <div data-testid="mock-hist">hist {bins.length}</div>
  ),
  BetaScatterChart: ({
    points,
    beta,
  }: {
    points: ReadonlyArray<unknown>;
    beta: string | null;
  }) => (
    <div data-testid="mock-beta">
      beta {points.length} {String(beta)}
    </div>
  ),
}));

import { fetchInstrumentRiskMetrics } from "@/api/instruments";

const mockRisk = vi.mocked(fetchInstrumentRiskMetrics);

afterEach(() => vi.clearAllMocks());

function makeWindow(
  key: string,
  partial: Partial<RiskWindowMetrics> = {},
): RiskWindowMetrics {
  return {
    window_key: key,
    cagr: null,
    excess_cagr_vs_spy: null,
    max_drawdown: null,
    current_drawdown: null,
    vol_annualized: null,
    beta: null,
    beta_r2: null,
    calmar: null,
    skew: null,
    excess_kurtosis: null,
    var_5: null,
    worst_day: null,
    best_day: null,
    trailing_1m: null,
    trailing_3m: null,
    trailing_6m: null,
    trailing_1y: null,
    excess_trailing_1m: null,
    excess_trailing_3m: null,
    excess_trailing_6m: null,
    excess_trailing_1y: null,
    n_returns: null,
    beta_n_obs: null,
    window_days: null,
    cagr_status: null,
    vol_status: null,
    beta_status: null,
    drawdown_status: null,
    distribution_status: null,
    calmar_status: null,
    trailing_status: null,
    excess_cagr_status: null,
    ...partial,
  };
}

function makePayload(
  overrides: Partial<InstrumentRiskMetrics> = {},
): InstrumentRiskMetrics {
  return {
    symbol: "AAPL",
    as_of_date: "2026-06-12",
    benchmark_symbol: "SPY",
    metric_version: "risk_v1",
    windows: [
      makeWindow("1y", { cagr: "0.464879" }),
      makeWindow("3y", { cagr: "0.167361" }),
      makeWindow("full", { cagr: "0.30" }),
    ],
    series: {
      drawdown_curve: [
        { date: "2023-01-02", drawdown: "-0.1" },
        { date: "2024-06-12", drawdown: "-0.2" },
        { date: "2025-07-01", drawdown: "-0.05" },
        { date: "2026-06-12", drawdown: "0" },
      ],
      rolling_vol: [
        { date: "2025-07-01", vol: "0.2" },
        { date: "2026-06-12", vol: "0.25" },
      ],
      return_histogram: [{ lower: "-0.05", upper: "0.05", count: 10 }],
      beta_scatter: [{ spy_return: "0.01", inst_return: "0.012" }],
      beta: "1.16",
      beta_r2: "0.49",
    },
    ...overrides,
  };
}

function renderPage(symbol = "AAPL") {
  return render(
    <MemoryRouter initialEntries={[`/instrument/${symbol}/risk`]}>
      <Routes>
        <Route path="instrument/:symbol/risk" element={<RiskPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("RiskPage", () => {
  it("renders the four risk cards + a scalar tile when data lands", async () => {
    mockRisk.mockResolvedValue(makePayload());
    renderPage();

    expect(await screen.findByTestId("mock-underwater")).toBeInTheDocument();
    expect(screen.getByTestId("mock-vol")).toBeInTheDocument();
    expect(screen.getByTestId("mock-hist")).toBeInTheDocument();
    expect(screen.getByTestId("mock-beta")).toBeInTheDocument();
    // CAGR tile from the default 1Y window.
    expect(screen.getByText(/46\.49%/)).toBeInTheDocument();
  });

  it("slices the time-series to the picked range and reselects the window scalar", async () => {
    mockRisk.mockResolvedValue(makePayload());
    renderPage();

    // Default 1Y: cutoff 2025-06-12 keeps the last two drawdown points.
    expect(await screen.findByTestId("mock-underwater")).toHaveTextContent(
      "dd 2",
    );

    await userEvent.click(screen.getByRole("button", { name: "All" }));

    // All: whole series (4 points) + the full-window CAGR scalar.
    await waitFor(() =>
      expect(screen.getByTestId("mock-underwater")).toHaveTextContent("dd 4"),
    );
    expect(screen.getByText(/30\.00%/)).toBeInTheDocument();
  });

  it("shows a benchmark-missing message on the beta card when no overlap", async () => {
    mockRisk.mockResolvedValue(
      makePayload({
        // benchmark_missing is systemic (no SPY overlap) → flagged on the
        // full window, which the full-history beta card reads from.
        windows: [makeWindow("full", { beta_status: "benchmark_missing" })],
        series: {
          drawdown_curve: [{ date: "2026-06-12", drawdown: "0" }],
          rolling_vol: [],
          return_histogram: [],
          beta_scatter: [],
          beta: null,
          beta_r2: null,
        },
      }),
    );
    renderPage();

    await screen.findByTestId("mock-underwater");
    expect(screen.queryByTestId("mock-beta")).not.toBeInTheDocument();
    expect(
      screen.getByText(/No benchmark \(SPY\) data available/i),
    ).toBeInTheDocument();
  });

  it("renders the empty state when no windows are persisted yet", async () => {
    mockRisk.mockResolvedValue(makePayload({ windows: [], series: null }));
    renderPage();

    expect(await screen.findByText(/No risk metrics yet/i)).toBeInTheDocument();
    expect(screen.queryByTestId("mock-underwater")).not.toBeInTheDocument();
  });

  it("renders the error state with a retry control on fetch failure", async () => {
    mockRisk.mockRejectedValue(new Error("risk endpoint down"));
    renderPage();

    await waitFor(() =>
      expect(
        screen.getByRole("button", { name: /retry/i }),
      ).toBeInTheDocument(),
    );
    expect(screen.queryByTestId("mock-underwater")).not.toBeInTheDocument();
  });

  it("back link points to /instrument/:symbol", async () => {
    mockRisk.mockResolvedValue(makePayload());
    renderPage();

    await screen.findByTestId("mock-underwater");
    const backLinks = screen.getAllByRole("link", { name: /Back to AAPL/i });
    expect(backLinks[0]).toHaveAttribute("href", "/instrument/AAPL");
  });
});
