import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";

import * as instrumentsApi from "@/api/instruments";
import type { InstrumentRiskMetrics, RiskWindowMetrics } from "@/api/types";
import {
  RiskInlinePanel,
  riskFlags,
  totalReturnCalmarText,
} from "@/components/instrument/RiskInlinePanel";

function win(partial: Partial<RiskWindowMetrics> = {}): RiskWindowMetrics {
  return {
    window_key: "3y",
    cagr: "0.12",
    excess_cagr_vs_spy: null,
    max_drawdown: "-0.3012",
    current_drawdown: null,
    vol_annualized: "0.2845",
    beta: "1.234",
    beta_r2: "0.456",
    calmar: "0.3984",
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
    n_returns: 752,
    beta_n_obs: 752,
    window_days: 1095,
    cagr_status: "ok",
    vol_status: "ok",
    beta_status: "ok",
    drawdown_status: "ok",
    distribution_status: "ok",
    calmar_status: "ok",
    trailing_status: "ok",
    excess_cagr_status: "ok",
    sector_beta: null,
    sector_beta_r2: null,
    sector_beta_n_obs: null,
    sector_beta_status: null,
    sector_excess_cagr: null,
    sector_excess_cagr_status: null,
    tr_cagr: "0.13",
    tr_calmar: "0.4316",
    tr_status: "ok",
    tr_n_periods: 12,
    ...partial,
  };
}

function payload(windows: RiskWindowMetrics[]): InstrumentRiskMetrics {
  return {
    symbol: "AAPL",
    as_of_date: "2026-09-19",
    benchmark_symbol: "SPY",
    sector_benchmark_symbol: "XLK",
    metric_version: "risk_v1",
    windows,
    series: null,
  };
}

function renderPanel() {
  return render(
    <MemoryRouter>
      <RiskInlinePanel symbol="AAPL" />
    </MemoryRouter>,
  );
}

describe("RiskInlinePanel", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("shows the 3y window with basis, coverage and a drill link", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentRiskMetrics").mockResolvedValue(
      payload([win({ window_key: "1y", vol_annualized: "0.99" }), win()]),
    );
    renderPanel();
    expect(await screen.findByText("28.45%")).toBeInTheDocument();
    expect(screen.queryByText("99.00%")).not.toBeInTheDocument();
    expect(screen.getByText("-30.12%")).toBeInTheDocument();
    expect(screen.getByText("0.4")).toBeInTheDocument();
    expect(screen.getByText("0.43")).toBeInTheDocument();
    expect(screen.getByText("1.23 (R² 0.46)")).toBeInTheDocument();
    expect(screen.getByText("beta vs SPY")).toBeInTheDocument();
    expect(screen.getByText(/752 daily returns/)).toBeInTheDocument();
    expect(screen.getByText(/Price-return basis unless marked/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Risk & returns/ })).toHaveAttribute(
      "href",
      "/instrument/AAPL/risk",
    );
  });

  it("renders an empty state when no 3y window is persisted", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentRiskMetrics").mockResolvedValue(payload([]));
    renderPanel();
    expect(await screen.findByText("No risk metrics computed")).toBeInTheDocument();
  });

  it("renders a retryable error without the exception text", async () => {
    vi.spyOn(instrumentsApi, "fetchInstrumentRiskMetrics").mockRejectedValue(
      new Error("boom internal"),
    );
    renderPanel();
    expect(await screen.findByRole("alert")).toBeInTheDocument();
    expect(screen.queryByText(/boom internal/)).not.toBeInTheDocument();
  });
});

describe("totalReturnCalmarText", () => {
  it("withholds an incomplete dividend record", () => {
    expect(totalReturnCalmarText(win({ tr_status: "tr_incomplete" }))).toBe("withheld");
    expect(totalReturnCalmarText(win({ tr_status: "no_dividends", tr_calmar: "0.3984" }))).toBe(
      "0.4",
    );
    expect(totalReturnCalmarText(win({ tr_status: null }))).toBe("—");
  });
});

describe("riskFlags", () => {
  it("lists flagged metrics only", () => {
    expect(riskFlags(win())).toEqual([]);
    const flags = riskFlags(
      win({ vol_status: "partial_window", beta_status: "benchmark_missing", tr_status: "tr_incomplete" }),
    );
    expect(flags).toHaveLength(3);
    expect(flags[0]).toMatch(/^Volatility: History shorter/);
    expect(flags[1]).toMatch(/^Beta: No benchmark/);
    expect(flags[2]).toMatch(/^Total return:/);
  });
});
