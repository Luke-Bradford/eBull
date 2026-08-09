import { StrictMode } from "react";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as strategiesApi from "@/api/strategies";
import type { FiredSignalsResponse, StrategyOverviewResponse } from "@/api/types";
import { StrategiesPage } from "@/pages/StrategiesPage";

const OVERVIEW: StrategyOverviewResponse = {
  as_of: "2026-08-09T12:00:00Z",
  observation_stage: "forward_observation",
  execution_enabled: false,
  storage_policy: "aggregate_results_only",
  strategies: [
    {
      strategy_id: "s4-volatility-compression-breakout",
      strategy_version: "strategy-registry-v1+abc",
      title: "Volatility compression breakout",
      runnable: false,
      exclusion_reason: "level-based entry has no outcome",
      scan: {
        frontier_date: "2026-08-07",
        updated_at: "2026-08-08T06:45:00Z",
        status: "current",
        fired_entries: 108,
        fired_exits: 0,
        not_fired: 5382,
        not_evaluable: 293,
        exclusions_by_reason: { quarantined_bar: 293 },
      },
      evidence_windows: [
        {
          window_id: "primary-2022-plus",
          label: "Primary: 2022 onward",
          window_start: "2022-01-01",
          window_end: "2026-07-08",
          status: "missing",
          arms: [],
        },
      ],
      legacy_result_count: 0,
      all_recent_evidence_complete: false,
      allocation_ready: false,
      allocation_refusals: ["execution_not_enabled", "recent_evidence_incomplete"],
    },
  ],
};

const SIGNALS: FiredSignalsResponse = {
  items: [
    {
      signal_id: 42,
      strategy_id: "s4-volatility-compression-breakout",
      strategy_version: "strategy-registry-v1+abc",
      instrument_id: 1,
      symbol: "AAA",
      company_name: "Alpha",
      signal_bar_date: "2026-08-06",
      signal_kind: "entry",
      fill_bar_date: "2026-08-07",
      fill_price: "12.34",
      universe: "survivor_only",
      outcome: null,
      exit_bar_date: null,
      exit_price: null,
      gross_return_pct: null,
      outcome_reason: null,
      observation_stage: "forward_observation",
      funding_status: "unfunded",
      funding_reason: "execution_not_enabled",
    },
  ],
  next_cursor: 42,
};

describe("StrategiesPage", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("keeps an excluded strategy and its unfunded signal visible under StrictMode", async () => {
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(OVERVIEW);
    vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue(SIGNALS);
    render(
      <StrictMode>
        <MemoryRouter>
          <StrategiesPage />
        </MemoryRouter>
      </StrictMode>,
    );
    expect(await screen.findByText("Volatility compression breakout")).toBeInTheDocument();
    expect(screen.getByText(/Backtest exclusion:/)).toBeInTheDocument();
    expect(screen.getByText("AAA")).toBeInTheDocument();
    expect(screen.getByText("unfunded · execution disabled")).toBeInTheDocument();
  });

  it("uses the server cursor when the operator requests older signals", async () => {
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(OVERVIEW);
    const signals = vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue(SIGNALS);
    render(
      <MemoryRouter>
        <StrategiesPage />
      </MemoryRouter>,
    );
    await screen.findByText("AAA");
    await userEvent.click(screen.getByRole("button", { name: "Older" }));
    await waitFor(() => expect(signals).toHaveBeenLastCalledWith(42));
  });

  it("renders actionable empty states for successful empty responses", async () => {
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue({ ...OVERVIEW, strategies: [] });
    vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue({ items: [], next_cursor: null });
    render(
      <MemoryRouter>
        <StrategiesPage />
      </MemoryRouter>,
    );
    expect(await screen.findByText("No registered strategies")).toBeInTheDocument();
    expect(screen.getByText("No fired signals")).toBeInTheDocument();
    expect(screen.getByText(/current strategy versions have not fired/i)).toBeInTheDocument();
  });
});
