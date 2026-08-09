import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as strategiesApi from "@/api/strategies";
import type { FiredSignalsResponse, StrategyOverviewResponse } from "@/api/types";
import { StrategiesPage } from "@/pages/StrategiesPage";

const OVERVIEW: StrategyOverviewResponse = {
  as_of: "2026-08-09T12:00:00Z",
  execution_enabled: true,
  live_execution_enabled: false,
  live_strategy_activation_available: false,
  live_strategy_activation_blocker: "live_strategy_broker_contract_not_validated",
  storage_policy: "fired_signals_and_material_mutations_only",
  entry_block: {
    new_entries_blocked: false,
    global_kill_active: false,
    global_kill_reason: null,
    global_kill_activated_at: null,
    global_kill_activated_by: null,
    execution_block_reasons: [],
  },
  paper_pool: {
    configured: true,
    enabled: true,
    capital_limit: "1000.000000",
    currency: "USD",
    reserved_capital: "250.000000",
    invested_capital: "200.000000",
    remaining_capital: "750.000000",
  },
  strategies: [{
    strategy_id: "s1-time-series-momentum",
    strategy_version: "strategy-registry-v1+abc",
    title: "Time-series momentum",
    runnable: true,
    exclusion_reason: null,
    scan: { frontier_date: "2026-08-07", updated_at: "2026-08-08T06:45:00Z", status: "current", fired_entries: 12, fired_exits: 0, not_fired: 100, not_evaluable: 0, exclusions_by_reason: {} },
    evidence_windows: [{ window_id: "primary", label: "2022 onward", window_start: "2022-01-01", window_end: "2026-07-08", status: "complete", arms: [] }],
    legacy_result_count: 0,
    all_recent_evidence_complete: true,
    stage: "paper_enabled",
    attribution: {
      fired_entries: 12,
      funded_entries: 3,
      rejected_entries: 9,
      resolved_entries: 10,
      winning_entries: 6,
      win_rate: "0.6",
      median_days_to_outcome: "4",
      signals_last_30_days: 3,
      shadow_average_return_pct: "1.25",
      funded_shadow_average_return_pct: "1.1",
      rejected_shadow_average_return_pct: "1.3",
      opportunity_gap_pct: "0.2",
      funded_capture_rate: "0.25",
      filled_entries: 3,
      broker_rejected_entries: 0,
      fill_rate: "1",
      broker_rejection_rate: "0",
      average_slippage_pct: "0.05",
      average_stressed_cost_usd: "0.50",
      max_observed_account_drawdown_pct: "1.5",
    },
    pnl: { currency: "USD", strategy_trade_count: 3, owned_position_count: 3, active_position_count: 1, close_event_count: 2, invested_capital: "200", realised_pnl: "40", unrealised_pnl: "10", total_pnl: "50", observed_fees: "1", complete: true, incomplete_reasons: [] },
    allocation: { deployment_id: 7, capital_limit: "1000", currency: "USD", enabled: true, revision: 2, reserved_capital: "250", invested_capital: "200", remaining_capital: "750", policy_configured: true, max_drawdown_limit_pct: "5" },
    allocation_ready: true,
    allocation_refusals: [],
  }],
};

const SIGNALS: FiredSignalsResponse = { items: [{ signal_id: 42, strategy_id: "s1-time-series-momentum", strategy_version: "strategy-registry-v1+abc", instrument_id: 1, symbol: "AAA", company_name: "Alpha", signal_bar_date: "2026-08-06", signal_kind: "entry", fill_bar_date: "2026-08-07", fill_price: "12.34", universe: "survivor_only", outcome: "tp_hit", exit_bar_date: "2026-08-09", exit_price: "13", gross_return_pct: "5", outcome_reason: null, funding_status: "funded", funding_reason: "allocated", funded_amount: "100", strategy_trade_id: 1, execution_status: "filled", actual_fill_price: "12.35", slippage_pct: "0.08" }], next_cursor: null };

describe("StrategiesPage", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(OVERVIEW);
    vi.spyOn(strategiesApi, "fetchStrategyPnlHistory").mockResolvedValue({ points: [{ date: "2026-08-09", total_pnl: "50", strategy_pnl: { "s1-time-series-momentum": "50" } }] });
    vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue(SIGNALS);
  });

  it("leads with money, outcome cadence and compact strategy controls", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByText("Automated P&L")).toBeInTheDocument();
    expect(screen.getAllByText(/50\.00/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/60\.00%/).length).toBeGreaterThan(0);
    expect(screen.getByText("4 days")).toBeInTheDocument();
    expect(screen.getByText("3 signals")).toBeInTheDocument();
    expect(screen.queryByText("Allocation blockers: none")).not.toBeInTheDocument();
  });

  it("keeps technical evidence and paginated signals in a flyout", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await screen.findByText("Time-series momentum");
    await userEvent.click(screen.getByRole("button", { name: "Details" }));
    expect(screen.getByRole("dialog", { name: "Time-series momentum details" })).toBeInTheDocument();
    expect(await screen.findByText("AAA")).toBeInTheDocument();
    expect(screen.getByText("Technical audit detail")).toBeInTheDocument();
  });

  it("updates the shared paper pot without exposing an audit-reason field", async () => {
    const update = vi.spyOn(strategiesApi, "updateStrategyPaperPool").mockResolvedValue({ ...OVERVIEW.paper_pool, capital_limit: "1500" });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const input = await screen.findByLabelText("Shared paper capital (USD)");
    await userEvent.clear(input);
    await userEvent.type(input, "1500");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(update).toHaveBeenCalledWith({ enabled: true, capital_limit: "1500.000000", reason: "Strategy workspace paper-pool update" }));
  });

  it("toggles an individual strategy for the next run", async () => {
    const update = vi.spyOn(strategiesApi, "updateStrategyAllocation").mockResolvedValue({ strategy_id: "s1-time-series-momentum", strategy_version: "strategy-registry-v1+abc", deployment_id: 7, capital_limit: "1000", currency: "USD", enabled: false, revision: 3 });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByRole("checkbox", { name: "On" }));
    await waitFor(() => expect(update).toHaveBeenCalledWith("s1-time-series-momentum", expect.objectContaining({ enabled: false, reason: "Paused from strategy workspace" })));
  });
});
