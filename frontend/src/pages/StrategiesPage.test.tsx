import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as strategiesApi from "@/api/strategies";
import type { StrategyOverviewResponse, StrategyResultArm } from "@/api/types";
import { StrategiesPage } from "@/pages/StrategiesPage";

const ARM: StrategyResultArm = {
  result_version: "result-v1",
  ambiguity_arm: "worst_case",
  quarantine_arm: "masked",
  universe_basis: "survivorship_free",
  corpus_version: "corpus-v1",
  cost_model_id: "cost-v1",
  sizing_rule: "size-v1",
  benchmark_rule: "benchmark-v1",
  position_rule_set_version: "position-v1",
  outcome_rule_set_version: "outcome-v1",
  input_rule_set_version: "input-v1",
  evaluated_instrument_count: 100,
  trade_count: 12,
  losing_trade_count: 3,
  open_trade_count: 0,
  unpriced_trade_count: 0,
  expectancy_per_trade_pct: "1.5",
  expectancy_ci_low_pct: "-0.5",
  expectancy_ci_high_pct: "2.5",
  total_return_pct: "18",
  cagr_pct: "4",
  sharpe: "1",
  sortino: "1.2",
  max_drawdown_pct: "-5",
  profit_factor: "1.5",
  exposure_time_pct: "25",
  turnover_annualised: "4",
  return_vs_buy_and_hold_pct: "2",
  deflated_sharpe: "0.8",
  promotion_refusals: [],
};

const OVERVIEW: StrategyOverviewResponse = {
  as_of: "2026-08-09T12:00:00Z",
  demo_connection: true,
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
    enabled: false,
    capital_limit: "1000.000000",
    currency: "USD",
    reserved_capital: "0.000000",
    invested_capital: "0.000000",
    remaining_capital: "1000.000000",
  },
  strategies: [{
    strategy_id: "s1-time-series-momentum",
    strategy_version: "strategy-registry-v1+abc",
    title: "Time-series momentum",
    description: "Follows established price trends and exits when the trend turns.",
    exit_timing: "Until the trend turns",
    runnable: true,
    exclusion_reason: null,
    scan: { frontier_date: "2026-08-07", updated_at: "2026-08-08T06:45:00Z", status: "current", fired_entries: 12, fired_exits: 0, not_fired: 100, not_evaluable: 0, exclusions_by_reason: {} },
    evidence_windows: [
      { window_id: "primary", label: "2022 onward", window_start: "2022-01-01", window_end: "2026-07-08", status: "complete", arms: [ARM] },
      { window_id: "holdout", label: "Holdout", window_start: "2025-01-01", window_end: "2026-07-08", status: "missing", arms: [] },
    ],
    legacy_result_count: 0,
    all_recent_evidence_complete: false,
    stage: null,
    attribution: {
      fired_entries: 12,
      funded_entries: 0,
      rejected_entries: 12,
      resolved_entries: 0,
      winning_entries: 0,
      win_rate: null,
      median_days_to_outcome: null,
      signals_last_30_days: 12,
      shadow_average_return_pct: null,
      funded_shadow_average_return_pct: null,
      rejected_shadow_average_return_pct: null,
      opportunity_gap_pct: null,
      funded_capture_rate: null,
      filled_entries: 0,
      broker_rejected_entries: 0,
      fill_rate: null,
      broker_rejection_rate: null,
      average_slippage_pct: null,
      average_stressed_cost_usd: null,
      max_observed_account_drawdown_pct: null,
    },
    pnl: { currency: "USD", strategy_trade_count: 0, owned_position_count: 0, active_position_count: 0, close_event_count: 0, invested_capital: "0", realised_pnl: "0", unrealised_pnl: "0", total_pnl: "0", observed_fees: "0", complete: true, incomplete_reasons: [] },
    allocation: { deployment_id: null, capital_limit: "0", currency: "USD", enabled: false, revision: null, reserved_capital: "0", invested_capital: "0", remaining_capital: "0", policy_configured: false, max_drawdown_limit_pct: null },
    allocation_ready: false,
    allocation_refusals: ["recent_evidence_incomplete", "paper_promotion_missing", "execution_policy_missing"],
  }],
};

function approvedOverview(): StrategyOverviewResponse {
  const strategy = OVERVIEW.strategies[0]!;
  return {
    ...OVERVIEW,
    paper_pool: { ...OVERVIEW.paper_pool, enabled: true, reserved_capital: "250", invested_capital: "200", remaining_capital: "750" },
    strategies: [{
      ...strategy,
      all_recent_evidence_complete: true,
      stage: "paper_enabled",
      attribution: {
        ...strategy.attribution,
        funded_entries: 3,
        rejected_entries: 9,
        resolved_entries: 10,
        winning_entries: 6,
        win_rate: "0.6",
        median_days_to_outcome: "4",
        shadow_average_return_pct: "1.25",
      },
      pnl: { ...strategy.pnl, strategy_trade_count: 3, owned_position_count: 3, active_position_count: 1, close_event_count: 2, invested_capital: "200", realised_pnl: "40", unrealised_pnl: "10", total_pnl: "50" },
      allocation: { deployment_id: 7, capital_limit: "1000", currency: "USD", enabled: true, revision: 2, reserved_capital: "250", invested_capital: "200", remaining_capital: "750", policy_configured: true, max_drawdown_limit_pct: "5" },
      allocation_ready: true,
      allocation_refusals: [],
    }],
  };
}

describe("StrategiesPage", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(OVERVIEW);
    vi.spyOn(strategiesApi, "fetchStrategyPnlHistory").mockResolvedValue({ points: [] });
  });

  it("keeps unapproved backtests out of portfolio performance", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByText("Portfolio performance")).toBeInTheDocument();
    const performance = screen.getByText("Portfolio performance").closest("section")!;
    expect(within(performance).getByText("US$0.00")).toBeInTheDocument();
    expect(within(performance).getAllByText("—")).toHaveLength(2);
    expect(within(performance).queryByText("+1.50%")).not.toBeInTheDocument();
    expect(screen.getByText("No automated P&L yet")).toBeInTheDocument();
    expect(screen.getByText("+1.50%")).toBeInTheDocument();
    expect(screen.getByText("Not proven")).toBeInTheDocument();
  });

  it("separates unapproved research from selectable strategies", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByText("No strategies are approved for automation.")).toBeInTheDocument();
    expect(screen.getByText("Nothing can trade yet")).toBeInTheDocument();
    expect(screen.getByText("Time-series momentum")).toBeInTheDocument();
    expect(screen.queryByRole("checkbox", { name: /Time-series momentum/ })).not.toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Allow new automated entries" })).toBeDisabled();
  });

  it("uses a compact capital control while still allowing the limit to be saved", async () => {
    const update = vi.spyOn(strategiesApi, "updateStrategyPaperPool").mockResolvedValue({ ...OVERVIEW.paper_pool, capital_limit: "1500" });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const input = await screen.findByLabelText("Trading capital (USD)");
    expect(input.parentElement).toHaveClass("w-48");
    await userEvent.clear(input);
    await userEvent.type(input, "1500");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(update).toHaveBeenCalledWith({ enabled: false, capital_limit: "1500.000000", reason: "Automated strategy workspace update" }));
  });

  it("does not present automation as enabled while the system-wide guard is off", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...approvedOverview(),
      execution_enabled: false,
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const master = await screen.findByRole("checkbox", { name: "Allow new automated entries" });
    expect(master).not.toBeChecked();
    expect(master).toBeDisabled();
    expect(screen.getByText("System-wide automatic trading is off. Enable that safety control before allowing new entries.")).toBeInTheDocument();
  });

  it("shows stable primary evidence without paging through missing windows", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByRole("button", { name: "View evidence" }));
    expect(screen.getByText("Primary evidence")).toBeInTheDocument();
    expect(screen.getByText("Evidence windows complete:")).toBeInTheDocument();
    expect(screen.getByText("1/2")).toBeInTheDocument();
    expect(screen.getByText("• Recent evidence windows are incomplete")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Next" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Previous" })).not.toBeInTheDocument();
  });

  it("summarises fired observations without rendering a ticker activity feed", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByText("Forward signal validation")).toBeInTheDocument();
    expect(screen.getByText("There is no “pending strategy” state and no near-trigger forecast in the current daily evaluator.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Activity" })).not.toBeInTheDocument();
    expect(screen.queryByText("Instrument")).not.toBeInTheDocument();
  });

  it("shows observed portfolio measures and controls for an approved strategy", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(approvedOverview());
    vi.mocked(strategiesApi.fetchStrategyPnlHistory).mockResolvedValue({ points: [{ date: "2026-08-09", total_pnl: "50", strategy_pnl: { "s1-time-series-momentum": "50" } }] });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const performance = (await screen.findByText("Portfolio performance")).closest("section")!;
    expect(within(performance).getByText("US$50.00")).toBeInTheDocument();
    expect(within(performance).getByText("+1.25%")).toBeInTheDocument();
    expect(within(performance).getByText("+60.00%")).toBeInTheDocument();
    expect(screen.getByText("1 approved")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Enabled" })).toBeEnabled();
  });

  it("toggles an approved strategy for the next run", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(approvedOverview());
    const update = vi.spyOn(strategiesApi, "updateStrategyAllocation").mockResolvedValue({ strategy_id: "s1-time-series-momentum", strategy_version: "strategy-registry-v1+abc", deployment_id: 7, capital_limit: "1000", currency: "USD", enabled: false, revision: 3 });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByRole("checkbox", { name: "Enabled" }));
    await waitFor(() => expect(update).toHaveBeenCalledWith("s1-time-series-momentum", expect.objectContaining({ enabled: false, reason: "Paused from automated strategy workspace" })));
  });

  it("does not enable an approved strategy without assigned capital", async () => {
    const approved = approvedOverview();
    const strategy = approved.strategies[0]!;
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...approved,
      paper_pool: { ...approved.paper_pool, enabled: false, capital_limit: "0", remaining_capital: "0" },
      strategies: [{ ...strategy, allocation: { ...strategy.allocation, enabled: false, capital_limit: "0" } }],
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByRole("checkbox", { name: "Paused" })).toBeDisabled();
  });
});
