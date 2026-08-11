import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as strategiesApi from "@/api/strategies";
import type { StrategyOverviewResponse, StrategyOwnedPosition, StrategyResultArm } from "@/api/types";
import { StrategiesPage } from "@/pages/StrategiesPage";

const ARM: StrategyResultArm = {
  result_version: "result-v1",
  purpose: "capital_candidate",
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
    capital_mode: "fixed",
    effective_capital: "1000.000000",
    currency: "USD",
    reserved_capital: "0.000000",
    invested_capital: "0.000000",
    remaining_capital: "1000.000000",
  },
  evidence_refresh: {
    frozen_through: "2026-07-08",
    completed_windows: 1,
    partial_windows: 0,
    total_windows: 8,
    status: "idle",
    request_id: null,
    requested_at: null,
    finished_at: null,
    last_error: null,
    progress: null,
  },
  strategies: [{
    strategy_id: "s1-time-series-momentum",
    strategy_version: "strategy-registry-v1+abc",
    purpose: "capital_candidate",
    title: "Time-series momentum",
    description: "Follows established price trends and exits when the trend turns.",
    exit_timing: "Until the trend turns",
    runnable: true,
    forward_outcome_supported: false,
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
    allocation: { deployment_id: null, capital_limit: "0", currency: "USD", enabled: false, revision: null, reserved_capital: "0", invested_capital: "0", remaining_capital: "0", policy_configured: false, max_drawdown_limit_pct: null, ticket_sizing_mode: null, ticket_value: null, max_ticket_amount: null },
    allocation_ready: false,
    allocation_refusals: ["recent_evidence_incomplete", "paper_promotion_missing", "execution_policy_missing"],
  }],
};

const OWNED_POSITION: StrategyOwnedPosition = {
  strategy_trade_id: 41,
  broker_position_id: 7001,
  strategy_id: "s1-time-series-momentum",
  strategy_version: "strategy-registry-v1+abc",
  strategy_title: "Time-series momentum",
  instrument_id: 101,
  symbol: "ACME",
  company_name: "Acme Corp",
  direction: "long",
  units: "5",
  assigned_value: "100",
  current_value: "110",
  unrealised_pnl: "10",
  unrealised_return_pct: "10",
  open_rate: "10",
  current_price: "12",
  stop_loss_rate: "9",
  take_profit_rate: "14",
  opened_at: "2026-08-08T12:00:00Z",
  currency: "USD",
  trade_status: "open",
  valuation_available: true,
};

function approvedOverview(): StrategyOverviewResponse {
  const strategy = OVERVIEW.strategies[0]!;
  return {
    ...OVERVIEW,
    paper_pool: { ...OVERVIEW.paper_pool, enabled: true, reserved_capital: "250", invested_capital: "200", remaining_capital: "750" },
    strategies: [{
      ...strategy,
      forward_outcome_supported: true,
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
      allocation: { deployment_id: 7, capital_limit: "1000", currency: "USD", enabled: true, revision: 2, reserved_capital: "250", invested_capital: "200", remaining_capital: "750", policy_configured: true, max_drawdown_limit_pct: "5", ticket_sizing_mode: "percent", ticket_value: "20", max_ticket_amount: "500" },
      allocation_ready: true,
      allocation_refusals: [],
    }],
  };
}

describe("StrategiesPage", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(OVERVIEW);
    vi.spyOn(strategiesApi, "fetchStrategyPnlHistory").mockResolvedValue({
      basis: "exact_owned_mark_to_market_nav",
      total_return_available: false,
      benchmark_comparison_available: false,
      points: [],
    });
    vi.spyOn(strategiesApi, "fetchStrategyOwnedPositions").mockResolvedValue({
      positions: [],
      live_quote_instrument_ids: [],
    });
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
    expect(await screen.findByText("No capital candidates are approved for automation.")).toBeInTheDocument();
    expect(screen.queryByText("Approved & managed strategies")).not.toBeInTheDocument();
    const research = screen.getByText("Research & validation").closest("details")!;
    expect(research).not.toHaveAttribute("open");
    expect(screen.getByText("Time-series momentum")).not.toBeVisible();
    await userEvent.click(within(research).getByText("Research & validation"));
    expect(screen.getByText("Time-series momentum")).toBeVisible();
    expect(screen.queryByRole("checkbox", { name: /Time-series momentum/ })).not.toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Allow new automated entries" })).toBeDisabled();
  });

  it("renders harness rules as compact controls without allocation actions", async () => {
    const strategy = OVERVIEW.strategies[0]!;
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...OVERVIEW,
      strategies: [{
        ...strategy,
        purpose: "harness_validation",
        evidence_windows: strategy.evidence_windows.map((window) => ({
          ...window,
          arms: window.arms.map((arm) => ({ ...arm, purpose: "harness_validation" })),
        })),
        allocation_refusals: ["harness_validation_only"],
      }],
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const research = (await screen.findByText("Research & validation")).closest("details")!;
    expect(screen.getByText("0 candidates · 1 control")).toBeVisible();
    expect(screen.getByText("Validation controls")).not.toBeVisible();
    await userEvent.click(within(research).getByText("Research & validation"));
    expect(screen.getByText("Validation controls")).toBeVisible();
    expect(screen.getByText("Harness evidence only · never eligible for capital · backtest only")).toBeVisible();
    expect(screen.getByText("No capital candidates")).toBeVisible();
    expect(screen.queryByRole("button", { name: "View evidence" })).not.toBeInTheDocument();
  });

  it("counts only strategies with a forward outcome resolver as open observations", async () => {
    const strategy = OVERVIEW.strategies[0]!;
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...OVERVIEW,
      strategies: [
        strategy,
        {
          ...strategy,
          strategy_id: "s4-volatility-compression-breakout",
          title: "Volatility compression breakout",
          forward_outcome_supported: true,
          attribution: {
            ...strategy.attribution,
            fired_entries: 108,
            resolved_entries: 2,
            winning_entries: 1,
            win_rate: "0.5",
            shadow_average_return_pct: "0.0509",
          },
        },
        {
          ...strategy,
          strategy_id: "harness-only-control",
          title: "Harness only control",
          purpose: "harness_validation",
          forward_outcome_supported: true,
          attribution: {
            ...strategy.attribution,
            fired_entries: 999,
            resolved_entries: 999,
            winning_entries: 999,
            win_rate: "1",
            shadow_average_return_pct: "99",
          },
        },
      ],
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const validation = (await screen.findByText("Forward signal validation")).closest("section")!;
    expect(within(validation).getByText("106")).toBeInTheDocument();
    expect(within(validation).getByText("2")).toBeInTheDocument();
    expect(within(validation).getAllByText("1", { selector: "dd" })).toHaveLength(2);
  });

  it("does not present a legacy enabled harness deployment as approved", async () => {
    const strategy = OVERVIEW.strategies[0]!;
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...OVERVIEW,
      strategies: [{
        ...strategy,
        purpose: "harness_validation",
        allocation: { ...strategy.allocation, enabled: true, capital_limit: "100" },
        allocation_refusals: ["harness_validation_only"],
      }],
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const research = (await screen.findByText("Research & validation")).closest("details")!;
    expect(research).not.toHaveAttribute("open");
    expect(screen.queryByText("Approved & managed strategies")).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox", { name: /Time-series momentum/ })).not.toBeInTheDocument();
  });

  it("uses a compact capital control while still allowing the limit to be saved", async () => {
    const update = vi.spyOn(strategiesApi, "updateStrategyPaperPool").mockResolvedValue({ ...OVERVIEW.paper_pool, capital_limit: "1500" });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const input = await screen.findByLabelText("Trading capital (USD)");
    expect(input.parentElement).toHaveClass("w-48");
    fireEvent.change(input, { target: { value: "1500" } });
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(update).toHaveBeenCalledWith({ enabled: false, capital_limit: "1500.000000", capital_mode: "fixed", reason: "Automated strategy workspace update" }));
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
    await userEvent.click(await screen.findByText("Research & validation"));
    await userEvent.click(await screen.findByRole("button", { name: "View evidence" }));
    expect(screen.getByText("Primary evidence")).toBeInTheDocument();
    expect(screen.getByText("Evidence windows complete:")).toBeInTheDocument();
    expect(screen.getByText("1/2")).toBeInTheDocument();
    expect(screen.getByText("• Recent evidence windows are incomplete")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Next" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Previous" })).not.toBeInTheDocument();
  });

  it("queues the fixed recent-evidence denominator from the research header", async () => {
    const refresh = vi.spyOn(strategiesApi, "requestStrategyEvidenceRefresh").mockResolvedValue({
      request_id: 42,
      status: "queued",
      already_active: false,
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);

    await userEvent.click(await screen.findByText("Research & validation"));
    expect(await screen.findByText(/Evidence 1\/8/)).toHaveTextContent("frozen through 08 Jul 2026");
    await userEvent.click(screen.getByRole("button", { name: "Refresh evidence" }));

    await waitFor(() => expect(refresh).toHaveBeenCalledOnce());
  });

  it("omits forward activity when no capital candidate has a forward resolver", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByText("Portfolio performance")).toBeInTheDocument();
    expect(screen.queryByText("Forward signal validation")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Activity" })).not.toBeInTheDocument();
    expect(screen.queryByText("Instrument")).not.toBeInTheDocument();
  });

  it("shows observed portfolio measures and controls for an approved strategy", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(approvedOverview());
    vi.mocked(strategiesApi.fetchStrategyPnlHistory).mockResolvedValue({
      basis: "exact_owned_mark_to_market_nav",
      total_return_available: false,
      benchmark_comparison_available: false,
      points: [{
        date: "2026-08-09",
        principal: "1000",
        external_flow: "1000",
        realised_pnl: "40",
        unrealised_pnl: "10",
        total_pnl: "50",
        pot_value: "1050",
        complete: true,
        incomplete_reasons: [],
      }],
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const performance = (await screen.findByText("Portfolio performance")).closest("section")!;
    expect(within(performance).getByText("US$50.00")).toBeInTheDocument();
    expect(within(performance).getByText("+1.25%")).toBeInTheDocument();
    expect(within(performance).getByText("+60.00%")).toBeInTheDocument();
    expect(within(performance).getByText(/Daily realised plus open P&L from exact automated positions/)).toBeInTheDocument();
    expect(screen.getByText("1 approved")).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Enabled" })).toBeEnabled();
  });

  it("keeps per-signal sizing behind the approved strategy disclosure", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(approvedOverview());
    const update = vi.spyOn(strategiesApi, "updateStrategySizing").mockResolvedValue({
      strategy_id: "s1-time-series-momentum",
      strategy_version: "strategy-registry-v1+abc",
      deployment_id: 7,
      revision: 3,
      ticket_sizing_mode: "fixed",
      ticket_value: "75",
      max_ticket_amount: "100",
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);

    await userEvent.click(await screen.findByText(/Per signal: 20%/));
    await userEvent.selectOptions(screen.getByLabelText("Method"), "fixed");
    fireEvent.change(screen.getByLabelText("Amount"), { target: { value: "75" } });
    fireEvent.change(screen.getByLabelText("Hard max USD"), { target: { value: "100" } });
    const save = screen.getByRole("button", { name: "Save sizing" });
    expect(save).toBeEnabled();
    await userEvent.click(save);

    await waitFor(() => expect(update).toHaveBeenCalledWith("s1-time-series-momentum", {
      strategy_version: "strategy-registry-v1+abc",
      ticket_sizing_mode: "fixed",
      ticket_value: "75.000000",
      max_ticket_amount: "100.000000",
      reason: "Per-signal sizing updated from automated strategy workspace",
    }));
  });

  it("shows a compact portfolio row for each exact automated position", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(approvedOverview());
    vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockResolvedValue({
      positions: [OWNED_POSITION],
      live_quote_instrument_ids: [101],
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const section = (await screen.findByText("Open automated positions")).closest("section")!;
    expect(within(section).getByText("ACME")).toBeInTheDocument();
    expect(within(section).getByText("US$100.00")).toBeInTheDocument();
    expect(within(section).getByText("US$110.00")).toBeInTheDocument();
    expect(within(section).getByText("+10.00%")).toBeInTheDocument();
    expect(within(section).getByText("US$9.00")).toBeInTheDocument();
    expect(within(section).getByText("US$14.00")).toBeInTheDocument();
  });

  it("submits an exact strategy-aware close and explains that manual positions are untouched", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(approvedOverview());
    vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockResolvedValue({
      positions: [OWNED_POSITION],
      live_quote_instrument_ids: [101],
    });
    const close = vi.spyOn(strategiesApi, "closeStrategyOwnedPosition").mockResolvedValue({
      strategy_trade_id: 41,
      broker_position_id: 7001,
      state: "submitted",
      reason_code: "broker_close_accepted",
      operation_id: 88,
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByRole("button", { name: "Close" }));
    expect(screen.getByText("A separate manual position in ACME is not part of this request and will remain untouched.")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Close position" }));
    await waitFor(() => expect(close).toHaveBeenCalledWith(41, 7001));
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
