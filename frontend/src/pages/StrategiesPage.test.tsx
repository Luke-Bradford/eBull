import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as strategiesApi from "@/api/strategies";
import type { FiredSignal, StrategyOverviewResponse, StrategyOwnedPosition, StrategyResultArm } from "@/api/types";
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
  return_basis: "split-dividend-adjusted-wealth-v1",
  ambiguity_rule_version: "ambiguity-verdict-2026-08-15-v2-matched-control-margin",
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
  // Matches every one of the 324 stored rows: written before the holding period
  // was measured, so the three hold figures are null and the cell must say which
  // result version that is rather than showing a blank or a zero (#2623 gap 1).
  metric_set_id: "criterion7-v1",
  median_hold_days: null,
  hold_days_p25: null,
  hold_days_p75: null,
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
  automation_readiness: {
    ready: false,
    state: "historical_validation_incomplete",
    capital_candidate_count: 1,
    historically_ready_candidate_count: 0,
    prospectively_ready_candidate_count: 0,
    assessment_policy_id: null,
    assessed_scope_count: 0,
    passed_scope_count: 0,
    fresh_passed_scope_count: 0,
    resolved_forecasts: 0,
    target_first_count: 0,
    stop_first_count: 0,
    timeout_count: 0,
    latest_checked_at: null,
    worst_normalized_brier_score: null,
    weakest_brier_skill_score: null,
    worst_classwise_calibration_error: null,
    blockers: ["historical_validation_incomplete"],
  },
  account_equity_evidence: {
    status: "unavailable",
    days_collected: 0,
    snapshot_date: null,
    observed_at: null,
    account_currency_id: null,
    currency: null,
    official_equity: null,
    official_available_cash: null,
    official_total_invested: null,
    official_unrealised_pnl: null,
    local_eod_currency: null,
    local_eod_value: null,
    local_eod_positions_priced: null,
    local_eod_stale_mark_positions: null,
    difference: null,
    comparable: false,
    incomplete_reasons: ["official_account_equity_missing"],
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
    mandate: {
      configured: true,
      policy_version: "portfolio-mandate-v1",
      risk_profile: "balanced",
      target_volatility_pct: "12.0000",
      max_portfolio_drawdown_pct: "15.0000",
      max_loss_per_position_pct: "0.7500",
      max_daily_loss_pct: "1.5000",
      active_risk_budget_pct: "20.0000",
      cash_reserve_pct: "15.0000",
      max_concurrent_positions: 8,
      shorts_allowed: false,
      leverage_allowed: false,
    },
    available_mandates: [
      {
        configured: true,
        policy_version: "portfolio-mandate-v1",
        risk_profile: "cautious",
        target_volatility_pct: "8.0000",
        max_portfolio_drawdown_pct: "10.0000",
        max_loss_per_position_pct: "0.5000",
        max_daily_loss_pct: "1.0000",
        active_risk_budget_pct: "10.0000",
        cash_reserve_pct: "25.0000",
        max_concurrent_positions: 4,
        shorts_allowed: false,
        leverage_allowed: false,
      },
      {
        configured: true,
        policy_version: "portfolio-mandate-v1",
        risk_profile: "balanced",
        target_volatility_pct: "12.0000",
        max_portfolio_drawdown_pct: "15.0000",
        max_loss_per_position_pct: "0.7500",
        max_daily_loss_pct: "1.5000",
        active_risk_budget_pct: "20.0000",
        cash_reserve_pct: "15.0000",
        max_concurrent_positions: 8,
        shorts_allowed: false,
        leverage_allowed: false,
      },
      {
        configured: true,
        policy_version: "portfolio-mandate-v1",
        risk_profile: "growth",
        target_volatility_pct: "18.0000",
        max_portfolio_drawdown_pct: "25.0000",
        max_loss_per_position_pct: "1.0000",
        max_daily_loss_pct: "2.5000",
        active_risk_budget_pct: "30.0000",
        cash_reserve_pct: "10.0000",
        max_concurrent_positions: 12,
        shorts_allowed: false,
        leverage_allowed: false,
      },
    ],
  },
  evidence_refresh: {
    frozen_through: "2024-09-27",
    completed_windows: 1,
    partial_windows: 0,
    total_windows: 6,
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
    scan: { frontier_date: "2026-08-07", updated_at: "2026-08-08T06:45:00Z", status: "current", rotation: null, fired_entries: 12, fired_exits: 0, not_fired: 100, not_evaluable: 0, exclusions_by_reason: {} },
    // ⚠ The declared ids, from app/services/strategy_recent_evidence.py. This
    // fixture used to say `primary` / `holdout`, neither of which the API can
    // emit — which is why `primaryEvidence()` matching a dead id went unnoticed
    // (#2624): the fallback branch covered for it in prod and in the test alike.
    evidence_windows: [
      { window_id: "primary-2022-plus", label: "Primary: 2022 onward", window_start: "2022-01-01", window_end: "2026-07-08", status: "complete", arms: [ARM] },
      { window_id: "year-2022", label: "Calendar 2022", window_start: "2022-01-01", window_end: "2022-12-31", status: "missing", arms: [] },
    ],
    prior_versions: [],
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
    // Mirrors what all four strategies actually return today: a share IS present
    // (0.5210 / 0.0000 / 0.0039 / 0.0340 on dev), while every one of them has a
    // single scan day and so has no weekly rate at all.
    fire_rate: {
      universe: "validated_us_equity",
      scanned_days: 1,
      fired_days: 1,
      fired_entry_signals: 1740,
      evaluable_entry_decisions: 3340,
      not_evaluable_entry_decisions: 620,
      fired_share_of_evaluable: "0.5210",
      entries_per_calendar_week: null,
      first_scanned_bar: "2026-08-12",
      last_scanned_bar: "2026-08-12",
      share_unavailable_reason: null,
      weekly_rate_unavailable_reason: "single_scan_day",
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

const FUNDED_SIGNAL: FiredSignal = {
  signal_id: 91,
  strategy_id: "s1-time-series-momentum",
  strategy_version: "strategy-registry-v1+abc",
  instrument_id: 101,
  symbol: "ACME",
  company_name: "Acme Corp",
  signal_bar_date: "2026-08-08",
  signal_kind: "entry",
  fill_bar_date: "2026-08-09",
  fill_price: "20",
  universe: "validated_us_equity",
  outcome: "target_first",
  exit_bar_date: "2026-08-12",
  exit_price: "22",
  gross_return_pct: "10",
  outcome_reason: "take_profit_reached",
  funding_status: "funded",
  funding_reason: "all_paper_entry_gates_passed",
  funded_amount: "100",
  strategy_trade_id: 41,
  execution_status: "filled",
  actual_fill_price: "20.10",
  slippage_pct: "0.5",
  trade_lifecycle: {
    trade_status: "closed",
    ownership_count: 1,
    broker_position_id: 7001,
    ownership_status: "released",
    position_claimed_at: "2026-08-09T10:00:00Z",
    position_released_at: "2026-08-12T10:00:00Z",
    position_release_reason: "broker_close_observed",
    latest_operation_type: "close",
    latest_operation_id: 88,
    latest_operation_order_id: 99,
    latest_operation_trigger: "operator_close",
    latest_operation_status: "applied",
    latest_operation_created_at: "2026-08-12T09:58:00Z",
    latest_operation_submitted_at: "2026-08-12T09:59:00Z",
    latest_operation_resolved_at: "2026-08-12T10:00:00Z",
    latest_operation_error: null,
    latest_reconciliation_state: "resolved",
    latest_reconciliation_broker_status: "closed",
    latest_reconciliation_attempt_count: 1,
    latest_reconciliation_updated_at: "2026-08-12T10:00:00Z",
    latest_reconciliation_error: null,
    close_event_count: 1,
    realised_pnl_usd: "8.75",
    observed_fees_usd: "1.25",
    close_history_status: "complete",
    incomplete_reasons: [],
  },
};

function approvedOverview(): StrategyOverviewResponse {
  const strategy = OVERVIEW.strategies[0]!;
  return {
    ...OVERVIEW,
    automation_readiness: {
      ...OVERVIEW.automation_readiness,
      ready: true,
      state: "ready",
      historically_ready_candidate_count: 1,
      prospectively_ready_candidate_count: 1,
      assessment_policy_id: "assessment-policy-v1",
      assessed_scope_count: 1,
      passed_scope_count: 1,
      fresh_passed_scope_count: 1,
      resolved_forecasts: 30,
      target_first_count: 18,
      stop_first_count: 7,
      timeout_count: 5,
      latest_checked_at: "2026-08-09T11:00:00Z",
      worst_normalized_brier_score: "0.12",
      weakest_brier_skill_score: "0.18",
      worst_classwise_calibration_error: "0.06",
      blockers: [],
    },
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
    vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue({ items: [], next_cursor: null });
  });

  it("keeps unapproved backtests out of portfolio performance", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByText("Portfolio performance")).toBeInTheDocument();
    const performance = screen.getByText("Portfolio performance").closest("section")!;
    expect(within(performance).getByText("US$0.00")).toBeInTheDocument();
    expect(within(performance).getAllByText("—")).toHaveLength(2);
    expect(within(performance).queryByText("+1.50%")).not.toBeInTheDocument();
    expect(screen.getByText("No automated P&L yet")).toBeInTheDocument();
    expect(screen.getAllByText("+1.50%")).toHaveLength(2);
    expect(screen.getByText("Not proven")).toBeInTheDocument();
    expect(screen.getByText("Official account equity starts collecting with the next portfolio sync.")).toBeInTheDocument();
  });

  it("shows broker account evidence without presenting it as automated return", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...OVERVIEW,
      account_equity_evidence: {
        status: "collecting",
        days_collected: 3,
        snapshot_date: "2026-08-11",
        observed_at: "2026-08-11T19:00:00Z",
        account_currency_id: 1,
        currency: "USD",
        official_equity: "1025.00",
        official_available_cash: "525.00",
        official_total_invested: "400.00",
        official_unrealised_pnl: "100.00",
        local_eod_currency: "USD",
        local_eod_value: "1020.00",
        local_eod_positions_priced: 2,
        local_eod_stale_mark_positions: 0,
        difference: "5.00",
        comparable: false,
        incomplete_reasons: ["local_eod_effective_time_unknown"],
      },
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const performance = (await screen.findByText("Portfolio performance")).closest("section")!;
    expect(within(performance).getByText("3 daily official snapshots")).toBeInTheDocument();
    expect(within(performance).getByText("US$1,025.00")).toBeInTheDocument();
    expect(within(performance).getByText("Reconciliation incomplete")).toBeInTheDocument();
    expect(within(performance).getByText("The effective dates of the local valuation marks were not recorded.")).toBeInTheDocument();
    expect(within(performance).getByText("No automated P&L yet")).toBeInTheDocument();
  });

  it("never paints a currency symbol on an account whose currency the broker did not name", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...OVERVIEW,
      account_equity_evidence: {
        status: "collecting",
        days_collected: 3,
        snapshot_date: "2026-08-11",
        observed_at: "2026-08-11T19:00:00Z",
        account_currency_id: 7,
        currency: null,
        official_equity: "1025.00",
        official_available_cash: "525.00",
        official_total_invested: "400.00",
        official_unrealised_pnl: "100.00",
        local_eod_currency: null,
        local_eod_value: null,
        local_eod_positions_priced: null,
        local_eod_stale_mark_positions: null,
        difference: null,
        comparable: false,
        incomplete_reasons: ["account_currency_not_documented"],
      },
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const performance = (await screen.findByText("Portfolio performance")).closest("section")!;
    expect(within(performance).getByText("Currency unverified")).toBeInTheDocument();
    expect(within(performance).queryByText("US$1,025.00")).not.toBeInTheDocument();
    expect(within(performance).queryByText(/1,025\.00/)).not.toBeInTheDocument();
    expect(within(performance).getByText("The broker reported an account currency that is not documented for this USD-only trading lane.")).toBeInTheDocument();
  });

  it("shows carried-forward mark magnitude and never hides an unknown account-evidence reason", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...OVERVIEW,
      account_equity_evidence: {
        status: "collecting",
        days_collected: 4,
        snapshot_date: "2026-08-12",
        observed_at: "2026-08-12T19:00:00Z",
        account_currency_id: 1,
        currency: "USD",
        official_equity: "1025.00",
        official_available_cash: "525.00",
        official_total_invested: "400.00",
        official_unrealised_pnl: "100.00",
        local_eod_currency: "USD",
        local_eod_value: "1020.00",
        local_eod_positions_priced: 7,
        local_eod_stale_mark_positions: 3,
        difference: "5.00",
        comparable: false,
        incomplete_reasons: [
          "account_currency_assumed_not_observed",
          "same_day_local_eod_snapshot_missing",
          "local_eod_currency_mismatch",
          "local_eod_valuation_incomplete",
          "local_eod_marks_carried_forward",
          "future_account_reason",
        ],
      },
    });

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const performance = (await screen.findByText("Portfolio performance")).closest("section")!;
    expect(within(performance).getByText("This snapshot predates observed broker account currency; its currency cannot be trusted.")).toBeInTheDocument();
    expect(within(performance).getByText("The same-day local end-of-day valuation is missing.")).toBeInTheDocument();
    expect(within(performance).getByText("The broker account and local valuation currencies do not match.")).toBeInTheDocument();
    expect(within(performance).getByText("The local valuation is missing at least one price or currency conversion.")).toBeInTheDocument();
    expect(within(performance).getByText("3 of 7 priced positions use a carried-forward closing mark.")).toBeInTheDocument();
    expect(within(performance).getByText("future_account_reason")).toBeInTheDocument();
  });

  it("states that reconciliation policy is missing when measurements have no named caveat", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...OVERVIEW,
      account_equity_evidence: {
        ...OVERVIEW.account_equity_evidence,
        status: "collecting",
        days_collected: 4,
        currency: "USD",
        official_equity: "1025.00",
        local_eod_currency: "USD",
        local_eod_value: "1020.00",
        local_eod_positions_priced: 2,
        local_eod_stale_mark_positions: 0,
        incomplete_reasons: [],
      },
    });

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const performance = (await screen.findByText("Portfolio performance")).closest("section")!;
    expect(within(performance).getByText("Comparison tolerance not defined")).toBeInTheDocument();
  });

  it("separates unapproved research from selectable strategies", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByText("Automation is not ready")).toBeInTheDocument();
    expect(screen.getByText("Candidate research has not cleared the recent after-cost validation gates.")).toBeInTheDocument();
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
    await waitFor(() => expect(update).toHaveBeenCalledWith({ enabled: false, capital_limit: "1500.000000", capital_mode: "fixed", risk_profile: "balanced", reason: "Automated strategy workspace update" }));
  });

  it("shows exact mandate limits and submits a changed risk profile", async () => {
    const update = vi.spyOn(strategiesApi, "updateStrategyPaperPool").mockResolvedValue({
      ...OVERVIEW.paper_pool,
      mandate: { ...OVERVIEW.paper_pool.mandate, risk_profile: "growth" },
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByText("Policy ceilings, not return forecasts. Long-only and unleveraged in this version.")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByLabelText("Risk profile"), "growth");
    expect(screen.getByText("+18.00%")).toBeInTheDocument();
    expect(screen.getByText("12")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(update).toHaveBeenCalledWith(expect.objectContaining({
      risk_profile: "growth",
    })));
  });

  it("uses the atomic paper-pool request for the first enable while the system-wide flag is off", async () => {
    const update = vi.spyOn(strategiesApi, "updateStrategyPaperPool").mockResolvedValue({
      ...OVERVIEW.paper_pool,
      enabled: true,
    });
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...approvedOverview(),
      execution_enabled: false,
      paper_pool: { ...approvedOverview().paper_pool, enabled: false },
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const master = await screen.findByRole("checkbox", { name: "Allow new automated entries" });
    expect(master).not.toBeChecked();
    expect(master).not.toBeDisabled();
    await userEvent.click(master);
    await userEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(update).toHaveBeenCalledWith(expect.objectContaining({
      enabled: true,
      capital_limit: "1000.000000",
      risk_profile: "balanced",
    })));
  });

  it("keeps first enable disabled when strategy evidence is not ready", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...OVERVIEW,
      execution_enabled: false,
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const master = await screen.findByRole("checkbox", { name: "Allow new automated entries" });
    expect(master).not.toBeChecked();
    expect(master).toBeDisabled();
    expect(screen.getByText("Automation stays off until at least one strategy passes validation.")).toBeInTheDocument();
  });

  it("keeps first enable disabled outside the demo environment", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...approvedOverview(),
      demo_connection: false,
      execution_enabled: false,
      paper_pool: { ...approvedOverview().paper_pool, enabled: false },
    });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const master = await screen.findByRole("checkbox", { name: "Allow new automated entries" });
    expect(master).toBeDisabled();
    expect(screen.getByText("Paper automation can only be enabled while connected to the demo environment.")).toBeInTheDocument();
  });

  it("shows compact prospective evidence when automation is ready", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(approvedOverview());
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);

    const readiness = (await screen.findByText("Automation evidence is current")).closest("section")!;
    expect(within(readiness).getByText("30")).toBeInTheDocument();
    expect(within(readiness).getAllByText("1")).toHaveLength(2);
    expect(screen.getByRole("checkbox", { name: "Allow new automated entries" })).not.toBeDisabled();
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
    expect(await screen.findByText(/Evidence 1\/6/)).toHaveTextContent("frozen through 27 Sept 2024");
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
    await waitFor(() => expect(strategiesApi.fetchFiredSignals).toHaveBeenCalledTimes(2));
  });

  it("shows funded and rejected decisions through execution and outcome without inventing orders", async () => {
    const rejected: FiredSignal = {
      ...FUNDED_SIGNAL,
      signal_id: 90,
      symbol: "SAFE",
      outcome: null,
      exit_bar_date: null,
      exit_price: null,
      gross_return_pct: null,
      outcome_reason: null,
      funding_status: "rejected",
      funding_reason: "paper_pool_disabled",
      funded_amount: null,
      strategy_trade_id: null,
      execution_status: null,
      actual_fill_price: null,
      slippage_pct: null,
      trade_lifecycle: null,
    };
    vi.mocked(strategiesApi.fetchFiredSignals).mockResolvedValue({
      items: [FUNDED_SIGNAL, rejected],
      next_cursor: null,
    });

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const section = (await screen.findByText("Generated trade activity")).closest("section")!;
    expect(within(section).getByText("Funded")).toBeInTheDocument();
    expect(within(section).getByText("Not funded")).toBeInTheDocument();
    expect(within(section).getByText("Trade #41")).toBeInTheDocument();
    expect(within(section).getByText("Position #7001 · released")).toBeInTheDocument();
    expect(within(section).getByText("close · applied")).toBeInTheDocument();
    expect(within(section).getByText("Operation #88 · order #99")).toBeInTheDocument();
    expect(within(section).getByText("Reconciliation resolved · broker closed")).toBeInTheDocument();
    expect(within(section).getByText("operator close")).toBeInTheDocument();
    expect(within(section).getByText("1 close event")).toBeInTheDocument();
    expect(within(section).getByText("US$8.75 realised")).toBeInTheDocument();
    expect(within(section).getByText("US$1.25 fees")).toBeInTheDocument();
    expect(within(section).getByText("No order submitted")).toBeInTheDocument();
    expect(within(section).getByText("No strategy trade")).toBeInTheDocument();
    expect(within(section).getByText("US$100.00 assigned")).toBeInTheDocument();
    expect(within(section).getByText("US$20.10 actual")).toBeInTheDocument();
    expect(within(section).getByText("+0.50% slippage")).toBeInTheDocument();
    expect(within(section).getByText("target first")).toBeInTheDocument();
    expect(within(section).getByText("+10.00%")).toBeInTheDocument();
    expect(within(section).getByText("Outcome unresolved")).toBeInTheDocument();
    expect(within(section).getByText("paper pool disabled")).toBeInTheDocument();
  });

  it("surfaces missing and ambiguous broker lifecycle evidence instead of selecting a position", async () => {
    const missing: FiredSignal = {
      ...FUNDED_SIGNAL,
      signal_id: 89,
      symbol: "MISS",
      trade_lifecycle: {
        ...FUNDED_SIGNAL.trade_lifecycle!,
        trade_status: "closed",
        close_event_count: 0,
        realised_pnl_usd: null,
        observed_fees_usd: null,
        close_history_status: "incomplete",
        incomplete_reasons: ["released_position_missing_close_history"],
      },
    };
    const ambiguous: FiredSignal = {
      ...FUNDED_SIGNAL,
      signal_id: 88,
      symbol: "AMB",
      trade_lifecycle: {
        ...FUNDED_SIGNAL.trade_lifecycle!,
        ownership_count: 2,
        broker_position_id: null,
        ownership_status: null,
        position_claimed_at: null,
        position_released_at: null,
        position_release_reason: null,
        latest_operation_type: null,
        latest_operation_id: null,
        latest_operation_order_id: null,
        latest_operation_trigger: null,
        latest_operation_status: null,
        latest_operation_created_at: null,
        latest_operation_submitted_at: null,
        latest_operation_resolved_at: null,
        latest_reconciliation_state: null,
        latest_reconciliation_broker_status: null,
        latest_reconciliation_attempt_count: null,
        latest_reconciliation_updated_at: null,
        latest_reconciliation_error: null,
        close_event_count: null,
        realised_pnl_usd: null,
        observed_fees_usd: null,
        close_history_status: "unavailable",
        incomplete_reasons: ["position_ownership_ambiguous"],
      },
    };
    vi.mocked(strategiesApi.fetchFiredSignals).mockResolvedValue({
      items: [missing, ambiguous],
      next_cursor: null,
    });

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const section = (await screen.findByText("Generated trade activity")).closest("section")!;
    expect(within(section).getByText("Close history incomplete")).toBeInTheDocument();
    expect(within(section).getByText("Released position is missing broker close history")).toBeInTheDocument();
    expect(within(section).getByText("2 ownership records · ambiguous")).toBeInTheDocument();
    expect(within(section).getByText("Close history unavailable")).toBeInTheDocument();
    expect(within(section).getByText("More than one broker-position ownership record exists")).toBeInTheDocument();
  });

  it("shows a submitted close as closing without presenting realised P&L", async () => {
    const closing: FiredSignal = {
      ...FUNDED_SIGNAL,
      trade_lifecycle: {
        ...FUNDED_SIGNAL.trade_lifecycle!,
        trade_status: "closing",
        ownership_status: "active",
        position_released_at: null,
        position_release_reason: null,
        latest_operation_trigger: "strategy_exit",
        latest_operation_status: "submitted",
        latest_operation_resolved_at: null,
        latest_reconciliation_state: "pending",
        latest_reconciliation_broker_status: "pending",
        latest_reconciliation_updated_at: "2026-08-12T09:59:00Z",
        close_event_count: 0,
        realised_pnl_usd: null,
        observed_fees_usd: null,
        close_history_status: "not_closed",
      },
    };
    vi.mocked(strategiesApi.fetchFiredSignals).mockResolvedValue({ items: [closing], next_cursor: null });

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const section = (await screen.findByText("Generated trade activity")).closest("section")!;
    expect(within(section).getByText("closing")).toBeInTheDocument();
    expect(within(section).getByText("close · submitted")).toBeInTheDocument();
    expect(within(section).getByText("strategy exit")).toBeInTheDocument();
    expect(within(section).getByText("Reconciliation pending · broker pending")).toBeInTheDocument();
    expect(within(section).getByText("No broker close yet")).toBeInTheDocument();
    const row = within(section).getByText("ACME").closest("tr")!;
    expect(within(row).queryByText(/realised/)).not.toBeInTheDocument();
  });

  it("renders failed and reconciliation-required trades as risk states", async () => {
    const failed: FiredSignal = {
      ...FUNDED_SIGNAL,
      signal_id: 101,
      symbol: "FAIL",
      trade_lifecycle: {
        ...FUNDED_SIGNAL.trade_lifecycle!,
        trade_status: "failed",
        incomplete_reasons: [],
      },
    };
    const reconcileRequired: FiredSignal = {
      ...FUNDED_SIGNAL,
      signal_id: 100,
      symbol: "RECON",
      trade_lifecycle: {
        ...FUNDED_SIGNAL.trade_lifecycle!,
        trade_status: "reconcile_required",
        incomplete_reasons: [],
      },
    };
    vi.mocked(strategiesApi.fetchFiredSignals).mockResolvedValue({
      items: [failed, reconcileRequired],
      next_cursor: null,
    });

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const section = (await screen.findByText("Generated trade activity")).closest("section")!;
    const failedBadge = within(within(section).getByText("FAIL").closest("tr")!).getByText("failed");
    const reconcileBadge = within(within(section).getByText("RECON").closest("tr")!).getByText("reconcile required");
    expect(failedBadge.parentElement).toHaveClass("border-red-300");
    expect(reconcileBadge.parentElement).toHaveClass("border-red-300");
  });

  it("loads older generated decisions through the bounded cursor", async () => {
    const older = { ...FUNDED_SIGNAL, signal_id: 75, symbol: "OLD" };
    vi.mocked(strategiesApi.fetchFiredSignals)
      .mockResolvedValueOnce({ items: [FUNDED_SIGNAL], next_cursor: 91 })
      .mockResolvedValueOnce({ items: [older], next_cursor: null });

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByRole("button", { name: "Load older activity" }));

    expect(await screen.findByText("OLD")).toBeInTheDocument();
    expect(strategiesApi.fetchFiredSignals).toHaveBeenNthCalledWith(2, 91);
    expect(screen.getByText("2 decisions shown")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Load older activity" })).not.toBeInTheDocument();
  });

  it("keeps current activity visible when an older page fails and retries the same cursor", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const older = { ...FUNDED_SIGNAL, signal_id: 75, symbol: "OLD" };
    vi.mocked(strategiesApi.fetchFiredSignals)
      .mockResolvedValueOnce({ items: [FUNDED_SIGNAL], next_cursor: 91 })
      .mockRejectedValueOnce(new Error("older page unavailable"))
      .mockResolvedValueOnce({ items: [older], next_cursor: null });

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByRole("button", { name: "Load older activity" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Older activity could not be loaded");
    expect(screen.getByText("ACME")).toBeInTheDocument();
    expect(screen.getByText("1 decision shown")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Retry older activity" }));
    expect(await screen.findByText("OLD")).toBeInTheDocument();
    expect(strategiesApi.fetchFiredSignals).toHaveBeenNthCalledWith(3, 91);
  });

  it("renders an honest empty activity state", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    expect(await screen.findByText("No generated decisions yet")).toBeInTheDocument();
    expect(screen.getByText("Fired entry and exit signals will appear here after the daily strategy scan.")).toBeInTheDocument();
  });

  it("isolates an activity failure and recovers on explicit retry", async () => {
    vi.mocked(strategiesApi.fetchFiredSignals)
      .mockRejectedValueOnce(new Error("activity unavailable"))
      .mockResolvedValueOnce({ items: [], next_cursor: null });

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    const heading = await screen.findByText("Generated trade activity");
    const section = heading.closest("section")!;
    expect(within(section).getByRole("alert")).toHaveTextContent("Failed to load");
    await userEvent.click(within(section).getByRole("button", { name: "Retry" }));
    expect(await screen.findByText("No generated decisions yet")).toBeInTheDocument();
  });

  it("toggles an approved strategy for the next run", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(approvedOverview());
    const update = vi.spyOn(strategiesApi, "updateStrategyAllocation").mockResolvedValue({ strategy_id: "s1-time-series-momentum", strategy_version: "strategy-registry-v1+abc", deployment_id: 7, capital_limit: "1000", currency: "USD", enabled: false, revision: 3 });
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByRole("checkbox", { name: "Enabled" }));
    await waitFor(() => expect(update).toHaveBeenCalledWith("s1-time-series-momentum", expect.objectContaining({ enabled: false, reason: "Paused from automated strategy workspace" })));
  });

  it("keeps the primary label on the primary window when a calendar year is the only complete one", async () => {
    // #2624: primaryEvidence() matched window_id === "primary", which the API
    // cannot emit (the declared id is "primary-2022-plus"), so it always fell
    // through to "first complete window". With the primary window partial and a
    // calendar year complete, the headline described 2022 alone under a label
    // that said primary. The fixture's own ids hid this — they were wrong too.
    const mixed = structuredClone(OVERVIEW);
    const strategy = mixed.strategies[0]!;
    strategy.evidence_windows = [
      { ...strategy.evidence_windows[0]!, window_id: "primary-2022-plus", label: "Primary: 2022 onward", status: "partial", arms: [{ ...ARM, trade_count: 111, open_trade_count: 0, unpriced_trade_count: 0 }] },
      { ...strategy.evidence_windows[0]!, window_id: "year-2022", label: "Calendar 2022", window_end: "2022-12-31", status: "complete", arms: [{ ...ARM, trade_count: 222, open_trade_count: 0, unpriced_trade_count: 0 }] },
    ];
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(mixed);

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));
    await userEvent.click(await screen.findByRole("button", { name: "View evidence" }));

    expect(screen.getByText(/Primary: 2022 onward/)).toBeInTheDocument();
    expect(screen.getByText("111")).toBeInTheDocument();
    expect(screen.queryByText("222")).not.toBeInTheDocument();
  });

  it("says a version rotated instead of claiming the strategy never ran", async () => {
    // #2624: a registry-touching merge mints a new strategy_version, the
    // watermark is keyed on it, so the CURRENT version has no scan and every
    // evidence window reads `missing`. The page used to render that as a blank
    // card — indistinguishable from a broken system.
    const rotated = structuredClone(OVERVIEW);
    const strategy = rotated.strategies[0]!;
    strategy.scan = {
      ...strategy.scan,
      frontier_date: null,
      updated_at: null,
      status: "rotated",
      rotation: {
        previous_version: "strategy-registry-v1+67dbf07c9d72",
        previous_frontier_date: "2026-08-11",
        previous_scanned_at: "2026-08-12T18:59:01Z",
      },
    };
    strategy.evidence_windows = strategy.evidence_windows.map((window) => ({ ...window, status: "missing", arms: [] }));
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(rotated);

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));
    await userEvent.click(await screen.findByRole("button", { name: "View evidence" }));

    expect(screen.getByText("Version rotated.")).toBeInTheDocument();
    expect(screen.getByText(/scanned through 11 Aug 2026 under the previous version/)).toBeInTheDocument();
    expect(screen.getByText(/A new track record is starting/)).toBeInTheDocument();
  });

  it("names the basis a previous version was measured on rather than splicing its figures in", async () => {
    // The prior version's numbers are deliberately absent: measured on a
    // different cost model and return basis, which ARE the result identity.
    const withHistory = structuredClone(OVERVIEW);
    withHistory.strategies[0]!.prior_versions = [
      {
        strategy_version: "strategy-registry-v1+2307ee566d7b",
        result_count: 60,
        last_scan_frontier_date: "2026-08-07",
        last_scan_at: "2026-08-09T06:46:00Z",
        comparable: false,
        incomparable_reasons: ["cost_model_id", "return_basis"],
      },
    ];
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(withHistory);

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));
    await userEvent.click(await screen.findByRole("button", { name: "View evidence" }));

    expect(screen.getByText("Previous versions")).toBeInTheDocument();
    expect(screen.getByText(/2307ee56/)).toBeInTheDocument();
    expect(screen.getByText(/60 stored results/)).toBeInTheDocument();
    expect(screen.getByText(/different cost model, return basis/)).toBeInTheDocument();
  });

  it("shows version history on a validation control, which is where every strategy actually renders", async () => {
    // All four strategies are harness_validation, so ValidationControl — not
    // EvidenceDetail — is the surface the operator sees (#2624). A candidate-only
    // fixture cannot catch a block that is missing there.
    const control = structuredClone(OVERVIEW);
    const strategy = control.strategies[0]!;
    strategy.purpose = "harness_validation";
    strategy.scan = {
      ...strategy.scan,
      frontier_date: null,
      updated_at: null,
      status: "rotated",
      rotation: { previous_version: "strategy-registry-v1+67dbf07c9d72", previous_frontier_date: "2026-08-11", previous_scanned_at: "2026-08-12T18:59:01Z" },
    };
    strategy.prior_versions = [
      { strategy_version: "strategy-registry-v1+2307ee566d7b", result_count: 60, last_scan_frontier_date: "2026-08-07", last_scan_at: "2026-08-09T06:46:00Z", comparable: false, incomparable_reasons: ["cost_model_id"] },
    ];
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(control);

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));

    expect(await screen.findByText("Version rotated.")).toBeInTheDocument();
    expect(screen.getByText("Previous versions")).toBeInTheDocument();
    expect(screen.getByText(/different cost model/)).toBeInTheDocument();
  });

  it("names the reason for each blank catalog fact, on the surface every strategy renders through", async () => {
    // ⚠ `harness_validation`, so `ValidationControl` — all four real strategies
    // are controls, and a candidate-only fixture cannot prove the block is on the
    // surface the operator sees (#2624). This is the day-one state exactly: a
    // share but no weekly rate, and a pre-`criterion7-v2` result row.
    const control = structuredClone(OVERVIEW);
    control.strategies[0]!.purpose = "harness_validation";
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(control);

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));

    // The share renders; the weekly rate names its own independent reason.
    // ⚠ Unsigned. `formatPct`'s `exceptZero` would render this "+52.10%", which
    // reads as a change in the share rather than the share itself.
    expect(await screen.findByText("52.10%")).toBeInTheDocument();
    expect(screen.getByText("1 scan day — needs a span")).toBeInTheDocument();
    // The holding period's blank is explained by the RESULT VERSION, not by
    // either fire-rate reason, and must not read as a zero.
    expect(screen.getByText("Not measured")).toBeInTheDocument();
    expect(screen.getByText(/Result version criterion7-v1/)).toBeInTheDocument();
  });

  it("distinguishes a result version that never measured the hold from one that closed no trades", async () => {
    // The other branch of the same blank. `sql/347` permits a null median under
    // `criterion7-v2` ONLY when trade_count is 0, so under the current version an
    // empty cell means "closed nothing" — a different claim from "we never
    // measured this", and the operator must not read one as the other.
    const measured = structuredClone(OVERVIEW);
    const strategy = measured.strategies[0]!;
    strategy.purpose = "harness_validation";
    const arm = strategy.evidence_windows[0]!.arms[0]!;
    arm.metric_set_id = "criterion7-v2";
    arm.median_hold_days = null;
    arm.trade_count = 0;
    arm.losing_trade_count = 0;
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(measured);

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));

    expect(await screen.findByText("No completed trades")).toBeInTheDocument();
    expect(screen.queryByText(/Result version/)).not.toBeInTheDocument();
  });

  it("never shows a median holding period without both of its exclusion counts", async () => {
    // The median is right-censored and the bias direction is not determinable a
    // priori, so open and unpriced counts are separate exclusions that must both
    // appear beside it — neither implies the other.
    const measured = structuredClone(OVERVIEW);
    const strategy = measured.strategies[0]!;
    strategy.purpose = "harness_validation";
    const arm = strategy.evidence_windows[0]!.arms[0]!;
    arm.metric_set_id = "criterion7-v2";
    arm.median_hold_days = "6.5";
    arm.hold_days_p25 = "3";
    arm.hold_days_p75 = "14";
    arm.open_trade_count = 4;
    arm.unpriced_trade_count = 2;
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(measured);

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));

    expect(await screen.findByText("6.5 days")).toBeInTheDocument();
    expect(screen.getByText("3–14 typical · 4 open, 2 unpriced excluded")).toBeInTheDocument();
  });

  it("reports the non-losing share rather than calling a breakeven trade a win", async () => {
    // `strategy_statistics` counts a losing trade as `value < 0.0` STRICTLY and
    // stores no winning count, so `trade_count - losing_trade_count` includes
    // breakevens. 12 trades with 3 losing is 9 NOT AT A LOSS — labelling that a
    // win rate would report a flat close as a win.
    const control = structuredClone(OVERVIEW);
    control.strategies[0]!.purpose = "harness_validation";
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(control);

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));

    expect(await screen.findByText("Not lost")).toBeInTheDocument();
    expect(screen.getByText("75.00%")).toBeInTheDocument();
    expect(screen.getByText("9 of 12 not at a loss")).toBeInTheDocument();
    expect(screen.queryByText("Won")).not.toBeInTheDocument();
  });

  it("counts backtest trades without subtracting exclusions that were never in the total", async () => {
    // `trade_count = len(net_returns)` and `backtest_run` appends a return only
    // on a realised close, so open and unpriced positions are reported alongside
    // it, never inside it. Subtracting them understated 300 of the 324 stored
    // rows. 12 trades with 4 open and 2 unpriced must still read 12, not 6.
    const censored = structuredClone(OVERVIEW);
    const arm = censored.strategies[0]!.evidence_windows[0]!.arms[0]!;
    arm.open_trade_count = 4;
    arm.unpriced_trade_count = 2;
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue(censored);

    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));
    await userEvent.click(await screen.findByRole("button", { name: "View evidence" }));

    const trades = screen.getByText("Trades").parentElement!;
    expect(within(trades).getByText("12")).toBeInTheDocument();
    expect(within(trades).getByText("4 open, 2 unpriced excluded")).toBeInTheDocument();
  });

  it("shows no previous-versions block when there is no history", async () => {
    render(<MemoryRouter><StrategiesPage /></MemoryRouter>);
    await userEvent.click(await screen.findByText("Research & validation"));
    await userEvent.click(await screen.findByRole("button", { name: "View evidence" }));

    expect(screen.queryByText("Previous versions")).not.toBeInTheDocument();
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
