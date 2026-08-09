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
  execution_enabled: false,
  live_execution_enabled: false,
  live_strategy_activation_available: false,
  live_strategy_activation_blocker: "live_strategy_broker_contract_not_validated",
  storage_policy: "fired_signals_and_material_mutations_only",
  entry_block: {
    new_entries_blocked: true,
    global_kill_active: false,
    global_kill_reason: null,
    global_kill_activated_at: null,
    global_kill_activated_by: null,
    execution_block_reasons: ["automatic trading disabled"],
  },
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
      stage: null,
      attribution: {
        fired_entries: 108,
        funded_entries: 0,
        rejected_entries: 108,
        resolved_entries: 0,
        shadow_average_return_pct: null,
        funded_shadow_average_return_pct: null,
        rejected_shadow_average_return_pct: null,
        opportunity_gap_pct: null,
        funded_capture_rate: "0",
        filled_entries: 0,
        broker_rejected_entries: 0,
        fill_rate: null,
        broker_rejection_rate: null,
        average_slippage_pct: null,
        average_stressed_cost_usd: null,
        max_observed_account_drawdown_pct: null,
      },
      pnl: {
        currency: "USD",
        strategy_trade_count: 0,
        owned_position_count: 0,
        active_position_count: 0,
        close_event_count: 0,
        invested_capital: "0",
        realised_pnl: "0",
        unrealised_pnl: "0",
        total_pnl: "0",
        observed_fees: "0",
        complete: true,
        incomplete_reasons: [],
      },
      allocation: {
        deployment_id: null,
        capital_limit: "0",
        currency: "USD",
        enabled: false,
        revision: null,
        reserved_capital: "0",
        invested_capital: "0",
        remaining_capital: "0",
        policy_configured: false,
        max_drawdown_limit_pct: null,
      },
      allocation_ready: false,
      allocation_refusals: ["recent_evidence_incomplete"],
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
      funding_status: "rejected",
      funding_reason: "not_evaluated_by_allocator",
      funded_amount: null,
      strategy_trade_id: null,
      execution_status: null,
      actual_fill_price: null,
      slippage_pct: null,
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
    expect(screen.getByText("New strategy entries are blocked")).toBeInTheDocument();
    expect(screen.getByText("Real-money strategy activation is unavailable")).toBeInTheDocument();
    expect(screen.getByText("Not funded")).toBeInTheDocument();
    expect(screen.getByText("Not evaluated by allocator")).toBeInTheDocument();
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

  it("keeps allocation controls visible but disabled when evidence is unavailable", async () => {
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(OVERVIEW);
    vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue(SIGNALS);
    render(
      <MemoryRouter>
        <StrategiesPage />
      </MemoryRouter>,
    );

    expect(await screen.findByRole("button", { name: "Save allocation" })).toBeDisabled();
    expect(screen.getByText(/Allocation needs complete recent evidence/i)).toBeInTheDocument();
  });

  it("keeps an existing disabled allocation editable for risk reduction", async () => {
    const base = OVERVIEW.strategies[0]!;
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue({
      ...OVERVIEW,
      strategies: [
        {
          ...base,
          allocation: {
            ...base.allocation,
            deployment_id: 7,
            capital_limit: "250.000000",
            revision: 2,
          },
        },
      ],
    });
    vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue(SIGNALS);
    render(
      <MemoryRouter>
        <StrategiesPage />
      </MemoryRouter>,
    );

    expect(await screen.findByLabelText("Maximum USD capital")).toBeEnabled();
    expect(screen.getByLabelText("Reason for this audited change")).toBeEnabled();
  });

  it("allows an enabled evidence-invalid sleeve to reduce without disabling", async () => {
    const base = OVERVIEW.strategies[0]!;
    const strategy = {
      ...base,
      allocation: {
        ...base.allocation,
        deployment_id: 7,
        capital_limit: "250.000000",
        enabled: true,
        revision: 2,
      },
    };
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue({
      ...OVERVIEW,
      strategies: [strategy],
    });
    vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue(SIGNALS);
    const update = vi.spyOn(strategiesApi, "updateStrategyAllocation").mockResolvedValue({
      strategy_id: strategy.strategy_id,
      strategy_version: strategy.strategy_version,
      deployment_id: 7,
      capital_limit: "200.000000",
      currency: "USD",
      enabled: true,
      revision: 3,
    });
    render(
      <MemoryRouter>
        <StrategiesPage />
      </MemoryRouter>,
    );

    const limit = await screen.findByLabelText("Maximum USD capital");
    await userEvent.clear(limit);
    await userEvent.type(limit, "200");
    await userEvent.type(screen.getByLabelText("Reason for this audited change"), "reduce risk");
    await userEvent.click(screen.getByRole("button", { name: "Save allocation" }));

    await waitFor(() =>
      expect(update).toHaveBeenCalledWith(strategy.strategy_id, {
        strategy_version: strategy.strategy_version,
        capital_limit: "200.000000",
        enabled: true,
        reason: "reduce risk",
      }),
    );
  });

  it("sends an explicit audited allocation and refetches the picker", async () => {
    const base = OVERVIEW.strategies[0]!;
    const available: StrategyOverviewResponse = {
      ...OVERVIEW,
      strategies: [
        {
          ...base,
          runnable: true,
          exclusion_reason: null,
          stage: "paper_enabled",
          allocation_ready: true,
          allocation_refusals: [],
          allocation: {
            ...base.allocation,
            deployment_id: 7,
            policy_configured: true,
          },
        },
      ],
    };
    const availableStrategy = available.strategies[0]!;
    const overview = vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(available);
    vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue(SIGNALS);
    const update = vi.spyOn(strategiesApi, "updateStrategyAllocation").mockResolvedValue({
      strategy_id: availableStrategy.strategy_id,
      strategy_version: availableStrategy.strategy_version,
      deployment_id: 7,
      capital_limit: "250.000000",
      currency: "USD",
      enabled: true,
      revision: 2,
    });
    render(
      <MemoryRouter>
        <StrategiesPage />
      </MemoryRouter>,
    );

    const limit = await screen.findByLabelText("Maximum USD capital");
    await userEvent.clear(limit);
    await userEvent.type(limit, "250");
    await userEvent.click(screen.getByLabelText("Enabled"));
    await userEvent.type(screen.getByLabelText("Reason for this audited change"), "bounded paper sleeve");
    await userEvent.click(screen.getByRole("button", { name: "Save allocation" }));

    await waitFor(() =>
      expect(update).toHaveBeenCalledWith(availableStrategy.strategy_id, {
        strategy_version: availableStrategy.strategy_version,
        capital_limit: "250.000000",
        enabled: true,
        reason: "bounded paper sleeve",
      }),
    );
    await waitFor(() => expect(overview).toHaveBeenCalledTimes(2));
  });
});
