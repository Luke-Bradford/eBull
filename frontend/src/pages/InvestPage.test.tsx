import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as strategiesApi from "@/api/strategies";
import type { StrategyOverviewResponse } from "@/api/types";
import { BENCHMARK_REFUSALS } from "@/components/strategies/__fixtures__/benchmarkRefusals";
import { InvestPage } from "@/pages/InvestPage";

/** Pot funded and on, no strategy passed, nothing blocking — the target state. */
const OVERVIEW = {
  benchmark_refusals: BENCHMARK_REFUSALS,
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
    capital_limit: "500.000000",
    capital_mode: "fixed",
    approval_mode: "autonomous",
    effective_capital: "500.000000",
    currency: "USD",
    reserved_capital: "225",
    invested_capital: "225",
    remaining_capital: "275",
    capital_observation_complete: true,
    mandate: { configured: true, risk_profile: "balanced" },
    available_mandates: [],
  },
  automation_readiness: { ready: false, state: "no_capital_candidates", capital_candidate_count: 0 },
  strategies: [{}, {}],
} as unknown as StrategyOverviewResponse;

const CORE_READY = {
  state: "ready",
  selected_symbol: "SPY.RTH",
  earliest_possible_verdict_at: "2026-09-02T20:00:00Z",
  mandate: { enabled: true },
  execution_action: "rebalance",
  blockers: [],
};

const SLEEVE_POSITION = {
  strategy_trade_id: 1,
  broker_position_id: 11,
  strategy_id: null,
  strategy_title: "Core sleeve",
  symbol: "SPY.RTH",
  currency: "USD",
  current_value: "231.50",
  unrealised_pnl: "6.46",
  valuation_available: true,
};

function renderPage() {
  return render(
    <MemoryRouter>
      <InvestPage />
    </MemoryRouter>,
  );
}

describe("InvestPage", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(OVERVIEW);
    vi.spyOn(strategiesApi, "fetchCoreSleeve").mockResolvedValue(CORE_READY as never);
    vi.spyOn(strategiesApi, "fetchStrategyOwnedPositions").mockResolvedValue({
      positions: [SLEEVE_POSITION],
      live_quote_instrument_ids: [],
    } as never);
    vi.spyOn(strategiesApi, "fetchStrategyPnlHistory").mockResolvedValue({ points: [], return_since: null } as never);
  });

  it("leads with the honest no-strategy message and the sleeve holding", async () => {
    renderPage();
    expect(
      await screen.findByText("No strategy has passed its tests yet. Your money is in the index sleeve (SPY.RTH)."),
    ).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Trading — core sleeve/ })).toBeInTheDocument();
    expect(screen.getByText("Amount assigned")).toBeInTheDocument();
    expect(screen.getByText("Fixed budget")).toBeInTheDocument();
    expect(screen.getByText("No licensed benchmark yet")).toBeInTheDocument();
    expect(screen.getByText(/2 strategies are under test/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Change amount, risk or on/off" })).toHaveAttribute(
      "href",
      "/strategies?view=setup",
    );
  });

  it("says a failed positions read failed instead of claiming the sleeve is empty", async () => {
    vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockRejectedValue(new Error("boom"));
    renderPage();
    expect(await screen.findByText(/What the index sleeve holds could not be loaded/)).toBeInTheDocument();
    expect(screen.queryByText("Nothing is held yet.")).not.toBeInTheDocument();
    expect(screen.queryByText(/Your money is in/)).not.toBeInTheDocument();
  });

  it("puts the kill switch in front of the numbers", async () => {
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...OVERVIEW,
      entry_block: {
        ...OVERVIEW.entry_block,
        new_entries_blocked: true,
        global_kill_active: true,
        global_kill_reason: "manual halt",
      },
    });
    renderPage();
    expect(await screen.findByText(/Kill switch is on — no order path is open — manual halt/)).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Not trading/ })).toBeInTheDocument();
  });
});
