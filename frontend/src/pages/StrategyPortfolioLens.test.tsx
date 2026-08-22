import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as strategiesApi from "@/api/strategies";
import type { StrategyOverviewResponse } from "@/api/types";
import { StrategiesHubPage } from "@/pages/StrategiesHubPage";
import { StrategyPortfolioLens } from "@/pages/StrategyPortfolioLens";

/** The state the operator actually has today: kill switch on, nothing funded. */
const BLOCKED = {
  as_of: "2026-08-23T00:00:00Z",
  demo_connection: true,
  execution_enabled: false,
  live_execution_enabled: false,
  live_strategy_activation_available: false,
  live_strategy_activation_blocker: "live_strategy_broker_contract_not_validated",
  storage_policy: "fired_signals_and_material_mutations_only",
  entry_block: {
    new_entries_blocked: true,
    global_kill_active: true,
    global_kill_reason: "autonomy loop unattended — block any order path (monitor boot)",
    global_kill_activated_at: "2026-06-28T01:32:30Z",
    global_kill_activated_by: "monitor",
    execution_block_reasons: ["automatic trading disabled"],
  },
  paper_pool: {
    configured: false,
    enabled: false,
    capital_limit: "0",
    capital_mode: "fixed",
    approval_mode: "manual",
    effective_capital: "0",
    currency: "USD",
    reserved_capital: "0",
    invested_capital: "0",
    remaining_capital: "0",
    mandate: { configured: false, policy_version: "portfolio-mandate-unconfigured", risk_profile: "unconfigured" },
    available_mandates: [{ risk_profile: "cautious" }, { risk_profile: "balanced" }, { risk_profile: "growth" }],
  },
  automation_readiness: { ready: false, state: "no_capital_candidates", capital_candidate_count: 0 },
  // Copied from the live dev payload — the research lens reads both, and a
  // minimal fixture crashed AccountEvidence rather than rendering empty.
  account_equity_evidence: {
  "status": "collecting",
  "reconciliation_state": "refused",
  "reconciliation_rule_version": "f0-reconcile-v1",
  "days_collected": 12,
  "snapshot_date": "2026-08-22",
  "observed_at": "2026-08-22T23:15:03.095940Z",
  "account_currency_id": 1,
  "currency": "USD",
  "official_equity": "99460.340000",
  "official_available_cash": "1703.460000",
  "official_total_invested": "104060.060000",
  "official_unrealised_pnl": "-6303.180000",
  "official_direct_long_market_value": "59315.01",
  "official_comparand": "61018.470000",
  "residual_not_in_local_book": "38441.870000",
  "local_eod_currency": null,
  "local_eod_value": null,
  "local_eod_value_in_account_currency": null,
  "local_eod_positions_priced": null,
  "local_eod_stale_mark_positions": null,
  "difference": null,
  "tolerance": null,
  "comparable": false,
  "incomplete_reasons": [
    "same_day_local_eod_snapshot_missing"
  ]
},
  evidence_refresh: {
  "frozen_through": "2024-09-27",
  "completed_windows": 0,
  "partial_windows": 0,
  "total_windows": 6,
  "status": "complete",
  "request_id": 472,
  "requested_at": "2026-08-21T09:44:22.852908Z",
  "finished_at": "2026-08-22T06:55:02.420521Z",
  "last_error": "completed 6 missing recent evidence window(s)",
  "progress": {
    "errors": {},
    "outcomes": {
      "completed": 6,
      "already_complete": 0
    },
    "candidates_seen": 6
  }
},
  strategies: [],
} as unknown as StrategyOverviewResponse;

function renderLens() {
  return render(
    <MemoryRouter>
      <StrategyPortfolioLens />
    </MemoryRouter>,
  );
}

describe("StrategyPortfolioLens", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(BLOCKED);
    vi.spyOn(strategiesApi, "fetchStrategyOwnedPositions").mockResolvedValue({
      positions: [],
      live_quote_instrument_ids: [],
    } as never);
  });

  it("leads with the verdict and the ordered reasons, not the money", async () => {
    renderLens();
    expect(await screen.findByText("Not trading")).toBeInTheDocument();
    const reasons = await screen.findAllByRole("listitem");
    // Kill switch is the outermost gate and must be reason 1 — funding the pot
    // underneath an active kill switch changes nothing.
    expect(reasons[0]).toHaveTextContent("Kill switch is on");
    expect(reasons[0]).toHaveTextContent("autonomy loop unattended");
    expect(reasons.map((r) => r.textContent)).toHaveLength(5);
  });

  it("shows the mandate as unset rather than as blank limits", async () => {
    renderLens();
    expect(await screen.findByText(/No risk mandate set/)).toBeInTheDocument();
  });

  it("renders a real empty state instead of a zeroed positions table", async () => {
    renderLens();
    expect(await screen.findByText("Nothing held")).toBeInTheDocument();
  });

  it("reports the pot as halted", async () => {
    renderLens();
    expect(await screen.findByText("halted")).toBeInTheDocument();
  });
});

describe("StrategiesHubPage", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(BLOCKED);
    vi.spyOn(strategiesApi, "fetchStrategyOwnedPositions").mockResolvedValue({
      positions: [],
      live_quote_instrument_ids: [],
    } as never);
    vi.spyOn(strategiesApi, "fetchStrategyPnlHistory").mockResolvedValue({ points: [] } as never);
    vi.spyOn(strategiesApi, "fetchFiredSignals").mockResolvedValue({ items: [], next_cursor: null } as never);
  });

  it("lands on the portfolio lens", async () => {
    render(
      <MemoryRouter initialEntries={["/strategies"]}>
        <StrategiesHubPage />
      </MemoryRouter>,
    );
    expect(await screen.findByText("Not trading")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Portfolio" })).toHaveAttribute("aria-selected", "true");
  });

  it("switches to the research lens on demand", async () => {
    render(
      <MemoryRouter initialEntries={["/strategies"]}>
        <StrategiesHubPage />
      </MemoryRouter>,
    );
    await screen.findByText("Not trading");
    await userEvent.click(screen.getByRole("tab", { name: "Research" }));
    await waitFor(() => {
      expect(screen.getByRole("tab", { name: "Research" })).toHaveAttribute("aria-selected", "true");
    });
    expect(screen.queryByText("Not trading")).not.toBeInTheDocument();
  });

  it("honours a deep link straight to the research lens", async () => {
    render(
      <MemoryRouter initialEntries={["/strategies?view=research"]}>
        <StrategiesHubPage />
      </MemoryRouter>,
    );
    expect(await screen.findByRole("tab", { name: "Research" })).toHaveAttribute("aria-selected", "true");
  });
});
