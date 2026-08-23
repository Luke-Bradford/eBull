import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, useLocation } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as configApi from "@/api/config";
import * as strategiesApi from "@/api/strategies";
import type { StrategyOverviewResponse } from "@/api/types";
import { BENCHMARK_REFUSALS } from "@/components/strategies/__fixtures__/benchmarkRefusals";
import { StrategiesHubPage } from "@/pages/StrategiesHubPage";
import { StrategyPortfolioLens } from "@/pages/StrategyPortfolioLens";

/** The state the operator actually has today: kill switch on, nothing funded. */
const BLOCKED = {
  as_of: "2026-08-23T00:00:00Z",
  benchmark_refusals: BENCHMARK_REFUSALS,
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
    vi.spyOn(strategiesApi, "fetchStrategyPnlHistory").mockResolvedValue({ points: [] } as never);
  });

  it("states what is blocking as facts, and offers the control for the one that has one", async () => {
    renderLens();
    const blocking = await screen.findByLabelText("Blocking conditions");
    // The kill switch is the only blocker the operator can act on from here, so
    // it is the only one carrying a control. The rest are one line of fact —
    // the narrated ordered lesson this replaced is what the operator objected to.
    expect(blocking).toHaveTextContent("Kill switch on — autonomy loop unattended");
    expect(within(blocking).getAllByRole("button")).toHaveLength(1);
    expect(within(blocking).getByRole("button", { name: "Clear" })).toBeInTheDocument();
    // Earned, not configured — so it belongs here, with its count.
    expect(blocking).toHaveTextContent("No strategy has passed the evidence bar — 0 of 0");
    // Capital, mandate and the on/off switch are fields in the Setup form
    // below; repeating them here would narrate a control already on screen.
    expect(blocking).not.toHaveTextContent("No capital is assigned");
    expect(blocking).not.toHaveTextContent("No risk mandate is set");
    expect(blocking).not.toHaveTextContent("Automatic trading is switched off");
  });

  it("clears the kill switch and re-reads the state", async () => {
    const post = vi.spyOn(configApi, "postKillSwitch").mockResolvedValue({
      active: false,
      activated_at: null,
      activated_by: null,
      reason: null,
    });
    renderLens();
    await userEvent.click(await screen.findByRole("button", { name: "Clear" }));
    await waitFor(() => expect(post).toHaveBeenCalledTimes(1));
    expect(post.mock.calls[0]?.[0]).toMatchObject({ active: false });
    // A reason is mandatory on this endpoint; sending a blank one is a 422.
    expect(post.mock.calls[0]?.[0].reason).not.toHaveLength(0);
    await waitFor(() => expect(strategiesApi.fetchStrategyOverview).toHaveBeenCalledTimes(2));
  });

  it("summarises the pot in four numbers", async () => {
    renderLens();
    // Scoped to the state section: the setup form below carries its own
    // "Available"/"Reserved" breakdown, so an unscoped query is ambiguous.
    const state = (await screen.findByText("Not trading")).closest("section")!;
    for (const label of ["Pot", "P&L", "Open", "Available"]) {
      expect(within(state).getByText(label)).toBeInTheDocument();
    }
  });

  it("renders a real empty state instead of a zeroed positions table", async () => {
    renderLens();
    expect(await screen.findByText("Nothing held")).toBeInTheDocument();
    // Nothing to close, so the destructive control is absent rather than disabled.
    expect(screen.queryByRole("button", { name: /Close all/ })).not.toBeInTheDocument();
  });

  it("names why there is no benchmark, by code and by evidence", async () => {
    renderLens();
    expect(await screen.findByText("No benchmark comparison")).toBeInTheDocument();
    expect(screen.getByText("S&P 500 total return")).toBeInTheDocument();
    expect(screen.getByText("CPIH real return")).toBeInTheDocument();
    expect(screen.getByText("benchmark_source_unlicensed")).toBeInTheDocument();
    expect(screen.getByText("benchmark_identity_unverified")).toBeInTheDocument();
    expect(screen.getByText("benchmark_series_not_ingested")).toBeInTheDocument();
    // The evidence is rendered, not hidden behind a hover: `title` is unreachable
    // on touch and unreliable for assistive tech, and the evidence is the point.
    expect(screen.getByText(/No CPI\/CPIH series is ingested/)).toBeInTheDocument();
  });

  it("still names the benchmark refusal when the P&L history request fails", async () => {
    // #2602 item 5, Codex ckpt-1. The refusal is fed from the overview, not from
    // the history response that also carries it — an absent benchmark must stay
    // explained in exactly the branch where the operator is most likely to fill
    // the gap with an assumption.
    vi.mocked(strategiesApi.fetchStrategyPnlHistory).mockRejectedValue(new Error("boom"));
    renderLens();
    expect(await screen.findByText("No benchmark comparison")).toBeInTheDocument();
    expect(screen.getByText("benchmark_series_not_ingested")).toBeInTheDocument();
  });

  it("reports the pot as halted", async () => {
    renderLens();
    const state = (await screen.findByText("Not trading")).closest("section")!;
    expect(within(state).getByText("halted")).toBeInTheDocument();
  });

  it("never reports zero open positions while the positions request is failing", async () => {
    // The tile reads the overview's count, which is already loaded, rather than
    // the length of a list that is empty only because its request has not
    // landed — a 0 next to an error message is a false statement (Codex ckpt-2).
    vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockRejectedValue(new Error("boom"));
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...BLOCKED,
      strategies: [
        { pnl: { total_pnl: "0", active_position_count: 2 }, attribution: {}, allocation: {}, purpose: "capital_candidate" },
      ],
    } as unknown as StrategyOverviewResponse);

    renderLens();
    const tile = (await screen.findByText("Open")).parentElement!;
    expect(tile.textContent).toContain("2");
  });

  describe("with open positions", () => {
    const POSITION = {
      strategy_trade_id: 1,
      broker_position_id: "p-1",
      instrument_id: 7,
      symbol: "AAPL",
      currency: "USD",
      units: "1",
      assigned_value: "100",
      current_price: "110",
    } as never;

    beforeEach(() => {
      vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockResolvedValue({
        positions: [POSITION],
        live_quote_instrument_ids: [7],
      } as never);
    });

    it("refreshes every read it owns after a close, the P&L chart included", async () => {
      // The chart moved onto this lens with the split, so a close that refreshed
      // only positions left it painting a stale valuation.
      vi.spyOn(strategiesApi, "closeStrategyOwnedPosition").mockResolvedValue({} as never);
      renderLens();
      await userEvent.click(await screen.findByRole("button", { name: /^close$/i }));
      await userEvent.click(await screen.findByRole("button", { name: /^Close position$/i }));
      await waitFor(() => expect(strategiesApi.fetchStrategyPnlHistory).toHaveBeenCalledTimes(2));
      expect(strategiesApi.fetchStrategyOwnedPositions).toHaveBeenCalledTimes(2);
    });

    it("requires confirmation before closing everything, and submits one at a time", async () => {
      const close = vi.spyOn(strategiesApi, "closeStrategyOwnedPosition").mockResolvedValue({} as never);
      renderLens();
      await userEvent.click(await screen.findByRole("button", { name: /Close all 1 position/ }));
      // Nothing is submitted on opening the dialog — the confirm is the trigger.
      expect(close).not.toHaveBeenCalled();
      await userEvent.click(screen.getByRole("button", { name: "Close 1" }));
      await waitFor(() => expect(close).toHaveBeenCalledTimes(1));
    });

    it("leaves an already-closing trade out of the bulk action", async () => {
      // Its own row disables Close; resubmitting it would be rejected and, since
      // the loop stops on first failure, would strand the open ones behind it.
      vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockResolvedValue({
        positions: [
          { ...(POSITION as object), strategy_trade_id: 2, broker_position_id: "p-2", trade_status: "closing" },
          POSITION,
        ],
        live_quote_instrument_ids: [7],
      } as never);
      const close = vi.spyOn(strategiesApi, "closeStrategyOwnedPosition").mockResolvedValue({} as never);

      renderLens();
      // One closable of two held.
      await userEvent.click(await screen.findByRole("button", { name: /Close all 1 position/ }));
      await userEvent.click(screen.getByRole("button", { name: "Close 1" }));
      await waitFor(() => expect(close).toHaveBeenCalledTimes(1));
      expect(close).toHaveBeenCalledWith(1, "p-1");
    });

    it("reports how many closed when a bulk close fails part-way", async () => {
      vi.spyOn(strategiesApi, "closeStrategyOwnedPosition").mockRejectedValue(new Error("broker said no"));
      renderLens();
      await userEvent.click(await screen.findByRole("button", { name: /Close all 1 position/ }));
      await userEvent.click(screen.getByRole("button", { name: "Close 1" }));
      // The count matters: a bulk action that half-succeeded must not read as a
      // clean failure, or the operator re-runs it against already-closed trades.
      expect(await screen.findByRole("alert")).toHaveTextContent("0 of 1 closed");
    });
  });
});

/** `MemoryRouter` exposes no history object, so read the URL from inside it. */
function LocationProbe(): JSX.Element {
  const { search } = useLocation();
  return <output data-testid="location-search">{search}</output>;
}

function currentParams(): URLSearchParams {
  return new URLSearchParams(screen.getByTestId("location-search").textContent ?? "");
}

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

  it("preserves query params it does not own when switching lens", async () => {
    render(
      <MemoryRouter initialEntries={["/strategies?symbol=AAPL"]}>
        <StrategiesHubPage />
        <LocationProbe />
      </MemoryRouter>,
    );
    await screen.findByText("Not trading");
    await userEvent.click(screen.getByRole("tab", { name: "Research" }));
    await waitFor(() => {
      expect(currentParams().get("view")).toBe("research");
    });
    // Neither lens owns URL state today; this pins the rule before one does.
    expect(currentParams().get("symbol")).toBe("AAPL");
  });
});
