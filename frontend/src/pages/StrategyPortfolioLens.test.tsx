import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, useLocation } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as configApi from "@/api/config";
import * as strategiesApi from "@/api/strategies";
import type { CoreSleeveResponse, StrategyOverviewResponse } from "@/api/types";
import { BENCHMARK_REFUSALS } from "@/components/strategies/__fixtures__/benchmarkRefusals";
import { StrategiesHubPage } from "@/pages/StrategiesHubPage";
import { StrategyPortfolioLens } from "@/pages/StrategyPortfolioLens";
import { StrategySetupLens } from "@/pages/StrategySetupLens";

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
    capital_observation_complete: true,
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
  "local_eod_value_at_official_marks": null,
  "local_eod_positions_priced": null,
  "local_eod_stale_mark_positions": null,
  "difference": null,
  "tolerance": null,
  "comparable": false,
  "incomplete_reasons": [
    "same_day_local_eod_snapshot_missing"
  ],
  "countdown_green_days": 0,
  "countdown_required_days": 5,
  "countdown_newest_counted_date": null,
  "countdown_stop_reason": "day_never_recorded",
  "countdown_rule_version": "f0-countdown-v1"
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

const CORE_COLLECTING = {
  state: "evidence_collecting",
  declared_outcome: null,
  selected_instrument_id: null,
  selected_symbol: null,
  evidence_ref: null,
  required_trading_days: 5,
  observed_trading_days: 1,
  earliest_possible_verdict_at: "2026-09-02T00:00:00Z",
  max_cost_bps: 60,
  candidates: [
    {
      instrument_id: 3417,
      symbol: "SPY.RTH",
      observed_trading_days: 1,
      first_observed_date: "2026-08-25",
      last_observed_date: "2026-08-25",
    },
    {
      instrument_id: 3434,
      symbol: "CSPX.L",
      observed_trading_days: 0,
      first_observed_date: null,
      last_observed_date: null,
    },
    {
      instrument_id: 3075,
      symbol: "IUSA.L",
      observed_trading_days: 0,
      first_observed_date: null,
      last_observed_date: null,
    },
  ],
  mandate: { configured: false },
  can_configure: false,
  can_enable_pool: false,
  can_rebalance: false,
  can_resume: false,
  pending_order_id: null,
  execution_action: "blocked",
  blockers: [
    {
      code: "core_evidence_collecting",
      detail: "#2833 has observations on 1 of 5 required common dates. The sealed verifier checks the complete populations after the fifth date closes; cash remains the fallback.",
    },
    { code: "core_mandate_unconfigured", detail: "No core/cash mandate has been configured." },
  ],
  environment: "demo",
  buy_only: true,
  alpha_input_used: false,
  household_tax_caveat:
    "No supported public-API route into eToro's Stocks & Shares ISA is established. Compare an ISA elsewhere using personal tax, FX and dealing costs, and expected turnover; #2915's £50,000 sensitivity was mixed, not a universal ISA advantage.",
  household_currency_caveat:
    "Sterling is not the unit here. Where the core sleeve holds a USD-quoted instrument, a household measuring it in GBP carries GBP/USD exposure on the whole position value and not only on its return, and this engine does not hedge it. A £ sign in the positions table is the broker's own label carried through unchanged, not a conversion this engine performed. Converting when you fund or withdraw is a household cost that the preregistered ceiling, which prices round-trip spread, does not include.",
} as const;

const CORE_READY = {
  ...CORE_COLLECTING,
  state: "ready",
  declared_outcome: "pass",
  selected_instrument_id: 3417,
  selected_symbol: "SPY.RTH",
  evidence_ref: "#2833 verdict",
  observed_trading_days: 5,
  mandate: {
    configured: true,
    enabled: true,
    core_instrument_id: 3417,
    core_target_pct: "80",
    liquidity_reserve_pct: "10",
    rebalance_band_pct: "5",
    min_rebalance_amount: "25",
  },
  can_configure: true,
  can_enable_pool: true,
  can_rebalance: true,
  can_resume: false,
  execution_action: "rebalance",
  blockers: [],
} as const;

function renderLens() {
  return render(
    <MemoryRouter>
      <StrategyPortfolioLens />
    </MemoryRouter>,
  );
}

/** #3334: funding, the core sleeve and its mandate moved to the Setup lens. */
function renderSetup() {
  return render(
    <MemoryRouter>
      <StrategySetupLens />
    </MemoryRouter>,
  );
}

describe("StrategyPortfolioLens", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.spyOn(strategiesApi, "fetchStrategyOverview").mockResolvedValue(BLOCKED);
    vi.spyOn(strategiesApi, "fetchCoreSleeve").mockResolvedValue(CORE_COLLECTING as never);
    vi.spyOn(strategiesApi, "fetchStrategyOwnedPositions").mockResolvedValue({
      positions: [],
      live_quote_instrument_ids: [],
    } as never);
    vi.spyOn(strategiesApi, "fetchStrategyPnlHistory").mockResolvedValue({ points: [] } as never);
  });

  it("shows cash as the evidence-gated fallback instead of an approved strategy", async () => {
    renderSetup();
    expect(await screen.findByRole("heading", { name: "Core & cash" })).toBeInTheDocument();
    expect(screen.getByText("Common dates seen")).toBeInTheDocument();
    expect(screen.getByText("Provisional until the sealed verifier opens")).toBeInTheDocument();
    expect(screen.getAllByText("1 / 5")).toHaveLength(2);
    expect(screen.getByText("Decided when the verdict opens")).toBeInTheDocument();
    expect(screen.getByText("02 Sept 2026")).toBeInTheDocument();
    expect(screen.getByText(/Lower bound if all five common sessions complete/i)).toBeInTheDocument();
    expect(screen.getByText(/cash remains the fallback/i)).toBeInTheDocument();
    expect(screen.getByText(/No supported public-API route into eToro's Stocks & Shares ISA/i)).toBeInTheDocument();
    expect(screen.getByText(/£50,000 sensitivity was mixed, not a universal ISA advantage/i)).toBeInTheDocument();
    // #2833 caveat (b). Asserted on the RENDER and not only in the fixture --
    // deleting the JSX would otherwise still pass. No instrument is selected in
    // this state, so the conditional opening is the part that has to survive.
    expect(screen.getByText(/Where the core sleeve holds a USD-quoted instrument/i)).toBeInTheDocument();
    expect(screen.getByText(/this engine does not hedge it/i)).toBeInTheDocument();
    expect(screen.getByText(/Converting when you fund or withdraw/i)).toBeInTheDocument();
    expect(screen.getByLabelText("Minimum cash reserve %")).toHaveValue(10);
    expect(screen.getByLabelText("Rebalance band (pp)")).toHaveValue(5);
    expect(screen.getByLabelText("Minimum amount (USD)")).toHaveValue(25);
    const coverage = screen.getByRole("table", { name: "Core candidate evidence coverage" });
    expect(within(coverage).getByRole("columnheader", { name: "Dates seen" })).toBeInTheDocument();
    expect(within(coverage).getByText("SPY.RTH")).toBeInTheDocument();
    expect(within(coverage).getByText("25 Aug 2026 – 25 Aug 2026")).toBeInTheDocument();
    expect(within(coverage).getAllByText("Awaiting first date")).toHaveLength(2);
  });

  // #3037: the badge used to be `state === "ready" ? "Ready" : "Cash"`, so every
  // non-ready state asserted `Cash` — one of the two TERMINAL answers #2833's sealed
  // verifier emits — while the study was still sealed at 1 of 5 common dates.
  it.each([
    ["evidence_collecting", "Collecting evidence", null],
    ["awaiting_verdict", "Verdict due", null],
    ["cash", "Cash", "cash"],
    ["unavailable", "Unavailable", null],
    ["ready", "Ready", "pass"],
  ])("badges the %s state as %s rather than deriving it from readiness", async (state, label, outcome) => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue({
      ...(state === "ready" ? CORE_READY : CORE_COLLECTING),
      state,
      declared_outcome: outcome,
    } as never);
    renderSetup();
    const heading = await screen.findByRole("heading", { name: "Core & cash" });
    // Scoped to the card header: in the `cash` state the Instrument tile ALSO reads
    // "Cash", which is correct — the badge is the assertion under test.
    const header = heading.parentElement?.parentElement as HTMLElement;
    expect(within(header).getByText(label)).toBeInTheDocument();
  });

  // Codex checkpoint 2: `unavailable` is reachable with the window still OPEN (a missing
  // candidate row, or inconsistent constants), so window copy keyed on `state` rendered
  // "Window closed" over a bound in the FUTURE.
  it("keeps provisional window copy when an unavailable state has not closed the window", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue({
      ...CORE_COLLECTING,
      state: "unavailable",
      observed_trading_days: 1,
    } as never);
    renderSetup();
    expect(await screen.findByRole("heading", { name: "Core & cash" })).toBeInTheDocument();
    expect(screen.getByText("Provisional until the sealed verifier opens")).toBeInTheDocument();
    expect(screen.queryByText("Window closed")).not.toBeInTheDocument();
  });

  it("says the window closed once the declared dates are complete", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue({
      ...CORE_COLLECTING,
      state: "awaiting_verdict",
      observed_trading_days: 5,
    } as never);
    renderSetup();
    expect(await screen.findByRole("heading", { name: "Core & cash" })).toBeInTheDocument();
    expect(screen.getByText("Window closed")).toBeInTheDocument();
    expect(screen.getByText("The declared window is complete")).toBeInTheDocument();
    expect(screen.queryByText("Provisional until the sealed verifier opens")).not.toBeInTheDocument();
  });

  it("never says Cash while the verdict is still sealed", async () => {
    renderSetup();
    expect(await screen.findByRole("heading", { name: "Core & cash" })).toBeInTheDocument();
    // ⚠ Scoped to the card's verdict surfaces, NOT the page: "Cash reserve" is a
    // legitimate mandate label elsewhere, and a page-wide assertion would fail for the
    // wrong reason and get deleted rather than fixed.
    expect(screen.queryByText("Cash")).not.toBeInTheDocument();
    expect(screen.getByText("Collecting evidence")).toBeInTheDocument();
  });

  it("refreshes the evidence status without hiding the current coverage", async () => {
    const refreshed = {
      ...CORE_COLLECTING,
      observed_trading_days: 2,
      candidates: CORE_COLLECTING.candidates.map((candidate) => ({
        ...candidate,
        observed_trading_days: 2,
        first_observed_date: "2026-08-25",
        last_observed_date: "2026-08-26",
      })),
      blockers: [{
        code: "core_evidence_collecting",
        detail: "#2833 has observations on 2 of 5 required common dates. The sealed verifier checks the complete populations after the fifth date closes; cash remains the fallback.",
      }],
    } as unknown as CoreSleeveResponse;
    let finishRefresh!: (value: CoreSleeveResponse) => void;
    vi.mocked(strategiesApi.fetchCoreSleeve)
      .mockResolvedValueOnce(CORE_COLLECTING as never)
      .mockImplementationOnce(() => new Promise((resolve) => {
        finishRefresh = resolve;
      }));
    renderSetup();

    await waitFor(() => expect(screen.getAllByText("1 / 5")).toHaveLength(2));
    await userEvent.click(screen.getByRole("button", { name: "Refresh status" }));

    expect(screen.getByRole("button", { name: "Refreshing…" })).toBeDisabled();
    expect(screen.getByRole("table", { name: "Core candidate evidence coverage" })).toBeInTheDocument();
    expect(screen.getAllByText("1 / 5")).toHaveLength(2);
    await act(async () => finishRefresh(refreshed));
    await waitFor(() => expect(strategiesApi.fetchCoreSleeve).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(screen.getAllByText("2 / 5")).toHaveLength(4));
    const coverage = screen.getByRole("table", { name: "Core candidate evidence coverage" });
    expect(within(coverage).getAllByText("25 Aug 2026 – 26 Aug 2026")).toHaveLength(3);
  });

  it("marks preserved status stale and withholds ready actions while refreshing", async () => {
    let finishRefresh!: (value: CoreSleeveResponse) => void;
    vi.mocked(strategiesApi.fetchCoreSleeve)
      .mockResolvedValueOnce(CORE_READY as never)
      .mockImplementationOnce(() => new Promise((resolve) => {
        finishRefresh = resolve;
      }));
    renderSetup();

    const rebalance = await screen.findByRole("button", { name: "Rebalance demo now" });
    await userEvent.selectOptions(screen.getByLabelText("Risk profile"), "balanced");
    const entries = screen.getByRole("checkbox", { name: "Allow new automated entries" });
    expect(rebalance).not.toBeDisabled();
    expect(entries).not.toBeDisabled();
    await userEvent.click(screen.getByRole("button", { name: "Refresh status" }));

    expect(screen.getByText("Status is stale — refreshing")).toBeInTheDocument();
    expect(rebalance).toBeDisabled();
    expect(entries).toBeDisabled();
    await act(async () => finishRefresh(CORE_READY as unknown as CoreSleeveResponse));
    await waitFor(() => expect(rebalance).not.toBeDisabled());
    expect(entries).not.toBeDisabled();
  });

  it("renders an actionable empty state when candidate coverage is unavailable", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue({
      ...CORE_COLLECTING,
      state: "unavailable",
      candidates: [],
      blockers: [{
        code: "core_candidates_missing",
        detail: "#2833 cannot collect evidence because candidate instrument ids are missing: 3417.",
      }],
    } as never);
    renderSetup();

    expect(await screen.findByText(/candidate instrument ids are missing: 3417/i)).toBeInTheDocument();
    expect(screen.getByText(/No candidate coverage is available; the core sleeve's blockers name what must be restored/i)).toBeInTheDocument();
  });

  it("lets the operator save a disabled draft while evidence is still collecting", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve)
      .mockResolvedValueOnce(CORE_COLLECTING as never)
      .mockResolvedValue({
        ...CORE_COLLECTING,
        mandate: {
          configured: true,
          enabled: false,
          core_instrument_id: null,
          core_target_pct: "80",
          liquidity_reserve_pct: "10",
          rebalance_band_pct: "5",
          min_rebalance_amount: "25",
        },
        blockers: [
          CORE_COLLECTING.blockers[0],
          { code: "core_mandate_disabled", detail: "The current core/cash mandate is disabled." },
        ],
      } as never);
    const save = vi.spyOn(strategiesApi, "updateCoreMandate").mockResolvedValue({
      configured: true,
      enabled: false,
      core_instrument_id: null,
      core_target_pct: "80",
      liquidity_reserve_pct: "10",
      rebalance_band_pct: "5",
      min_rebalance_amount: "25",
    } as never);
    renderSetup();

    expect(await screen.findByText(/Save these values as a disabled draft now/i)).toBeInTheDocument();
    expect(screen.getByLabelText("Enable demo core sleeve")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Rebalance demo now" })).toBeDisabled();

    await userEvent.type(screen.getByLabelText("Audit reason"), "Prepare the core mandate before selection");
    await userEvent.click(screen.getByRole("button", { name: "Save mandate" }));

    await waitFor(() => expect(save).toHaveBeenCalledTimes(1));
    expect(save.mock.calls[0]?.[0]).toMatchObject({
      enabled: false,
      core_instrument_id: null,
      core_target_pct: "80",
      reason: "Prepare the core mandate before selection",
    });
    await waitFor(() => expect(screen.getByRole("button", { name: "Save mandate" })).toBeDisabled());
    await userEvent.click(screen.getByRole("button", { name: "Save mandate" }));
    expect(save).toHaveBeenCalledTimes(1);
  });

  it("saves a materially changed ready mandate with an explicit audit reason", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue(CORE_READY as never);
    const save = vi.spyOn(strategiesApi, "updateCoreMandate").mockResolvedValue(CORE_READY.mandate as never);
    renderSetup();

    const target = await screen.findByLabelText("Core target %");
    await userEvent.clear(target);
    await userEvent.type(target, "75");
    await userEvent.type(await screen.findByLabelText("Audit reason"), "Adopt reviewed #2833 sleeve");
    await userEvent.click(screen.getByRole("button", { name: "Save mandate" }));

    await waitFor(() => expect(save).toHaveBeenCalledTimes(1));
    expect(save.mock.calls[0]?.[0]).toMatchObject({
      enabled: true,
      core_instrument_id: 3417,
      core_target_pct: "75",
      reason: "Adopt reviewed #2833 sleeve",
      environment: "demo",
    });
  });

  it("can fund the evidence-ready core lane when no alpha strategy passed", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue(CORE_READY as never);
    const update = vi.spyOn(strategiesApi, "updateStrategyPaperPool").mockResolvedValue({
      ...BLOCKED.paper_pool,
      configured: true,
      enabled: true,
      capital_limit: "1000.000000",
    } as never);
    renderSetup();

    const entries = await screen.findByRole("checkbox", { name: "Allow new automated entries" });
    await userEvent.selectOptions(screen.getByLabelText("Risk profile"), "balanced");
    expect(entries).not.toBeDisabled();
    await userEvent.click(entries);
    await userEvent.clear(screen.getByLabelText("Trading capital (USD)"));
    await userEvent.type(screen.getByLabelText("Trading capital (USD)"), "1000");
    await userEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(update).toHaveBeenCalledWith(expect.objectContaining({
      enabled: true,
      capital_limit: "1000.000000",
      risk_profile: "balanced",
    })));
  });

  it("does not offer a reason-only save for an unchanged configured mandate", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue(CORE_READY as never);
    renderSetup();

    await userEvent.type(await screen.findByLabelText("Audit reason"), "No material change");

    expect(screen.getByRole("button", { name: "Save mandate" })).toBeDisabled();
  });

  it("offers a policy-only mandate upgrade without inventing a value change", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue({
      ...CORE_READY,
      mandate: { ...CORE_READY.mandate, policy_version: "core-mandate-v1" },
      can_enable_pool: false,
      can_rebalance: false,
      execution_action: "blocked",
      blockers: [{
        code: "core_mandate_policy_unsupported",
        detail: "The current core/cash mandate uses a superseded policy; save a reviewed revision.",
      }],
    } as never);
    const save = vi.spyOn(strategiesApi, "updateCoreMandate").mockResolvedValue(CORE_READY.mandate as never);
    renderSetup();

    await userEvent.type(await screen.findByLabelText("Audit reason"), "Advance reviewed policy stamp");
    const button = screen.getByRole("button", { name: "Save mandate" });
    expect(button).not.toBeDisabled();
    await userEvent.click(button);

    await waitFor(() => expect(save).toHaveBeenCalledWith(expect.objectContaining({
      core_target_pct: "80",
      reason: "Advance reviewed policy stamp",
    })));
  });

  it("cannot confirm a rebalance against unsaved mandate edits", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue(CORE_READY as never);
    renderSetup();

    const target = await screen.findByLabelText("Core target %");
    await userEvent.clear(target);
    await userEvent.type(target, "70");

    expect(screen.getByRole("button", { name: "Rebalance demo now" })).toBeDisabled();
  });

  it("confirms a demo rebalance and labels acceptance as pending reconciliation", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue(CORE_READY as never);
    const rebalance = vi.spyOn(strategiesApi, "rebalanceCoreSleeve").mockResolvedValue({
      state: "submitted",
      reason_code: "broker_accepted_pending_reconciliation",
      intent_id: 11,
      trade_id: 21,
      order_id: 31,
      amount: "49.9",
      submission_policy_version: "core-submission-v2",
      preflight_policy_version: "core-preflight-v2",
      broker_preflight_policy_version: "core-broker-preflight-v2",
    });
    renderSetup();

    await userEvent.click(await screen.findByRole("button", { name: "Rebalance demo now" }));
    expect(rebalance).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole("button", { name: "Confirm demo rebalance" }));

    expect(await screen.findByRole("status")).toHaveTextContent("Broker accepted; fill reconciliation is pending");
    expect(rebalance).toHaveBeenCalledTimes(1);
  });

  it("labels an unresolved authority as resume and cannot present it as a new rebalance", async () => {
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue({
      ...CORE_READY,
      can_rebalance: false,
      can_resume: true,
      pending_order_id: 31,
      execution_action: "resume",
      blockers: [{ code: "core_order_unresolved", detail: "Order 31 is unresolved." }],
    } as never);
    const resume = vi.spyOn(strategiesApi, "rebalanceCoreSleeve").mockResolvedValue({
      state: "held",
      reason_code: "core_order_reconciled",
      intent_id: 11,
      trade_id: 21,
      order_id: 31,
      amount: "49.9",
      submission_policy_version: "core-submission-v2",
      preflight_policy_version: "core-preflight-v2",
      broker_preflight_policy_version: "core-broker-preflight-v2",
    });
    renderSetup();

    expect(await screen.findByText("Order 31 is unresolved.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Rebalance demo now" })).not.toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Settle demo order" }));
    expect(screen.getByRole("heading", { name: "Settle demo order 31?" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Check with broker" }));
    expect(resume).toHaveBeenCalledTimes(1);
    expect(await screen.findByRole("status")).toHaveTextContent("existing broker order was reconciled");
  });

  it("does not claim the band was checked when something else resolved the order first", async () => {
    // #2962. `core_resume_already_resolved` is `held`, and before this it fell
    // through to the band sentence — a claim about an evaluation that never ran.
    // The scheduled cycle now reconciles core orders every five minutes, so the
    // operator loading this page and clicking resume can lose that race in
    // ordinary use rather than only against a second operator.
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue({
      ...CORE_READY,
      can_rebalance: false,
      can_resume: true,
      pending_order_id: 31,
      execution_action: "resume",
      blockers: [{ code: "core_order_unresolved", detail: "Order 31 is unresolved." }],
    } as never);
    vi.spyOn(strategiesApi, "rebalanceCoreSleeve").mockResolvedValue({
      state: "held",
      reason_code: "core_resume_already_resolved",
      intent_id: 11,
      trade_id: 21,
      order_id: 31,
      amount: "49.9",
      submission_policy_version: "core-submission-v2",
      preflight_policy_version: "core-preflight-v2",
      broker_preflight_policy_version: "core-broker-preflight-v2",
    });
    renderSetup();

    await userEvent.click(await screen.findByRole("button", { name: "Settle demo order" }));
    await userEvent.click(screen.getByRole("button", { name: "Check with broker" }));

    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("already reconciled by the scheduled cycle");
    expect(status).not.toHaveTextContent("remains inside its band");
  });

  it("does not claim the sleeve is inside its band when the floor is what stopped the trade", async () => {
    // #3123. `below_min_rebalance_amount` is `held` and means the OPPOSITE of the band
    // sentence: the sleeve is OUTSIDE the band and the gap is under the mandate's
    // minimum, so the floor wins and the breach is reported rather than traded through.
    // Reachable because #3123 re-enabled the affordance while the sleeve holds a
    // position, which is the state a small drift lives in.
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue(CORE_READY as never);
    vi.spyOn(strategiesApi, "rebalanceCoreSleeve").mockResolvedValue({
      state: "held",
      reason_code: "below_min_rebalance_amount",
      intent_id: 11,
      trade_id: null,
      order_id: null,
      amount: "0",
      submission_policy_version: "core-submission-v2",
      preflight_policy_version: "core-preflight-v2",
      broker_preflight_policy_version: "core-broker-preflight-v2",
    });
    renderSetup();

    await userEvent.click(await screen.findByRole("button", { name: "Rebalance demo now" }));
    await userEvent.click(screen.getByRole("button", { name: "Confirm demo rebalance" }));

    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("below the mandate's minimum rebalance amount");
    expect(status).not.toHaveTextContent("remains inside its band");
  });

  it("does not blame the operator's mandate when it was the BROKER minimum that bound", async () => {
    // Codex checkpoint 2 on #3123. `assess_core_broker_preflight` emits the SAME
    // `below_min_rebalance_amount` code as a `refused`, where the broker's floor is what
    // bound. Lowering `min_rebalance_amount` cannot resolve that, so the mandate wording
    // must not be reachable from a refusal. The response carries no `floor_source`, so
    // the honest fallback is the generic refusal line.
    vi.mocked(strategiesApi.fetchCoreSleeve).mockResolvedValue(CORE_READY as never);
    vi.spyOn(strategiesApi, "rebalanceCoreSleeve").mockResolvedValue({
      state: "refused",
      reason_code: "below_min_rebalance_amount",
      intent_id: 11,
      trade_id: null,
      order_id: null,
      amount: "0",
      submission_policy_version: "core-submission-v2",
      preflight_policy_version: "core-preflight-v2",
      broker_preflight_policy_version: "core-broker-preflight-v2",
    });
    renderSetup();

    await userEvent.click(await screen.findByRole("button", { name: "Rebalance demo now" }));
    await userEvent.click(screen.getByRole("button", { name: "Confirm demo rebalance" }));

    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("Rebalance refused: below_min_rebalance_amount");
    expect(status).not.toHaveTextContent("mandate's minimum");
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

  it("leads with the pot's summary strip and no configuration form", async () => {
    renderLens();
    const state = (await screen.findByText("Not trading")).closest("section")!;
    // #3334: results first. Every tile is pot-level; the strategy-only roll-up
    // ("Strategy total P&L", #3222 residual 2) moved down into performance.
    for (const label of ["Pot value", "P&L since start", "Last day", "vs S&P 500", "Cash available"]) {
      expect(within(state).getByText(label)).toBeInTheDocument();
    }
    // Monitor ≠ configure: no funding or mandate write lives on this lens.
    expect(screen.queryByLabelText("Trading capital (USD)")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Save mandate" })).not.toBeInTheDocument();
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
    // ⚠⚠ #3222 REVERSED this test's MECHANISM and kept its claim. It used to
    // assert the tile falls back to the overview's `active_position_count` sum
    // (here: "2"), on the Codex ckpt-2 argument that a 0 beside an error message
    // is a false statement. That argument still holds and is what this test is
    // for. The fallback does not: that sum is per REGISTERED strategy, and the
    // core sleeve's position carries `strategy_id: null`, so on dev 2026-09-19 it
    // read 0 while the `Close all` button beside it read 1. Swapping one false
    // number for another is not a fix.
    //
    // The tile now refuses instead — `—`, not a count — which satisfies the
    // claim in the test's own name without asserting a figure it cannot know.
    vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockRejectedValue(new Error("boom"));
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...BLOCKED,
      strategies: [
        { pnl: { total_pnl: "0", active_position_count: 2 }, attribution: {}, allocation: {}, purpose: "capital_candidate" },
      ],
    } as unknown as StrategyOverviewResponse);

    renderLens();
    // ⚠ The VALUE div, not the tile: the hint reads "0 strategies approved", so
    // a `toContain("0")` over the whole tile can never fail and would be a test
    // that passes by construction.
    const holdings = await screen.findByRole("heading", { name: /Holdings/ });
    await waitFor(() => expect(holdings.textContent).toContain("· — open"));
  });

  it("counts the core sleeve's position, which belongs to no registered strategy", async () => {
    // #3222 — the defect measured on dev 2026-09-19: `/strategies/positions`
    // returned one row (`strategy_id: null`, `strategy_title: "Core / cash
    // mandate"`) and the page rendered "Close all 1 positions", while the tile
    // beside the button read 0 because it summed `active_position_count` over
    // the registered strategies, all of which held nothing.
    vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockResolvedValue({
      positions: [
        { strategy_trade_id: 2, broker_position_id: "3601264304", strategy_id: null, instrument_id: 3417, symbol: "SPY.RTH", currency: "USD", units: "0.296155", assigned_value: "168.64", current_price: "570.72", trade_status: "open" },
      ],
      live_quote_instrument_ids: [3417],
    } as never);
    vi.mocked(strategiesApi.fetchStrategyOverview).mockResolvedValue({
      ...BLOCKED,
      strategies: [
        { pnl: { total_pnl: "0", active_position_count: 0 }, attribution: {}, allocation: {}, purpose: "harness_validation" },
      ],
    } as unknown as StrategyOverviewResponse);

    renderLens();
    const holdings = await screen.findByRole("heading", { name: /Holdings/ });
    await waitFor(() => expect(holdings.textContent).toContain("· 1 open"));
    expect(await screen.findByText("Close all 1 positions")).toBeInTheDocument();
  });

  it("names the position its P&L total cannot account for, with the currency that blocks it", async () => {
    // #3222 residual 2, measured on dev 2026-09-19: every registered strategy
    // reports `total_pnl: "0"`, so the tile rendered a confident `$0.00` while
    // the pot held an open core position with non-zero P&L. The figure is right
    // for the strategies it sums — so the fix names the scope and states the
    // exclusion rather than folding in a GBP number under a USD symbol, which
    // is `fx_unmodelled` (#2363).
    vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockResolvedValue({
      positions: [
        { strategy_trade_id: 2, broker_position_id: "3601264304", strategy_id: null, strategy_title: "Core / cash mandate", instrument_id: 3417, symbol: "SPY.RTH", currency: "GBP", units: "0.296155", assigned_value: "168.64", current_price: "570.72", trade_status: "open" },
      ],
      live_quote_instrument_ids: [3417],
    } as never);

    renderLens();
    const caveat = await screen.findByText(/exclude 1 position held outside the strategies/);
    expect(caveat.textContent).toContain("Core / cash mandate");
    // The currency IS the reason, so it must be in the sentence: "excluded" on
    // its own reads as an oversight rather than a refusal with a cause.
    expect(caveat.textContent).toContain("GBP");
  });

  it("claims no conversion problem when the excluded position is already in the pool's currency", async () => {
    // Review WARNING on PR #3226. The exclusion and the conversion problem are
    // two different facts: a position is left out because it sits outside the
    // strategy roll-up (always true of it), while currency is only why it
    // cannot simply be added (true only sometimes). The core instrument is
    // natively USD — `instruments.currency = 'USD'` for 3417,
    // `openConversionRate = 1.0` — and this fixture reports it that way, so
    // "reported in USD, which this USD total cannot convert" was reachable and
    // asserted an FX conflict that does not exist.
    vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockResolvedValue({
      positions: [
        { strategy_trade_id: 2, broker_position_id: "3601264304", strategy_id: null, strategy_title: "Core / cash mandate", instrument_id: 3417, symbol: "SPY.RTH", currency: "USD", units: "0.296155", assigned_value: "225.04", current_price: "570.72", trade_status: "open" },
      ],
      live_quote_instrument_ids: [3417],
    } as never);

    renderLens();
    const caveat = await screen.findByText(/exclude 1 position held outside the strategies/);
    // Still excluded, and still said so — only the CAUSE clause drops.
    expect(caveat.textContent).toContain("Core / cash mandate");
    expect(caveat.textContent).not.toContain("cannot convert");
  });

  it("raises no exclusion caveat when every position belongs to a strategy the total sums", async () => {
    // The negative case, and the point of the test: a caveat that is always
    // present is not a signal. Same page, same tile, one field different.
    vi.mocked(strategiesApi.fetchStrategyOwnedPositions).mockResolvedValue({
      positions: [
        { strategy_trade_id: 2, broker_position_id: "3601264304", strategy_id: "s4", strategy_title: "Volatility breakout", instrument_id: 3417, symbol: "SPY.RTH", currency: "USD", units: "0.296155", assigned_value: "168.64", current_price: "570.72", trade_status: "open" },
      ],
      live_quote_instrument_ids: [3417],
    } as never);

    renderLens();
    // Anchored on the tile so this cannot pass by rendering nothing at all.
    expect(await screen.findByText("Strategy total P&L")).toBeInTheDocument();
    expect(screen.queryByText(/held outside the strategies/)).not.toBeInTheDocument();
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

  it("moves every pot write to the Setup lens, with policy-fixed rules labelled read-only", async () => {
    vi.spyOn(strategiesApi, "fetchCoreSleeve").mockResolvedValue(CORE_COLLECTING as never);
    render(
      <MemoryRouter initialEntries={["/strategies"]}>
        <StrategiesHubPage />
      </MemoryRouter>,
    );
    await screen.findByText("Not trading");
    await userEvent.click(screen.getByRole("tab", { name: "Setup" }));
    expect(await screen.findByLabelText("Trading capital (USD)")).toBeInTheDocument();
    expect(screen.getByLabelText("Budget mode")).toHaveValue("fixed");
    expect(screen.getByText(/Fixed: the engine may never use more than this amount/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save mandate" })).toBeInTheDocument();
    const policy = screen.getByRole("heading", { name: "Set by policy" }).closest("section")!;
    expect(within(policy).getByText("Stop loss / take profit")).toBeInTheDocument();
    // The BLOCKED fixture has the kill switch on; the policy row reports it, the
    // Clear control stays with the blocker on the Portfolio lens.
    expect(within(policy).getByText("Kill switch").nextElementSibling?.textContent).toBe("On");
    expect(within(policy).getByText("Demo")).toBeInTheDocument();
    expect(within(policy).getByRole("table", { name: "Core candidate evidence coverage" })).toBeInTheDocument();
    expect(within(policy).queryByRole("button")).not.toBeInTheDocument();
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
