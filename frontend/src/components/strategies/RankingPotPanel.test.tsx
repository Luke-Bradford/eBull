import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as rankingPotApi from "@/api/rankingPot";
import type {
  AiTrialJobFire,
  RankingPotPosition,
  RankingPotReadoutResponse,
  RankingPotStatusResponse,
} from "@/api/types";
import { RankingPotPanel } from "@/components/strategies/RankingPotPanel";

const FIRE: AiTrialJobFire = {
  job_name: "ranking_pot_rebalance",
  next_fire_at: "2026-10-02T11:40:00Z",
  last_started_at: "2026-10-02T10:40:05Z",
  last_finished_at: "2026-10-02T10:40:05Z",
  last_status: "success",
  last_note: "no ranking-pot declaration",
};

const EMPTY: RankingPotStatusResponse = {
  strategy_id: "ranking-pot-v1",
  strategy_version: "v1",
  build_complete: true,
  declaration: null,
  jobs: [FIRE, { ...FIRE, job_name: "ranking_pot_step" }, { ...FIRE, job_name: "ranking_pot_execute" }],
  rebalances: [],
  step: null,
  looks: [],
  held: [],
  recent: [],
};

const OPEN: RankingPotPosition = {
  lifecycle_id: 1,
  slot: 1,
  instrument_id: 2842,
  symbol: "AAPL",
  status: "open",
  trade_status: "open",
  target_session: "2026-11-02",
  funding_reason: "t",
  exit_session: null,
  exit_reason: null,
  amount: "200",
  ask: "100.05",
  quote_at: "2026-11-02T15:00:00Z",
  sent_stop_loss: "94.05",
  sent_take_profit: "112.05",
  held_observed_at: null,
  held_stop_loss: null,
  held_take_profit: null,
  held_no_stop_loss: null,
  held_no_take_profit: null,
  realized_pnl_usd: null,
  unrealized_pnl_usd: "12.5",
  marked_on: "2026-11-03",
  ticket: {
    rule_id: "ranking-pot-v1:enter-frank-le-N",
    r_rank: 1,
    f_rank: 1,
    score: {
      model_version: "v1.5-balanced",
      total_score: "0.71",
      raw_total: "0.74",
      families: { quality: "0.8", value: "0.6" },
      penalties: [{ deduction: "0.03" }],
      rewards: [],
      reconciles: true,
    },
    thesis: { thesis_id: 77, age_days: 12, model: "claude", prompt_version: "p3" },
    exit_rule: "broker SL = entry − 3×ATR14",
    planned_levels: { stop_loss: "94", take_profit: "112", atr14: "2" },
  },
  ticket_verified: false,
};

const EXECUTING: RankingPotStatusResponse = {
  ...EMPTY,
  declaration: {
    declaration_id: 4,
    frozen_at: "2026-10-03T12:00:00Z",
    state: "executing",
    state_reason: "activate",
    state_at: "2026-10-05T12:00:00Z",
    pot_capital: "5000",
  },
  rebalances: [
    {
      month: "2026-11-01",
      target_session: "2026-11-02",
      outcome: "decided",
      refusal: null,
      fired_at: "2026-10-31T23:40:00Z",
      executed_state: "executing",
      entries_allowed: false,
      v1_active: true,
    },
  ],
  held: [OPEN],
  recent: [
    {
      ...OPEN,
      lifecycle_id: 2,
      slot: 2,
      symbol: "MSFT",
      status: "closed",
      exit_reason: "rank_exit",
      sent_stop_loss: null,
      sent_take_profit: null,
      unrealized_pnl_usd: null,
      realized_pnl_usd: "-4",
      ticket: { ...OPEN.ticket, thesis: null, score: { ...OPEN.ticket.score, families: { momentum: "0.5" } } },
      ticket_verified: true,
    },
  ],
};

describe("RankingPotPanel", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("says the pot is built but not frozen, and never fetches a readout", async () => {
    vi.spyOn(rankingPotApi, "fetchRankingPotStatus").mockResolvedValue(EMPTY);
    const readout = vi.spyOn(rankingPotApi, "fetchRankingPotReadout");
    render(<RankingPotPanel />);
    expect(await screen.findByText("Not frozen")).toBeInTheDocument();
    expect(screen.getByText(/Built, not frozen yet/)).toBeInTheDocument();
    expect(screen.getByText("Monthly rebalance")).toBeInTheDocument();
    expect(readout).not.toHaveBeenCalled();
  });

  it("shows each position with its ticket, P&L and why entries were withheld", async () => {
    vi.spyOn(rankingPotApi, "fetchRankingPotStatus").mockResolvedValue(EXECUTING);
    const view: RankingPotReadoutResponse = {
      declaration_id: 4,
      endpoint: null,
      readout: null,
      reason: "invariant_violation",
    };
    vi.spyOn(rankingPotApi, "fetchRankingPotReadout").mockResolvedValue(view);
    render(<RankingPotPanel />);
    expect(await screen.findByText("Executing")).toBeInTheDocument();
    expect(screen.getByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText(/broker levels not observed/)).toBeInTheDocument();
    expect(screen.getByText(/no entries \(AI trial v1 active\)/)).toBeInTheDocument();
    expect(screen.getByText(/quality 0\.800 · value 0\.600/)).toBeInTheDocument();
    expect(screen.getByText(/#77, 12 days old/)).toBeInTheDocument();
    // Only the unverified ticket carries the warning.
    expect(screen.getAllByText(/does not match its recorded hash/)).toHaveLength(1);
    expect(screen.getByText("MSFT")).toBeInTheDocument();
    expect(await screen.findByText(/figures are withheld/)).toBeInTheDocument();
  });
});
