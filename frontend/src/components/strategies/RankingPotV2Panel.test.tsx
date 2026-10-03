import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as rankingPotApi from "@/api/rankingPot";
import type {
  AiTrialJobFire,
  RankingPotV2Holding,
  RankingPotV2ReadoutResponse,
  RankingPotV2StatusResponse,
} from "@/api/types";
import { RankingPotV2Panel } from "@/components/strategies/RankingPotV2Panel";

const FIRE: AiTrialJobFire = {
  job_name: "ranking_pot_v2_rebalance",
  next_fire_at: "2026-10-03T12:50:00Z",
  last_started_at: "2026-10-03T11:50:05Z",
  last_finished_at: "2026-10-03T11:50:05Z",
  last_status: "success",
  last_note: "no ranking-pot-v2 declaration",
};

const EMPTY: RankingPotV2StatusResponse = {
  strategy_id: "ranking-pot-v2",
  strategy_version: "v1",
  build_complete: false,
  declaration: null,
  jobs: [FIRE, { ...FIRE, job_name: "ranking_pot_v2_step" }],
  rebalances: [],
  step: null,
  shadow_nav: null,
  looks: [],
  looks_withheld: false,
  holdings: [],
};

const HELD: RankingPotV2Holding = {
  instrument_id: 1,
  symbol: "AAPL",
  state: "held",
  slot: 0,
  entry_session: "2026-11-02",
  entry_fill: "100",
  invested: "0.04",
  value: "0.044",
  last_close: "110",
  last_close_session: "2026-11-04",
  stop_loss: "94",
  take_profit: "118",
  reasons: {
    instrument_id: 1,
    u_score: "1/2",
    u_dtc: "3/4",
    u_ins: "1",
    composite: "3/4",
    dtc: "1.5",
    dtc_missing: null,
    dtc_settlement_date: "2026-10-15",
    dtc_source_document_id: 9,
    accessions: ["0000320193-26-000101"],
    pairs: [["0001", "0000320193", 2026, "opportunistic", [[2023, 3], [2024, 7], [2025, 11]]]],
  },
};

const SHADOW: RankingPotV2StatusResponse = {
  ...EMPTY,
  build_complete: true,
  declaration: {
    declaration_id: 17,
    frozen_at: "2026-10-04T12:00:00Z",
    state: "shadow_only",
    state_reason: "freeze",
    state_at: "2026-10-04T12:00:00Z",
    pot_capital: null,
  },
  step: { latest_session: "2026-11-05", refusal_session: null, refusal_reason: null, refusal_at: null },
  shadow_nav: "1.012",
  looks: [
    {
      look_id: 3,
      look_months: 12,
      endpoint: "2027-11-01",
      v2_verdict: "not_passed",
      harm: false,
      reasons: [],
      reference_condition: false,
      turnover_condition: true,
      invalidated_by: 4,
      invalidated_note: "bad bars",
    },
  ],
  holdings: [
    HELD,
    {
      ...HELD,
      instrument_id: 2,
      symbol: "MSFT",
      state: "pending",
      slot: null,
      entry_fill: null,
      reasons: { ...HELD.reasons!, instrument_id: 2, u_score: "1/4", dtc: null, dtc_missing: "not_available", accessions: [], pairs: [] },
    },
  ],
};

describe("RankingPotV2Panel", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("says v2 is still being built and never fetches a readout", async () => {
    vi.spyOn(rankingPotApi, "fetchRankingPotV2Status").mockResolvedValue(EMPTY);
    const readout = vi.spyOn(rankingPotApi, "fetchRankingPotV2Readout");
    render(<RankingPotV2Panel />);
    expect(await screen.findByText("Not frozen")).toBeInTheDocument();
    expect(screen.getByText(/Still being built/)).toBeInTheDocument();
    expect(screen.getByText("Monthly rebalance")).toBeInTheDocument();
    expect(readout).not.toHaveBeenCalled();
  });

  it("shows each shadow holding with why it entered, and an invalidated look as invalidated", async () => {
    vi.spyOn(rankingPotApi, "fetchRankingPotV2Status").mockResolvedValue(SHADOW);
    const view: RankingPotV2ReadoutResponse = { declaration_id: 17, endpoint: null, readout: null, reason: "not_stepped" };
    vi.spyOn(rankingPotApi, "fetchRankingPotV2Readout").mockResolvedValue(view);
    render(<RankingPotV2Panel />);
    expect(await screen.findByText("Shadow only — no orders")).toBeInTheDocument();
    expect(screen.getByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText(/0\.750 = mean of score 0\.500 · days-to-cover 0\.750 · insider 1\.000/)).toBeInTheDocument();
    expect(screen.getByText(/1 opportunistic insider pair · Form 4 0000320193-26-000101/)).toBeInTheDocument();
    expect(screen.getByText(/missing \(not_available\)/)).toBeInTheDocument();
    expect(screen.getByText(/2026 purchase classed opportunistic from prior-year trades in 2023-03, 2024-07, 2025-11/)).toBeInTheDocument();
    // The close predates the NAV's session (a missing bar): flagged, never shown as current.
    expect(screen.getByText(/stale: no bar since/)).toBeInTheDocument();
    expect(screen.getByText(/enters/)).toBeInTheDocument();
    expect(screen.getByText("Invalidated: bad bars")).toBeInTheDocument();
    expect(screen.queryByText("Not passed")).not.toBeInTheDocument();
    expect(screen.getByText(/v1 reference not met · turnover met/)).toBeInTheDocument();
    expect(await screen.findByText(/No session has been stepped yet/)).toBeInTheDocument();
  });
});
