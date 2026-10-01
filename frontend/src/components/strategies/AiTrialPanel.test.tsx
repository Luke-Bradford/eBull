import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as aiTrialApi from "@/api/aiTrial";
import type { AiTrialJobFire, AiTrialOpenLeg, AiTrialStatusResponse } from "@/api/types";
import { AiTrialPanel } from "@/components/strategies/AiTrialPanel";

const FIRE: AiTrialJobFire = {
  job_name: "ai_trial_decision_run",
  next_fire_at: "2026-10-05T23:30:00Z",
  last_started_at: "2026-10-04T23:30:00Z",
  last_finished_at: "2026-10-04T23:31:00Z",
  last_status: "success",
  last_note: "status=decided session=2026-10-05 run_id=3 pairs=1 orphans_killed=0",
};

const LEG: AiTrialOpenLeg = {
  leg: "arm",
  pair_seq: 0,
  strategy_trade_id: 11,
  symbol: "AAPL",
  trade_status: "open",
  entry_price: "100",
  requested_stop: "95",
  requested_target: "110",
  broker_observed: false,
  broker_stop: null,
  broker_target: null,
  pnl_usd: null,
  exit_deadline_session: "2026-11-02",
};

const ACTIVE: AiTrialStatusResponse = {
  arm_strategy_id: "ai-discretionary-v1",
  strategy_version: "v1",
  state: "active",
  declaration_id: 9,
  state_reason: "start",
  state_at: "2026-10-01T13:00:00Z",
  decision_job: FIRE,
  execute_job: { ...FIRE, job_name: "ai_trial_execute", last_started_at: null, last_finished_at: null, last_status: null, last_note: null },
  sessions: [
    {
      session_date: "2026-10-07",
      decision: { state: "not_run", label: "not_run", legs: 0, refusal_reason: null, decision_refusals: {} },
      execution: [],
    },
    {
      session_date: "2026-10-06",
      decision: { state: "abstained", label: "abstained", legs: 0, refusal_reason: null, decision_refusals: {} },
      execution: [],
    },
    {
      session_date: "2026-10-05",
      decision: { state: "legs_published", label: "legs_published:2", legs: 2, refusal_reason: null, decision_refusals: {} },
      execution: [
        { state: "submitted", label: "submitted:1", count: 1, reason: null },
        { state: "refused", label: "refused:trial_cost_cap×1", count: 1, reason: "trial_cost_cap" },
      ],
    },
  ],
  open_legs: [LEG],
  loss: [
    { leg: "arm", pnl_usd: "-150", unmeasured: 1, limit_usd: "600", headroom_usd: "450" },
    { leg: "control", pnl_usd: "0", unmeasured: 0, limit_usd: "600", headroom_usd: "600" },
  ],
};

describe("AiTrialPanel", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("names each session's outcome so no two no-trade days look alike", async () => {
    vi.spyOn(aiTrialApi, "fetchAiTrialStatus").mockResolvedValue(ACTIVE);
    render(<AiTrialPanel />);
    expect(await screen.findByText("Running")).toBeInTheDocument();
    expect(screen.getByText("Decision job did not run")).toBeInTheDocument();
    expect(screen.getByText("Model chose no trade")).toBeInTheDocument();
    expect(screen.getByText("2 legs published")).toBeInTheDocument();
    expect(screen.getByText("1 submitted")).toBeInTheDocument();
    expect(screen.getByText("1 refused: trial_cost_cap")).toBeInTheDocument();
    expect(screen.getByText(/never run/)).toBeInTheDocument();
    expect(screen.getByText(/broker levels not observed/)).toBeInTheDocument();
    expect(screen.getByText("P&L unmeasured")).toBeInTheDocument();
    expect(screen.getByText(/1 trade\(s\) unmeasured/)).toBeInTheDocument();
  });

  it("shows a broker position with no stop as having none, not as unobserved", async () => {
    vi.spyOn(aiTrialApi, "fetchAiTrialStatus").mockResolvedValue({
      ...ACTIVE,
      open_legs: [{ ...LEG, broker_observed: true, broker_stop: null, broker_target: "112" }],
    });
    render(<AiTrialPanel />);
    expect(await screen.findByText(/broker SL none \/ TP US\$112/)).toBeInTheDocument();
    expect(screen.queryByText(/broker levels not observed/)).not.toBeInTheDocument();
  });

  it("says the trial has not started when nothing is declared", async () => {
    vi.spyOn(aiTrialApi, "fetchAiTrialStatus").mockResolvedValue({
      ...ACTIVE,
      state: "not_declared",
      declaration_id: null,
      state_reason: null,
      state_at: null,
      sessions: [],
      open_legs: [],
      loss: [],
    });
    render(<AiTrialPanel />);
    expect(await screen.findByText("Not started")).toBeInTheDocument();
    expect(screen.getByText(/No frozen declaration yet/)).toBeInTheDocument();
    expect(screen.queryByText("Sessions")).not.toBeInTheDocument();
  });

  it("puts a halt and its reason first", async () => {
    vi.spyOn(aiTrialApi, "fetchAiTrialStatus").mockResolvedValue({
      ...ACTIVE,
      state: "halted_loss",
      state_reason: "loss_halt:leg=arm:loss_usd=601.25:limit_usd=600.00:unmeasured=0",
    });
    render(<AiTrialPanel />);
    expect(await screen.findByText("Halted — loss limit")).toBeInTheDocument();
    expect(screen.getByText(/loss_halt:leg=arm/)).toBeInTheDocument();
  });

  it("reads the version it is given, under its own heading (#3515)", async () => {
    const fetch = vi.spyOn(aiTrialApi, "fetchAiTrialStatus").mockResolvedValue({
      ...ACTIVE,
      arm_strategy_id: aiTrialApi.AI_TRIAL_FUND_V1_ARM,
    });
    render(<AiTrialPanel arm={aiTrialApi.AI_TRIAL_FUND_V1_ARM} title="AI trial (fund-v1)" />);
    expect(await screen.findByRole("heading", { name: /AI trial \(fund-v1\)/ })).toBeInTheDocument();
    expect(fetch).toHaveBeenCalledWith(aiTrialApi.AI_TRIAL_FUND_V1_ARM);
  });

  it("shows a fixed error phrase when the read fails", async () => {
    vi.spyOn(aiTrialApi, "fetchAiTrialStatus").mockRejectedValue(new Error("boom"));
    render(<AiTrialPanel />);
    expect(await screen.findByRole("button", { name: /retry/i })).toBeInTheDocument();
    expect(screen.queryByText("boom")).not.toBeInTheDocument();
  });
});
