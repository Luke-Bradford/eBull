import { render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as strategiesApi from "@/api/strategies";
import type { AiTrialJobFire, EngineBookRiskLatest, EngineBookRiskResponse } from "@/api/types";
import { EngineBookRiskPanel } from "@/components/strategies/EngineBookRiskPanel";

const JOB: AiTrialJobFire = {
  job_name: "engine_book_risk_snapshot",
  next_fire_at: "2026-10-03T09:00:00Z",
  last_started_at: "2026-10-02T09:00:01Z",
  last_finished_at: "2026-10-02T09:00:02Z",
  last_status: "failure",
  last_note: "benchmark_not_ready",
};

// The first dev-DB row (2026-10-01), as served.
const LATEST: EngineBookRiskLatest = {
  session_date: "2026-10-01",
  measured_at: "2026-10-02T17:45:46Z",
  pool_event_id: 7,
  capital_usd: "40000.000000",
  gross_usd: "16078.026146",
  position_count: 4,
  instrument_count: 1,
  open_trade_count: 4,
  cost_marked_count: 0,
  stale_count: 0,
  largest_share_pct: "100.00000000",
  top5_share_pct: "100.00000000",
  hhi: "10000.00000000",
  hist_vol_pct: "5.12124925",
  ewma_vol_pct: "4.04665230",
  beta: "0.38635045",
  vol_n_obs: 235,
  beta_n_obs: 235,
  sample_first: "2025-10-28",
  sample_last: "2026-10-01",
  history_status: "ok",
  beta_defaulted_count: 0,
  beta_defaulted_weight_pct: "0E-8",
  stress_2020_pct: "-13.17638420",
  stress_2022_pct: "-9.79806906",
  checks: {
    stale_marks: { status: "evaluated", value: "0", limit: "0", flagged: false },
    forecast_vol_vs_target: { status: "evaluated", value: "4.04665230", limit: "18.0000", flagged: false },
    positions_vs_max_concurrent: { status: "evaluated", value: "4", limit: "30", flagged: false },
    stress_2020_vs_max_drawdown: { status: "evaluated", value: "-13.17638420", limit: "25.0000", flagged: false },
    stress_2022_vs_max_drawdown: { status: "evaluated", value: "-9.79806906", limit: "25.0000", flagged: false },
  },
  positions: [
    {
      trade_id: 3,
      position_id: 3603268128,
      instrument_id: 3417,
      symbol: "SPY.RTH",
      units: "0.16227400",
      mark: "763.680000",
      mark_date: "2026-10-01",
      market_value_usd: "123.92540832",
      weight_of_capital_pct: "0.30981352",
      beta: "0.96118875",
      beta_n_obs: 235,
    },
  ],
};

function mockRisk(view: EngineBookRiskResponse) {
  vi.spyOn(strategiesApi, "fetchEngineBookRisk").mockResolvedValue(view);
}

describe("EngineBookRiskPanel", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("says no snapshot exists yet and shows the job's refusal reason", async () => {
    mockRisk({ policy_version: "engine-book-risk-v1", job: JOB, latest: null, recent: [] });
    render(<EngineBookRiskPanel />);
    expect(await screen.findByText(/No snapshot yet/)).toBeInTheDocument();
    expect(screen.getByText(/benchmark_not_ready/)).toBeInTheDocument();
    expect(screen.queryByText("Checks within limits")).not.toBeInTheDocument();
  });

  it("lists every check against its limit, the stress as a signed loss", async () => {
    mockRisk({ policy_version: "engine-book-risk-v1", job: JOB, latest: LATEST, recent: [] });
    render(<EngineBookRiskPanel />);
    expect(await screen.findByText("Checks within limits")).toBeInTheDocument();
    expect(screen.getByText(/Not measured yet/)).toBeInTheDocument();
    const checks = within(screen.getByRole("list", { name: "Mandate checks" }));
    const stress = checks.getByText("2020 crash stress vs max drawdown").closest("li")!;
    expect(stress).toHaveTextContent("-13.18% vs -25.00%");
    expect(checks.getByText("Forecast volatility vs target").closest("li")!).toHaveTextContent("4.05% vs 18.00%");
    expect(screen.getByText(/US\$16,078\.03 of US\$40,000\.00 \(40\.20%\)/)).toBeInTheDocument();
  });

  it("flags an over-limit check, and does not call unset limits compliance", async () => {
    const over = {
      ...LATEST,
      checks: {
        ...LATEST.checks,
        stale_marks: { status: "evaluated", value: "2", limit: "0", flagged: true },
      },
    };
    mockRisk({ policy_version: "engine-book-risk-v1", job: JOB, latest: over, recent: [] });
    const { unmount } = render(<EngineBookRiskPanel />);
    expect(await screen.findByText("1 over limit")).toBeInTheDocument();
    unmount();

    const unset = {
      ...LATEST,
      checks: {
        ...LATEST.checks,
        forecast_vol_vs_target: { status: "no_limit", value: "4.04665230", limit: null, flagged: false },
      },
    };
    mockRisk({ policy_version: "engine-book-risk-v1", job: JOB, latest: unset, recent: [] });
    render(<EngineBookRiskPanel />);
    expect(await screen.findByText("Partly checked")).toBeInTheDocument();
    expect(screen.getByText("no limit set")).toBeInTheDocument();
  });

  it("shows a retry on a failed fetch", async () => {
    vi.spyOn(strategiesApi, "fetchEngineBookRisk").mockRejectedValue(new Error("boom"));
    render(<EngineBookRiskPanel />);
    expect(await screen.findByRole("button", { name: /retry/i })).toBeInTheDocument();
    expect(screen.queryByText(/boom/)).not.toBeInTheDocument();
  });
});
