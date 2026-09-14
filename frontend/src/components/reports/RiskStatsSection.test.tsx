/**
 * Volatility is a standard deviation, so it cannot be negative — rendering it
 * through `formatPct` printed "+14.30%", a magnitude wearing a return's sign.
 * Same defect as #3032's win rate, on the surface the sibling fix missed.
 *
 * ⚠ Max drawdown is asserted here too, as the CONTROL: `reporting.py` computes
 * it as `min(worst, drawdown)` starting from 0, so it is always ≤ 0 and its
 * minus sign is real information. It must keep the signed formatter.
 */
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import type { MonthlySnapshotV2, RiskV2, TradeStatsV2 } from "@/api/reportSnapshot";
import { RiskStatsSection } from "@/components/reports/MonthlySections";

const RISK: RiskV2 = {
  holding_count: 4,
  concentration_top5_pct: "0.82",
  sector_exposure: {},
  volatility: "0.1430",
  max_drawdown: "-0.0925",
  observations: 12,
  observation_label: "12 monthly periods",
  insufficient_history: false,
};

const TRADE_STATS: TradeStatsV2 = {
  total_closed: 0,
  winners: 0,
  win_rate_pct: null,
  payoff_ratio: null,
  avg_win_pct: null,
  avg_loss_pct: null,
};

function renderRisk(risk: RiskV2 = RISK) {
  return render(
    <MemoryRouter>
      <RiskStatsSection
        risk={risk}
        tradeStats={TRADE_STATS}
        bestTrade={null as MonthlySnapshotV2["best_trade"]}
        worstTrade={null as MonthlySnapshotV2["worst_trade"]}
        marker={{}}
      />
    </MemoryRouter>,
  );
}

afterEach(cleanup);

describe("RiskStatsSection percent signs", () => {
  it("renders volatility unsigned — a standard deviation has no direction", () => {
    renderRisk();
    expect(screen.getByText("14.30%")).toBeInTheDocument();
    expect(screen.queryByText("+14.30%")).not.toBeInTheDocument();
  });

  it("keeps max drawdown signed — it is always <= 0 and the sign is the information", () => {
    renderRisk();
    expect(screen.getByText("-9.25%")).toBeInTheDocument();
  });
});
