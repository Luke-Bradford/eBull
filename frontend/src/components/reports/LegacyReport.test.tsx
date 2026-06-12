/**
 * Pins the corrected v1 legacy branch (spec §3.2): the three §2
 * phantom-key bugs stay retired, dead sections stay dropped, and the
 * branch renders old snapshots only (no v2 chrome).
 */
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { LegacyReport } from "@/components/reports/LegacyReport";
import { V1_MONTHLY, V1_WEEKLY } from "@/components/reports/__fixtures__/v1Snapshots";

function renderLegacy(report: typeof V1_WEEKLY) {
  return render(
    <MemoryRouter>
      <LegacyReport report={report} />
    </MemoryRouter>,
  );
}

afterEach(cleanup);

describe("LegacyReport — v1 weekly fixture", () => {
  it("reads the keys the v1 builder actually wrote (§2 bug 1 retired)", () => {
    renderLegacy(V1_WEEKLY);
    // realized_pnl 150 + unrealized_pnl 320.50 render — the phantom
    // realised_pnl / portfolio_value / cash tiles are gone.
    expect(screen.getByText("Net realised gains")).toBeInTheDocument();
    expect(screen.getByText("US$150.00")).toBeInTheDocument();
    expect(screen.getByText("US$320.50")).toBeInTheDocument();
    expect(screen.getByText("US$470.50")).toBeInTheDocument();
    expect(screen.queryByText("Portfolio value")).not.toBeInTheDocument();
  });

  it("performers render unrealized_pnl as currency (§2 bug 2 retired)", () => {
    renderLegacy(V1_WEEKLY);
    expect(screen.getByText("Top performers (unrealised P&L)")).toBeInTheDocument();
    expect(screen.getByText("US$200.00")).toBeInTheDocument();
    expect(screen.getByText("-US$50.00")).toBeInTheDocument();
  });

  it("drops the retired upcoming_earnings section", () => {
    renderLegacy(V1_WEEKLY);
    expect(screen.queryByText(/upcoming earnings/i)).not.toBeInTheDocument();
  });

  it("marks the snapshot as legacy", () => {
    renderLegacy(V1_WEEKLY);
    expect(screen.getByText(/Legacy snapshot \(v1 schema\)/)).toBeInTheDocument();
  });
});

describe("LegacyReport — v1 monthly fixture", () => {
  it("best/worst trades read gross_return_pct as a FRACTION (§2 bug 2)", () => {
    renderLegacy(V1_MONTHLY);
    // 0.15 fraction → +15.00%, never 0.15%. The value appears in both
    // the best-trade line and the thesis-outcome list (same trade).
    expect(screen.getAllByText("+15.00%").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("-8.00%").length).toBeGreaterThanOrEqual(1);
    expect(screen.queryByText("0.15%")).not.toBeInTheDocument();
  });

  it("thesis accuracy renders the per-trade list, no phantom aggregates (§2 bug 3)", () => {
    renderLegacy(V1_MONTHLY);
    expect(screen.getByText("Thesis outcomes (2 closed trades)")).toBeInTheDocument();
    expect(screen.getByText("hit")).toBeInTheDocument();
    expect(screen.getByText("miss")).toBeInTheDocument();
    expect(screen.queryByText(/buy theses that worked/i)).not.toBeInTheDocument();
  });

  it("win rate renders percent-basis with average holding period", () => {
    renderLegacy(V1_MONTHLY);
    expect(screen.getByText("66.67%")).toBeInTheDocument();
    expect(screen.getByText("12 days")).toBeInTheDocument();
  });
});
