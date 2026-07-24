import { describe, expect, it } from "vitest";
import type { PortfolioResponse } from "@/api/types";
import { portfolioTotals } from "@/lib/portfolioTotals";

/** Minimal PortfolioResponse carrying only the fields portfolioTotals reads. */
function resp(over: Partial<PortfolioResponse>): PortfolioResponse {
  return {
    positions: [],
    mirrors: [],
    display_currency: "USD",
    fx_incomplete: false,
    ...over,
  } as PortfolioResponse;
}

describe("portfolioTotals", () => {
  it("sums P&L and cost over positions AND mirrors", () => {
    const t = portfolioTotals(
      resp({
        positions: [
          { unrealized_pnl: 100, cost_basis: 1000 },
          { unrealized_pnl: -40, cost_basis: 600 },
        ] as PortfolioResponse["positions"],
        mirrors: [{ unrealized_pnl: 40, funded: 400 }] as PortfolioResponse["mirrors"],
      }),
    );
    expect(t.totalPnl).toBe(100);
    expect(t.totalCost).toBe(2000);
    expect(t.pnlFraction).toBeCloseTo(0.05); // 100 / 2000
  });

  it("returns null pnlFraction when total cost is 0 (never divides by zero)", () => {
    const t = portfolioTotals(
      resp({ positions: [{ unrealized_pnl: 0, cost_basis: 0 }] as PortfolioResponse["positions"] }),
    );
    expect(t.pnlFraction).toBeNull();
  });

  it("passes through display_currency and fx_incomplete (#2129, single source)", () => {
    const t = portfolioTotals(resp({ display_currency: "GBP", fx_incomplete: true }));
    expect(t.displayCurrency).toBe("GBP");
    expect(t.hasUnconverted).toBe(true);
  });

  it("handles empty positions and absent mirrors", () => {
    const t = portfolioTotals(resp({ positions: [], mirrors: undefined as unknown as [] }));
    expect(t.totalPnl).toBe(0);
    expect(t.totalCost).toBe(0);
    expect(t.pnlFraction).toBeNull();
  });
});
