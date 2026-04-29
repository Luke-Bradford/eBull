import { describe, expect, it } from "vitest";

import type { InstrumentFinancialRow } from "@/api/types";
import {
  buildCashflowWaterfall,
  buildDebtStructure,
  buildDupont,
  buildFcf,
  buildMargins,
  buildPnlBuckets,
  buildRoic,
  buildYoyGrowth,
  joinStatements,
  latestBalanceStructure,
  safeDiv,
} from "@/lib/fundamentalsMetrics";

function row(
  period_end: string,
  values: Record<string, string | null>,
  period_type: string = "Q1",
): InstrumentFinancialRow {
  return { period_end, period_type, values };
}

describe("safeDiv", () => {
  it("returns null for null operands or zero divisor", () => {
    expect(safeDiv(null, 1)).toBeNull();
    expect(safeDiv(1, null)).toBeNull();
    expect(safeDiv(1, 0)).toBeNull();
    expect(safeDiv(0, 1)).toBe(0);
    expect(safeDiv(-2, 4)).toBe(-0.5);
  });
});

describe("joinStatements", () => {
  it("joins on (period_end, period_type) and sorts ascending", () => {
    const income = [
      row("2026-03-31", { revenue: "100", net_income: "10" }),
      row("2025-12-31", { revenue: "90", net_income: "9" }),
    ];
    const balance = [
      row("2026-03-31", { total_assets: "1000" }),
    ];
    const cashflow = [
      row("2025-12-31", { operating_cf: "20" }),
    ];
    const joined = joinStatements(income, balance, cashflow);
    expect(joined.map((r) => r.period_end)).toEqual([
      "2025-12-31",
      "2026-03-31",
    ]);
    expect(joined[0]!.operating_cf).toBe(20);
    expect(joined[1]!.total_assets).toBe(1000);
    expect(joined[0]!.total_assets).toBeNull();
  });

  it("treats malformed numeric strings as null", () => {
    const income = [row("2026-03-31", { revenue: "not-a-number" })];
    const joined = joinStatements(income, [], []);
    expect(joined[0]!.revenue).toBeNull();
  });
});

describe("buildPnlBuckets", () => {
  it("sums R&D + SG&A into opex; either component on its own still counts", () => {
    const periods = joinStatements(
      [
        row("2026-03-31", {
          revenue: "100",
          cost_of_revenue: "40",
          operating_income: "20",
          research_and_dev: "15",
          sga_expense: "25",
        }),
        row("2026-06-30", {
          revenue: "100",
          cost_of_revenue: "40",
          operating_income: "30",
          research_and_dev: "30",
          // no sga
        }),
        row("2026-09-30", {
          revenue: "100",
          cost_of_revenue: "40",
          operating_income: "30",
          // no rd, no sga
        }),
      ],
      [],
      [],
    );
    const buckets = buildPnlBuckets(periods);
    expect(buckets[0]!.opex).toBe(40);
    expect(buckets[1]!.opex).toBe(30);
    expect(buckets[2]!.opex).toBeNull();
  });
});

describe("buildMargins", () => {
  it("computes gross/operating/net as percentages with null on missing revenue", () => {
    const periods = joinStatements(
      [
        row("2026-03-31", {
          revenue: "100",
          gross_profit: "60",
          operating_income: "20",
          net_income: "10",
        }),
        row("2026-06-30", {
          revenue: null,
          gross_profit: "60",
        }),
      ],
      [],
      [],
    );
    const margins = buildMargins(periods);
    expect(margins[0]).toEqual({
      period_end: "2026-03-31",
      gross_pct: 60,
      operating_pct: 20,
      net_pct: 10,
    });
    expect(margins[1]!.gross_pct).toBeNull();
  });
});

describe("buildYoyGrowth", () => {
  it("compares quarterly periods to 4 lags back when period='quarterly'", () => {
    // Real backend period_types — Q1/Q2/Q3/Q4, never the literal
    // "quarterly". The helper must derive lag from the page-level
    // arg, not the row's period_type.
    const quarterly = [
      row("2025-03-31", { revenue: "100", eps_diluted: "1" }, "Q1"),
      row("2025-06-30", { revenue: "110", eps_diluted: "1.1" }, "Q2"),
      row("2025-09-30", { revenue: "120", eps_diluted: "1.2" }, "Q3"),
      row("2025-12-31", { revenue: "130", eps_diluted: "1.3" }, "Q4"),
      row("2026-03-31", { revenue: "150", eps_diluted: "1.5" }, "Q1"),
    ];
    const periods = joinStatements(quarterly, [], []);
    const yoy = buildYoyGrowth(periods, "quarterly");
    expect(yoy[0]!.revenue_yoy_pct).toBeNull();
    expect(yoy[3]!.revenue_yoy_pct).toBeNull();
    // 2026-Q1 vs 2025-Q1: (150 - 100) / 100 * 100 = 50
    expect(yoy[4]!.revenue_yoy_pct).toBe(50);
    expect(yoy[4]!.eps_yoy_pct).toBeCloseTo(50);
  });

  it("compares annual (FY) periods to 1 lag back when period='annual'", () => {
    // Backend emits FY for annual rows — make sure the helper uses
    // a lag of 1 in that mode regardless of row period_type.
    const periods = joinStatements(
      [
        row("2025-12-31", { revenue: "100", eps_diluted: "1" }, "FY"),
        row("2026-12-31", { revenue: "120", eps_diluted: "1.5" }, "FY"),
      ],
      [],
      [],
    );
    const yoy = buildYoyGrowth(periods, "annual");
    expect(yoy[0]!.revenue_yoy_pct).toBeNull();
    expect(yoy[1]!.revenue_yoy_pct).toBe(20);
    expect(yoy[1]!.eps_yoy_pct).toBe(50);
  });

  it("defaults to quarterly when no period arg is passed", () => {
    const periods = joinStatements(
      [row("2025-12-31", { revenue: "100" }, "FY")],
      [],
      [],
    );
    // Single row with default lag=4 → null comparator
    expect(buildYoyGrowth(periods)[0]!.revenue_yoy_pct).toBeNull();
  });

  it("uses |prior| in the denominator so swings from negative to positive read positive", () => {
    const periods = joinStatements(
      [
        row("2025-12-31", { revenue: "100", eps_diluted: "-2" }, "FY"),
        row("2026-12-31", { revenue: "120", eps_diluted: "1" }, "FY"),
      ],
      [],
      [],
    );
    const yoy = buildYoyGrowth(periods, "annual");
    // EPS swings from -2 to +1: (1 - (-2)) / |-2| * 100 = 150
    expect(yoy[1]!.eps_yoy_pct).toBe(150);
  });

  it("computes FCF-YoY from operating_cf - capex", () => {
    const cashflow = [
      row("2025-12-31", { operating_cf: "100", capex: "30" }, "FY"),
      row("2026-12-31", { operating_cf: "150", capex: "30" }, "FY"),
    ];
    const periods = joinStatements([], [], cashflow);
    const yoy = buildYoyGrowth(periods, "annual");
    // FCF: 70 → 120, growth = 50/70 = ~71.4%
    expect(yoy[1]!.fcf_yoy_pct).toBeCloseTo(71.43, 1);
  });
});

describe("buildCashflowWaterfall", () => {
  it("returns the four-step waterfall with running cumulatives", () => {
    const periods = joinStatements(
      [],
      [],
      [
        row("2026-03-31", {
          operating_cf: "100",
          investing_cf: "-30",
          financing_cf: "-50",
        }),
      ],
    );
    const steps = buildCashflowWaterfall(periods[0]!);
    expect(steps).toEqual([
      { label: "Operating", value: 100, cumulative: 100, is_total: false },
      { label: "Investing", value: -30, cumulative: 70, is_total: false },
      { label: "Financing", value: -50, cumulative: 20, is_total: false },
      { label: "Net change", value: 20, cumulative: 20, is_total: true },
    ]);
  });

  it("returns null when every flow is missing", () => {
    const periods = joinStatements([], [], [row("2026-03-31", {})]);
    expect(buildCashflowWaterfall(periods[0]!)).toBeNull();
  });
});

describe("latestBalanceStructure", () => {
  it("walks backwards to find the most-recent complete snapshot", () => {
    const periods = joinStatements(
      [],
      [
        row("2025-Q1", {
          total_assets: "1000",
          total_liabilities: "600",
          shareholders_equity: "400",
        }),
        row("2026-Q1", {
          total_assets: "1500",
          total_liabilities: null,
          shareholders_equity: "600",
        }),
      ],
      [],
    );
    const snap = latestBalanceStructure(periods);
    expect(snap?.period_end).toBe("2025-Q1");
    expect(snap?.assets).toBe(1000);
  });

  it("returns null when no period is complete", () => {
    expect(latestBalanceStructure([])).toBeNull();
  });
});

describe("buildDebtStructure", () => {
  it("computes interest coverage and clamps negative coverage to null", () => {
    const periods = joinStatements(
      [
        row("2025", { operating_income: "100", interest_expense: "10" }, "annual"),
        row("2026", { operating_income: "-20", interest_expense: "10" }, "annual"),
      ],
      [
        row("2025", { long_term_debt: "200", short_term_debt: "50" }, "annual"),
        row("2026", { long_term_debt: "300", short_term_debt: "60" }, "annual"),
      ],
      [],
    );
    const rows = buildDebtStructure(periods);
    expect(rows[0]!.interest_coverage).toBe(10);
    // Negative coverage clamped to null — loss-making quarter says
    // nothing about future coverage capacity.
    expect(rows[1]!.interest_coverage).toBeNull();
    expect(rows[0]!.long_term).toBe(200);
  });
});

describe("buildDupont", () => {
  it("computes ROE = NPM × Asset Turnover × Equity Multiplier", () => {
    const periods = joinStatements(
      [
        row("2026", { revenue: "1000", net_income: "100" }, "annual"),
      ],
      [
        row("2026", { total_assets: "2000", shareholders_equity: "500" }, "annual"),
      ],
      [],
    );
    const dp = buildDupont(periods);
    // NPM 0.1, turnover 0.5, multiplier 4 → ROE 0.2
    expect(dp[0]!.net_margin).toBeCloseTo(0.1);
    expect(dp[0]!.asset_turnover).toBeCloseTo(0.5);
    expect(dp[0]!.equity_multiplier).toBeCloseTo(4);
    expect(dp[0]!.roe).toBeCloseTo(0.2);
  });

  it("returns null ROE when any component is null", () => {
    const periods = joinStatements(
      [row("2026", { revenue: "1000", net_income: "100" }, "annual")],
      [],
      [],
    );
    expect(buildDupont(periods)[0]!.roe).toBeNull();
  });
});

describe("buildRoic", () => {
  it("falls back to 21% effective tax when pre-tax income is non-positive or tax is missing", () => {
    const periods = joinStatements(
      [
        row("2026", {
          operating_income: "100",
          net_income: "10",
          // no income_tax
        }, "annual"),
      ],
      [
        row("2026", {
          long_term_debt: "200",
          short_term_debt: "100",
          shareholders_equity: "200",
        }, "annual"),
      ],
      [],
    );
    const r = buildRoic(periods);
    // NOPAT = 100 * (1 - 0.21) = 79; invested = 500; ROIC = 0.158
    expect(r[0]!.roic).toBeCloseTo(0.158, 3);
  });

  it("uses observed tax rate when pre-tax income is positive", () => {
    const periods = joinStatements(
      [
        row("2026", {
          operating_income: "100",
          net_income: "70",
          income_tax: "30",
        }, "annual"),
      ],
      [
        row("2026", {
          long_term_debt: "200",
          short_term_debt: "0",
          shareholders_equity: "300",
        }, "annual"),
      ],
      [],
    );
    const r = buildRoic(periods);
    // Effective rate = 30 / (70 + 30) = 0.30; NOPAT = 100 * 0.70 = 70
    // invested = 500; ROIC = 0.14
    expect(r[0]!.roic).toBeCloseTo(0.14);
  });

  it("returns null when invested capital is zero or negative", () => {
    const periods = joinStatements(
      [row("2026", { operating_income: "100", net_income: "10" }, "annual")],
      [row("2026", {
        long_term_debt: "0",
        short_term_debt: "0",
        shareholders_equity: "0",
      }, "annual")],
      [],
    );
    expect(buildRoic(periods)[0]!.roic).toBeNull();
  });
});

describe("buildFcf", () => {
  it("returns operating_cf - capex per period", () => {
    const periods = joinStatements(
      [],
      [],
      [row("2026", { operating_cf: "150", capex: "40" }, "annual")],
    );
    expect(buildFcf(periods)[0]!.fcf).toBe(110);
  });

  it("returns null when either side is missing", () => {
    const periods = joinStatements(
      [],
      [],
      [row("2026", { operating_cf: "150" }, "annual")],
    );
    expect(buildFcf(periods)[0]!.fcf).toBeNull();
  });
});
