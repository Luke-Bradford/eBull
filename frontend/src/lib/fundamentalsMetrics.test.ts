import { describe, expect, it } from "vitest";

import type { InstrumentFinancialRow } from "@/api/types";
import {
  buildCashflowWaterfall,
  buildDebtStructure,
  buildDupont,
  buildFcf,
  buildMargins,
  buildNetDebt,
  buildPnlBuckets,
  buildRoic,
  buildYoyGrowth,
  joinStatements,
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

  it("derives FCF YoY with the SAME capex rule as buildFcf when capex is absent (Codex ckpt-2, #2185)", () => {
    // The two panes had diverged: buildFcf was corrected to the settled
    // `operating_cf - ABS(COALESCE(capex, 0))` rule while buildYoyGrowth kept
    // gating on `capex === null`. A capex-omitting filer — 25% of FY
    // instruments that report OCF — got an FCF line on the FCF pane and a gap
    // on the YoY pane for the very same periods.
    const statements = [
      row("2024-12-31", { operating_cf: "100", capex: null }, "FY"),
      row("2025-12-31", { operating_cf: "150", capex: null }, "FY"),
    ];
    const periods = joinStatements([], [], statements);

    // Both derivations must agree, and neither may be null.
    const fcf = buildFcf(periods);
    expect(fcf.map((r) => r.fcf)).toEqual([100, 150]);

    const yoy = buildYoyGrowth(periods, "annual");
    expect(yoy[1]!.fcf_yoy_pct).toBe(50); // (150 - 100) / |100| * 100
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

  it("nulls annual YoY across a multi-year gap instead of fabricating a spike (#1839)", () => {
    // AAPL-shaped: FY2012 then FY2023 (FY2013-2022 aged out). The positional
    // prior is 11 years back, so (383285-156508)/156508 ≈ +145% — bogus.
    const periods = joinStatements(
      [
        row("2012-09-29", { revenue: "156508", eps_diluted: "1" }, "FY"),
        row("2023-09-30", { revenue: "383285", eps_diluted: "6" }, "FY"),
        row("2024-09-28", { revenue: "391035", eps_diluted: "6.5" }, "FY"),
      ],
      [],
      [],
    );
    const yoy = buildYoyGrowth(periods, "annual");
    expect(yoy[0]!.revenue_yoy_pct).toBeNull(); // no prior
    expect(yoy[1]!.revenue_yoy_pct).toBeNull(); // 11-year gap → not adjacent
    expect(yoy[1]!.eps_yoy_pct).toBeNull();
    // 2024 vs 2023 is a real consecutive year → computes normally.
    expect(yoy[2]!.revenue_yoy_pct).toBeCloseTo(2.02, 1);
  });

  it("nulls quarterly YoY when the prior-year quarter is missing (#1839)", () => {
    // 4 lags back lands on a quarter ~2 years away (a year of quarters is
    // missing), so the 300-430 day guard rejects it.
    const periods = joinStatements(
      [
        row("2023-03-31", { revenue: "100" }, "Q1"),
        row("2023-06-30", { revenue: "100" }, "Q2"),
        row("2023-09-30", { revenue: "100" }, "Q3"),
        row("2023-12-31", { revenue: "100" }, "Q4"),
        row("2025-03-31", { revenue: "150" }, "Q1"),
      ],
      [],
      [],
    );
    const yoy = buildYoyGrowth(periods, "quarterly");
    // periods[4] vs periods[0]: 2025-03-31 − 2023-03-31 ≈ 731 days → nulled.
    expect(yoy[4]!.revenue_yoy_pct).toBeNull();
  });

  it("accepts a 53-week fiscal quarterly gap (~371 days) within the guard (#1839)", () => {
    // 53-week filers shift the prior-year quarter ~371 days back — must stay
    // inside the 300-430 window so legit YoY still computes.
    const periods = joinStatements(
      [
        row("2024-02-03", { revenue: "100" }, "Q1"),
        row("2024-05-04", { revenue: "100" }, "Q2"),
        row("2024-08-03", { revenue: "100" }, "Q3"),
        row("2024-11-02", { revenue: "100" }, "Q4"),
        row("2025-02-08", { revenue: "120" }, "Q1"), // 371 days after 2024-02-03
      ],
      [],
      [],
    );
    const yoy = buildYoyGrowth(periods, "quarterly");
    expect(yoy[4]!.revenue_yoy_pct).toBe(20);
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

// `latestBalanceStructure` and its chart were deleted in #2185: assets and
// (liabilities + equity) are equal by the accounting identity, so the chart
// could not vary. `buildNetDebt` replaces it — the cases below pin the repo's
// documented debt treatment rather than the identity.
describe("buildNetDebt", () => {
  function balance(values: Record<string, string | null>) {
    return joinStatements([], [row("2026-03-31", values)], []);
  }

  it("sums both debt components against cash", () => {
    const [r] = buildNetDebt(
      balance({ long_term_debt: "800", short_term_debt: "200", cash: "300" }),
    );
    expect(r?.debt).toBe(1000);
    expect(r?.net_debt).toBe(700);
  });

  it("COALESCEs a missing component to 0 when the OTHER one is reported", () => {
    // The settled rule (app/services/fundamentals/__init__.py:152-154):
    // COALESCE(long,0) + COALESCE(short,0), guarded by "at least one is NOT
    // NULL". short_term_debt is sparse (12% coverage) because most filers have
    // none to report — treating that as a data gap would blank the chart for
    // the large majority of instruments.
    const [lt] = buildNetDebt(
      balance({ long_term_debt: "500", short_term_debt: null, cash: "100" }),
    );
    expect(lt?.debt).toBe(500);
    expect(lt?.net_debt).toBe(400);

    const [st] = buildNetDebt(
      balance({ long_term_debt: null, short_term_debt: "50", cash: "20" }),
    );
    expect(st?.debt).toBe(50);
    expect(st?.net_debt).toBe(30);
  });

  it("nulls gross debt only when BOTH components are missing", () => {
    const [r] = buildNetDebt(
      balance({ long_term_debt: null, short_term_debt: null, cash: "100" }),
    );
    expect(r?.debt).toBeNull();
    expect(r?.net_debt).toBeNull();
  });

  it("nulls net debt when cash is missing rather than COALESCE-ing it to 0", () => {
    // A missing `cash` is a genuine data gap (fair_value_band.py:1014-1016
    // records the same reasoning for EV). COALESCE-ing it to 0 would overstate
    // net debt by the entire cash balance — gross debt still renders.
    const [r] = buildNetDebt(
      balance({ long_term_debt: "800", short_term_debt: "200", cash: null }),
    );
    expect(r?.debt).toBe(1000);
    expect(r?.net_debt).toBeNull();
  });

  it("goes negative when cash exceeds debt (net cash)", () => {
    const [r] = buildNetDebt(
      balance({ long_term_debt: "100", short_term_debt: null, cash: "900" }),
    );
    expect(r?.net_debt).toBe(-800);
  });

  it("returns an empty array for no periods", () => {
    expect(buildNetDebt([])).toEqual([]);
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

  it("treats a missing capex as zero — capex must NOT gate the series", () => {
    // Settled rule: `operating_cf - ABS(COALESCE(capex, 0))`
    // (app/services/fcf_yield.py:111 quarterly, :132 annual). Spec §3.4:
    // "Any implementation that gates the FCF line on `capex IS NOT NULL` is
    // wrong." 1,142 FY instruments report OCF and never report capex.
    const periods = joinStatements(
      [],
      [],
      [row("2026", { operating_cf: "150" }, "annual")],
    );
    expect(buildFcf(periods)[0]!.fcf).toBe(150);
  });

  it("returns null only when operating cash flow is missing", () => {
    const periods = joinStatements(
      [],
      [],
      [row("2026", { capex: "40" }, "annual")],
    );
    expect(buildFcf(periods)[0]!.fcf).toBeNull();
  });

  it("normalises capex sign — abs() before subtracting (prevention-log #596)", () => {
    const periods = joinStatements(
      [],
      [],
      [row("2026", { operating_cf: "150", capex: "-40" }, "annual")],
    );
    // A filer reporting capex as a negative outflow must not INFLATE FCF:
    // 150 - abs(-40) = 110, never 150 - (-40) = 190.
    expect(buildFcf(periods)[0]!.fcf).toBe(110);
  });
});
