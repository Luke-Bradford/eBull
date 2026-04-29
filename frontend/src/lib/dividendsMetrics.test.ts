import { describe, expect, it } from "vitest";

import type { DividendPeriod } from "@/api/instruments";
import type { InstrumentFinancialRow } from "@/api/types";
import {
  buildCumulativeDps,
  buildDpsSeries,
  buildPayoutRatio,
  buildYieldOnCost,
} from "@/lib/dividendsMetrics";

function div(
  partial: Partial<DividendPeriod> & { period_end_date: string },
): DividendPeriod {
  return {
    period_end_date: partial.period_end_date,
    period_type: partial.period_type ?? "Q1",
    fiscal_year: partial.fiscal_year ?? 2026,
    fiscal_quarter: partial.fiscal_quarter ?? null,
    dps_declared: partial.dps_declared ?? null,
    dividends_paid: partial.dividends_paid ?? null,
    reported_currency: partial.reported_currency ?? "USD",
  };
}

function cashflowRow(
  period_end: string,
  values: Record<string, string | null>,
): InstrumentFinancialRow {
  return { period_end, period_type: "FY", values };
}

describe("buildDpsSeries", () => {
  it("sorts chronologically and parses dps as numbers", () => {
    const out = buildDpsSeries([
      div({ period_end_date: "2026-03-31", dps_declared: "0.50" }),
      div({ period_end_date: "2025-12-31", dps_declared: "0.45" }),
    ]);
    expect(out[0]!.period_end_date).toBe("2025-12-31");
    expect(out[0]!.dps).toBe(0.45);
    expect(out[1]!.dps).toBe(0.5);
  });

  it("emits null for malformed dps without dropping the row", () => {
    const out = buildDpsSeries([
      div({ period_end_date: "2026-03-31", dps_declared: "not-a-num" }),
    ]);
    expect(out).toHaveLength(1);
    expect(out[0]!.dps).toBeNull();
  });
});

describe("buildCumulativeDps", () => {
  it("running-sums DPS in chronological order; null source rows render as gaps", () => {
    const out = buildCumulativeDps([
      div({ period_end_date: "2025-03-31", dps_declared: "0.10" }),
      div({ period_end_date: "2025-06-30", dps_declared: null }),
      div({ period_end_date: "2025-09-30", dps_declared: "0.12" }),
    ]);
    expect(out.map((p) => p.cumulative_dps)).toEqual([0.1, null, 0.22]);
  });

  it("propagates the most recent non-null currency forward", () => {
    const out = buildCumulativeDps([
      div({ period_end_date: "2025-03-31", dps_declared: "0.10", reported_currency: "USD" }),
      div({ period_end_date: "2025-06-30", dps_declared: "0.10", reported_currency: null }),
    ]);
    expect(out[1]!.currency).toBe("USD");
  });

  it("drops the FY row when quarterly rows exist for the same fiscal year (cadence dedupe)", () => {
    // Real-world risk: SEC filers' 10-K row landing alongside the
    // four 10-Q rows. Without dedupe the cumulative would jump by
    // the year's total a second time.
    const out = buildCumulativeDps([
      div({ period_end_date: "2025-03-31", period_type: "Q1", fiscal_year: 2025, dps_declared: "0.10" }),
      div({ period_end_date: "2025-06-30", period_type: "Q2", fiscal_year: 2025, dps_declared: "0.10" }),
      div({ period_end_date: "2025-09-30", period_type: "Q3", fiscal_year: 2025, dps_declared: "0.10" }),
      div({ period_end_date: "2025-12-31", period_type: "Q4", fiscal_year: 2025, dps_declared: "0.10" }),
      div({ period_end_date: "2025-12-31", period_type: "FY", fiscal_year: 2025, dps_declared: "0.40" }),
    ]);
    // 4 quarterly entries, FY dropped → cumulative tops out at 0.40
    expect(out).toHaveLength(4);
    expect(out[3]!.cumulative_dps).toBeCloseTo(0.4);
  });

  it("keeps the FY row when no quarterly rows exist for that fiscal year", () => {
    const out = buildCumulativeDps([
      div({ period_end_date: "2024-12-31", period_type: "FY", fiscal_year: 2024, dps_declared: "0.36" }),
      div({ period_end_date: "2025-12-31", period_type: "FY", fiscal_year: 2025, dps_declared: "0.40" }),
    ]);
    expect(out).toHaveLength(2);
    expect(out[1]!.cumulative_dps).toBeCloseTo(0.76);
  });
});

describe("buildPayoutRatio", () => {
  it("computes dividends_paid / FCF as percentage with abs() on dividends sign quirk", () => {
    const out = buildPayoutRatio([
      cashflowRow("2025-12-31", {
        operating_cf: "1000",
        capex: "200",
        dividends_paid: "-200", // some issuers report negative outflow
      }),
    ]);
    // FCF = 800; payout = 200 / 800 = 25%
    expect(out[0]!.payout_pct).toBe(25);
  });

  it("returns null when FCF is non-positive (ratio undefined)", () => {
    const out = buildPayoutRatio([
      cashflowRow("2025-12-31", {
        operating_cf: "100",
        capex: "300",
        dividends_paid: "50",
      }),
    ]);
    expect(out[0]!.payout_pct).toBeNull();
  });

  it("returns null when either dividends_paid or FCF input is missing", () => {
    const out = buildPayoutRatio([
      cashflowRow("2025-12-31", {
        operating_cf: "1000",
        capex: "200",
        // no dividends_paid
      }),
      cashflowRow("2026-12-31", {
        operating_cf: "1000",
        // no capex
        dividends_paid: "100",
      }),
    ]);
    expect(out[0]!.payout_pct).toBeNull();
    expect(out[1]!.payout_pct).toBeNull();
  });

  it("sorts the output chronologically", () => {
    const out = buildPayoutRatio([
      cashflowRow("2026-12-31", {
        operating_cf: "1000",
        capex: "200",
        dividends_paid: "100",
      }),
      cashflowRow("2025-12-31", {
        operating_cf: "800",
        capex: "100",
        dividends_paid: "70",
      }),
    ]);
    expect(out[0]!.period_end_date).toBe("2025-12-31");
    expect(out[1]!.period_end_date).toBe("2026-12-31");
  });
});

describe("buildYieldOnCost", () => {
  const history: DividendPeriod[] = [
    div({ period_end_date: "2025-03-31", dps_declared: "0.10", fiscal_year: 2025 }),
    div({ period_end_date: "2025-06-30", dps_declared: "0.10", fiscal_year: 2025 }),
    div({ period_end_date: "2025-09-30", dps_declared: "0.12", fiscal_year: 2025 }),
    div({ period_end_date: "2025-12-31", dps_declared: "0.12", fiscal_year: 2025 }),
    div({ period_end_date: "2026-03-31", dps_declared: "0.13", fiscal_year: 2026 }),
  ];

  it("buckets by fiscal year, sums DPS, divides by avg_entry as percent", () => {
    const out = buildYieldOnCost(history, 50);
    expect(out).toEqual([
      { fiscal_year: 2025, annual_dps: 0.44, yoc_pct: (0.44 / 50) * 100 },
      { fiscal_year: 2026, annual_dps: 0.13, yoc_pct: (0.13 / 50) * 100 },
    ]);
  });

  it("returns null when no position is held (avg_entry is null)", () => {
    expect(buildYieldOnCost(history, null)).toBeNull();
  });

  it("returns null when avg_entry is zero or negative", () => {
    expect(buildYieldOnCost(history, 0)).toBeNull();
    expect(buildYieldOnCost(history, -5)).toBeNull();
  });

  it("returns an empty array when no DPS rows have a numeric value", () => {
    expect(
      buildYieldOnCost(
        [
          div({ period_end_date: "2025-03-31", dps_declared: null }),
          div({ period_end_date: "2025-06-30", dps_declared: null }),
        ],
        50,
      ),
    ).toEqual([]);
  });

  it("dedupes FY rows when quarterly rows exist for the same fiscal year", () => {
    // Same risk as buildCumulativeDps: the 10-K aggregate landing on
    // top of four 10-Q rows would double the year's sum and the YoC
    // tracker would render a phantom doubling.
    const mixed: DividendPeriod[] = [
      div({ period_end_date: "2025-03-31", period_type: "Q1", fiscal_year: 2025, dps_declared: "0.10" }),
      div({ period_end_date: "2025-06-30", period_type: "Q2", fiscal_year: 2025, dps_declared: "0.10" }),
      div({ period_end_date: "2025-09-30", period_type: "Q3", fiscal_year: 2025, dps_declared: "0.10" }),
      div({ period_end_date: "2025-12-31", period_type: "Q4", fiscal_year: 2025, dps_declared: "0.10" }),
      div({ period_end_date: "2025-12-31", period_type: "FY", fiscal_year: 2025, dps_declared: "0.40" }),
    ];
    const out = buildYieldOnCost(mixed, 50);
    expect(out).toEqual([
      { fiscal_year: 2025, annual_dps: 0.4, yoc_pct: (0.4 / 50) * 100 },
    ]);
  });
});
