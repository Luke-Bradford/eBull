import { describe, expect, it } from "vitest";

import type { RiskWindowMetrics } from "@/api/types";
import {
  parseDecimal,
  pickWindow,
  rangeDays,
  rangeToWindowKey,
  rebaseDrawdownToWindowPeak,
  riskStatusCopy,
  sliceByRange,
} from "./riskView";

function makeWindow(key: string): RiskWindowMetrics {
  // Only window_key matters for pickWindow; rest are null placeholders.
  return {
    window_key: key,
    cagr: null,
    excess_cagr_vs_spy: null,
    max_drawdown: null,
    current_drawdown: null,
    vol_annualized: null,
    beta: null,
    beta_r2: null,
    calmar: null,
    skew: null,
    excess_kurtosis: null,
    var_5: null,
    worst_day: null,
    best_day: null,
    trailing_1m: null,
    trailing_3m: null,
    trailing_6m: null,
    trailing_1y: null,
    excess_trailing_1m: null,
    excess_trailing_3m: null,
    excess_trailing_6m: null,
    excess_trailing_1y: null,
    n_returns: null,
    beta_n_obs: null,
    window_days: null,
    cagr_status: null,
    vol_status: null,
    beta_status: null,
    drawdown_status: null,
    distribution_status: null,
    calmar_status: null,
    trailing_status: null,
    excess_cagr_status: null,
  };
}

describe("rangeToWindowKey", () => {
  it("maps each range to its persisted window; 5Y and All both fold to full", () => {
    expect(rangeToWindowKey("1Y")).toBe("1y");
    expect(rangeToWindowKey("3Y")).toBe("3y");
    expect(rangeToWindowKey("5Y")).toBe("full");
    expect(rangeToWindowKey("All")).toBe("full");
  });
});

describe("rangeDays", () => {
  it("bounds 1Y/3Y and leaves 5Y/All unbounded (whole series)", () => {
    expect(rangeDays("1Y")).toBe(365);
    expect(rangeDays("3Y")).toBe(365 * 3);
    expect(rangeDays("5Y")).toBeNull();
    expect(rangeDays("All")).toBeNull();
  });
});

describe("parseDecimal", () => {
  it("parses a finite Decimal string", () => {
    expect(parseDecimal("0.1234")).toBeCloseTo(0.1234);
    expect(parseDecimal("-0.05")).toBeCloseTo(-0.05);
  });
  it("returns null for null and non-finite", () => {
    expect(parseDecimal(null)).toBeNull();
    expect(parseDecimal("not-a-number")).toBeNull();
  });
});

describe("pickWindow", () => {
  const windows = [makeWindow("1y"), makeWindow("3y"), makeWindow("full")];
  it("finds the window matching the range", () => {
    expect(pickWindow(windows, "1Y")?.window_key).toBe("1y");
    expect(pickWindow(windows, "3Y")?.window_key).toBe("3y");
    expect(pickWindow(windows, "5Y")?.window_key).toBe("full");
    expect(pickWindow(windows, "All")?.window_key).toBe("full");
  });
  it("returns null when the mapped window is absent (thin history)", () => {
    expect(pickWindow([makeWindow("full")], "3Y")).toBeNull();
  });
});

describe("sliceByRange", () => {
  const points = [
    { date: "2023-01-02" },
    { date: "2024-06-12" },
    { date: "2025-06-12" },
    { date: "2026-06-12" },
  ];
  it("cuts to the trailing year, counting back from asOf", () => {
    const out = sliceByRange(points, "2026-06-12", "1Y");
    expect(out.map((p) => p.date)).toEqual(["2025-06-12", "2026-06-12"]);
  });
  it("keeps the whole series for All / 5Y", () => {
    expect(sliceByRange(points, "2026-06-12", "All")).toHaveLength(4);
    expect(sliceByRange(points, "2026-06-12", "5Y")).toHaveLength(4);
  });
  it("returns the whole series when asOf is null", () => {
    expect(sliceByRange(points, null, "1Y")).toHaveLength(4);
  });
});

describe("rebaseDrawdownToWindowPeak", () => {
  const dp = (date: string, drawdown: string) => ({ date, drawdown });

  it("returns [] for an empty slice", () => {
    expect(rebaseDrawdownToWindowPeak([])).toEqual([]);
  });

  it("re-anchors to the window peak; the least-negative point becomes 0", () => {
    // Slice opens 40% underwater relative to the all-time peak, recovers to
    // −30% (the window's own high-water mark), then falls to the trough.
    const sliced = [dp("d1", "-0.40"), dp("d2", "-0.30"), dp("d3", "-0.55")];
    const out = rebaseDrawdownToWindowPeak(sliced);
    const v = out.map((p) => Number(p.drawdown));
    // Window peak = d2 (−0.30). d'(d2) = (1−0.30)/(1−0.30) − 1 = 0.
    expect(v[1]).toBeCloseTo(0, 10);
    // d'(d1) = (1−0.40)/(1−0.40) − 1 = 0 (d1 is the running peak until d2).
    expect(v[0]).toBeCloseTo(0, 10);
    // d'(d3) = (1−0.55)/(1−0.30) − 1 = 0.45/0.70 − 1 ≈ −0.357143.
    expect(v[2]).toBeCloseTo(0.45 / 0.7 - 1, 10);
  });

  it("matches the GME 1Y trough example from #1963 (−0.2798)", () => {
    // Curve minimum sits at −0.5940 against an all-time peak; the window's own
    // high-water mark within the slice is −0.4364, so re-basing the trough
    // yields −0.2798 == the Max drawdown tile.
    const sliced = [dp("a", "-0.4364"), dp("b", "-0.5940")];
    const out = rebaseDrawdownToWindowPeak(sliced);
    expect(Number(out[1]!.drawdown)).toBeCloseTo(
      (1 - 0.594) / (1 - 0.4364) - 1,
      6,
    );
    expect(Number(out[1]!.drawdown)).toBeCloseTo(-0.2798, 3);
  });

  it("resets the anchor at a fresh all-time high (drawdown returns to 0)", () => {
    const sliced = [dp("a", "-0.20"), dp("b", "0"), dp("c", "-0.10")];
    const out = rebaseDrawdownToWindowPeak(sliced).map((p) =>
      Number(p.drawdown),
    );
    expect(out[0]).toBeCloseTo(0, 10); // running peak until the true high
    expect(out[1]).toBeCloseTo(0, 10); // new all-time high resets the anchor
    expect(out[2]).toBeCloseTo(-0.1, 10); // −10% below that fresh peak
  });

  it("passes unparseable points through unchanged without advancing the peak", () => {
    const sliced = [dp("a", "-0.30"), dp("b", "nope"), dp("c", "-0.50")];
    const out = rebaseDrawdownToWindowPeak(sliced);
    expect(out[1]!.drawdown).toBe("nope");
    // Peak is still −0.30 (the bad point did not advance it).
    expect(Number(out[2]!.drawdown)).toBeCloseTo((1 - 0.5) / (1 - 0.3) - 1, 10);
  });
});

describe("riskStatusCopy", () => {
  it("returns null for ok / null so the caller renders the chart", () => {
    expect(riskStatusCopy("ok")).toBeNull();
    expect(riskStatusCopy(null)).toBeNull();
  });
  it("returns operator copy for each flagging status", () => {
    expect(riskStatusCopy("insufficient_history")).toMatch(/not enough/i);
    expect(riskStatusCopy("benchmark_missing")).toMatch(/benchmark/i);
    expect(riskStatusCopy("partial_window")).toMatch(/provisional/i);
    expect(riskStatusCopy("stale")).toMatch(/stale/i);
  });
});
