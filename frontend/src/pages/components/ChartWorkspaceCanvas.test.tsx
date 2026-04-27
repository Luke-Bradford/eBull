/**
 * Unit tests for ChartWorkspaceCanvas pure math helpers (#576 Phase 2 + Phase 3).
 *
 * We test only the exported pure functions —
 * lightweight-charts cannot render in jsdom (Canvas API absent) so the
 * React component itself is not exercised here. The component's integration
 * is covered by ChartPage.test.tsx via a stub.
 */
import { describe, expect, it } from "vitest";
import {
  computeEMA,
  computeSMA,
  linearRegressionLine,
  normalizeToPercent,
  rangeChannel,
} from "./ChartWorkspaceCanvas";

describe("computeSMA", () => {
  it("returns correct rolling average for period 3", () => {
    // [1,2,3,4,5] with period 3: first valid at i=2
    const result = computeSMA([1, 2, 3, 4, 5], 3);
    expect(result[0]).toBeNull();
    expect(result[1]).toBeNull();
    expect(result[2]).toBeCloseTo(2); // (1+2+3)/3
    expect(result[3]).toBeCloseTo(3); // (2+3+4)/3
    expect(result[4]).toBeCloseTo(4); // (3+4+5)/3
  });

  it("returns all null when closes shorter than period", () => {
    const result = computeSMA([1, 2], 3);
    expect(result).toHaveLength(2);
    expect(result.every((v) => v === null)).toBe(true);
  });

  it("returns single value when closes length equals period", () => {
    const result = computeSMA([2, 4, 6], 3);
    expect(result[0]).toBeNull();
    expect(result[1]).toBeNull();
    expect(result[2]).toBeCloseTo(4); // (2+4+6)/3
  });

  it("returns all null for empty array", () => {
    const result = computeSMA([], 20);
    expect(result).toHaveLength(0);
  });

  it("handles period 1 — returns each value", () => {
    const result = computeSMA([3, 6, 9], 1);
    expect(result[0]).toBeCloseTo(3);
    expect(result[1]).toBeCloseTo(6);
    expect(result[2]).toBeCloseTo(9);
  });
});

describe("computeEMA", () => {
  it("seeds with SMA and applies EMA formula for period 3", () => {
    // closes: [1, 2, 3, 4, 5], period 3
    // Seed (i=2): SMA([1,2,3]) = 2
    // k = 2/(3+1) = 0.5
    // i=3: 4*0.5 + 2*0.5 = 3
    // i=4: 5*0.5 + 3*0.5 = 4
    const result = computeEMA([1, 2, 3, 4, 5], 3);
    expect(result[0]).toBeNull();
    expect(result[1]).toBeNull();
    expect(result[2]).toBeCloseTo(2);
    expect(result[3]).toBeCloseTo(3);
    expect(result[4]).toBeCloseTo(4);
  });

  it("returns all null when closes shorter than period", () => {
    const result = computeEMA([1, 2], 3);
    expect(result).toHaveLength(2);
    expect(result.every((v) => v === null)).toBe(true);
  });

  it("returns all null for empty array", () => {
    const result = computeEMA([], 20);
    expect(result).toHaveLength(0);
  });

  it("returns single value when closes length equals period", () => {
    // seed = SMA = (10+20+30)/3 = 20; no further values
    const result = computeEMA([10, 20, 30], 3);
    expect(result[0]).toBeNull();
    expect(result[1]).toBeNull();
    expect(result[2]).toBeCloseTo(20);
  });

  it("EMA reacts faster than SMA to a price spike", () => {
    // flat series then a spike: [1,1,1,1,1,10]
    // SMA(5) at index 5 = (1+1+1+1+10)/5 = 2.8
    // EMA(5) at index 5 > SMA(5) because EMA weighs the spike more heavily
    const closes = [1, 1, 1, 1, 1, 10];
    const sma = computeSMA(closes, 5);
    const ema = computeEMA(closes, 5);
    const lastSma = sma[sma.length - 1]!;
    const lastEma = ema[ema.length - 1]!;
    expect(lastEma).toBeGreaterThan(lastSma);
  });
});

describe("normalizeToPercent", () => {
  it("returns empty array for empty input", () => {
    expect(normalizeToPercent([])).toEqual([]);
  });

  it("first element is always 0% (base = itself)", () => {
    const result = normalizeToPercent([100, 110, 90]);
    expect(result[0]).toBe(0);
  });

  it("flat series → all zeros", () => {
    const result = normalizeToPercent([50, 50, 50, 50]);
    for (const v of result) {
      expect(v).toBeCloseTo(0);
    }
  });

  it("computes correct percent change from base", () => {
    // base=100; 110 → +10%; 90 → -10%; 150 → +50%
    const result = normalizeToPercent([100, 110, 90, 150]);
    expect(result[0]).toBeCloseTo(0);
    expect(result[1]).toBeCloseTo(10);
    expect(result[2]).toBeCloseTo(-10);
    expect(result[3]).toBeCloseTo(50);
  });

  it("returns null array when base is zero", () => {
    const result = normalizeToPercent([0, 10, 20]);
    expect(result.every((v) => v === null)).toBe(true);
  });

  it("returns null array when base is non-finite", () => {
    const result = normalizeToPercent([NaN, 10, 20]);
    expect(result.every((v) => v === null)).toBe(true);
  });

  it("single element returns [0]", () => {
    const result = normalizeToPercent([42]);
    expect(result).toHaveLength(1);
    expect(result[0]).toBeCloseTo(0);
  });
});

describe("linearRegressionLine", () => {
  it("returns all null for empty array", () => {
    const result = linearRegressionLine([]);
    expect(result).toHaveLength(0);
  });

  it("returns [null] for single-element array", () => {
    const result = linearRegressionLine([5]);
    expect(result).toHaveLength(1);
    expect(result[0]).toBeNull();
  });

  it("exact line [1,2,3,4,5] → slope=1, intercept=1", () => {
    // x=[0,1,2,3,4], y=[1,2,3,4,5] → y = x + 1
    const result = linearRegressionLine([1, 2, 3, 4, 5]);
    expect(result[0]).toBeCloseTo(1);
    expect(result[1]).toBeCloseTo(2);
    expect(result[2]).toBeCloseTo(3);
    expect(result[3]).toBeCloseTo(4);
    expect(result[4]).toBeCloseTo(5);
  });

  it("flat series → returns constant line equal to the value", () => {
    const result = linearRegressionLine([7, 7, 7, 7]);
    for (const v of result) {
      expect(v).toBeCloseTo(7);
    }
  });

  it("two-point line returns exact endpoints", () => {
    // x=[0,1], y=[2,4] → slope=2, intercept=2 → [2, 4]
    const result = linearRegressionLine([2, 4]);
    expect(result[0]).toBeCloseTo(2);
    expect(result[1]).toBeCloseTo(4);
  });

  it("descending series → negative slope", () => {
    const result = linearRegressionLine([10, 8, 6, 4, 2]);
    // slope should be -2
    expect(result[0]).toBeCloseTo(10);
    expect(result[4]).toBeCloseTo(2);
  });

  it("output length matches input length", () => {
    const closes = [1, 3, 2, 5, 4, 7];
    const result = linearRegressionLine(closes);
    expect(result).toHaveLength(closes.length);
  });
});

describe("rangeChannel", () => {
  it("returns {high: null, low: null} for empty array", () => {
    expect(rangeChannel([])).toEqual({ high: null, low: null });
  });

  it("single element: high === low === that element", () => {
    expect(rangeChannel([42])).toEqual({ high: 42, low: 42 });
  });

  it("returns correct high and low", () => {
    const result = rangeChannel([5, 3, 8, 1, 7]);
    expect(result.high).toBe(8);
    expect(result.low).toBe(1);
  });

  it("all same values → high === low", () => {
    const result = rangeChannel([4, 4, 4, 4]);
    expect(result.high).toBe(4);
    expect(result.low).toBe(4);
  });

  it("handles negative values", () => {
    const result = rangeChannel([-5, -2, -10, -1]);
    expect(result.high).toBe(-1);
    expect(result.low).toBe(-10);
  });
});
