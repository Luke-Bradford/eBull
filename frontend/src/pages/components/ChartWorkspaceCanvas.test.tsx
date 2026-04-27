/**
 * Unit tests for ChartWorkspaceCanvas pure math helpers (#576 Phase 2).
 *
 * We test only the exported `computeSMA` and `computeEMA` functions —
 * lightweight-charts cannot render in jsdom (Canvas API absent) so the
 * React component itself is not exercised here. The component's integration
 * is covered by ChartPage.test.tsx via a stub.
 */
import { describe, expect, it } from "vitest";
import { computeSMA, computeEMA } from "./ChartWorkspaceCanvas";

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
