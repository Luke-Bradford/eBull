/**
 * Tests for the chart-data gap filler.
 */
import { describe, expect, it } from "vitest";

import { fillIntrasessionGaps, type NormalisedBar } from "@/lib/chartData";

const T0 = Math.floor(Date.UTC(2026, 3, 27, 14, 30) / 1000); // 14:30 UTC

function bar(time: number, close: string): NormalisedBar {
  return { time, open: close, high: close, low: close, close, volume: "100" };
}

describe("fillIntrasessionGaps", () => {
  it("returns input unchanged when fewer than 2 bars", () => {
    expect(fillIntrasessionGaps([], 60, 7200)).toEqual([]);
    const single: NormalisedBar[] = [bar(T0, "10")];
    expect(fillIntrasessionGaps(single, 60, 7200)).toEqual(single);
  });

  it("does NOT fill when bars are contiguous", () => {
    const bars: NormalisedBar[] = [
      bar(T0, "10"),
      bar(T0 + 60, "11"),
      bar(T0 + 120, "12"),
    ];
    expect(fillIntrasessionGaps(bars, 60, 7200)).toEqual(bars);
  });

  it("fills a 5-minute gap with carry-forward synthetic bars", () => {
    const bars: NormalisedBar[] = [
      bar(T0, "10"),
      bar(T0 + 360, "12"), // 6 minute gap → 5 missing minutes
    ];
    const filled = fillIntrasessionGaps(bars, 60, 7200);
    expect(filled).toHaveLength(7); // 2 originals + 5 synthetic
    // First original kept
    expect(filled[0]).toEqual(bars[0]);
    // 5 synthetic bars carrying close=10
    for (let i = 1; i <= 5; i++) {
      expect(filled[i]).toEqual({
        time: T0 + i * 60,
        open: "10",
        high: "10",
        low: "10",
        close: "10",
        volume: "0",
      });
    }
    // Real bar at end
    expect(filled[6]).toEqual(bars[1]);
  });

  it("does NOT fill gaps wider than maxGapSeconds (overnight closure)", () => {
    const bars: NormalisedBar[] = [
      bar(T0, "10"),
      bar(T0 + 12 * 60 * 60, "12"), // 12-hour gap, exceeds 2h cap
    ];
    const filled = fillIntrasessionGaps(bars, 60, 2 * 60 * 60);
    // No synthetic fill — both originals preserved as-is.
    expect(filled).toEqual(bars);
  });

  it("fills the boundary case at exactly maxGapSeconds", () => {
    const bars: NormalisedBar[] = [
      bar(T0, "10"),
      bar(T0 + 7200, "11"), // exactly 2h gap
    ];
    const filled = fillIntrasessionGaps(bars, 60, 7200);
    // 2h / 60s = 120 minutes, minus the two originals = 119 synthetic
    expect(filled).toHaveLength(121);
    expect(filled[0]).toEqual(bars[0]);
    expect(filled[120]).toEqual(bars[1]);
  });

  it("synthetic volume is always '0' regardless of original volume", () => {
    const bars: NormalisedBar[] = [
      { time: T0, open: "10", high: "10", low: "10", close: "10", volume: "999999" },
      { time: T0 + 180, open: "11", high: "11", low: "11", close: "11", volume: "111" },
    ];
    const filled = fillIntrasessionGaps(bars, 60, 7200);
    expect(filled[1]?.volume).toBe("0");
    expect(filled[2]?.volume).toBe("0");
  });

  it("works for daily range (86400s bucket)", () => {
    const day = 86400;
    const monday = Math.floor(Date.UTC(2026, 3, 27) / 1000);
    const wednesday = monday + 2 * day;
    const bars: NormalisedBar[] = [bar(monday, "100"), bar(wednesday, "102")];
    const filled = fillIntrasessionGaps(bars, day, 7 * day);
    // Tuesday filled
    expect(filled).toHaveLength(3);
    expect(filled[1]?.time).toBe(monday + day);
    expect(filled[1]?.close).toBe("100");
  });
});
