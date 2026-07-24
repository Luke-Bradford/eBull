import { describe, expect, it } from "vitest";

import { drawdownDomainMin } from "@/components/risk/riskCharts";

describe("drawdownDomainMin", () => {
  it("keeps 0 as the top of the axis for a shallow series (the #1908 clipping bug)", () => {
    // Before: recharts' auto-domain fitted [-0.02, -0.002] and the peak line
    // at 0 fell off the top of the chart.
    expect(drawdownDomainMin(-0.02)).toBe(-0.02);
  });

  it("does not clip a deep drawdown", () => {
    expect(drawdownDomainMin(-0.743)).toBe(-0.743);
  });

  it("holds the anchor at 0 for a series that never drew down", () => {
    expect(drawdownDomainMin(0)).toBe(0);
  });

  it("still returns 0 as the LOWER bound when a malformed row makes even the minimum positive", () => {
    // Note this is only the lower bound. The `0` upper bound is deliberately
    // left expandable (no `allowDataOverflow`), so such a row shows up as a
    // visible anomaly above the peak line rather than being clipped away.
    expect(drawdownDomainMin(0.05)).toBe(0);
  });
});
