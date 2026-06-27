import { describe, expect, it } from "vitest";

import {
  buildDensity,
  buildHeatmap,
  categorizeFiling,
  quarterRange,
} from "@/lib/filingsAnalytics";
import type { FilingQuarterCount } from "@/api/types";

function c(quarter: string, filing_type: string, count: number): FilingQuarterCount {
  return { quarter, filing_type, count };
}

describe("categorizeFiling", () => {
  it("maps the real SEC filing_type values to chart categories", () => {
    expect(categorizeFiling("10-K")).toBe("10-K");
    expect(categorizeFiling("10-K/A")).toBe("10-K");
    expect(categorizeFiling("10-Q")).toBe("10-Q");
    expect(categorizeFiling("8-K")).toBe("8-K");
    expect(categorizeFiling("8-K/A")).toBe("8-K");
    expect(categorizeFiling("DEF 14A")).toBe("Proxy");
    expect(categorizeFiling("DEFA14A")).toBe("Proxy");
    expect(categorizeFiling("SC 13G/A")).toBe("13D/G");
    expect(categorizeFiling("SCHEDULE 13D")).toBe("13D/G");
    expect(categorizeFiling("3")).toBe("Insider");
    expect(categorizeFiling("4")).toBe("Insider");
    expect(categorizeFiling("4/A")).toBe("Insider");
    expect(categorizeFiling("144")).toBe("Insider");
    expect(categorizeFiling("424B2")).toBe("Other");
    expect(categorizeFiling("6-K")).toBe("Other");
  });
});

describe("quarterRange", () => {
  it("fills the gap between earliest and latest quarter (inclusive)", () => {
    expect(quarterRange(["2024-Q4", "2024-Q1"])).toEqual([
      "2024-Q1",
      "2024-Q2",
      "2024-Q3",
      "2024-Q4",
    ]);
  });
  it("crosses a year boundary", () => {
    expect(quarterRange(["2023-Q3", "2024-Q1"])).toEqual(["2023-Q3", "2023-Q4", "2024-Q1"]);
  });
  it("returns [] for no quarters", () => {
    expect(quarterRange([])).toEqual([]);
  });
});

describe("buildDensity", () => {
  it("buckets by quarter, excludes insider, fills gaps, totals the shown categories", () => {
    const rows = buildDensity([
      c("2024-Q1", "8-K", 3),
      c("2024-Q1", "10-Q", 1),
      c("2024-Q1", "4", 9), // insider — excluded from shown counts + total
      c("2024-Q3", "10-K", 1), // skips Q2 -> gap-filled to 0
    ]);
    expect(rows.map((r) => r.quarter)).toEqual(["2024-Q1", "2024-Q2", "2024-Q3"]);
    const q1 = rows[0]!;
    expect(q1["8-K"]).toBe(3);
    expect(q1["10-Q"]).toBe(1);
    expect(q1.total).toBe(4); // insider's 9 is NOT counted
    const q2 = rows[1]!;
    expect(q2.total).toBe(0); // gap-filled empty quarter
    expect(rows[2]!["10-K"]).toBe(1);
  });
});

describe("buildHeatmap", () => {
  it("exposes per-cell counts + the max, excluding insider", () => {
    const h = buildHeatmap([
      c("2024-Q1", "8-K", 5),
      c("2024-Q1", "4", 99), // insider — excluded, must not become max
      c("2024-Q2", "8-K", 2),
    ]);
    expect(h.quarters).toEqual(["2024-Q1", "2024-Q2"]);
    expect(h.get("8-K", "2024-Q1")).toBe(5);
    expect(h.get("8-K", "2024-Q2")).toBe(2);
    expect(h.get("10-K", "2024-Q1")).toBe(0);
    expect(h.max).toBe(5); // not 99 (insider excluded)
  });
});
