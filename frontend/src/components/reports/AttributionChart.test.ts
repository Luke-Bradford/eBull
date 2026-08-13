/**
 * Pure-aggregator coverage for §4.4 (chart internals stay untested in
 * jsdom per the InsiderByOfficer convention).
 */
import { describe, expect, it } from "vitest";

import { buildAttributionRows } from "@/components/reports/AttributionChart";

describe("buildAttributionRows", () => {
  it("merges top-5 contributors and detractors sorted by delta desc", () => {
    const rows = buildAttributionRows({
      contributors: [
        { instrument_id: 1, symbol: "AAPL", pnl_delta: "120.00", pnl_pct: "0.06" },
        { instrument_id: 2, symbol: "MSFT", pnl_delta: "80.00", pnl_pct: null },
      ],
      drags: [{ instrument_id: 3, symbol: "GME", pnl_delta: "-30.00", pnl_pct: "-0.03" }],
    });
    expect(rows.map((r) => r.symbol)).toEqual(["AAPL", "MSFT", "GME"]);
    expect(rows[0]?.delta).toBeCloseTo(120);
    expect(rows[2]?.delta).toBeCloseTo(-30);
  });

  it("caps each side at 5 rows", () => {
    const many = Array.from({ length: 8 }, (_, i) => ({
      instrument_id: i,
      symbol: `S${i}`,
      pnl_delta: `${100 - i}`,
      pnl_pct: null,
    }));
    const rows = buildAttributionRows({ contributors: many, drags: [] });
    expect(rows).toHaveLength(5);
  });

  it("drops rows with unparseable deltas", () => {
    const rows = buildAttributionRows({
      contributors: [{ instrument_id: 1, symbol: "AAPL", pnl_delta: null, pnl_pct: null }],
      drags: [],
    });
    expect(rows).toHaveLength(0);
  });
});
