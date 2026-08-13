import { describe, expect, it } from "vitest";

import type { ReportSnapshot } from "@/api/reports";
import { buildTrailingSeries, dec, formatPeriodRange } from "@/components/reports/snapshotMath";

function snapRow(
  periodEnd: string,
  periodReturn: string | null,
  benchmarkClose: string | null,
): ReportSnapshot {
  return {
    snapshot_id: Math.floor(Math.random() * 1e6),
    report_type: "weekly",
    period_start: periodEnd,
    period_end: periodEnd,
    computed_at: `${periodEnd}T00:00:00Z`,
    snapshot_json: {
      schema_version: 2,
      report_type: "weekly",
      period_start: periodEnd,
      period_end: periodEnd,
      performance: {
        portfolio_value: "1000",
        period_return: periodReturn,
        benchmark:
          benchmarkClose !== null
            ? {
                symbol: "SPX500",
                label: "S&P 500 (price index)",
                close_start: null,
                close_end: benchmarkClose,
                return_pct: null,
              }
            : null,
        observations: 0,
        fx_mode: "generation_date",
        method: "modified_dietz_v1",
      },
    },
  };
}

describe("dec", () => {
  it("parses Decimal strings and rejects junk", () => {
    expect(dec("0.523810")).toBeCloseTo(0.52381);
    expect(dec(null)).toBeNull();
    expect(dec(undefined)).toBeNull();
    expect(dec("")).toBeNull();
    expect(dec("not-a-number")).toBeNull();
    // Number.isFinite guard — Infinity must not leak into formatters.
    expect(dec("Infinity")).toBeNull();
  });
});

describe("buildTrailingSeries", () => {
  it("chain-links stamped period returns from 100", () => {
    const points = buildTrailingSeries([
      snapRow("2026-06-07", "0.10", null),
      snapRow("2026-05-31", null, null), // first snapshot: no baseline
    ]);
    expect(points).toHaveLength(2);
    expect(points[0]?.period_end).toBe("2026-05-31");
    expect(points[0]?.portfolio).toBe(100);
    expect(points[1]?.portfolio).toBeCloseTo(110);
  });

  it("indexes benchmark closes to 100 at first observation", () => {
    const points = buildTrailingSeries([
      snapRow("2026-06-07", "0.0", "5100"),
      snapRow("2026-05-31", null, "5000"),
    ]);
    expect(points[0]?.benchmark).toBe(100);
    expect(points[1]?.benchmark).toBeCloseTo(102);
  });

  it("skips v1 snapshots and carries null-return points flat", () => {
    const v1: ReportSnapshot = {
      snapshot_id: 1,
      report_type: "weekly",
      period_start: "2026-05-24",
      period_end: "2026-05-24",
      computed_at: "2026-05-24T00:00:00Z",
      snapshot_json: { pnl: {} }, // no schema_version → v1
    };
    const points = buildTrailingSeries([
      v1,
      snapRow("2026-05-31", null, null),
      snapRow("2026-06-07", null, null), // null mid-chain → flat
    ]);
    expect(points).toHaveLength(2);
    expect(points[1]?.portfolio).toBe(100);
  });

  it("windows to trailing N and re-bases both series to 100", () => {
    const rows = [
      snapRow("2026-06-21", "0.10", "5500"),
      snapRow("2026-06-14", "0.10", "5250"),
      snapRow("2026-06-07", "0.10", "5000"),
      snapRow("2026-05-31", null, "4800"),
    ];
    const points = buildTrailingSeries(rows, 2);
    expect(points).toHaveLength(2);
    // Window start re-bases to exactly 100 — a raw slice of the longer
    // chain would start at 121.
    expect(points[0]?.portfolio).toBe(100);
    expect(points[1]?.portfolio).toBeCloseTo(110);
    expect(points[0]?.benchmark).toBe(100);
    expect(points[1]?.benchmark).toBeCloseTo((5500 / 5250) * 100);
  });

  it("returns empty for an all-v1 list", () => {
    const v1: ReportSnapshot = {
      snapshot_id: 1,
      report_type: "weekly",
      period_start: "2026-05-24",
      period_end: "2026-05-24",
      computed_at: "2026-05-24T00:00:00Z",
      snapshot_json: {},
    };
    expect(buildTrailingSeries([v1])).toHaveLength(0);
  });
});

describe("formatPeriodRange", () => {
  it("same-month range collapses the start month", () => {
    expect(formatPeriodRange("2026-06-02", "2026-06-08")).toBe("2–8 Jun 2026");
  });
  it("cross-month range names both months", () => {
    expect(formatPeriodRange("2026-05-25", "2026-06-08")).toBe("25 May – 8 Jun 2026");
  });
  it("cross-year range names both years", () => {
    expect(formatPeriodRange("2025-12-29", "2026-01-04")).toBe("29 Dec 2025 – 4 Jan 2026");
  });
});
