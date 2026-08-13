import { describe, expect, it } from "vitest";

import {
  buildHeatmap,
  buildRadar,
  buildScatter,
  peerCoverage,
} from "@/lib/peerComparison";
import type { CandleBar, PeerComparison, PeerFactor, PeerInstrument } from "@/api/types";

function factor(p: Partial<PeerFactor> & { key: string; better_when: "higher" | "lower" }): PeerFactor {
  return {
    key: p.key,
    label: p.label ?? p.key,
    instrument_value: p.instrument_value ?? null,
    cohort_median: p.cohort_median ?? null,
    cohort_n: p.cohort_n ?? 100,
    dev_limited: p.dev_limited ?? false,
    better_when: p.better_when,
  };
}

function peer(symbol: string, factors: Record<string, number | null>): PeerInstrument {
  return { instrument_id: symbol.length, symbol, company_name: `${symbol} Inc`, size_proxy: 1e9, factors };
}

function pc(factors: PeerFactor[], peers: PeerInstrument[]): PeerComparison {
  return {
    symbol: "AAA",
    instrument_id: 1,
    cohort_sic: "3571",
    cohort_sic_label: "Electronic Computers",
    cohort_sic_level: 4,
    cohort_member_count: 951,
    factors,
    peers,
  };
}

function cb(date: string, close: string | null): CandleBar {
  return { date, open: null, high: null, low: null, close, volume: null };
}

describe("buildRadar", () => {
  it("normalizes outward=better for higher-is-better factors", () => {
    const r = buildRadar(
      pc(
        [factor({ key: "roe", better_when: "higher", instrument_value: 0.9, cohort_median: 0.1 })],
        [peer("F", { roe: 0.5 })],
      ),
    );
    // cohort {0.1, 0.5, 0.9}: instrument 0.9 → 1.0 (best), median 0.1 → 0.0 (worst)
    expect(r[0]!.instrument).toBeCloseTo(1, 6);
    expect(r[0]!.median).toBeCloseTo(0, 6);
    expect(r[0]!.instrumentRaw).toBe(0.9);
  });

  it("inverts for lower-is-better factors (low P/E scores outward)", () => {
    const r = buildRadar(
      pc(
        [factor({ key: "pe", better_when: "lower", instrument_value: 40, cohort_median: 50 })],
        [peer("F", { pe: 60 })],
      ),
    );
    // cohort {40,50,60}: instrument 40 → norm 0 → oriented 1.0 (lowest P/E = best)
    expect(r[0]!.instrument).toBeCloseTo(1, 6);
    expect(r[0]!.median).toBeCloseTo(0.5, 6);
  });

  it("degenerate cohort (all equal) → 0.5 neutral", () => {
    const r = buildRadar(
      pc([factor({ key: "x", better_when: "higher", instrument_value: 5, cohort_median: 5 })], [peer("F", { x: 5 })]),
    );
    expect(r[0]!.instrument).toBeCloseTo(0.5, 6);
    expect(r[0]!.median).toBeCloseTo(0.5, 6);
  });

  it("gaps a null instrument_value AND a null cohort_median", () => {
    const r = buildRadar(
      pc(
        [
          factor({ key: "g", better_when: "higher", instrument_value: null, cohort_median: 2.6 }),
          factor({ key: "h", better_when: "higher", instrument_value: 0.3, cohort_median: null }),
        ],
        [peer("F", { g: 0.1, h: 0.2 })],
      ),
    );
    expect(r[0]!.instrument).toBeNull();
    expect(r[0]!.median).not.toBeNull();
    expect(r[1]!.median).toBeNull();
    expect(r[1]!.instrument).not.toBeNull();
  });
});

describe("buildHeatmap", () => {
  it("pins the instrument as the first row and scores cells", () => {
    const h = buildHeatmap(
      pc(
        [factor({ key: "roe", better_when: "higher", instrument_value: 0.9, cohort_median: 0.1 })],
        [peer("F", { roe: 0.5 }), peer("GM", { roe: null })],
      ),
    );
    expect(h.rows[0]!.isInstrument).toBe(true);
    expect(h.rows[0]!.symbol).toBe("AAA");
    expect(h.rows[0]!.cells.roe!.score).toBeCloseTo(1, 6);
    expect(h.rows[1]!.cells.roe!.score).toBeCloseTo(0.5, 6); // F=0.5 mid
    expect(h.rows[2]!.cells.roe!.score).toBeNull(); // GM null
    expect(h.rows[2]!.cells.roe!.raw).toBeNull();
  });
});

describe("buildScatter", () => {
  it("excludes a peer return whose prev candle is missing (same-interval guard)", () => {
    const candles: Record<string, CandleBar[]> = {
      AAA: [cb("2026-06-01", "100"), cb("2026-06-02", "110"), cb("2026-06-03", "121")],
      F: [cb("2026-06-01", "100"), cb("2026-06-02", "105"), cb("2026-06-03", "110.25")],
      // GM skips 06-02 → its 06-03 return would span 2 days; must be excluded
      GM: [cb("2026-06-01", "100"), cb("2026-06-03", "130")],
    };
    const s = buildScatter("AAA", ["F", "GM"], candles);
    expect(s.points).toHaveLength(2);
    // Both points: only F qualifies (GM missing 06-02 for both pairs)
    expect(s.points.every((p) => p.nPeers === 1)).toBe(true);
    // Instrument +10%/day, F +5%/day → instrument outperformed (x > y, below diagonal)
    expect(s.points[0]!.x).toBeCloseTo(0.1, 6);
    expect(s.points[0]!.y).toBeCloseTo(0.05, 6);
    expect(s.points[0]!.x).toBeGreaterThan(s.points[0]!.y);
  });

  it("takes the median across qualifying peers and a symmetric domain", () => {
    const candles: Record<string, CandleBar[]> = {
      AAA: [cb("2026-06-01", "100"), cb("2026-06-02", "100")], // 0% return
      F: [cb("2026-06-01", "100"), cb("2026-06-02", "110")], // +10%
      GM: [cb("2026-06-01", "100"), cb("2026-06-02", "120")], // +20%
      KO: [cb("2026-06-01", "100"), cb("2026-06-02", "130")], // +30%
    };
    const s = buildScatter("AAA", ["F", "GM", "KO"], candles);
    expect(s.points[0]!.x).toBeCloseTo(0, 6);
    expect(s.points[0]!.y).toBeCloseTo(0.2, 6); // median(0.1,0.2,0.3)
    expect(s.points[0]!.nPeers).toBe(3);
    expect(s.domain).toBeCloseTo(0.2, 6);
  });

  it("skips non-positive / unparseable closes without throwing", () => {
    const candles: Record<string, CandleBar[]> = {
      AAA: [cb("2026-06-01", "0"), cb("2026-06-02", "abc"), cb("2026-06-03", "121"), cb("2026-06-04", "133.1")],
      F: [cb("2026-06-03", "100"), cb("2026-06-04", "105")],
    };
    const s = buildScatter("AAA", ["F"], candles);
    // Only the 06-03→06-04 pair has valid closes on both sides for both symbols
    expect(s.points).toHaveLength(1);
    expect(s.points[0]!.date).toBe("2026-06-04");
  });
});

describe("peerCoverage", () => {
  it("collects dev_limited keys and the min cohort_n", () => {
    const cov = peerCoverage(
      pc(
        [
          factor({ key: "pe", better_when: "lower", dev_limited: true, cohort_n: 2 }),
          factor({ key: "roe", better_when: "higher", cohort_n: 813 }),
          factor({ key: "rev", better_when: "higher", cohort_n: 39 }),
        ],
        [],
      ),
    );
    expect(cov.devLimitedKeys).toEqual(["pe"]);
    expect(cov.minCohortN).toBe(2);
  });
});
