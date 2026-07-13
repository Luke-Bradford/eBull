/**
 * Pure aggregation for the peer-comparison drill (#594). Consumes
 * `PeerComparison` (+ peer candles) and shapes the radar, cohort heatmap, and
 * return scatter. No DB, no render — table-tested in `peerComparison.test.ts`
 * (the "pure policy over real DB" prevention-log lesson).
 *
 * Normalization is DISPLAY-only (evidence layer) — it never feeds scoring, so
 * the scoring "no cohort-relative normalization" ban does not apply
 * (docs/settled-decisions.md §Scoring; mirrors instrument_risk_metrics).
 * `better_when` is always read from the API, never hardcoded.
 */
import type { CandleBar, PeerComparison } from "@/api/types";

type Orientation = "higher" | "lower";

interface Cohort {
  readonly lo: number;
  readonly hi: number;
}

/**
 * Per-factor scale across the visible cohort: the instrument value, the cohort
 * median, and every peer's value (nulls excluded). Returns null when nothing is
 * available for that factor.
 */
function factorCohort(
  pc: PeerComparison,
  key: string,
  instrumentValue: number | null,
  cohortMedian: number | null,
): Cohort | null {
  const vals: number[] = [];
  if (instrumentValue !== null) vals.push(instrumentValue);
  if (cohortMedian !== null) vals.push(cohortMedian);
  for (const p of pc.peers) {
    const v = p.factors[key];
    if (v !== null && v !== undefined) vals.push(v);
  }
  if (vals.length === 0) return null;
  return { lo: Math.min(...vals), hi: Math.max(...vals) };
}

/**
 * Normalize `value` to [0,1] within `cohort`, oriented so OUTWARD = better.
 * Degenerate cohort (hi==lo) → 0.5 (neutral). null value / cohort → null.
 */
function orientedScore(
  value: number | null,
  cohort: Cohort | null,
  betterWhen: Orientation,
): number | null {
  if (value === null || cohort === null) return null;
  const { lo, hi } = cohort;
  const norm = hi === lo ? 0.5 : (value - lo) / (hi - lo);
  return betterWhen === "lower" ? 1 - norm : norm;
}

// ---------------------------------------------------------------------------
// 1. Multi-factor radar — instrument vs cohort median (2 overlays)
// ---------------------------------------------------------------------------

export interface RadarPoint {
  readonly key: string;
  readonly label: string;
  readonly devLimited: boolean;
  readonly cohortN: number;
  readonly betterWhen: Orientation;
  /** Normalized scores [0,1], outward=better (recharts dataKeys). null = gap. */
  readonly instrument: number | null;
  readonly median: number | null;
  /** Raw values for the tooltip (the score is layout-only, never shown). */
  readonly instrumentRaw: number | null;
  readonly medianRaw: number | null;
}

export function buildRadar(pc: PeerComparison): RadarPoint[] {
  return pc.factors.map((f) => {
    const cohort = factorCohort(pc, f.key, f.instrument_value, f.cohort_median);
    return {
      key: f.key,
      label: f.label,
      devLimited: f.dev_limited,
      cohortN: f.cohort_n,
      betterWhen: f.better_when,
      instrument: orientedScore(f.instrument_value, cohort, f.better_when),
      median: orientedScore(f.cohort_median, cohort, f.better_when),
      instrumentRaw: f.instrument_value,
      medianRaw: f.cohort_median,
    };
  });
}

// ---------------------------------------------------------------------------
// 2. Cohort heatmap — (instrument + peers) rows × factor columns
// ---------------------------------------------------------------------------

export interface HeatFactor {
  readonly key: string;
  readonly label: string;
  readonly devLimited: boolean;
  readonly cohortN: number;
}

export interface HeatCell {
  readonly raw: number | null;
  /** Oriented [0,1] (outward=better) within the factor column; null = empty. */
  readonly score: number | null;
}

export interface HeatRow {
  readonly symbol: string;
  readonly companyName: string | null;
  readonly isInstrument: boolean;
  readonly cells: Record<string, HeatCell>;
}

export interface Heatmap {
  readonly factors: HeatFactor[];
  readonly rows: HeatRow[];
}

/**
 * Heatmap over the same per-factor cohort as the radar (instrument + median +
 * peers), so the radar and heatmap colour scales agree. The instrument is the
 * pinned first row; cells coloured by oriented score in the chart.
 */
export function buildHeatmap(pc: PeerComparison): Heatmap {
  const factors: HeatFactor[] = pc.factors.map((f) => ({
    key: f.key,
    label: f.label,
    devLimited: f.dev_limited,
    cohortN: f.cohort_n,
  }));

  // Cohort + orientation per factor, computed once.
  const cohorts = new Map<string, { cohort: Cohort | null; betterWhen: Orientation }>();
  for (const f of pc.factors) {
    cohorts.set(f.key, {
      cohort: factorCohort(pc, f.key, f.instrument_value, f.cohort_median),
      betterWhen: f.better_when,
    });
  }

  const cellsFor = (lookup: (key: string) => number | null): Record<string, HeatCell> => {
    const out: Record<string, HeatCell> = {};
    for (const f of pc.factors) {
      const raw = lookup(f.key);
      const c = cohorts.get(f.key);
      out[f.key] = {
        raw,
        score: c ? orientedScore(raw, c.cohort, c.betterWhen) : null,
      };
    }
    return out;
  };

  const instrumentRow: HeatRow = {
    symbol: pc.symbol,
    companyName: null,
    isInstrument: true,
    cells: cellsFor((key) => pc.factors.find((f) => f.key === key)?.instrument_value ?? null),
  };
  const peerRows: HeatRow[] = pc.peers.map((p) => ({
    symbol: p.symbol,
    companyName: p.company_name,
    isInstrument: false,
    cells: cellsFor((key) => p.factors[key] ?? null),
  }));

  return { factors, rows: [instrumentRow, ...peerRows] };
}

// ---------------------------------------------------------------------------
// 3. Peer return scatter — instrument vs median-peer SAME-DAY return
// ---------------------------------------------------------------------------

export interface ScatterPoint {
  readonly date: string;
  /** Instrument same-day simple return. */
  readonly x: number;
  /** Median peer same-day return over the SAME interval. */
  readonly y: number;
  readonly nPeers: number;
}

export interface ScatterData {
  readonly points: ScatterPoint[];
  /** Symmetric axis half-extent so the y=x diagonal centres (min 0.01). */
  readonly domain: number;
}

function parseClose(raw: string | null): number | null {
  if (raw === null) return null;
  const n = Number.parseFloat(raw);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  if (s.length % 2 === 1) return s[mid] ?? 0;
  return ((s[mid - 1] ?? 0) + (s[mid] ?? 0)) / 2;
}

/**
 * Scatter the instrument's same-day return (x) vs the median peer return (y).
 * Iterates the instrument's own consecutive candle pairs; a peer contributes a
 * return for that pair ONLY if it has closes at BOTH the prev and current dates
 * (same interval — a peer that skipped `prev` would otherwise yield a multi-day
 * return masquerading as a one-day one). Below the y=x diagonal (y < x) = the
 * instrument OUTperformed the sector that day (NOT temporal lead/lag).
 */
export function buildScatter(
  instrumentSymbol: string,
  peerSymbols: readonly string[],
  candlesBySymbol: Record<string, CandleBar[]>,
): ScatterData {
  const inst = (candlesBySymbol[instrumentSymbol] ?? [])
    .map((b) => ({ date: b.date, close: parseClose(b.close) }))
    .filter((b): b is { date: string; close: number } => b.close !== null)
    .sort((a, b) => a.date.localeCompare(b.date));

  const peerMaps = peerSymbols.map((sym) => {
    const m = new Map<string, number>();
    for (const b of candlesBySymbol[sym] ?? []) {
      const c = parseClose(b.close);
      if (c !== null) m.set(b.date, c);
    }
    return m;
  });

  const points: ScatterPoint[] = [];
  let domain = 0;
  for (let i = 1; i < inst.length; i++) {
    const prev = inst[i - 1];
    const cur = inst[i];
    if (prev === undefined || cur === undefined) continue;
    const x = cur.close / prev.close - 1;

    const peerRets: number[] = [];
    for (const m of peerMaps) {
      const cp = m.get(prev.date);
      const cc = m.get(cur.date);
      if (cp !== undefined && cc !== undefined && cp > 0) peerRets.push(cc / cp - 1);
    }
    if (peerRets.length === 0) continue;

    const y = median(peerRets);
    points.push({ date: cur.date, x, y, nPeers: peerRets.length });
    domain = Math.max(domain, Math.abs(x), Math.abs(y));
  }

  return { points, domain: domain > 0 ? domain : 0.01 };
}

// ---------------------------------------------------------------------------
// Coverage / dev-honesty affordances
// ---------------------------------------------------------------------------

export interface PeerCoverage {
  /** Factors flagged thin (price-gated like P/E, or <20% cohort coverage) — greyed in the UI. */
  readonly devLimitedKeys: string[];
  /** Min cohort_n across factors — low n means a noisy median. */
  readonly minCohortN: number;
}

export function peerCoverage(pc: PeerComparison): PeerCoverage {
  const devLimitedKeys = pc.factors.filter((f) => f.dev_limited).map((f) => f.key);
  const minCohortN = pc.factors.reduce(
    (m, f) => Math.min(m, f.cohort_n),
    pc.factors.length > 0 ? Infinity : 0,
  );
  return { devLimitedKeys, minCohortN: Number.isFinite(minCohortN) ? minCohortN : 0 };
}
