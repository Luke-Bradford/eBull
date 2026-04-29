/**
 * Dividend derived-metric helpers (#590).
 *
 * Pure functions over the rows shipped by:
 *   - GET /instruments/{symbol}/dividends           → DividendPeriod[]
 *   - GET /instruments/{symbol}/financials?statement=cashflow&period=annual
 *                                                   → InstrumentFinancialRow[]
 *   - GET /portfolio/instruments/{id}               → InstrumentPositionDetail
 *
 * The page owns the fetching; every chart consumes pre-computed
 * series from these helpers. Calling convention: helpers accept
 * the **chronological** array (oldest → newest by `period_end_date`).
 *
 * Null-handling: missing fields propagate as `null` rather than
 * dropping the row, so the time axis stays continuous and the chart
 * renders a gap. Recharts' `connectNulls` is left at its default
 * `false` everywhere so a missing quarter is visually obvious.
 */

import type { DividendPeriod } from "@/api/instruments";
import type { InstrumentFinancialRow } from "@/api/types";

function num(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function compareDate(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/** Cadence dedupe. Some issuers' `dividend_history` rows mix
 *  quarterly entries and an `FY` aggregate covering the same fiscal
 *  year (the annual 10-K's summary picked up alongside per-quarter
 *  10-Q filings). Aggregating both into a cumulative or yield-on-cost
 *  total double-counts the year. Rule: when any quarterly cadence
 *  row exists for a fiscal_year, drop the `FY` row for that year;
 *  when only FY rows exist, keep them. The DPS-line chart skips this
 *  filter so the raw, audit-grade data still surfaces. */
function dropOverlappingFy(
  history: ReadonlyArray<DividendPeriod>,
): DividendPeriod[] {
  const yearsWithQuarterly = new Set<number>();
  for (const p of history) {
    if (p.period_type !== "FY") {
      yearsWithQuarterly.add(p.fiscal_year);
    }
  }
  return history.filter(
    (p) => !(p.period_type === "FY" && yearsWithQuarterly.has(p.fiscal_year)),
  );
}

// ---------------------------------------------------------------------------
// 1. DPS series (line chart input)
// ---------------------------------------------------------------------------

export interface DpsPoint {
  readonly period_end_date: string;
  readonly dps: number | null;
  readonly currency: string | null;
}

/** Chronological DPS-per-period series. Quarterly + FY rows are
 *  preserved together in source order — the chart renders whatever
 *  cadence the issuer files. Issuers that only file FY rows render
 *  a flat-then-jump line; mixed-cadence is rare in practice but is
 *  not normalised here so that the source data stays auditable. */
export function buildDpsSeries(
  history: ReadonlyArray<DividendPeriod>,
): DpsPoint[] {
  return [...history]
    .sort((a, b) => compareDate(a.period_end_date, b.period_end_date))
    .map((p) => ({
      period_end_date: p.period_end_date,
      dps: num(p.dps_declared),
      currency: p.reported_currency,
    }));
}

// ---------------------------------------------------------------------------
// 2. Cumulative DPS series (line chart input)
// ---------------------------------------------------------------------------

export interface CumulativePoint {
  readonly period_end_date: string;
  /** Running total. `null` when the source row had no DPS — recharts
   *  renders a gap there, which reads as "missing" rather than the
   *  misleading "issuer paid zero this period" that a held-flat line
   *  would imply. The running counter still tracks across the gap so
   *  the next non-null period resumes at the correct cumulative. */
  readonly cumulative_dps: number | null;
  readonly currency: string | null;
}

/** Running sum of DPS in chronological order, with cadence dedupe
 *  so years with both quarterly and FY rows don't double-count. Null
 *  source rows propagate as `null` in the output — the running
 *  counter still advances on non-null rows, but the chart shows a
 *  gap at the missing period. */
export function buildCumulativeDps(
  history: ReadonlyArray<DividendPeriod>,
): CumulativePoint[] {
  const sorted = [...dropOverlappingFy(history)].sort((a, b) =>
    compareDate(a.period_end_date, b.period_end_date),
  );
  let running = 0;
  let lastCurrency: string | null = null;
  return sorted.map((p) => {
    const dps = num(p.dps_declared);
    if (p.reported_currency !== null) lastCurrency = p.reported_currency;
    if (dps === null) {
      return {
        period_end_date: p.period_end_date,
        cumulative_dps: null,
        currency: lastCurrency,
      };
    }
    running += dps;
    return {
      period_end_date: p.period_end_date,
      cumulative_dps: running,
      currency: lastCurrency,
    };
  });
}

// ---------------------------------------------------------------------------
// 3. Payout ratio series (line chart input)
// ---------------------------------------------------------------------------

export interface PayoutRatioPoint {
  readonly period_end_date: string;
  /** Dividends-paid ÷ FCF as a percentage. Null when either side is
   *  missing or FCF is non-positive (the ratio is ill-defined for a
   *  cash-burning year — clamping rather than rendering a meaningless
   *  three-digit "ratio" against a $0.50M FCF tail). */
  readonly payout_pct: number | null;
}

interface CashflowRow {
  readonly period_end: string;
  readonly dividends_paid: number | null;
  readonly fcf: number | null;
}

function joinCashflow(
  rows: ReadonlyArray<InstrumentFinancialRow>,
): CashflowRow[] {
  return rows
    .map((r) => {
      const op = num(r.values["operating_cf"]);
      const cx = num(r.values["capex"]);
      const fcf = op !== null && cx !== null ? op - cx : null;
      const div = num(r.values["dividends_paid"]);
      // SEC XBRL `PaymentsOfDividends` is a positive outflow. Some
      // issuers under-report by emitting it as a negative number;
      // normalise to absolute so the payout ratio's sign matches the
      // textbook "what fraction of FCF went to shareholders" reading.
      const divAbs = div !== null ? Math.abs(div) : null;
      return {
        period_end: r.period_end,
        dividends_paid: divAbs,
        fcf,
      };
    })
    .sort((a, b) => compareDate(a.period_end, b.period_end));
}

/** Annual payout-ratio series. Quarterly cashflow is too noisy to
 *  read as a payout ratio (a single one-off capex blip pushes the
 *  fraction to 200%); the page passes the annual cashflow rows. */
export function buildPayoutRatio(
  rows: ReadonlyArray<InstrumentFinancialRow>,
): PayoutRatioPoint[] {
  return joinCashflow(rows).map((r) => {
    if (r.dividends_paid === null || r.fcf === null) {
      return { period_end_date: r.period_end, payout_pct: null };
    }
    if (r.fcf <= 0) {
      // FCF-negative year — ratio is undefined. Render as gap.
      return { period_end_date: r.period_end, payout_pct: null };
    }
    return {
      period_end_date: r.period_end,
      payout_pct: (r.dividends_paid / r.fcf) * 100,
    };
  });
}

// ---------------------------------------------------------------------------
// 4. Yield-on-cost series (line chart input)
// ---------------------------------------------------------------------------

export interface YieldOnCostPoint {
  readonly fiscal_year: number;
  readonly annual_dps: number;
  readonly yoc_pct: number;
}

/** Yield-on-cost progression. Buckets `history` by fiscal_year, sums
 *  DPS within each, divides by the operator's `avg_entry` (per-share
 *  cost basis) and emits a percentage. Returns `null` when no
 *  position is held or the avg entry is non-positive — the page
 *  hides the chart entirely in that case rather than rendering a
 *  divide-by-zero "Infinity%" line.
 *
 *  Why fiscal_year rather than rolling-trailing-12: the YoC story
 *  operators care about is "as DPS grows over the years I've held,
 *  my fixed cost basis earns me more" — a per-FY view tells that
 *  story cleanly. Quarter-to-quarter ripples obscure the trend
 *  without changing the message. */
export function buildYieldOnCost(
  history: ReadonlyArray<DividendPeriod>,
  avgEntry: number | null,
): YieldOnCostPoint[] | null {
  if (avgEntry === null || avgEntry <= 0) return null;
  const deduped = dropOverlappingFy(history);
  const map = new Map<number, number>();
  for (const p of deduped) {
    const dps = num(p.dps_declared);
    if (dps === null) continue;
    map.set(p.fiscal_year, (map.get(p.fiscal_year) ?? 0) + dps);
  }
  if (map.size === 0) return [];
  return [...map.entries()]
    .sort(([a], [b]) => a - b)
    .map(([fy, total]) => ({
      fiscal_year: fy,
      annual_dps: total,
      yoc_pct: (total / avgEntry) * 100,
    }));
}
