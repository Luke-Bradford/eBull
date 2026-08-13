/**
 * Pure view helpers for the risk drill page (#591 PR-C).
 *
 * Display-only: range→window mapping, client-side date slicing of the
 * full series, per-metric status copy. NONE of this is risk math — the
 * backend (risk_metrics.py) owns every estimator; these functions only
 * pick and slice the already-computed payload for presentation.
 */

import type { DrawdownPoint, RiskStatus, RiskWindowMetrics } from "@/api/types";

/** The range picker tokens. 5Y ≡ full given the ~4yr data ceiling, so both
 *  5Y and All map to the persisted `full` window (spec: no separate 5Y row). */
export type RiskRange = "1Y" | "3Y" | "5Y" | "All";

export const RISK_RANGES: readonly RiskRange[] = ["1Y", "3Y", "5Y", "All"];

/** Range → persisted `window_key` for the scalar tiles. */
export function rangeToWindowKey(range: RiskRange): string {
  switch (range) {
    case "1Y":
      return "1y";
    case "3Y":
      return "3y";
    case "5Y":
    case "All":
      return "full";
  }
}

/** Lookback in days for slicing the time-series charts; null = whole series. */
export function rangeDays(range: RiskRange): number | null {
  switch (range) {
    case "1Y":
      return 365;
    case "3Y":
      return 365 * 3;
    case "5Y":
    case "All":
      return null;
  }
}

/** Parse a wire Decimal string to a finite number, or null. */
export function parseDecimal(v: string | null): number | null {
  if (v === null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/** Pick the persisted window matching the selected range, or null. */
export function pickWindow(
  windows: ReadonlyArray<RiskWindowMetrics>,
  range: RiskRange,
): RiskWindowMetrics | null {
  const key = rangeToWindowKey(range);
  return windows.find((w) => w.window_key === key) ?? null;
}

/**
 * Slice a dated series to the selected range, counting back from `asOf`
 * (the series' as-of snapshot date). ISO `YYYY-MM-DD` strings sort
 * lexically, so the cutoff comparison is a plain string compare. Returns
 * the whole series when range has no bound or `asOf` is null.
 */
export function sliceByRange<T extends { readonly date: string }>(
  points: ReadonlyArray<T>,
  asOf: string | null,
  range: RiskRange,
): T[] {
  const days = rangeDays(range);
  if (days === null || asOf === null) return [...points];
  const end = new Date(asOf);
  if (Number.isNaN(end.getTime())) return [...points];
  end.setUTCDate(end.getUTCDate() - days);
  const cutoff = end.toISOString().slice(0, 10);
  return points.filter((p) => p.date >= cutoff);
}

/**
 * Re-baseline a range-sliced underwater curve to the SELECTED window's own
 * high-water mark, so the chart agrees with the window-local `max_drawdown` /
 * `current_drawdown` tiles.
 *
 * The backend emits ONE full-history curve keyed to the ALL-TIME running peak
 * (`risk_metrics.py::drawdown_curve`); {@link sliceByRange} only cuts the
 * x-axis, so a window that opens below the all-time high shows all-time-relative
 * drawdown while the scalar tiles reset their peak at the window start. This
 * pure transform re-anchors the sliced series to the window peak using ONLY the
 * drawdown series — prices cancel:
 *
 *   d'(t) = (1 + d(t)) / (1 + max_{s≤t} d(s)) − 1
 *
 * `max_{s≤t} d(s)` is the running maximum (least-negative) drawdown so far in
 * the slice — the point closest to the window's own peak. It resets to 0 at
 * each fresh all-time high (where d(t)=0), so this reproduces the window-local
 * peak/trough math exactly. Display-only normalization, NOT a new estimator
 * (settled-decisions §Risk-metrics: `instrument_risk_metrics` is display/
 * evidence, no `metric_version` bump for presentation).
 *
 * Points whose drawdown fails to parse pass through unchanged and do not
 * advance the running peak. The denominator `1 + peak` is always > 0: the
 * backend curve only includes valid closes (> 0, `risk_metrics.py::_valid_close`)
 * so every `d = close/peak − 1 > −1`, hence the running max `peak > −1`.
 */
export function rebaseDrawdownToWindowPeak(
  points: ReadonlyArray<DrawdownPoint>,
): DrawdownPoint[] {
  let peak: number | null = null; // running max of parsed drawdown (≤ 0)
  return points.map((p) => {
    const d = parseDecimal(p.drawdown);
    if (d === null) return p;
    if (peak === null || d > peak) peak = d;
    const rebased = (1 + d) / (1 + peak) - 1;
    return { ...p, drawdown: String(rebased) };
  });
}

/**
 * Operator copy for a flagging metric status, or null when the metric is
 * `ok` / unflagged (caller renders the chart). One shared source so the
 * page and any future surface read the same phrase.
 */
export function riskStatusCopy(status: string | null): string | null {
  switch (status as RiskStatus | null) {
    case "insufficient_history":
      return "Not enough price history yet for this metric.";
    case "partial_window":
      return "History shorter than this window — figure shown is provisional.";
    case "benchmark_missing":
      return "No benchmark (SPY) data available to compare against.";
    case "benchmark_insufficient_history":
      return "Benchmark overlap too short to fit a reliable beta.";
    case "invalid_price_chain":
      return "Price history has gaps that break the return series.";
    case "stale":
      return "Backing price snapshot is stale; figure may be out of date.";
    case "ok":
    case null:
    default:
      return null;
  }
}
