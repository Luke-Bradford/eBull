/**
 * Pure helpers for the period-statement page (#1592 child 2).
 *
 * Decimal-string parsing, the client-side trailing performance line
 * (assembled from the fetched list of immutable snapshots — assembly
 * of stamped points, never recomputation; spec §3.1), and period
 * label formatting.
 */

import type { ReportSnapshot } from "@/api/reports";
import { isSnapshotV2, type SnapshotV2 } from "@/api/reportSnapshot";

/**
 * Parse a builder Decimal-string to a number for display. Guards with
 * Number.isFinite (not isNaN) so "Infinity"/overflow strings degrade
 * to null instead of leaking into formatters (prevention log:
 * "Infinity/out-of-range numeric inputs bypass Number.isNaN guards").
 */
export function dec(raw: string | null | undefined): number | null {
  if (raw === null || raw === undefined || raw === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

export interface TrailingPoint {
  /** Snapshot period_end (ISO date) — x-axis category. */
  period_end: string;
  /** Chain-linked portfolio index, 100 = window start. */
  portfolio: number;
  /** Benchmark close indexed to 100 at its first observation; null
   *  when that snapshot stamped no benchmark close. */
  benchmark: number | null;
}

/**
 * Chain-link each snapshot's own stamped `performance.period_return`
 * into an indexed line (both series = 100 at window start). v1
 * snapshots (no performance key) are skipped — the line only carries
 * stamped v2 points. Benchmark is indexed from each snapshot's
 * `benchmark.close_end` sample (spec §4.2: sampled at snapshot dates).
 *
 * `windowSize` keeps the trailing N points (13 weekly / 12 monthly,
 * §4.2) and RE-BASES both series so the window start reads 100 — a
 * slice of a longer chain would otherwise start mid-index.
 */
export function buildTrailingSeries(
  snapshots: ReportSnapshot[],
  windowSize?: number,
): TrailingPoint[] {
  const v2: SnapshotV2[] = [];
  for (const row of snapshots) {
    if (isSnapshotV2(row.snapshot_json)) v2.push(row.snapshot_json);
  }
  v2.sort((a, b) => (a.period_end < b.period_end ? -1 : a.period_end > b.period_end ? 1 : 0));

  const points: TrailingPoint[] = [];
  let index = 100;
  let benchmarkBase: number | null = null;
  for (const snap of v2) {
    const r = dec(snap.performance.period_return);
    // First point anchors at 100; later points compound their own
    // stamped return. A null mid-chain return (no prior-snapshot
    // baseline at generation) carries the index flat — a stated
    // point, not invented performance.
    if (points.length > 0 && r !== null) {
      index = index * (1 + r);
    }
    const close = dec(snap.performance.benchmark?.close_end ?? null);
    if (close !== null && benchmarkBase === null) benchmarkBase = close;
    points.push({
      period_end: snap.period_end,
      portfolio: index,
      benchmark: close !== null && benchmarkBase !== null ? (close / benchmarkBase) * 100 : null,
    });
  }

  if (windowSize === undefined || points.length <= windowSize) return points;
  const windowed = points.slice(-windowSize);
  const portfolioBase = windowed[0]?.portfolio ?? 100;
  const benchBase = windowed.find((p) => p.benchmark !== null)?.benchmark ?? null;
  return windowed.map((p) => ({
    period_end: p.period_end,
    portfolio: (p.portfolio / portfolioBase) * 100,
    benchmark: p.benchmark !== null && benchBase !== null ? (p.benchmark / benchBase) * 100 : null,
  }));
}

const DAY_MONTH = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
const DAY_MONTH_YEAR = new Intl.DateTimeFormat("en-GB", {
  day: "numeric",
  month: "short",
  year: "numeric",
  timeZone: "UTC",
});

/** "2–8 Jun 2026" / "25 May – 8 Jun 2026" / "1 Dec 2025 – 4 Jan 2026". */
export function formatPeriodRange(startIso: string, endIso: string): string {
  const start = new Date(startIso);
  const end = new Date(endIso);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return "—";
  if (start.getUTCFullYear() === end.getUTCFullYear()) {
    if (start.getUTCMonth() === end.getUTCMonth()) {
      return `${start.getUTCDate()}–${DAY_MONTH_YEAR.format(end)}`;
    }
    return `${DAY_MONTH.format(start)} – ${DAY_MONTH_YEAR.format(end)}`;
  }
  return `${DAY_MONTH_YEAR.format(start)} – ${DAY_MONTH_YEAR.format(end)}`;
}
