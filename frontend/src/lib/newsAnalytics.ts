/**
 * Pure aggregation for the news-analytics drill (#593). Consumes the
 * `/news/{instrument_id}` items (`NewsItem[]`) and shapes them for three
 * charts: a bicolor sentiment trend, weekly news volume, and a source
 * breakdown. No DB, no render — table-tested in `newsAnalytics.test.ts`
 * (the "pure policy over real DB" prevention-log lesson).
 *
 * All date math is UTC (`getUTC*`): `event_time` is a tz-aware ISO string
 * whose serialized offset we must not let leak into calendar bucketing —
 * a near-midnight event must bucket identically regardless of the
 * viewer's timezone (Codex ckpt-1 #5).
 */
import type { NewsItem } from "@/api/types";

const DAY_MS = 86_400_000;

function pad2(n: number): string {
  return n < 10 ? `0${n}` : String(n);
}

/** UTC calendar day key `YYYY-MM-DD` for a tz-aware ISO timestamp. */
function utcDayKey(iso: string): string {
  const d = new Date(iso);
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

/** UTC midnight epoch-ms for a `YYYY-MM-DD` key. */
function dayKeyToMs(key: string): number {
  const p = key.split("-");
  return Date.UTC(Number(p[0]), Number(p[1]) - 1, Number(p[2]));
}

// ---------------------------------------------------------------------------
// 1. Sentiment trend — daily mean + 7-day trailing rolling mean (minPeriods=1)
// ---------------------------------------------------------------------------

const ROLLING_WINDOW_DAYS = 7;

export interface SentimentPoint {
  /** UTC calendar day `YYYY-MM-DD`. */
  readonly date: string;
  /** Mean `sentiment_score` of scored items that day; null if none. */
  readonly mean: number | null;
  /** 7-day trailing rolling mean of scored items (minPeriods=1); null if
   *  no scored item in the trailing window. This is the plotted series. */
  readonly rolling: number | null;
  /** Item count that day (all items, scored or not) — tooltip context. */
  readonly count: number;
}

export interface SentimentSeries {
  readonly points: readonly SentimentPoint[];
  /** Min / max of the non-null `rolling` values (0 / 0 when empty). */
  readonly min: number;
  readonly max: number;
  /**
   * Gradient stop offset for the bicolor split, as a fraction from the
   * TOP of the y-domain `[min(0,min) .. max(0,max)]`. Emerald above the
   * stop (≥0), red below (<0). 1 = all-non-negative (full emerald), 0 =
   * all-negative (full red) — verified by the sign-regime tests in
   * newsAnalytics.test.ts. Drives `<stop offset={splitOffset}>`.
   */
  readonly splitOffset: number;
}

interface DayBucket {
  scoredSum: number;
  scoredN: number;
  count: number;
}

/**
 * Build the daily sentiment series over a gap-free UTC day axis (earliest
 * → latest item day inclusive), so quiet days still advance the rolling
 * window rather than being skipped. Items with a null `sentiment_score`
 * count toward volume but not the mean (no fabricated zeros).
 */
export function buildSentimentSeries(items: readonly NewsItem[]): SentimentSeries {
  const buckets = new Map<string, DayBucket>();
  for (const it of items) {
    if (it.event_time === null || it.event_time === undefined) continue;
    const key = utcDayKey(it.event_time);
    const b = buckets.get(key) ?? { scoredSum: 0, scoredN: 0, count: 0 };
    b.count += 1;
    if (it.sentiment_score !== null && it.sentiment_score !== undefined) {
      b.scoredSum += it.sentiment_score;
      b.scoredN += 1;
    }
    buckets.set(key, b);
  }
  if (buckets.size === 0) {
    return { points: [], min: 0, max: 0, splitOffset: 1 };
  }

  const keys = [...buckets.keys()].sort();
  const loMs = dayKeyToMs(keys[0] ?? "");
  const hiMs = dayKeyToMs(keys[keys.length - 1] ?? "");

  // Gap-free axis: one {date, bucket} per UTC day from first to last.
  const days: { readonly date: string; readonly bucket: DayBucket | undefined }[] = [];
  for (let ms = loMs; ms <= hiMs; ms += DAY_MS) {
    const d = new Date(ms);
    const date = `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
    days.push({ date, bucket: buckets.get(date) });
  }

  const points: SentimentPoint[] = [];
  let min = Infinity;
  let max = -Infinity;
  for (let i = 0; i < days.length; i++) {
    const day = days[i];
    if (day === undefined) continue; // in-bounds; guards strict index access
    const b = day.bucket;
    const mean = b && b.scoredN > 0 ? b.scoredSum / b.scoredN : null;

    // Trailing 7-day rolling mean over scored items (minPeriods=1).
    let rollSum = 0;
    let rollN = 0;
    for (let j = Math.max(0, i - (ROLLING_WINDOW_DAYS - 1)); j <= i; j++) {
      const bj = days[j]?.bucket;
      if (bj && bj.scoredN > 0) {
        rollSum += bj.scoredSum;
        rollN += bj.scoredN;
      }
    }
    const rolling = rollN > 0 ? rollSum / rollN : null;
    if (rolling !== null) {
      if (rolling < min) min = rolling;
      if (rolling > max) max = rolling;
    }
    points.push({ date: day.date, mean, rolling, count: b?.count ?? 0 });
  }

  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    // No scored items at all → flat empty domain.
    return { points, min: 0, max: 0, splitOffset: 1 };
  }

  // y-domain spans zero so the bicolor split sits on the real zero line.
  const domLo = Math.min(0, min);
  const domHi = Math.max(0, max);
  const span = domHi - domLo;
  // Offset of the zero line from the TOP of the domain (0..1). span===0
  // (all values exactly 0) → treat as fully non-negative (emerald = 1).
  const splitOffset = span === 0 ? 1 : domHi / span;
  return { points, min, max, splitOffset };
}

// ---------------------------------------------------------------------------
// 2. News volume — count per ISO week (ISO week-year, UTC)
// ---------------------------------------------------------------------------

export interface WeeklyVolumePoint {
  /** ISO week-year label `YYYY-Www` (e.g. `2026-W26`). */
  readonly week: string;
  readonly count: number;
}

/** ISO-8601 week + week-year (UTC) for a date. Jan 1 may fall in the prior
 *  ISO year; Dec 29-31 may fall in the next (Codex ckpt-1 #5). */
export function isoWeek(d: Date): { year: number; week: number } {
  // Shift to the Thursday of this week — ISO weeks are owned by their Thursday.
  const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dayNum = (t.getUTCDay() + 6) % 7; // Mon=0 .. Sun=6
  t.setUTCDate(t.getUTCDate() - dayNum + 3);
  const isoYear = t.getUTCFullYear();
  const firstThursday = new Date(Date.UTC(isoYear, 0, 4));
  const firstDayNum = (firstThursday.getUTCDay() + 6) % 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDayNum + 3);
  const week = 1 + Math.round((t.getTime() - firstThursday.getTime()) / (7 * DAY_MS));
  return { year: isoYear, week };
}

function isoWeekKey(iso: string): string {
  const { year, week } = isoWeek(new Date(iso));
  return `${year}-W${pad2(week)}`;
}

/** Monday (UTC) of the ISO week containing `iso`, as epoch-ms. */
function isoWeekMondayMs(iso: string): number {
  const d = new Date(iso);
  const t = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  const dayNum = (new Date(t).getUTCDay() + 6) % 7; // Mon=0
  return t - dayNum * DAY_MS;
}

/**
 * Count per ISO week over a gap-free weekly axis (first → last present
 * week), so a quiet week renders as a 0 bar and a burst reads true.
 * Iterates by Monday + 7d to stay correct across year boundaries.
 */
export function buildWeeklyVolume(items: readonly NewsItem[]): WeeklyVolumePoint[] {
  const counts = new Map<string, number>();
  let minMonday = Infinity;
  let maxMonday = -Infinity;
  for (const it of items) {
    if (it.event_time === null || it.event_time === undefined) continue;
    const key = isoWeekKey(it.event_time);
    counts.set(key, (counts.get(key) ?? 0) + 1);
    const mon = isoWeekMondayMs(it.event_time);
    if (mon < minMonday) minMonday = mon;
    if (mon > maxMonday) maxMonday = mon;
  }
  if (counts.size === 0) return [];

  const out: WeeklyVolumePoint[] = [];
  for (let ms = minMonday; ms <= maxMonday; ms += 7 * DAY_MS) {
    const key = isoWeekKey(new Date(ms).toISOString());
    out.push({ week: key, count: counts.get(key) ?? 0 });
  }
  return out;
}

// ---------------------------------------------------------------------------
// 3. Source breakdown — count by source (null/blank → "Unknown")
// ---------------------------------------------------------------------------

export interface SourceSlice {
  readonly source: string;
  readonly count: number;
}

export const UNKNOWN_SOURCE = "Unknown";

/** Count by `source`, coalescing null/blank to "Unknown" (Codex ckpt-1 #6).
 *  Sorted count-desc, then name-asc for a stable legend/pie order. */
export function buildSourceBreakdown(items: readonly NewsItem[]): SourceSlice[] {
  const counts = new Map<string, number>();
  for (const it of items) {
    const raw = (it.source ?? "").trim();
    const source = raw === "" ? UNKNOWN_SOURCE : raw;
    counts.set(source, (counts.get(source) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([source, count]) => ({ source, count }))
    .sort((a, b) => b.count - a.count || a.source.localeCompare(b.source));
}

// ---------------------------------------------------------------------------
// Low-history affordance — honest dev-sparsity caption (mirrors #594's
// dev_limited posture: build the real chart, annotate the limitation).
// ---------------------------------------------------------------------------

export interface NewsCoverage {
  readonly weeks: number;
  readonly sources: number;
  /** True when the data is too thin for the charts to be representative. */
  readonly limited: boolean;
}

export function newsCoverage(items: readonly NewsItem[]): NewsCoverage {
  const weeks = new Set<string>();
  const sources = new Set<string>();
  for (const it of items) {
    if (it.event_time !== null && it.event_time !== undefined) weeks.add(isoWeekKey(it.event_time));
    const raw = (it.source ?? "").trim();
    sources.add(raw === "" ? UNKNOWN_SOURCE : raw);
  }
  return {
    weeks: weeks.size,
    sources: sources.size,
    limited: weeks.size < 2 || sources.size < 2,
  };
}
