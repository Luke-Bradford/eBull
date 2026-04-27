/**
 * Unified chart-data fetcher (#601).
 *
 * The chart UI exposes 9 range buttons (1D · 5D · 1M · 3M · 6M · YTD · 1Y · 5Y · MAX).
 * Backend serves them via two endpoints:
 *
 *   * Daily (existing): `/instruments/{symbol}/candles?range=...` — reads
 *     from the persisted `price_daily` series. Used for YTD / 1Y / 5Y / MAX.
 *   * Intraday (#600):  `/instruments/{symbol}/intraday-candles?interval=...&count=...`
 *     — eToro REST pass-through with TTL cache, no DB persistence.
 *     Used for 1D / 5D / 1M / 3M / 6M.
 *
 * This module owns the (range → endpoint) dispatch table and the
 * normalisation that flattens both responses into a single
 * `NormalisedChartCandles` shape, so chart components can render either
 * source without branching on endpoint.
 *
 * Lightweight-charts consumes time as a UTC epoch-second number
 * (`UTCTimestamp`). Both daily (`YYYY-MM-DD`) and intraday
 * (`YYYY-MM-DDTHH:MM:SSZ`) values normalise into that one shape.
 */

import {
  fetchInstrumentCandles,
  fetchInstrumentIntradayCandles,
} from "@/api/instruments";
import type {
  CandleRange,
  ChartRange,
  IntradayInterval,
} from "@/api/types";

export type ChartDataKind = "intraday" | "daily";

interface IntradayPlan {
  readonly kind: "intraday";
  readonly interval: IntradayInterval;
  readonly count: number;
}

interface DailyPlan {
  readonly kind: "daily";
  readonly range: CandleRange;
}

export type ChartRangePlan = IntradayPlan | DailyPlan;

/**
 * Range → endpoint translation table.
 *
 * Counts target ~250–600 bars for sane chart density:
 *   * 1d  →   ~390 1-min bars (one US trading day)
 *   * 5d  →   ~390 5-min bars (five US trading days)
 *   * 1m  →   ~330 30-min bars (~22 trading days)
 *   * 3m  →   ~525 1-h bars (~65 trading days)
 *   * 6m  →   ~390 4-h bars (~130 trading days)
 *
 * Daily-tier ranges defer to `price_daily`. Operator's "5Y" is mapped
 * to the daily endpoint capped at 1000 bars per #603 (eToro's hard
 * ceiling) — about 4 calendar years of trading-day price points.
 */
export const CHART_RANGE_PLAN: Record<ChartRange, ChartRangePlan> = {
  "1d": { kind: "intraday", interval: "OneMinute", count: 390 },
  "5d": { kind: "intraday", interval: "FiveMinutes", count: 390 },
  "1m": { kind: "intraday", interval: "ThirtyMinutes", count: 330 },
  "3m": { kind: "intraday", interval: "OneHour", count: 525 },
  "6m": { kind: "intraday", interval: "FourHours", count: 390 },
  ytd: { kind: "daily", range: "ytd" },
  "1y": { kind: "daily", range: "1y" },
  "5y": { kind: "daily", range: "5y" },
  max: { kind: "daily", range: "max" },
};

export function planFor(range: ChartRange): ChartRangePlan {
  return CHART_RANGE_PLAN[range];
}

export function isIntraday(range: ChartRange): boolean {
  return CHART_RANGE_PLAN[range].kind === "intraday";
}

/**
 * Bar duration in seconds for each range. Used by the live-tick
 * aggregator (#602) to decide whether an incoming tick updates the
 * chart's last bar or rolls into a new bucket.
 *
 * Daily/weekly/monthly ranges all use 86400 (one day). The eToro
 * tick stream is intraday-resolution regardless of the chart's
 * range, so a 1Y chart gets one bucket per calendar day.
 */
export const INTERVAL_SECONDS: Record<string, number> = {
  OneMinute: 60,
  FiveMinutes: 300,
  TenMinutes: 600,
  FifteenMinutes: 900,
  ThirtyMinutes: 1800,
  OneHour: 3600,
  FourHours: 14400,
};

export function intervalSecondsFor(range: ChartRange): number {
  const plan = CHART_RANGE_PLAN[range];
  if (plan.kind === "intraday") return INTERVAL_SECONDS[plan.interval] ?? 60;
  return 86400;
}

/**
 * Floor an epoch-second timestamp to the start of the bucket of the
 * given interval. UTC-aligned because the daily endpoint stores
 * `price_date` as UTC midnight and the intraday endpoint emits UTC
 * timestamps. Mixing local-time floors here would silently misalign
 * live-tick updates against the rendered last bar.
 */
export function floorToBucket(epochSeconds: number, intervalSeconds: number): number {
  return Math.floor(epochSeconds / intervalSeconds) * intervalSeconds;
}

/**
 * Bar shape consumed by the chart components. Time is a UTC epoch
 * second so lightweight-charts can plot it directly without further
 * conversion. OHLCV values stay as nullable strings to match the
 * existing daily contract — null bars are dropped at the chart layer.
 */
export interface NormalisedBar {
  readonly time: number;
  readonly open: string | null;
  readonly high: string | null;
  readonly low: string | null;
  readonly close: string | null;
  readonly volume: string | null;
}

export interface NormalisedChartCandles {
  readonly symbol: string;
  readonly range: ChartRange;
  readonly kind: ChartDataKind;
  readonly rows: NormalisedBar[];
}

function dateToEpochSeconds(date: string): number | null {
  const parts = date.split("-");
  if (parts.length !== 3) return null;
  const y = Number(parts[0]);
  const m = Number(parts[1]);
  const d = Number(parts[2]);
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
  const ts = Date.UTC(y, m - 1, d);
  if (!Number.isFinite(ts)) return null;
  return Math.floor(ts / 1000);
}

function isoToEpochSeconds(iso: string): number | null {
  // `Date(iso)` accepts both `YYYY-MM-DDTHH:MM:SSZ` and offset forms.
  // NaN is returned by `Date#getTime()` if parsing fails — guard so a
  // malformed bar is dropped rather than poisoning the time scale.
  const ms = Date.parse(iso);
  if (!Number.isFinite(ms)) return null;
  return Math.floor(ms / 1000);
}

/**
 * Resolve a chart range to bars, dispatched to the correct endpoint.
 * Returns `null` for any row whose timestamp can't be parsed; the
 * chart's existing valid-row gate filters those out.
 */
export async function fetchChartCandles(
  symbol: string,
  range: ChartRange,
): Promise<NormalisedChartCandles> {
  const plan = CHART_RANGE_PLAN[range];
  if (plan.kind === "intraday") {
    const res = await fetchInstrumentIntradayCandles(symbol, plan.interval, plan.count);
    return {
      symbol: res.symbol,
      range,
      kind: "intraday",
      rows: res.rows.flatMap((b) => {
        const time = isoToEpochSeconds(b.timestamp);
        if (time === null) return [];
        return [
          {
            time,
            open: b.open,
            high: b.high,
            low: b.low,
            close: b.close,
            volume: b.volume === null ? null : String(b.volume),
          },
        ];
      }),
    };
  }
  const res = await fetchInstrumentCandles(symbol, plan.range);
  return {
    symbol: res.symbol,
    range,
    kind: "daily",
    rows: res.rows.flatMap((b) => {
      const time = dateToEpochSeconds(b.date);
      if (time === null) return [];
      return [
        {
          time,
          open: b.open,
          high: b.high,
          low: b.low,
          close: b.close,
          volume: b.volume,
        },
      ];
    }),
  };
}
