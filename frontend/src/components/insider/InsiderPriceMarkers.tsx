/**
 * InsiderPriceMarkers — daily price line with Form 4 transaction
 * markers overlaid (#588). Bloomberg INSI / OpenInsider convention:
 * acquired transactions plot as green up-arrows below the bar,
 * disposed as red down-arrows above. Marker size scales with
 * notional value (or share count when price is missing) so a
 * $50M open-market buy stands out against a routine sell-to-cover.
 *
 * Range: trailing 24 months — chosen to match `InsiderNetByMonth`
 * so an operator scanning both panels reads the same time window.
 * Data: daily endpoint at 5y range, sliced client-side; daily
 * granularity is enough for Form 4 dates (which are calendar dates,
 * not timestamps) and avoids paying intraday fetch latency for what
 * is fundamentally a long-horizon view.
 *
 * One marker per (date × direction) pair: same-day acquireds bucket
 * into one green marker, same-day disposeds into one red marker.
 * Without bucketing a busy ticker (heavy RSU vest day) renders 30
 * stacked arrows on one bar, which obscures the price line.
 */

import { useEffect, useMemo, useRef } from "react";
import {
  LineSeries,
  LineType,
  createChart,
  createSeriesMarkers,
  type IChartApi,
  type ISeriesApi,
  type SeriesMarker,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";

import type { InsiderTransactionDetail } from "@/api/instruments";
import type { CandleBar } from "@/api/types";
import { chartTheme } from "@/lib/chartTheme";
import { tickFormatter } from "@/lib/chartFormatters";
import {
  directionOf,
  notionalValue,
  startOfMonthUtcMs,
} from "@/lib/insiderClassify";

const MONTHS_BACK = 23; // 24-month inclusive window (current + 23 prior)

export interface MarkerBucket {
  /** UTC midnight epoch seconds — keys both axis and marker time. */
  readonly time: UTCTimestamp;
  readonly direction: "acquired" | "disposed";
  readonly notional: number;
  readonly shares: number;
  readonly count: number;
}

function dateToEpochSeconds(iso: string): number | null {
  const ms = Date.parse(`${iso}T00:00:00Z`);
  if (!Number.isFinite(ms)) return null;
  return Math.floor(ms / 1000);
}

export function bucketTransactionsForMarkers(
  rows: ReadonlyArray<InsiderTransactionDetail>,
  cutoffMs: number,
): MarkerBucket[] {
  // (epochSeconds × direction) → bucket
  const map = new Map<string, MarkerBucket>();
  for (const row of rows) {
    if (row.is_derivative) continue;
    const dir = directionOf(row.acquired_disposed_code, row.txn_code);
    if (dir === "unknown") continue;
    const sec = dateToEpochSeconds(row.txn_date);
    if (sec === null) continue;
    if (sec * 1000 < cutoffMs) continue;
    const key = `${sec}:${dir}`;
    const sharesNum = row.shares !== null ? Number(row.shares) : 0;
    const validShares = Number.isFinite(sharesNum) ? sharesNum : 0;
    const valueAdd = notionalValue(row.shares, row.price);
    // Fallback when no price filed — share count keeps relative
    // ordering meaningful.
    const sizeAdd = valueAdd > 0 ? valueAdd : validShares;
    const existing = map.get(key);
    if (existing) {
      map.set(key, {
        time: existing.time,
        direction: existing.direction,
        notional: existing.notional + sizeAdd,
        shares: existing.shares + validShares,
        count: existing.count + 1,
      });
    } else {
      map.set(key, {
        time: sec as UTCTimestamp,
        direction: dir,
        notional: sizeAdd,
        shares: validShares,
        count: 1,
      });
    }
  }
  return [...map.values()].sort((a, b) => (a.time as number) - (b.time as number));
}

/** lightweight-charts marker `size` is a numeric scalar (default 1).
 *  Map notional buckets to 1 / 2 / 3 so the heaviest trades visibly
 *  pop without making routine vests invisible. Thresholds chosen to
 *  cover ~80% of Form 4 notional distribution at the small end. */
function markerSize(notional: number): number {
  if (notional >= 5_000_000) return 3;
  if (notional >= 500_000) return 2;
  return 1;
}

function formatShares(n: number): string {
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${(abs / 1e3).toFixed(1)}K`;
  return abs.toLocaleString();
}

function formatNotional(n: number): string {
  if (n >= 1e9) return `$${(n / 1e9).toFixed(2)}B`;
  if (n >= 1e6) return `$${(n / 1e6).toFixed(2)}M`;
  if (n >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
  return `$${Math.round(n).toLocaleString()}`;
}

function bucketLabel(b: MarkerBucket): string {
  const sign = b.direction === "acquired" ? "+" : "-";
  const sharesLabel = `${sign}${formatShares(b.shares)}sh`;
  if (b.notional > 0) {
    return `${sharesLabel} · ${formatNotional(b.notional)}${b.count > 1 ? ` · ${b.count} txns` : ""}`;
  }
  return `${sharesLabel}${b.count > 1 ? ` · ${b.count} txns` : ""}`;
}

export interface InsiderPriceMarkersProps {
  readonly candles: ReadonlyArray<CandleBar>;
  readonly transactions: ReadonlyArray<InsiderTransactionDetail>;
}

export function InsiderPriceMarkers({
  candles,
  transactions,
}: InsiderPriceMarkersProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const lineRef = useRef<ISeriesApi<"Line"> | null>(null);
  const markersRef = useRef<ReturnType<typeof createSeriesMarkers<Time>> | null>(
    null,
  );

  // Anchor at the start of the calendar month 23 months ago so the
  // marker pane shares its window with `InsiderNetByMonth` exactly.
  // A rolling-millisecond cutoff would slice the oldest month and
  // disagree with the bar pane on the same day.
  const cutoffMs = useMemo(() => startOfMonthUtcMs(MONTHS_BACK), []);

  // Construct chart once. The two data effects below own subsequent
  // updates so the chart instance survives data re-fetches.
  useEffect(() => {
    const container = containerRef.current;
    if (container === null) return;
    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: chartTheme.bg },
        textColor: chartTheme.textSecondary,
        fontSize: 11,
      },
      grid: {
        vertLines: { color: chartTheme.gridLine },
        horzLines: { color: chartTheme.gridLine },
      },
      rightPriceScale: { borderColor: chartTheme.borderColor },
      timeScale: {
        borderColor: chartTheme.borderColor,
        timeVisible: false,
        secondsVisible: false,
      },
      crosshair: {
        vertLine: { width: 1, color: chartTheme.crosshair, style: 3 },
        horzLine: { width: 1, color: chartTheme.crosshair, style: 3 },
      },
    });
    chart.timeScale().applyOptions({
      tickMarkFormatter: tickFormatter,
    } as unknown as Parameters<ReturnType<IChartApi["timeScale"]>["applyOptions"]>[0]);

    const line = chart.addSeries(LineSeries, {
      color: chartTheme.primaryLine,
      lineWidth: 2,
      lineType: LineType.Curved,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    chartRef.current = chart;
    lineRef.current = line;
    return () => {
      markersRef.current?.detach();
      markersRef.current = null;
      chart.remove();
      chartRef.current = null;
      lineRef.current = null;
    };
  }, []);

  // Feed price data. Filter to the last 24m window so the time axis
  // matches the markers' window.
  useEffect(() => {
    const line = lineRef.current;
    const chart = chartRef.current;
    if (!line || !chart) return;
    const points = candles
      .flatMap((c) => {
        const sec = dateToEpochSeconds(c.date);
        if (sec === null) return [];
        if (sec * 1000 < cutoffMs) return [];
        if (c.close === null) return [];
        const close = Number(c.close);
        if (!Number.isFinite(close)) return [];
        return [{ time: sec as Time, value: close }];
      })
      .sort((a, b) => (a.time as number) - (b.time as number));
    line.setData(points);
    if (points.length > 0) {
      chart.timeScale().fitContent();
    }
  }, [candles, cutoffMs]);

  // Feed markers. Re-running setMarkers replaces the prior set —
  // detach + re-attach the plugin so we never accumulate state.
  useEffect(() => {
    const line = lineRef.current;
    if (!line) return;
    const buckets = bucketTransactionsForMarkers(transactions, cutoffMs);
    const markers: SeriesMarker<Time>[] = buckets.map((b) => ({
      time: b.time as Time,
      position: b.direction === "acquired" ? "belowBar" : "aboveBar",
      color: b.direction === "acquired" ? chartTheme.up : chartTheme.down,
      shape: b.direction === "acquired" ? "arrowUp" : "arrowDown",
      size: markerSize(b.notional),
      text: bucketLabel(b),
    }));
    markersRef.current?.detach();
    markersRef.current = createSeriesMarkers(line, markers);
  }, [transactions, cutoffMs]);

  return (
    <div className="relative h-72 w-full" data-testid="insider-price-markers">
      <div ref={containerRef} className="h-full w-full" />
    </div>
  );
}
