/**
 * PriceChart — candlestick + volume chart backed by TradingView's
 * lightweight-charts (#204). Replaces the Slice B hand-rolled SVG so
 * the operator gets proper OHLC rendering, pinch-zoom, and crosshair
 * tooltip without us maintaining drawing code.
 *
 * Library choice: `lightweight-charts` v5, MIT, ~45 KB gzip, Canvas-
 * rendered. We import the specific series types (tree-shake-friendly);
 * the full bundle is not pulled in. Layering approach:
 *
 *   candlestick series → right price scale (auto-scale)
 *   volume series (Histogram) → overlay price scale pinned to bottom
 *                               30% via scaleMargins
 *
 * Range picker: 1w · 1m · 3m · 6m · 1y · 5y · max. URL-synced via
 * `?chart=<range>` so the operator's choice survives tab switches
 * inside the research page.
 */
import { useEffect, useRef, useState, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import {
  CandlestickSeries,
  HistogramSeries,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";

import { fetchInstrumentCandles } from "@/api/instruments";
import type { CandleBar, CandleRange, InstrumentCandles } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

const RANGES: { id: CandleRange; label: string }[] = [
  { id: "1w", label: "1W" },
  { id: "1m", label: "1M" },
  { id: "3m", label: "3M" },
  { id: "6m", label: "6M" },
  { id: "1y", label: "1Y" },
  { id: "5y", label: "5Y" },
  { id: "max", label: "MAX" },
];

const VALID_RANGES: readonly CandleRange[] = [
  "1w",
  "1m",
  "3m",
  "6m",
  "1y",
  "5y",
  "max",
];

function parseNum(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Convert a YYYY-MM-DD date string to a UTC-midnight Unix seconds
 * timestamp. lightweight-charts requires monotonically-increasing
 * `Time` values; epoch seconds give us a stable ordering across DST
 * shifts that BusinessDay does not.
 */
function dateToTime(date: string): UTCTimestamp {
  // Date.UTC handles out-of-range inputs by rolling over; we assume
  // the backend emits valid ISO dates (it does — price_daily.d is a
  // DATE column).
  const [y, m, d] = date.split("-").map((n) => parseInt(n, 10));
  return (Date.UTC(y ?? 1970, (m ?? 1) - 1, d ?? 1) / 1000) as UTCTimestamp;
}

interface HoverState {
  date: string;
  close: number;
}

export interface PriceChartProps {
  symbol: string;
  initialRange?: CandleRange;
}

export function PriceChart({
  symbol,
  initialRange = "1m",
}: PriceChartProps): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const rawChart = searchParams.get("chart");
  const range: CandleRange = VALID_RANGES.includes(rawChart as CandleRange)
    ? (rawChart as CandleRange)
    : initialRange;

  const setRange = useCallback(
    (next: CandleRange) => {
      const params = new URLSearchParams(searchParams);
      if (next === initialRange) {
        params.delete("chart");
      } else {
        params.set("chart", next);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams, initialRange],
  );

  const { data, error, loading, refetch } = useAsync<InstrumentCandles>(
    () => fetchInstrumentCandles(symbol, range),
    [symbol, range],
  );

  // Between a range click and useAsync's effect firing, React renders
  // one frame with loading=false and the prior range's data still in
  // state. Gate chart rendering on `data.range === range` so the old
  // chart doesn't flash under the new range label.
  const dataMatchesRange = data?.range === range;
  const effectivelyLoading = loading || !dataMatchesRange;

  const rows = dataMatchesRange && data ? data.rows : null;
  // Candlestick rendering needs all four OHLC values non-null;
  // `close`-only isn't enough (lightweight-charts silently drops
  // partial bars, leaving a blank canvas). CandleBar explicitly
  // allows null O/H/L/C so we have to check each.
  const hasChartData =
    rows !== null &&
    rows.filter(
      (r) =>
        parseNum(r.open) !== null &&
        parseNum(r.high) !== null &&
        parseNum(r.low) !== null &&
        parseNum(r.close) !== null,
    ).length >= 2;

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <div className="flex gap-1">
          {RANGES.map((r) => (
            <button
              key={r.id}
              type="button"
              onClick={() => setRange(r.id)}
              className={`rounded px-2 py-0.5 text-xs font-medium ${
                r.id === range
                  ? "bg-slate-800 text-white"
                  : "bg-slate-100 text-slate-600 hover:bg-slate-200"
              }`}
              data-testid={`chart-range-${r.id}`}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {effectivelyLoading && error === null ? (
        <SectionSkeleton rows={6} />
      ) : null}
      {error !== null ? <SectionError onRetry={refetch} /> : null}
      {!effectivelyLoading && error === null && dataMatchesRange && !hasChartData ? (
        <EmptyState
          title="No price data"
          description="No candles in the local price_daily store for this range. Widen the range or wait for the next market-data refresh."
        />
      ) : null}

      {hasChartData && rows !== null ? (
        <ChartCanvas rows={rows} symbol={symbol} />
      ) : null}
    </div>
  );
}

function ChartCanvas({
  rows,
  symbol,
}: {
  rows: CandleBar[];
  symbol: string;
}): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const [hover, setHover] = useState<HoverState | null>(null);

  // One-shot chart construction. lightweight-charts owns the DOM
  // canvas and its own lifecycle; we give it an empty div and clean
  // up on unmount via `chart.remove()`.
  useEffect(() => {
    const container = containerRef.current;
    if (container === null) return;

    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: "#ffffff" },
        textColor: "#64748b",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "#f1f5f9" },
        horzLines: { color: "#f1f5f9" },
      },
      rightPriceScale: {
        borderColor: "#e2e8f0",
        // Leave room at the bottom for the volume overlay — matches
        // the TradingView default chart feel.
        scaleMargins: { top: 0.08, bottom: 0.3 },
      },
      timeScale: {
        borderColor: "#e2e8f0",
        timeVisible: false,
        secondsVisible: false,
      },
      crosshair: {
        vertLine: { width: 1, color: "#94a3b8", style: 3 },
        horzLine: { width: 1, color: "#94a3b8", style: 3 },
      },
    });

    const candle = chart.addSeries(CandlestickSeries, {
      upColor: "#10b981",
      downColor: "#ef4444",
      wickUpColor: "#10b981",
      wickDownColor: "#ef4444",
      borderVisible: false,
    });

    // Volume on its own overlay price scale pinned to the bottom 25%.
    // priceScaleId: 'volume' is an arbitrary identifier — any string
    // other than the built-in 'right'/'left' creates an overlay.
    const volume = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      priceFormat: { type: "volume" },
    });
    chart
      .priceScale("volume")
      .applyOptions({ scaleMargins: { top: 0.75, bottom: 0 } });

    chart.subscribeCrosshairMove((param) => {
      const cp = candleRef.current;
      if (!param.time || !cp) {
        setHover(null);
        return;
      }
      const bar = param.seriesData.get(cp);
      if (!bar || typeof bar !== "object" || !("close" in bar)) {
        setHover(null);
        return;
      }
      // `time` arrives as our UTC-seconds input; format it back.
      const ts = param.time as number;
      const date = new Date(ts * 1000).toISOString().slice(0, 10);
      setHover({ date, close: (bar as { close: number }).close });
    });

    chartRef.current = chart;
    candleRef.current = candle;
    volumeRef.current = volume;

    return () => {
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      volumeRef.current = null;
    };
  }, []);

  // Feed data on every rows change. lightweight-charts replaces the
  // series wholesale via setData — no incremental diffing needed.
  useEffect(() => {
    const candle = candleRef.current;
    const volume = volumeRef.current;
    const chart = chartRef.current;
    if (!candle || !volume || !chart) return;

    const clean = rows.filter(
      (r) =>
        parseNum(r.open) !== null &&
        parseNum(r.high) !== null &&
        parseNum(r.low) !== null &&
        parseNum(r.close) !== null,
    );

    candle.setData(
      clean.map((r) => ({
        time: dateToTime(r.date) as Time,
        open: parseNum(r.open) ?? 0,
        high: parseNum(r.high) ?? 0,
        low: parseNum(r.low) ?? 0,
        close: parseNum(r.close) ?? 0,
      })),
    );

    volume.setData(
      clean.map((r, i) => {
        const curr = parseNum(r.close) ?? 0;
        const prev = i > 0 ? (parseNum(clean[i - 1]!.close) ?? curr) : curr;
        return {
          time: dateToTime(r.date) as Time,
          value: parseNum(r.volume) ?? 0,
          color: curr >= prev ? "rgba(16,185,129,0.4)" : "rgba(239,68,68,0.4)",
        };
      }),
    );

    chart.timeScale().fitContent();
  }, [rows]);

  return (
    <div className="relative">
      {hover !== null ? (
        <div className="absolute right-2 top-2 z-10 rounded bg-white/90 px-2 py-1 text-xs tabular-nums shadow-sm">
          <span className="text-slate-400">{hover.date}</span>
          <span className="ml-2 font-medium text-slate-700">
            {hover.close.toLocaleString(undefined, {
              maximumFractionDigits: 2,
            })}
          </span>
        </div>
      ) : null}
      <div
        ref={containerRef}
        data-testid={`price-chart-${symbol}`}
        className="h-[340px] w-full"
      />
    </div>
  );
}
