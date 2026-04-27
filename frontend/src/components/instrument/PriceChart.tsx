/**
 * PriceChart — overview candlestick / line / area + volume chart backed
 * by lightweight-charts (#204, polished in #587). Lives inside a
 * clickable Pane on the instrument page; clicking the card drills to
 * the full chart workspace at `/instrument/:symbol/chart`.
 *
 * Layering:
 *   - candlestick / line / area series share the right price scale
 *     (only one is visible at a time, toggled via `?type=`)
 *   - volume series (Histogram) on its own overlay price scale pinned
 *     to the bottom 30% via `scaleMargins`
 *
 * URL params (replace, not push):
 *   - `?chart=<range>` → 1w | 1m | 3m | 6m | 1y | 5y | max (default 1m)
 *   - `?type=line|area` → series type (default candle, no param)
 *   - `?scale=log` → logarithmic right price scale (default linear, no param)
 *
 * Hover tooltip shows date + OHLC + volume + %Δ-from-prior; matches the
 * pattern in `ChartWorkspaceCanvas.RichTooltip` so an operator's mental
 * model is consistent between the overview pane and the workspace.
 */
import { useEffect, useRef, useState, useCallback, type JSX } from "react";
import { useSearchParams } from "react-router-dom";
import {
  AreaSeries,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  LineType,
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
import { chartTheme } from "@/lib/chartTheme";
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

export type ChartType = "candle" | "line" | "area";
const VALID_TYPES: readonly ChartType[] = ["candle", "line", "area"];
const TYPES: { id: ChartType; label: string }[] = [
  { id: "candle", label: "Candle" },
  { id: "line", label: "Line" },
  { id: "area", label: "Area" },
];

export type PriceScaleMode = "linear" | "log";
// lightweight-charts: 0 = Normal (linear), 1 = Logarithmic.
const SCALE_MODE_NUM: Record<PriceScaleMode, 0 | 1> = { linear: 0, log: 1 };

function parseNum(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Convert a YYYY-MM-DD date string to a UTC-midnight Unix seconds
 * timestamp. We use epoch seconds rather than BusinessDay because
 * BusinessDay mode requires the library to know which dates are
 * trading days — weekends/holidays produce gaps that confuse its
 * internal scaling.
 *
 * Returns null for anything that doesn't parse cleanly so a bad row
 * is dropped rather than poisoning the time scale with NaN.
 */
function dateToTime(date: string): UTCTimestamp | null {
  const parts = date.split("-");
  if (parts.length !== 3) return null;
  const y = Number(parts[0]);
  const m = Number(parts[1]);
  const d = Number(parts[2]);
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
  const ts = Date.UTC(y, m - 1, d);
  if (!Number.isFinite(ts)) return null;
  return (ts / 1000) as UTCTimestamp;
}

interface RichHoverState {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  changePct: number | null;
}

interface NumericBar {
  time: UTCTimestamp;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
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
  const rawType = searchParams.get("type");
  const chartType: ChartType = VALID_TYPES.includes(rawType as ChartType)
    ? (rawType as ChartType)
    : "candle";
  const priceScale: PriceScaleMode = searchParams.get("scale") === "log" ? "log" : "linear";

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

  const setChartType = useCallback(
    (next: ChartType) => {
      const params = new URLSearchParams(searchParams);
      if (next === "candle") {
        params.delete("type");
      } else {
        params.set("type", next);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const togglePriceScale = useCallback(() => {
    const params = new URLSearchParams(searchParams);
    if (priceScale === "log") {
      params.delete("scale");
    } else {
      params.set("scale", "log");
    }
    setSearchParams(params, { replace: true });
  }, [searchParams, setSearchParams, priceScale]);

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
  // Candlestick rendering needs all four OHLC values non-null AND a
  // parseable date; `close`-only isn't enough (lightweight-charts
  // silently drops partial bars, leaving a blank canvas). Mirrors the
  // filter in `ChartCanvas`'s setData effect exactly so the gate
  // can't accept rows the effect will then drop.
  const hasChartData =
    rows !== null &&
    rows.filter(
      (r) =>
        parseNum(r.open) !== null &&
        parseNum(r.high) !== null &&
        parseNum(r.low) !== null &&
        parseNum(r.close) !== null &&
        dateToTime(r.date) !== null,
    ).length >= 2;

  return (
    <div className="space-y-2">
      {/*
        Controls swallow click events so they do NOT trigger the
        Pane's card-click drill (which navigates to the full chart
        workspace). Clicks anywhere else inside this component — chart
        canvas, hover tooltip, empty state — bubble up to the Pane.
      */}
      <div
        className="flex items-center justify-between gap-2"
        onClick={(e) => e.stopPropagation()}
        data-testid="chart-controls"
      >
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
        <div className="flex items-center gap-2">
          <div className="flex gap-1">
            {TYPES.map((t) => (
              <button
                key={t.id}
                type="button"
                onClick={() => setChartType(t.id)}
                className={`rounded px-2 py-0.5 text-xs font-medium ${
                  t.id === chartType
                    ? "bg-slate-800 text-white"
                    : "bg-slate-100 text-slate-600 hover:bg-slate-200"
                }`}
                data-testid={`chart-type-${t.id}`}
              >
                {t.label}
              </button>
            ))}
          </div>
          <button
            type="button"
            onClick={togglePriceScale}
            aria-pressed={priceScale === "log"}
            className={`rounded px-2 py-0.5 text-xs font-medium ${
              priceScale === "log"
                ? "bg-slate-800 text-white"
                : "bg-slate-100 text-slate-600 hover:bg-slate-200"
            }`}
            data-testid="chart-scale-log"
            title="Toggle logarithmic price scale"
          >
            Log
          </button>
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
        <ChartCanvas
          rows={rows}
          symbol={symbol}
          chartType={chartType}
          priceScale={priceScale}
        />
      ) : null}
    </div>
  );
}

export interface ChartCanvasProps {
  rows: CandleBar[];
  symbol: string;
  chartType?: ChartType;
  priceScale?: PriceScaleMode;
  containerClassName?: string;
}

export function ChartCanvas({
  rows,
  symbol,
  chartType = "candle",
  priceScale = "linear",
  containerClassName,
}: ChartCanvasProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const lineRef = useRef<ISeriesApi<"Line"> | null>(null);
  const areaRef = useRef<ISeriesApi<"Area"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  // Stable ref into `clean` for the crosshair handler — avoids stale
  // closure capture when rows update.
  const cleanRowsRef = useRef<NumericBar[]>([]);
  const [hover, setHover] = useState<RichHoverState | null>(null);

  // One-shot chart construction. lightweight-charts owns the DOM
  // canvas and its own lifecycle; we give it an empty div and clean
  // up on unmount via `chart.remove()`. All three price series mount
  // here; toggling `chartType` only flips visibility.
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
      rightPriceScale: {
        borderColor: chartTheme.borderColor,
        // Leave room at the bottom for the volume overlay — matches
        // the TradingView default chart feel.
        scaleMargins: { top: 0.08, bottom: 0.3 },
      },
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

    const candle = chart.addSeries(CandlestickSeries, {
      upColor: chartTheme.up,
      downColor: chartTheme.down,
      wickUpColor: chartTheme.up,
      wickDownColor: chartTheme.down,
      borderVisible: false,
    });
    // Robinhood-style smooth line. `LineType.Curved` (cardinal-spline)
    // applies to both Line and Area series; we set it here once and the
    // subsequent visibility toggles preserve the smoothing.
    const line = chart.addSeries(LineSeries, {
      color: chartTheme.primaryLine,
      lineWidth: 2,
      lineType: LineType.Curved,
      priceLineVisible: false,
      lastValueVisible: false,
      visible: false,
    });
    const area = chart.addSeries(AreaSeries, {
      lineColor: chartTheme.primaryLine,
      topColor: chartTheme.volumeUpAlpha,
      bottomColor: "rgba(30,41,59,0.0)",
      lineWidth: 2,
      lineType: LineType.Curved,
      priceLineVisible: false,
      lastValueVisible: false,
      visible: false,
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
      if (!param.time) {
        setHover(null);
        return;
      }
      // We feed UTCTimestamp (epoch seconds), so `param.time` should
      // come back as a number. Guard against the library ever
      // returning a BusinessDay object so we don't silently render
      // "1970-01-01" from a NaN cast.
      if (typeof param.time !== "number") {
        setHover(null);
        return;
      }
      const time = param.time as UTCTimestamp;
      const idx = cleanRowsRef.current.findIndex((b) => b.time === time);
      if (idx < 0) {
        setHover(null);
        return;
      }
      const bar = cleanRowsRef.current[idx]!;
      const prev = idx > 0 ? cleanRowsRef.current[idx - 1] : null;
      const changePct =
        prev !== null && prev !== undefined && prev.close !== 0
          ? ((bar.close - prev.close) / prev.close) * 100
          : null;
      const date = new Date(time * 1000).toISOString().slice(0, 10);
      setHover({
        date,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
        changePct,
      });
    });

    chartRef.current = chart;
    candleRef.current = candle;
    lineRef.current = line;
    areaRef.current = area;
    volumeRef.current = volume;

    return () => {
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      lineRef.current = null;
      areaRef.current = null;
      volumeRef.current = null;
    };
  }, []);

  // Toggle which price series is visible whenever `chartType` changes.
  // The data effect always feeds all three series, so flipping
  // visibility never reveals a stale series.
  useEffect(() => {
    const candle = candleRef.current;
    const line = lineRef.current;
    const area = areaRef.current;
    if (!candle || !line || !area) return;
    candle.applyOptions({ visible: chartType === "candle" });
    line.applyOptions({ visible: chartType === "line" });
    area.applyOptions({ visible: chartType === "area" });
  }, [chartType]);

  // Apply linear / logarithmic price scale.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    chart.priceScale("right").applyOptions({ mode: SCALE_MODE_NUM[priceScale] });
  }, [priceScale]);

  // Feed data on every rows change. lightweight-charts replaces the
  // series wholesale via setData — no incremental diffing needed.
  useEffect(() => {
    const candle = candleRef.current;
    const line = lineRef.current;
    const area = areaRef.current;
    const volume = volumeRef.current;
    const chart = chartRef.current;
    if (!candle || !line || !area || !volume || !chart) return;

    // Pre-convert to numeric bars so downstream `setData` calls work
    // with guaranteed-non-null values (no dead `?? 0` fallbacks). Rows
    // that fail any numeric or date parse are dropped here.
    const clean: NumericBar[] = rows.flatMap((r) => {
      const time = dateToTime(r.date);
      const open = parseNum(r.open);
      const high = parseNum(r.high);
      const low = parseNum(r.low);
      const close = parseNum(r.close);
      if (time === null || open === null || high === null || low === null || close === null) {
        return [];
      }
      return [{ time, open, high, low, close, volume: parseNum(r.volume) ?? 0 }];
    });
    cleanRowsRef.current = clean;

    candle.setData(
      clean.map((b) => ({
        time: b.time as Time,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
      })),
    );

    const closeData = clean.map((b) => ({ time: b.time as Time, value: b.close }));
    line.setData(closeData);
    area.setData(closeData);

    volume.setData(
      clean.map((b, i) => {
        const prev = i > 0 ? clean[i - 1]!.close : b.close;
        return {
          time: b.time as Time,
          value: b.volume,
          color: b.close >= prev ? chartTheme.volumeUpAlpha : chartTheme.volumeDownAlpha,
        };
      }),
    );

    chart.timeScale().fitContent();
  }, [rows]);

  return (
    <div className="relative">
      {hover !== null ? <RichTooltip hover={hover} /> : null}
      <div
        ref={containerRef}
        data-testid={`price-chart-${symbol}`}
        className={containerClassName ?? "h-[340px] w-full"}
      />
    </div>
  );
}

function RichTooltip({ hover }: { hover: RichHoverState }): JSX.Element {
  const fmt = (n: number) => n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return (
    <div
      className="absolute right-2 top-2 z-10 min-w-[160px] rounded bg-white/95 px-3 py-2 text-xs shadow-md"
      data-testid="price-chart-tooltip"
    >
      <div className="text-slate-500">{hover.date}</div>
      <dl className="mt-1 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 tabular-nums">
        <dt className="text-slate-500">O</dt>
        <dd>{fmt(hover.open)}</dd>
        <dt className="text-slate-500">H</dt>
        <dd>{fmt(hover.high)}</dd>
        <dt className="text-slate-500">L</dt>
        <dd>{fmt(hover.low)}</dd>
        <dt className="text-slate-500">C</dt>
        <dd className="font-medium text-slate-800">{fmt(hover.close)}</dd>
        <dt className="text-slate-500">Vol</dt>
        <dd>{fmt(hover.volume)}</dd>
        {hover.changePct !== null ? (
          <>
            <dt className="text-slate-500">Δ%</dt>
            <dd className={hover.changePct >= 0 ? "text-emerald-600" : "text-red-600"}>
              {hover.changePct >= 0 ? "+" : ""}
              {hover.changePct.toFixed(2)}%
            </dd>
          </>
        ) : null}
      </dl>
    </div>
  );
}
