/**
 * PriceChart — overview candlestick / line / area + volume chart backed
 * by lightweight-charts. Lives inside a clickable Pane on the
 * instrument page; clicking the card drills to the full chart workspace
 * at `/instrument/:symbol/chart`.
 *
 * Range mapping (#601): 9 buttons across two endpoints —
 *   1D · 5D · 1M · 3M · 6M  → intraday endpoint at the right interval
 *   YTD · 1Y · 5Y · MAX     → daily endpoint (price_daily)
 * The dispatch table lives in `@/lib/chartData` so the same plan is
 * shared with the chart workspace and any future drill page.
 *
 * URL params (replace, not push):
 *   - `?chart=<range>` → 1d | 5d | 1m | 3m | 6m | ytd | 1y | 5y | max (default 1m)
 *   - `?type=line|area` → series type (default candle, no param)
 *   - `?scale=log` → logarithmic right price scale (default linear, no param)
 *
 * Hover tooltip shows date (or date+time for intraday) + OHLC + volume +
 * %Δ-from-prior; matches the pattern in `ChartWorkspaceCanvas.RichTooltip`
 * so an operator's mental model is consistent between overview and workspace.
 */
import { useCallback, useEffect, useRef, useState, type JSX } from "react";
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

import type { ChartRange } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import {
  fetchChartCandles,
  intervalSecondsFor,
  isIntraday,
  type NormalisedBar,
  type NormalisedChartCandles,
} from "@/lib/chartData";
import { formatHoverLabel, humanizeVolume, tickFormatter } from "@/lib/chartFormatters";
import { chartTheme } from "@/lib/chartTheme";
import { useAsync } from "@/lib/useAsync";
import { useLiveLastBar } from "@/lib/useLiveLastBar";

const RANGES: { id: ChartRange; label: string }[] = [
  { id: "1d", label: "1D" },
  { id: "5d", label: "5D" },
  { id: "1m", label: "1M" },
  { id: "3m", label: "3M" },
  { id: "6m", label: "6M" },
  { id: "ytd", label: "YTD" },
  { id: "1y", label: "1Y" },
  { id: "5y", label: "5Y" },
  { id: "max", label: "MAX" },
];

const VALID_RANGES: readonly ChartRange[] = [
  "1d",
  "5d",
  "1m",
  "3m",
  "6m",
  "ytd",
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

interface RichHoverState {
  label: string;
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

// formatHoverLabel / tickFormatter / humanizeVolume now live in
// @/lib/chartFormatters so PriceChart and ChartWorkspaceCanvas share
// one definition (#601 review feedback — duplicated copies would
// silently drift).

export interface PriceChartProps {
  symbol: string;
  /** Provider-native instrument id used for the SSE live-tick subscription
   *  (#602). When omitted, the chart renders without live updates — the
   *  historical fetch is unaffected. */
  instrumentId?: number | null;
  initialRange?: ChartRange;
}

export function PriceChart({
  symbol,
  instrumentId = null,
  initialRange = "1m",
}: PriceChartProps): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const rawChart = searchParams.get("chart");
  const range: ChartRange = VALID_RANGES.includes(rawChart as ChartRange)
    ? (rawChart as ChartRange)
    : initialRange;
  const rawType = searchParams.get("type");
  const chartType: ChartType = VALID_TYPES.includes(rawType as ChartType)
    ? (rawType as ChartType)
    : "candle";
  const priceScale: PriceScaleMode = searchParams.get("scale") === "log" ? "log" : "linear";

  const setRange = useCallback(
    (next: ChartRange) => {
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

  const { data, error, loading, refetch } = useAsync<NormalisedChartCandles>(
    () => fetchChartCandles(symbol, range),
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
  // parseable time; `close`-only isn't enough (lightweight-charts
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
        parseNum(r.close) !== null,
    ).length >= 2;

  const intraday = isIntraday(range);

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
        <div className="flex flex-wrap gap-1">
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
          description={
            intraday
              ? "No intraday bars from the provider for this range. Try a longer range or check that the broker connection is healthy."
              : "No candles in the local price_daily store for this range. Widen the range or wait for the next market-data refresh."
          }
        />
      ) : null}

      {hasChartData && rows !== null ? (
        <ChartCanvas
          rows={rows}
          symbol={symbol}
          instrumentId={instrumentId}
          range={range}
          chartType={chartType}
          priceScale={priceScale}
          intraday={intraday}
        />
      ) : null}
    </div>
  );
}

export interface ChartCanvasProps {
  rows: ReadonlyArray<NormalisedBar>;
  symbol: string;
  /** When set, opens a live SSE quote stream and keeps the last bar
   *  updating in real time via lightweight-charts series.update(). */
  instrumentId?: number | null;
  /** Required for live-tick aggregation — picks the bucket size that
   *  matches the chart's interval. When omitted the live-tick path
   *  is disabled. */
  range?: ChartRange;
  chartType?: ChartType;
  priceScale?: PriceScaleMode;
  /** When true, hover label includes HH:MM and the time scale shows time. */
  intraday?: boolean;
  containerClassName?: string;
}

export function ChartCanvas({
  rows,
  symbol,
  instrumentId = null,
  range,
  chartType = "candle",
  priceScale = "linear",
  intraday = false,
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
  const intradayRef = useRef<boolean>(intraday);
  const [hover, setHover] = useState<RichHoverState | null>(null);

  // Keep the ref in sync so the crosshair handler (registered once at
  // mount) sees the current intraday flag without re-subscribing.
  useEffect(() => {
    intradayRef.current = intraday;
  }, [intraday]);

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
        // Explicit formatter — without this the daily axis renders
        // weekday abbreviations ("Mon Tue Wed") instead of dates and
        // intraday axis hides the time component below ~minute zoom.
        // The DeepPartial<HorzScaleOptions> type strips function
        // properties; pass the formatter via the post-mount
        // `applyOptions` effect below instead.
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
      setHover({
        label: formatHoverLabel(time, intradayRef.current),
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

  // Show time on the X-axis only for intraday data; the tick formatter
  // adapts via the tickMarkType param so it does not need re-installing.
  // tickMarkFormatter is typed via DeepPartial which strips function
  // properties, so cast through `unknown` to keep the contract.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    chart.timeScale().applyOptions({
      timeVisible: intraday,
      secondsVisible: false,
      tickMarkFormatter: tickFormatter,
    } as unknown as Parameters<ReturnType<IChartApi["timeScale"]>["applyOptions"]>[0]);
  }, [intraday]);

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
    // that fail any numeric parse are dropped here.
    const clean: NumericBar[] = rows.flatMap((r) => {
      const open = parseNum(r.open);
      const high = parseNum(r.high);
      const low = parseNum(r.low);
      const close = parseNum(r.close);
      if (open === null || high === null || low === null || close === null) {
        return [];
      }
      return [
        {
          time: r.time as UTCTimestamp,
          open,
          high,
          low,
          close,
          volume: parseNum(r.volume) ?? 0,
        },
      ];
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

  // Live-tick aggregator (#602). Subscribes to the page-level
  // LiveQuoteProvider stream for `instrumentId` and updates the last
  // bar's H/L/C — or appends a new bar when a tick crosses the bucket
  // boundary. Disabled when `instrumentId` or `range` is missing
  // (caller hasn't wired live updates for this chart).
  //
  // Anchor against the LAST RENDERED bar from `cleanRowsRef`, not the
  // raw `rows` tail — the rendering effect drops rows with null OHLC,
  // so anchoring against `rows[rows.length - 1]` could bucket ticks
  // against an invisible bar (Codex pre-push #602).
  const lastRenderedBar = cleanRowsRef.current.length > 0
    ? cleanRowsRef.current[cleanRowsRef.current.length - 1]!
    : null;
  const histLastBar = lastRenderedBar !== null
    ? {
        time: lastRenderedBar.time as number,
        open: lastRenderedBar.open,
        high: lastRenderedBar.high,
        low: lastRenderedBar.low,
      }
    : null;
  const bucketSeconds = range !== undefined ? intervalSecondsFor(range) : 60;
  const { connected, unavailable } = useLiveLastBar({
    instrumentId: range !== undefined ? instrumentId : null,
    bucketSeconds,
    historicalLastBar: histLastBar,
    refs: {
      candle: candleRef.current,
      line: lineRef.current,
      area: areaRef.current,
    },
  });
  const liveActive = connected && !unavailable && instrumentId !== null && range !== undefined;

  return (
    <div className="relative">
      {hover !== null ? <RichTooltip hover={hover} /> : null}
      {liveActive ? (
        <div
          className="absolute right-2 top-2 z-10 flex items-center gap-1 text-[10px] uppercase tracking-wider text-emerald-600"
          data-testid="price-chart-live-indicator"
        >
          <span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-500" />
          Live
        </div>
      ) : null}
      <div
        ref={containerRef}
        data-testid={`price-chart-${symbol}`}
        className={containerClassName ?? "h-[340px] w-full"}
      />
    </div>
  );
}

/**
 * TradingView-style status-line strip — single inline row at top-left.
 * Replaces the prior multi-row floating box. Compact text, no shadow,
 * transparent so the chart shows through cleanly. Color cues only on
 * the close + delta to keep the eye on price action.
 */
function RichTooltip({ hover }: { hover: RichHoverState }): JSX.Element {
  const fmt = (n: number) => n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  const deltaClass =
    hover.changePct === null
      ? "text-slate-500"
      : hover.changePct >= 0
        ? "text-emerald-600"
        : "text-red-600";
  return (
    <div
      className="absolute left-2 top-2 z-10 flex flex-wrap items-baseline gap-x-2 text-[11px] tabular-nums leading-tight text-slate-700"
      data-testid="price-chart-tooltip"
    >
      <span className="text-slate-500">{hover.label}</span>
      <span className="text-slate-400">·</span>
      <span>
        <span className="text-slate-400">O</span> {fmt(hover.open)}
      </span>
      <span>
        <span className="text-slate-400">H</span> {fmt(hover.high)}
      </span>
      <span>
        <span className="text-slate-400">L</span> {fmt(hover.low)}
      </span>
      <span className="font-medium text-slate-800">
        <span className="font-normal text-slate-400">C</span> {fmt(hover.close)}
      </span>
      <span>
        <span className="text-slate-400">V</span> {humanizeVolume(hover.volume)}
      </span>
      {hover.changePct !== null ? (
        <span className={deltaClass}>
          {hover.changePct >= 0 ? "+" : ""}
          {hover.changePct.toFixed(2)}%
        </span>
      ) : null}
    </div>
  );
}
