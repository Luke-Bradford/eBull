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
import { useCallback, useEffect, useMemo, useRef, useState, type JSX } from "react";
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
import { SessionBands } from "@/components/instrument/SessionBands";
import { EmptyState } from "@/components/states/EmptyState";
import {
  fetchChartCandles,
  floorToBucket,
  intervalSecondsFor,
  isIntraday,
  type NormalisedBar,
  type NormalisedChartCandles,
} from "@/lib/chartData";
import {
  classifyUsSession,
  formatHoverLabel,
  humanizeVolume,
  tickFormatter,
} from "@/lib/chartFormatters";
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
  // Session-visibility toggles. Default ON (omit param to keep clean URLs);
  // set `?pm=0` / `?ah=0` to hide pre-market / after-hours bars + bands.
  const showPm = searchParams.get("pm") !== "0";
  const showAh = searchParams.get("ah") !== "0";

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

  const toggleParam = useCallback(
    (key: "pm" | "ah", currentlyOn: boolean) => {
      const params = new URLSearchParams(searchParams);
      if (currentlyOn) {
        params.set(key, "0");
      } else {
        params.delete(key);
      }
      setSearchParams(params, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const { data, error, loading, refetch } = useAsync<NormalisedChartCandles>(
    () => fetchChartCandles(symbol, range),
    [symbol, range],
  );

  // Coarser-grained candle-window refetch as a backstop. The
  // backend's REST live-rate poller (#602) keeps the in-progress bar
  // ticking at 5s; this refetch picks up any historical bar
  // corrections eToro emits (rare). 60s on intraday is plenty.
  useEffect(() => {
    const intervalMs = isIntraday(range) ? 60_000 : 300_000;
    const id = setInterval(() => {
      refetch();
    }, intervalMs);
    return () => clearInterval(id);
  }, [range, refetch]);

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
    // Flex column with `flex-1 min-h-0` so when mounted inside a Pane
    // with `fillHeight` (DensityGrid chart cell uses lg:row-span-2),
    // the chart canvas below expands to fill the grid cell instead
    // of sitting at its 340px intrinsic height. In a non-fillHeight
    // (block-layout) parent, the modifiers are inert and intrinsic
    // sizing is preserved.
    <div className="flex flex-1 flex-col gap-2 min-h-0">
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
          {/*
            Session-visibility + previous-close toggles. Hidden on
            daily-tier ranges (YTD/1Y/5Y/MAX) — those have one bar per
            session so PM/AH boundaries don't apply, and the
            previous-close reference is implicit in the daily series.
          */}
          {isIntraday(range) ? (
            <>
              <button
                type="button"
                onClick={() => toggleParam("pm", showPm)}
                aria-pressed={showPm}
                className={`rounded px-2 py-0.5 text-xs font-medium ${
                  showPm
                    ? "bg-slate-800 text-white"
                    : "bg-slate-100 text-slate-600 hover:bg-slate-200"
                }`}
                data-testid="chart-toggle-pm"
                title="Show / hide pre-market (04:00–09:30 ET)"
              >
                PM
              </button>
              <button
                type="button"
                onClick={() => toggleParam("ah", showAh)}
                aria-pressed={showAh}
                className={`rounded px-2 py-0.5 text-xs font-medium ${
                  showAh
                    ? "bg-slate-800 text-white"
                    : "bg-slate-100 text-slate-600 hover:bg-slate-200"
                }`}
                data-testid="chart-toggle-ah"
                title="Show / hide after-hours (16:00–20:00 ET)"
              >
                AH
              </button>
            </>
          ) : null}
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
          showPm={showPm}
          showAh={showAh}
          // Override the default `h-[340px] w-full` so the chart
          // fills the Pane fillHeight layout instead of sitting at
          // the intrinsic 340px. min-h-[340px] preserves the
          // sensible floor when the cell happens to be small (e.g.
          // narrow viewport).
          containerClassName="h-full w-full min-h-[340px]"
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
  /** Show pre-market (04:00–09:30 ET) bars + tint band. Default true. */
  showPm?: boolean;
  /** Show after-hours (16:00–20:00 ET) bars + tint band. Default true. */
  showAh?: boolean;
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
  showPm = true,
  showAh = true,
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
  // Track which `range` we've already auto-fit. fitContent should
  // only run on the first non-empty load for a given range — the
  // 60s candle-window refetch (PriceChart's backstop polling) reuses
  // the same range and would otherwise re-fit on every tick of the
  // poll, shifting the visible right edge and re-anchoring axis
  // ticks to whatever bar happens to be rightmost.
  const fittedRangeRef = useRef<string | null>(null);
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
        // rightOffset: 5 leaves a 5-bar buffer past the last bar so
        // the rightmost time-axis tick lands on a clean grid position
        // (e.g. 20:55) instead of the just-painted live bar
        // (e.g. 20:57). TradingView uses the same trick — without
        // it the live tick repeatedly shifts the visible-range edge,
        // which forces lightweight-charts' tick generator to label
        // whatever the rightmost bar happens to be.
        rightOffset: 5,
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

  // Numeric / null-filtered rows. Computed during render (not in an
  // effect) so values are available to the live-tick aggregator's
  // historical anchor on the very first render. Otherwise `histLastBar`
  // would always be null until React next re-rendered, and live ticks
  // would all bucket as "no anchor" → fresh bars, never extending the
  // historical last candle.
  //
  // FULL set — used by the previous-close detector + SessionBands so
  // they can see ALL sessions regardless of visibility toggles.
  const cleanAll = useMemo<NumericBar[]>(() => {
    return rows.flatMap((r) => {
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
  }, [rows]);

  // Visibility-filtered set — what gets fed into the price/volume
  // series. PM/AH bars drop when the operator toggles them off, but
  // RTH and `closed` bars are never hidden (closed bars are already
  // collapsed by the ordinal axis).
  const clean = useMemo<NumericBar[]>(() => {
    if (!intraday || (showPm && showAh)) return cleanAll;
    return cleanAll.filter((b) => {
      const k = classifyUsSession(b.time);
      if (k === "pre" && !showPm) return false;
      if (k === "ah" && !showAh) return false;
      return true;
    });
  }, [cleanAll, intraday, showPm, showAh]);

  // Mirror `clean` into the crosshair handler's ref. The handler is
  // registered once at mount; reading from a ref avoids stale-closure
  // capture.
  cleanRowsRef.current = clean;

  // Feed data on every clean change. lightweight-charts replaces the
  // series wholesale via setData — no incremental diffing needed.
  useEffect(() => {
    const candle = candleRef.current;
    const line = lineRef.current;
    const area = areaRef.current;
    const volume = volumeRef.current;
    const chart = chartRef.current;
    if (!candle || !line || !area || !volume || !chart) return;

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

    // Re-fit only on the first non-empty load for a given range. The
    // chart auto-extends visible width as new bars append; calling
    // fitContent on every clean change would flush the operator's
    // pan/zoom state every 60s and re-anchor the rightmost tick.
    const fingerprint = range ?? "?";
    if (clean.length > 0 && fittedRangeRef.current !== fingerprint) {
      chart.timeScale().fitContent();
      fittedRangeRef.current = fingerprint;
    }
  }, [clean, range]);


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
  const lastRenderedBar = clean.length > 0 ? clean[clean.length - 1]! : null;
  // Memoize on the primitive OHLC fields so the object identity is
  // stable while the data is — without this, `useLiveLastBar`'s tick
  // effect would re-fire on every parent render and re-apply the
  // last tick (Codex pre-push #602 round 2).
  const lastTime = lastRenderedBar !== null ? (lastRenderedBar.time as number) : null;
  const lastOpen = lastRenderedBar !== null ? lastRenderedBar.open : null;
  const lastHigh = lastRenderedBar !== null ? lastRenderedBar.high : null;
  const lastLow = lastRenderedBar !== null ? lastRenderedBar.low : null;
  const histLastBar = useMemo(
    () =>
      lastTime !== null && lastOpen !== null && lastHigh !== null && lastLow !== null
        ? { time: lastTime, open: lastOpen, high: lastHigh, low: lastLow }
        : null,
    [lastTime, lastOpen, lastHigh, lastLow],
  );
  const bucketSeconds = range !== undefined ? intervalSecondsFor(range) : 60;
  const { connected, unavailable, appliedTicks, lastAppliedAt, lastVerdict } =
    useLiveLastBar({
      instrumentId: range !== undefined ? instrumentId : null,
      bucketSeconds,
      historicalLastBar: histLastBar,
      refs: {
        candle: candleRef,
        line: lineRef,
        area: areaRef,
      },
      acceptPre: showPm,
      acceptAh: showAh,
    });
  const liveActive = connected && !unavailable && instrumentId !== null && range !== undefined;
  const lastTickHHMM = lastAppliedAt !== null
    ? (() => {
        const d = new Date(lastAppliedAt);
        return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
      })()
    : null;

  // Bars passed to SessionBands — track the rendered (`clean`) set so
  // toggling PM/AH off removes both the bars AND the corresponding
  // tint, keeping the visual consistent. The ordinal axis collapses
  // gaps either way, so band coordinates align with the rendered bars.
  //
  // Live-bar bucket: when the live-tick aggregator appends a fresh
  // bucket past the historical tail (verdict `append`), the candle
  // moves immediately via `series.update()` but `clean` doesn't
  // refresh until the next REST refetch (60s on intraday). Append
  // the live bucket here so the tint follows the candle without lag.
  // Codex review #602.
  const liveBucketTime = useMemo(() => {
    if (lastAppliedAt === null) return null;
    const tickEpoch = Math.floor(new Date(lastAppliedAt).getTime() / 1000);
    if (!Number.isFinite(tickEpoch)) return null;
    return floorToBucket(tickEpoch, bucketSeconds);
  }, [lastAppliedAt, bucketSeconds]);
  const bandBars = useMemo(() => {
    const base = clean.map((b) => ({ time: b.time as number }));
    if (
      liveBucketTime !== null &&
      (base.length === 0 || base[base.length - 1]!.time < liveBucketTime)
    ) {
      base.push({ time: liveBucketTime });
    }
    return base;
  }, [clean, liveBucketTime]);

  return (
    // `relative` for the absolutely-positioned hover tooltip + LIVE
    // indicator children. `flex-1` makes the canvas wrapper absorb
    // the remaining height in a flex parent (PriceChart's wrapper
    // when fillHeight is on); inert when the parent is block layout
    // (every other caller). NOTHING ELSE — the previous attempt at
    // this fix added `flex flex-col min-h-0` here too and broke the
    // chart's visible time range, see PR #652 revert.
    <div className="relative flex-1">
      <div
        ref={containerRef}
        data-testid={`price-chart-${symbol}`}
        className={containerClassName ?? "h-[340px] w-full"}
      />
      <SessionBands
        chartRef={chartRef}
        bars={bandBars}
        enabled={intraday && (showPm || showAh)}
      />
      {hover !== null ? <RichTooltip hover={hover} /> : null}
      {liveActive ? (
        <div
          className="absolute right-2 top-2 z-10 flex items-center gap-1.5 text-[10px] tabular-nums tracking-wide text-emerald-600"
          data-testid="price-chart-live-indicator"
          title={`SSE connected · ${appliedTicks} ticks applied · last verdict: ${lastVerdict ?? "(none yet)"}`}
        >
          <span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-500" />
          <span className="uppercase">Live</span>
          <span className="text-slate-400">·</span>
          <span className="text-slate-500">{appliedTicks} ticks</span>
          {lastTickHHMM !== null ? (
            <>
              <span className="text-slate-400">·</span>
              <span className="text-slate-500">{lastTickHHMM}</span>
            </>
          ) : null}
        </div>
      ) : null}
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
