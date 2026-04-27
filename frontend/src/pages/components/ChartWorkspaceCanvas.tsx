/**
 * ChartWorkspaceCanvas — full-viewport chart canvas for ChartPage (#576 Phase 3).
 *
 * Phase 2: SMA/EMA overlay LineSeries (toggleable via `indicators` prop),
 *          rich OHLC + volume + Δ% + per-indicator crosshair tooltip.
 * Phase 3: compare-ticker overlays (normalized % change), linear regression
 *          line, range channel (highest-high / lowest-low), tooltip swap.
 *
 * Deliberately separate from ChartCanvas (compact instrument-page chart) so
 * the compact component stays focused and this one can evolve independently.
 */
import { useEffect, useRef, useState, type JSX } from "react";
import {
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";

import type { NormalisedBar } from "@/lib/chartData";
import { formatHoverLabel, humanizeVolume, tickFormatter } from "@/lib/chartFormatters";
import { chartTheme } from "@/lib/chartTheme";

export type IndicatorId = "sma20" | "sma50" | "ema20" | "ema50";
export const INDICATOR_IDS: IndicatorId[] = ["sma20", "sma50", "ema20", "ema50"];

// Keep palette keys exhaustively typed against IndicatorId so a missing or
// misspelled key in chartTheme.indicator fails typecheck rather than
// returning undefined at runtime.
const SMA_COLORS: Record<IndicatorId, string> = chartTheme.indicator;

const SMA_LABELS: Record<IndicatorId, string> = {
  sma20: "SMA(20)",
  sma50: "SMA(50)",
  ema20: "EMA(20)",
  ema50: "EMA(50)",
};

// Fixed palette for compare overlays — distinct from SMA colors.
export const COMPARE_COLORS: readonly string[] = chartTheme.compare;

export interface CompareSeries {
  readonly symbol: string;
  readonly rows: ReadonlyArray<NormalisedBar>;
}

interface NumericBar {
  time: UTCTimestamp;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface RichHoverState {
  date: string;
  mode: "ohlcv" | "compare";
  // OHLCV mode
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
  changePct?: number | null;
  indicators?: Array<{ id: IndicatorId; label: string; value: number }>;
  // Compare mode
  primaryPct?: number | null;
  primarySymbol?: string;
  comparePcts?: Array<{ symbol: string; color: string; value: number | null }>;
}

function parseNum(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// formatHoverLabel / tickFormatter / humanizeVolume share definitions
// with PriceChart via @/lib/chartFormatters — see comment there.

// Pure functions — exported for unit testing.

export function computeSMA(closes: number[], period: number): Array<number | null> {
  const out: Array<number | null> = new Array(closes.length).fill(null);
  let sum = 0;
  for (let i = 0; i < closes.length; i++) {
    sum += closes[i] as number;
    if (i >= period) sum -= closes[i - period] as number;
    if (i >= period - 1) out[i] = sum / period;
  }
  return out;
}

export function computeEMA(closes: number[], period: number): Array<number | null> {
  const out: Array<number | null> = new Array(closes.length).fill(null);
  if (closes.length < period) return out;
  const k = 2 / (period + 1);
  // Seed with SMA of the first `period` values.
  let sum = 0;
  for (let i = 0; i < period; i++) sum += closes[i] as number;
  let ema = sum / period;
  out[period - 1] = ema;
  for (let i = period; i < closes.length; i++) {
    ema = (closes[i] as number) * k + ema * (1 - k);
    out[i] = ema;
  }
  return out;
}

// Pure: normalize closes to % change from the first value.
// The first non-null close is the base. NaN/null → returned as null.
export function normalizeToPercent(closes: number[]): Array<number | null> {
  if (closes.length === 0) return [];
  const base = closes[0] as number;
  if (!Number.isFinite(base) || base === 0) return closes.map(() => null);
  return closes.map((c) => ((c - base) / base) * 100);
}

// Pure: linear regression over [(0, y0), (1, y1), ..., (n, yN)].
// Returns the line evaluated at each x (plottable as a series).
export function linearRegressionLine(closes: number[]): Array<number | null> {
  const n = closes.length;
  if (n < 2) return new Array(n).fill(null);
  let sumX = 0,
    sumY = 0,
    sumXY = 0,
    sumXX = 0;
  for (let i = 0; i < n; i++) {
    const y = closes[i] as number;
    sumX += i;
    sumY += y;
    sumXY += i * y;
    sumXX += i * i;
  }
  const denom = n * sumXX - sumX * sumX;
  if (denom === 0) return new Array(n).fill(null);
  const slope = (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;
  return closes.map((_, i) => slope * i + intercept);
}

// Pure: returns {high, low} over the array.  Both null if length 0.
export function rangeChannel(closes: number[]): { high: number | null; low: number | null } {
  if (closes.length === 0) return { high: null, low: null };
  let high = closes[0] as number;
  let low = closes[0] as number;
  for (const v of closes) {
    if (v > high) high = v;
    if (v < low) low = v;
  }
  return { high, low };
}

function computeIndicator(id: IndicatorId, closes: number[]): Array<number | null> {
  switch (id) {
    case "sma20":
      return computeSMA(closes, 20);
    case "sma50":
      return computeSMA(closes, 50);
    case "ema20":
      return computeEMA(closes, 20);
    case "ema50":
      return computeEMA(closes, 50);
  }
}

export interface ChartWorkspaceCanvasProps {
  readonly rows: ReadonlyArray<NormalisedBar>;
  readonly symbol: string;
  readonly indicators: ReadonlyArray<IndicatorId>;
  readonly compares?: ReadonlyArray<CompareSeries>;
  readonly showRegression?: boolean;
  readonly showChannel?: boolean;
  /** Show time on the x-axis (intraday data). */
  readonly intraday?: boolean;
  readonly containerClassName?: string;
}

export function ChartWorkspaceCanvas({
  rows,
  symbol,
  indicators,
  compares = [],
  showRegression = false,
  showChannel = false,
  intraday = false,
  containerClassName,
}: ChartWorkspaceCanvasProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  // Primary normalized line (used in compare mode instead of candles).
  const primaryLineRef = useRef<ISeriesApi<"Line"> | null>(null);
  const indicatorRefs = useRef<Map<IndicatorId, ISeriesApi<"Line">>>(new Map());
  const compareLineRefs = useRef<Map<string, ISeriesApi<"Line">>>(new Map());
  const regressionRef = useRef<ISeriesApi<"Line"> | null>(null);
  const channelHighRef = useRef<ISeriesApi<"Line"> | null>(null);
  const channelLowRef = useRef<ISeriesApi<"Line"> | null>(null);
  // Stable refs for crosshair closure — no stale-closure risk.
  const cleanRowsRef = useRef<NumericBar[]>([]);
  const indicatorValuesRef = useRef<Map<IndicatorId, Array<number | null>>>(new Map());
  // Compare mode normalized values keyed by symbol; primary is keyed by symbol prop.
  const compareNormRef = useRef<Map<string, Array<number | null>>>(new Map());
  // Symbol → color, derived from compares prop order. Single source of
  // truth so the tooltip and the LineSeries always agree on color even
  // after add/remove cycles change Map insertion order.
  const compareColorRef = useRef<Map<string, string>>(new Map());
  // Stable ref so the crosshair handler (subscribed once at mount)
  // sees the current intraday flag without re-subscribing every time
  // the prop changes.
  const intradayRef = useRef<boolean>(intraday);
  const [hover, setHover] = useState<RichHoverState | null>(null);

  const compareMode = compares.length > 0;

  // Keep intraday ref + time-axis options in sync with the prop.
  useEffect(() => {
    intradayRef.current = intraday;
    const chart = chartRef.current;
    if (chart) {
      chart.timeScale().applyOptions({
        timeVisible: intraday,
        secondsVisible: false,
        tickMarkFormatter: tickFormatter,
      } as unknown as Parameters<ReturnType<IChartApi["timeScale"]>["applyOptions"]>[0]);
    }
  }, [intraday]);

  // One-shot chart construction — mirrors ChartCanvas setup.
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

    const volume = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      priceFormat: { type: "volume" },
    });
    chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.75, bottom: 0 } });

    const primaryLine = chart.addSeries(LineSeries, {
      color: chartTheme.primaryLine,
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    chart.subscribeCrosshairMove((param) => {
      if (!param.time || typeof param.time !== "number") {
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

      if (compareNormRef.current.size > 0) {
        // Compare mode: show normalized % values.
        const primaryNorm = compareNormRef.current.get("__primary__");
        const primaryPct = primaryNorm ? (primaryNorm[idx] ?? null) : null;
        const comparePcts = Array.from(compareNormRef.current.entries())
          .filter(([k]) => k !== "__primary__")
          .map(([sym, norm]) => ({
            symbol: sym,
            // Read color from the symbol-keyed ref populated when the
            // LineSeries was created — keeps tooltip in sync with the
            // actual rendered series even if Map iteration order drifts.
            color: compareColorRef.current.get(sym) ?? chartTheme.compare[0],
            value: norm[idx] ?? null,
          }));
        const date = formatHoverLabel(time, intradayRef.current);
        setHover({
          date,
          mode: "compare",
          primaryPct,
          primarySymbol: symbol,
          comparePcts,
        });
        return;
      }

      // OHLCV mode.
      const prev = idx > 0 ? cleanRowsRef.current[idx - 1] : null;
      const changePct =
        prev !== null && prev !== undefined && prev.close !== 0
          ? ((bar.close - prev.close) / prev.close) * 100
          : null;

      const indicatorRows: Array<{ id: IndicatorId; label: string; value: number }> = [];
      for (const id of INDICATOR_IDS) {
        const series = indicatorValuesRef.current.get(id);
        if (series === undefined) continue;
        const v = series[idx];
        if (v === null || v === undefined) continue;
        indicatorRows.push({ id, label: SMA_LABELS[id]!, value: v });
      }

      // Use the same date formatter as the compare path so intraday
      // hovers show HH:MM (the date-only branch above used to drop
      // the time component — Codex flagged this).
      const date = formatHoverLabel(time, intradayRef.current);
      setHover({
        date,
        mode: "ohlcv",
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
        changePct,
        indicators: indicatorRows,
      });
    });

    chartRef.current = chart;
    candleRef.current = candle;
    volumeRef.current = volume;
    primaryLineRef.current = primaryLine;

    return () => {
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      volumeRef.current = null;
      primaryLineRef.current = null;
      regressionRef.current = null;
      channelHighRef.current = null;
      channelLowRef.current = null;
      indicatorRefs.current.clear();
      compareLineRefs.current.clear();
    };
  }, [symbol]);

  // Feed candle + volume data on rows change; handle compare mode switching.
  useEffect(() => {
    const candle = candleRef.current;
    const volume = volumeRef.current;
    const primaryLine = primaryLineRef.current;
    const chart = chartRef.current;
    if (!candle || !volume || !chart || !primaryLine) return;

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

    if (compareMode) {
      // In compare mode: hide candles + volume, show normalized primary line.
      candle.setData([]);
      volume.setData([]);
      const closes = clean.map((b) => b.close);
      const normalized = normalizeToPercent(closes);
      compareNormRef.current.set("__primary__", normalized);
      const lineData: LineData[] = [];
      for (let i = 0; i < normalized.length; i++) {
        const v = normalized[i];
        const bar = clean[i];
        if (v === null || v === undefined || !bar) continue;
        lineData.push({ time: bar.time as Time, value: v });
      }
      primaryLine.applyOptions({ visible: true, color: chartTheme.primaryLine, lineWidth: 2 });
      primaryLine.setData(lineData);
    } else {
      // Normal mode: show candles + volume; hide primary normalized line.
      compareNormRef.current.clear();
      primaryLine.setData([]);

      candle.setData(
        clean.map((b) => ({
          time: b.time as Time,
          open: b.open,
          high: b.high,
          low: b.low,
          close: b.close,
        })),
      );

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
    }

    chart.timeScale().fitContent();
  }, [rows, compareMode]);

  // Compare series: fetch + render normalized lines per compare symbol.
  // Tears down series for symbols no longer in the list.
  useEffect(() => {
    const chart = chartRef.current;
    const clean = cleanRowsRef.current;
    if (!chart) return;

    // Remove series for symbols no longer in compares.
    for (const [sym, series] of compareLineRefs.current.entries()) {
      if (!compares.some((c) => c.symbol === sym)) {
        chart.removeSeries(series);
        compareLineRefs.current.delete(sym);
        compareNormRef.current.delete(sym);
        compareColorRef.current.delete(sym);
      }
    }

    if (!compareMode) return;

    // Add/update series for each compare symbol.
    compares.forEach((cs, colorIdx) => {
      const color =
        COMPARE_COLORS[colorIdx % COMPARE_COLORS.length] ?? chartTheme.compare[0];
      compareColorRef.current.set(cs.symbol, color);

      const compareClean: NumericBar[] = cs.rows.flatMap((r) => {
        const close = parseNum(r.close);
        if (close === null) return [];
        return [
          {
            time: r.time as UTCTimestamp,
            open: close,
            high: close,
            low: close,
            close,
            volume: 0,
          },
        ];
      });

      // Build a time→close map so we can align with the primary series times.
      const compareMap = new Map<number, number>();
      for (const b of compareClean) {
        compareMap.set(b.time, b.close);
      }

      // Align compare closes to primary timestamps (use null when missing).
      const alignedCloses: (number | null)[] = clean.map((b) => compareMap.get(b.time) ?? null);
      // Compute base as first non-null value.
      const firstNonNull = alignedCloses.find((v) => v !== null);
      const base = firstNonNull ?? null;

      const normalized: Array<number | null> = alignedCloses.map((v) => {
        if (v === null || base === null || !Number.isFinite(base) || base === 0) return null;
        return ((v - base) / base) * 100;
      });
      compareNormRef.current.set(cs.symbol, normalized);

      let series = compareLineRefs.current.get(cs.symbol);
      if (!series) {
        series = chart.addSeries(LineSeries, {
          color,
          lineWidth: 2,
          priceLineVisible: false,
          lastValueVisible: false,
        });
        compareLineRefs.current.set(cs.symbol, series);
      } else {
        series.applyOptions({ color });
      }

      const lineData: LineData[] = [];
      for (let i = 0; i < normalized.length; i++) {
        const v = normalized[i];
        const bar = clean[i];
        if (v === null || v === undefined || !bar) continue;
        lineData.push({ time: bar.time as Time, value: v });
      }
      series.setData(lineData);
    });
  }, [compares, compareMode, rows]);

  // Add/remove indicator LineSeries based on `indicators` prop.
  // `rows` is in the dep array (alongside `indicators`) so this effect
  // re-runs after the prior data effect refreshes `cleanRowsRef`. Without
  // it, toggling indicators on a re-fetched range would compute SMAs over
  // the previous range's `cleanRowsRef`. Do not "simplify" by removing
  // `rows` from the deps.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const closes = cleanRowsRef.current.map((b) => b.close);

    // Remove series no longer enabled.
    for (const [id, series] of indicatorRefs.current.entries()) {
      if (!indicators.includes(id)) {
        chart.removeSeries(series);
        indicatorRefs.current.delete(id);
        indicatorValuesRef.current.delete(id);
      }
    }

    // Add new / update existing enabled indicators.
    for (const id of indicators) {
      const values = computeIndicator(id, closes);
      indicatorValuesRef.current.set(id, values);
      let series = indicatorRefs.current.get(id);
      if (!series) {
        series = chart.addSeries(LineSeries, {
          color: SMA_COLORS[id]!,
          lineWidth: 2,
          priceLineVisible: false,
          lastValueVisible: false,
        });
        indicatorRefs.current.set(id, series);
      }
      const data: LineData[] = [];
      for (let i = 0; i < values.length; i++) {
        const v = values[i];
        const bar = cleanRowsRef.current[i];
        if (v === null || v === undefined || !bar) continue;
        data.push({ time: bar.time as Time, value: v });
      }
      series.setData(data);
    }
  }, [indicators, rows]);

  // Trend overlays: linear regression + range channel.
  // In compare mode the visible axis is % change, so we compute trends on
  // the normalized primary series to keep overlays on the same scale.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const clean = cleanRowsRef.current;
    const rawCloses = clean.map((b) => b.close);
    // When compare mode is active, the primary series is displayed as % change.
    // Compute trends on the same normalized values so they render at the right
    // scale.  normalizeToPercent returns all-nulls only when base === 0 or
    // non-finite; in that degenerate case filter produces [] and the trend
    // helpers return empty arrays — overlays simply render no points, which is
    // correct.
    const closes = compares.length > 0
      ? normalizeToPercent(rawCloses).filter((v): v is number => v !== null)
      : rawCloses;

    // --- Linear regression ---
    if (showRegression) {
      const regValues = linearRegressionLine(closes);
      if (!regressionRef.current) {
        regressionRef.current = chart.addSeries(LineSeries, {
          color: chartTheme.regression,
          lineWidth: 1,
          lineStyle: 2, // dashed
          priceLineVisible: false,
          lastValueVisible: false,
        });
      }
      const regData: LineData[] = [];
      for (let i = 0; i < regValues.length; i++) {
        const v = regValues[i];
        const bar = clean[i];
        if (v === null || v === undefined || !bar) continue;
        regData.push({ time: bar.time as Time, value: v });
      }
      regressionRef.current.setData(regData);
    } else {
      if (regressionRef.current) {
        chart.removeSeries(regressionRef.current);
        regressionRef.current = null;
      }
    }

    // --- Channel: horizontal-ish dotted lines for range high + low ---
    const { high, low } = rangeChannel(closes);
    if (showChannel && high !== null && low !== null && clean.length >= 2) {
      if (!channelHighRef.current) {
        channelHighRef.current = chart.addSeries(LineSeries, {
          color: chartTheme.channelHigh,
          lineWidth: 1,
          lineStyle: 3, // dotted
          priceLineVisible: false,
          lastValueVisible: false,
        });
      }
      if (!channelLowRef.current) {
        channelLowRef.current = chart.addSeries(LineSeries, {
          color: chartTheme.channelLow,
          lineWidth: 1,
          lineStyle: 3, // dotted
          priceLineVisible: false,
          lastValueVisible: false,
        });
      }
      // Flat lines spanning the entire range.
      const firstBar = clean[0]!;
      const lastBar = clean[clean.length - 1]!;
      channelHighRef.current.setData([
        { time: firstBar.time as Time, value: high },
        { time: lastBar.time as Time, value: high },
      ]);
      channelLowRef.current.setData([
        { time: firstBar.time as Time, value: low },
        { time: lastBar.time as Time, value: low },
      ]);
    } else {
      if (channelHighRef.current) {
        chart.removeSeries(channelHighRef.current);
        channelHighRef.current = null;
      }
      if (channelLowRef.current) {
        chart.removeSeries(channelLowRef.current);
        channelLowRef.current = null;
      }
    }
  }, [showRegression, showChannel, rows, compares]);

  return (
    <div className="relative">
      {hover !== null ? <RichTooltip hover={hover} /> : null}
      <div
        ref={containerRef}
        data-testid={`chart-workspace-${symbol}`}
        className={containerClassName ?? "h-[70vh] w-full"}
      />
    </div>
  );
}

/**
 * TradingView-style status-line tooltip — single inline row at top-left,
 * no shadow box. Replaces the prior multi-row floating box that covered
 * the price-axis on the workspace chart. Indicator readouts wrap onto
 * a second row when present.
 */
function RichTooltip({ hover }: { hover: RichHoverState }): JSX.Element {
  const fmt = (n: number) => n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  const fmtPct = (v: number | null | undefined) => {
    if (v === null || v === undefined) return "—";
    return `${v >= 0 ? "+" : ""}${v.toFixed(2)}%`;
  };

  if (hover.mode === "compare") {
    const primaryClass =
      (hover.primaryPct ?? 0) >= 0 ? "text-emerald-600" : "text-red-600";
    return (
      <div className="absolute left-2 top-2 z-10 flex flex-wrap items-baseline gap-x-2 text-[11px] tabular-nums leading-tight text-slate-700">
        <span className="text-slate-500">{hover.date}</span>
        <span className="text-slate-400">·</span>
        <span className="font-medium text-slate-800">{hover.primarySymbol}</span>
        <span className={primaryClass}>{fmtPct(hover.primaryPct)}</span>
        {hover.comparePcts?.map((cp) => (
          <span key={cp.symbol} className="flex items-baseline gap-1">
            <span style={{ color: cp.color }} className="font-medium">
              {cp.symbol}
            </span>
            <span
              className={
                cp.value !== null && cp.value >= 0 ? "text-emerald-600" : "text-red-600"
              }
            >
              {fmtPct(cp.value)}
            </span>
          </span>
        ))}
      </div>
    );
  }

  // OHLCV mode — single inline row, indicator chips wrap on overflow.
  const deltaClass =
    hover.changePct === null || hover.changePct === undefined
      ? "text-slate-500"
      : hover.changePct >= 0
        ? "text-emerald-600"
        : "text-red-600";
  return (
    <div className="absolute left-2 top-2 z-10 flex flex-wrap items-baseline gap-x-2 text-[11px] tabular-nums leading-tight text-slate-700">
      <span className="text-slate-500">{hover.date}</span>
      <span className="text-slate-400">·</span>
      <span>
        <span className="text-slate-400">O</span> {fmt(hover.open ?? 0)}
      </span>
      <span>
        <span className="text-slate-400">H</span> {fmt(hover.high ?? 0)}
      </span>
      <span>
        <span className="text-slate-400">L</span> {fmt(hover.low ?? 0)}
      </span>
      <span className="font-medium text-slate-800">
        <span className="font-normal text-slate-400">C</span> {fmt(hover.close ?? 0)}
      </span>
      <span>
        <span className="text-slate-400">V</span> {humanizeVolume(hover.volume ?? 0)}
      </span>
      {hover.changePct !== null && hover.changePct !== undefined ? (
        <span className={deltaClass}>
          {hover.changePct >= 0 ? "+" : ""}
          {hover.changePct.toFixed(2)}%
        </span>
      ) : null}
      {hover.indicators !== undefined && hover.indicators.length > 0 ? (
        <>
          <span className="text-slate-400">·</span>
          {hover.indicators.map((row) => (
            <span key={row.id} className="flex items-baseline gap-1">
              <span className="text-slate-400">{row.label}</span>
              <span style={{ color: SMA_COLORS[row.id] }}>{fmt(row.value)}</span>
            </span>
          ))}
        </>
      ) : null}
    </div>
  );
}
