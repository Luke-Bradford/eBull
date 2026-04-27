/**
 * ChartWorkspaceCanvas — full-viewport chart canvas for ChartPage (#576 Phase 2).
 *
 * Extends the lightweight-charts setup with:
 *   - SMA/EMA overlay LineSeries (toggleable via `indicators` prop)
 *   - Rich OHLC + volume + Δ% + per-indicator crosshair tooltip
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

import type { CandleBar } from "@/api/types";

const SMA_COLORS: Record<string, string> = {
  sma20: "#3b82f6", // blue-500
  sma50: "#a855f7", // purple-500
  ema20: "#0ea5e9", // sky-500
  ema50: "#ec4899", // pink-500
};

const SMA_LABELS: Record<string, string> = {
  sma20: "SMA(20)",
  sma50: "SMA(50)",
  ema20: "EMA(20)",
  ema50: "EMA(50)",
};

export type IndicatorId = "sma20" | "sma50" | "ema20" | "ema50";
export const INDICATOR_IDS: IndicatorId[] = ["sma20", "sma50", "ema20", "ema50"];

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
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  changePct: number | null;
  indicators: Array<{ id: IndicatorId; label: string; value: number }>;
}

function parseNum(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

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
  readonly rows: CandleBar[];
  readonly symbol: string;
  readonly indicators: ReadonlyArray<IndicatorId>;
  readonly containerClassName?: string;
}

export function ChartWorkspaceCanvas({
  rows,
  symbol,
  indicators,
  containerClassName,
}: ChartWorkspaceCanvasProps): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const indicatorRefs = useRef<Map<IndicatorId, ISeriesApi<"Line">>>(new Map());
  // Stable refs for crosshair closure — no stale-closure risk.
  const cleanRowsRef = useRef<NumericBar[]>([]);
  const indicatorValuesRef = useRef<Map<IndicatorId, Array<number | null>>>(new Map());
  const [hover, setHover] = useState<RichHoverState | null>(null);

  // One-shot chart construction — mirrors ChartCanvas setup.
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

    const volume = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      priceFormat: { type: "volume" },
    });
    chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.75, bottom: 0 } });

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

      const date = new Date(time * 1000).toISOString().slice(0, 10);
      setHover({
        date,
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

    return () => {
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      volumeRef.current = null;
      indicatorRefs.current.clear();
    };
  }, []);

  // Feed candle + volume data on rows change.
  useEffect(() => {
    const candle = candleRef.current;
    const volume = volumeRef.current;
    const chart = chartRef.current;
    if (!candle || !volume || !chart) return;

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

    volume.setData(
      clean.map((b, i) => {
        const prev = i > 0 ? clean[i - 1]!.close : b.close;
        return {
          time: b.time as Time,
          value: b.volume,
          color: b.close >= prev ? "rgba(16,185,129,0.4)" : "rgba(239,68,68,0.4)",
        };
      }),
    );

    chart.timeScale().fitContent();
  }, [rows]);

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

  return (
    <div className="relative">
      {hover !== null ? <RichTooltip hover={hover} symbol={symbol} /> : null}
      <div
        ref={containerRef}
        data-testid={`chart-workspace-${symbol}`}
        className={containerClassName ?? "h-[70vh] w-full"}
      />
    </div>
  );
}

function RichTooltip({ hover, symbol }: { hover: RichHoverState; symbol: string }): JSX.Element {
  const fmt = (n: number) =>
    n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return (
    <div className="absolute right-2 top-2 z-10 min-w-[180px] rounded bg-white/95 px-3 py-2 text-xs shadow-md">
      <div className="font-semibold text-slate-800">{symbol}</div>
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
      {hover.indicators.length > 0 ? (
        <div className="mt-1 border-t border-slate-100 pt-1">
          {hover.indicators.map((row) => (
            <div key={row.id} className="flex justify-between text-[11px]">
              <span className="text-slate-500">{row.label}</span>
              <span className="tabular-nums" style={{ color: SMA_COLORS[row.id] }}>
                {fmt(row.value)}
              </span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}
