/**
 * Recharts subcomponents for the risk/returns drill page (#591 PR-C).
 *
 * STRICTLY PURE RENDERERS. Every risk estimator (drawdown, rolling vol,
 * histogram bins, OLS beta) is computed by the backend (risk_metrics.py)
 * and arrives pre-computed in the `/risk-metrics` `series` payload. No risk
 * math happens in this file — the only client-side arithmetic is
 * *descriptive* summary of the bars/points already on screen (the histogram
 * centroid for the mean/σ caption, the scatter centroid to position the
 * backend-supplied β fit line). Neither re-derives a persisted estimator.
 *
 * Wire values are Pydantic `Decimal` → JSON strings; parse to number only
 * here, at the chart boundary.
 */

import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

import type {
  BetaScatterPoint,
  DrawdownPoint,
  HistogramBin,
  RollingVolPoint,
} from "@/api/types";
import type { ChartTheme } from "@/lib/chartTheme";
import { useChartTheme } from "@/lib/useChartTheme";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const CHART_HEIGHT = 240;

/** Parse a wire Decimal string to a finite number, or null. */
function num(v: string | null): number | null {
  if (v === null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/** Fraction → signed percent string (0.0123 → "+1.23%"). Presentation only. */
function pctSigned(fraction: number, digits = 2): string {
  const sign = fraction > 0 ? "+" : "";
  return `${sign}${(fraction * 100).toFixed(digits)}%`;
}

function pct(fraction: number, digits = 1): string {
  return `${(fraction * 100).toFixed(digits)}%`;
}

function formatDay(date: string): string {
  // Series dates are `YYYY-MM-DD`. Render as `Mar '26` to keep axis ticks
  // compact across multi-year windows.
  const y = date.slice(2, 4);
  const m = Number(date.slice(5, 7));
  const months = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
  ];
  if (m >= 1 && m <= 12) return `${months[m - 1]} '${y}`;
  return date;
}

function NoData({ message }: { readonly message: string }) {
  return <p className="px-2 py-3 text-xs text-slate-500">{message}</p>;
}

function sharedAxis(theme: ChartTheme) {
  return {
    stroke: theme.textSecondary,
    tick: { fill: theme.textMuted, fontSize: 10 } as const,
  };
}

function SharedGrid({ theme }: { readonly theme: ChartTheme }): JSX.Element {
  return <CartesianGrid stroke={theme.gridLine} vertical={false} />;
}

// ---------------------------------------------------------------------------
// 1. Drawdown underwater area — running peak-to-trough %, fills below 0.
// ---------------------------------------------------------------------------

export interface UnderwaterProps {
  /** Range-sliced drawdown points (drawdown ≤ 0, as a fraction). */
  readonly points: ReadonlyArray<DrawdownPoint>;
}

export function UnderwaterChart({ points }: UnderwaterProps): JSX.Element {
  const theme = useChartTheme();
  const data = points
    .map((p) => ({ date: p.date, drawdown: num(p.drawdown) }))
    .filter((p): p is { date: string; drawdown: number } => p.drawdown !== null);
  if (data.length === 0) {
    return <NoData message="No price history to chart drawdown." />;
  }
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis
            dataKey="date"
            tickFormatter={formatDay}
            interval="preserveStartEnd"
            minTickGap={28}
            {...sharedAxis(theme)}
          />
          <YAxis
            tickFormatter={(v: number) => pct(v, 0)}
            width={48}
            {...sharedAxis(theme)}
          />
          <ReferenceLine y={0} stroke={theme.borderColor} />
          <Tooltip
            formatter={(value: number) => [pctSigned(value), "Drawdown"]}
            labelFormatter={formatDay}
            contentStyle={{ fontSize: "11px" }}
          />
          <Area
            type="monotone"
            dataKey="drawdown"
            name="Drawdown"
            stroke={theme.down}
            fill={theme.down}
            fillOpacity={0.18}
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 2. Rolling annualized volatility — line.
// ---------------------------------------------------------------------------

export interface RollingVolProps {
  /** Range-sliced rolling-vol points (annualized fraction). */
  readonly points: ReadonlyArray<RollingVolPoint>;
}

export function RollingVolChart({ points }: RollingVolProps): JSX.Element {
  const theme = useChartTheme();
  const data = points
    .map((p) => ({ date: p.date, vol: num(p.vol) }))
    .filter((p): p is { date: string; vol: number } => p.vol !== null);
  if (data.length === 0) {
    return (
      <NoData message="Not enough history for a rolling-volatility window yet." />
    );
  }
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis
            dataKey="date"
            tickFormatter={formatDay}
            interval="preserveStartEnd"
            minTickGap={28}
            {...sharedAxis(theme)}
          />
          <YAxis
            tickFormatter={(v: number) => pct(v, 0)}
            width={48}
            {...sharedAxis(theme)}
          />
          <Tooltip
            formatter={(value: number) => [pct(value), "Annualized vol"]}
            labelFormatter={formatDay}
            contentStyle={{ fontSize: "11px" }}
          />
          <Line
            type="monotone"
            dataKey="vol"
            name="Annualized vol"
            stroke={theme.accent[3]}
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 3. Returns histogram — backend-binned daily returns, mean / σ annotation.
// ---------------------------------------------------------------------------

export interface ReturnsHistogramProps {
  readonly bins: ReadonlyArray<HistogramBin>;
}

/** Descriptive centroid + spread of the *displayed* bins (NOT a risk
 *  estimator — purely to caption the bars on screen). */
function describeBins(
  bins: ReadonlyArray<HistogramBin>,
): { mean: number; std: number } | null {
  let total = 0;
  let sum = 0;
  const mids: Array<{ mid: number; count: number }> = [];
  for (const b of bins) {
    const lo = num(b.lower);
    const hi = num(b.upper);
    if (lo === null || hi === null) continue;
    const mid = (lo + hi) / 2;
    mids.push({ mid, count: b.count });
    total += b.count;
    sum += mid * b.count;
  }
  if (total === 0) return null;
  const mean = sum / total;
  let variance = 0;
  for (const { mid, count } of mids) variance += count * (mid - mean) ** 2;
  return { mean, std: Math.sqrt(variance / total) };
}

export function ReturnsHistogram({ bins }: ReturnsHistogramProps): JSX.Element {
  const theme = useChartTheme();
  const stats = describeBins(bins);
  const data = bins.map((b) => {
    const lo = num(b.lower) ?? 0;
    const hi = num(b.upper) ?? 0;
    return { mid: (lo + hi) / 2, count: b.count };
  });
  const hasData = data.some((d) => d.count > 0);
  if (!hasData || stats === null) {
    return <NoData message="Not enough returns to chart a distribution." />;
  }
  return (
    <div className="w-full">
      <p className="px-2 pb-1 text-[11px] text-slate-500">
        Daily returns, full history · mean {pctSigned(stats.mean)} · σ{" "}
        {pct(stats.std)}
      </p>
      <div style={{ height: CHART_HEIGHT }} className="w-full">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
            <SharedGrid theme={theme} />
            <XAxis
              dataKey="mid"
              type="number"
              domain={["dataMin", "dataMax"]}
              tickFormatter={(v: number) => pctSigned(v, 1)}
              {...sharedAxis(theme)}
            />
            <YAxis width={36} allowDecimals={false} {...sharedAxis(theme)} />
            <ReferenceLine
              x={stats.mean}
              stroke={theme.textSecondary}
              strokeDasharray="4 4"
              label={{ value: "mean", position: "top", fill: theme.textMuted, fontSize: 10 }}
            />
            <ReferenceLine x={stats.mean - stats.std} stroke={theme.borderColor} strokeDasharray="2 4" />
            <ReferenceLine x={stats.mean + stats.std} stroke={theme.borderColor} strokeDasharray="2 4" />
            <Tooltip
              formatter={(value: number) => [String(value), "Days"]}
              labelFormatter={(v: number) => `~${pctSigned(v, 2)}`}
              contentStyle={{ fontSize: "11px" }}
            />
            <Bar
              dataKey="count"
              name="Days"
              fill={theme.accent[1]}
              isAnimationActive={false}
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 4. Beta scatter vs benchmark — points + OLS fit line (β, R²).
// ---------------------------------------------------------------------------

export interface BetaScatterProps {
  readonly points: ReadonlyArray<BetaScatterPoint>;
  /** Full-series OLS slope from the backend. Null → no benchmark overlap. */
  readonly beta: string | null;
  readonly r2: string | null;
  readonly benchmarkSymbol: string | null;
}

export function BetaScatterChart({
  points,
  beta,
  r2,
  benchmarkSymbol,
}: BetaScatterProps): JSX.Element {
  const theme = useChartTheme();
  const data = points
    .map((p) => ({ x: num(p.spy_return), y: num(p.inst_return) }))
    .filter((p): p is { x: number; y: number } => p.x !== null && p.y !== null);
  const slope = num(beta);
  if (data.length === 0 || slope === null) {
    return (
      <NoData message="No overlapping benchmark history to fit a beta." />
    );
  }
  // The OLS line passes through the cloud centroid (x̄, ȳ); using the
  // backend β as the slope reconstructs the exact fit without an alpha term.
  // Centroid is a descriptive stat of the displayed points, not an estimator.
  const n = data.length;
  const xBar = data.reduce((s, p) => s + p.x, 0) / n;
  const yBar = data.reduce((s, p) => s + p.y, 0) / n;
  const xMin = Math.min(...data.map((p) => p.x));
  const xMax = Math.max(...data.map((p) => p.x));
  const fitSegment = [
    { x: xMin, y: yBar + slope * (xMin - xBar) },
    { x: xMax, y: yBar + slope * (xMax - xBar) },
  ];
  const bench = benchmarkSymbol ?? "benchmark";
  const r2n = num(r2);
  return (
    <div className="w-full">
      <p className="px-2 pb-1 text-[11px] text-slate-500">
        Daily returns vs {bench}, full history · β {slope.toFixed(2)}
        {r2n !== null ? ` · R² ${r2n.toFixed(2)}` : ""}
      </p>
      <div style={{ height: CHART_HEIGHT }} className="w-full">
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
            <SharedGrid theme={theme} />
            <XAxis
              type="number"
              dataKey="x"
              name={bench}
              tickFormatter={(v: number) => pctSigned(v, 1)}
              {...sharedAxis(theme)}
            />
            <YAxis
              type="number"
              dataKey="y"
              name="instrument"
              width={48}
              tickFormatter={(v: number) => pctSigned(v, 1)}
              {...sharedAxis(theme)}
            />
            <ZAxis range={[10, 10]} />
            <ReferenceLine
              segment={fitSegment}
              stroke={theme.regression}
              strokeWidth={2}
              ifOverflow="extendDomain"
            />
            <Tooltip
              cursor={{ strokeDasharray: "3 3" }}
              formatter={(value: number, name: string) => [pctSigned(value), name]}
              contentStyle={{ fontSize: "11px" }}
            />
            <Scatter
              data={data}
              fill={theme.accent[0]}
              fillOpacity={0.45}
              isAnimationActive={false}
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
