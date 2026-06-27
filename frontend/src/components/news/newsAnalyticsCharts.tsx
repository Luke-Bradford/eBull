/**
 * Charts for the news-analytics drill (#593): a bicolor sentiment trend, a
 * weekly news-volume bar, and a source-breakdown donut. All read the pure
 * shapes from `@/lib/newsAnalytics` and take every colour from
 * `useChartTheme()` `theme.*` — never `lightTheme.*` (prevention-log 1917,
 * the #591 risk-drill regression).
 */
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { ChartTooltip } from "@/components/charts/ChartTooltip";
import { type ChartTheme } from "@/lib/chartTheme";
import {
  type SentimentSeries,
  type SourceSlice,
  type WeeklyVolumePoint,
} from "@/lib/newsAnalytics";
import { useChartTheme } from "@/lib/useChartTheme";

const CHART_HEIGHT = 280;

function NoNews({ message }: { message: string }): JSX.Element {
  return <p className="px-2 py-6 text-xs text-slate-500">{message}</p>;
}

/** `2026-06-22` → `06-22` for a compact daily tick. */
function dayTick(date: string): string {
  return date.slice(5);
}

// ---------------------------------------------------------------------------
// 1. Sentiment trend — 7-day rolling mean, emerald above 0 / red below 0
// ---------------------------------------------------------------------------

interface SentimentTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: { date: string; rolling: number | null; mean: number | null; count: number } }>;
}

function SentimentTooltip({ active, payload }: SentimentTooltipProps): JSX.Element | null {
  if (active !== true || !payload || payload.length === 0) return null;
  const row = payload[0]?.payload;
  if (!row) return null;
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">{row.date}</div>
      <div className="tabular-nums text-slate-600 dark:text-slate-300">
        7d sentiment {row.rolling === null ? "—" : row.rolling.toFixed(3)}
      </div>
      <div className="tabular-nums text-slate-500 dark:text-slate-400">
        day mean {row.mean === null ? "—" : row.mean.toFixed(3)} · {row.count} item
        {row.count === 1 ? "" : "s"}
      </div>
    </ChartTooltip>
  );
}

export function SentimentTrendChart({ series }: { series: SentimentSeries }): JSX.Element {
  const theme = useChartTheme();
  const hasSignal = series.points.some((p) => p.rolling !== null);
  if (series.points.length === 0 || !hasSignal) {
    return <NoNews message="No scored sentiment in the window." />;
  }
  // Axis domain MUST equal the fill bbox [min(0,min) .. max(0,max)] (no
  // padding) so the axis-positioned zero line coincides with the gradient's
  // emerald/red boundary — see lib/newsAnalytics splitOffset note.
  const domLo = Math.min(0, series.min);
  const domHi = Math.max(0, series.max);
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={[...series.points]} margin={{ top: 8, right: 8, left: 0, bottom: 4 }}>
          <defs>
            <linearGradient id="newsSentFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset={series.splitOffset} stopColor={theme.up} stopOpacity={0.45} />
              <stop offset={series.splitOffset} stopColor={theme.down} stopOpacity={0.45} />
            </linearGradient>
            <linearGradient id="newsSentStroke" x1="0" y1="0" x2="0" y2="1">
              <stop offset={series.splitOffset} stopColor={theme.up} />
              <stop offset={series.splitOffset} stopColor={theme.down} />
            </linearGradient>
          </defs>
          <CartesianGrid stroke={theme.gridLine} vertical={false} />
          <XAxis
            dataKey="date"
            tickFormatter={dayTick}
            stroke={theme.textSecondary}
            tick={{ fill: theme.textMuted, fontSize: 10 }}
            interval="preserveStartEnd"
            minTickGap={20}
          />
          <YAxis
            domain={[domLo, domHi]}
            stroke={theme.textSecondary}
            tick={{ fill: theme.textMuted, fontSize: 10 }}
            width={40}
            tickFormatter={(v: number) => v.toFixed(2)}
          />
          <ReferenceLine y={0} stroke={theme.borderColor} />
          <Tooltip content={<SentimentTooltip />} cursor={{ stroke: theme.gridLine }} />
          <Area
            type="monotone"
            dataKey="rolling"
            baseValue={0}
            stroke="url(#newsSentStroke)"
            strokeWidth={1.5}
            fill="url(#newsSentFill)"
            connectNulls
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 2. News volume — count per ISO week
// ---------------------------------------------------------------------------

interface VolumeTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: WeeklyVolumePoint }>;
}

function VolumeTooltip({ active, payload }: VolumeTooltipProps): JSX.Element | null {
  if (active !== true || !payload || payload.length === 0) return null;
  const row = payload[0]?.payload;
  if (!row) return null;
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">{row.week}</div>
      <div className="tabular-nums text-slate-600 dark:text-slate-300">
        {row.count} item{row.count === 1 ? "" : "s"}
      </div>
    </ChartTooltip>
  );
}

export function NewsVolumeChart({ data }: { data: readonly WeeklyVolumePoint[] }): JSX.Element {
  const theme = useChartTheme();
  if (data.length === 0) {
    return <NoNews message="No news in the window." />;
  }
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={[...data]} margin={{ top: 8, right: 8, left: 0, bottom: 4 }}>
          <CartesianGrid stroke={theme.gridLine} vertical={false} />
          <XAxis
            dataKey="week"
            stroke={theme.textSecondary}
            tick={{ fill: theme.textMuted, fontSize: 10 }}
            interval="preserveStartEnd"
            minTickGap={16}
          />
          <YAxis
            allowDecimals={false}
            stroke={theme.textSecondary}
            tick={{ fill: theme.textMuted, fontSize: 10 }}
            width={32}
          />
          <Tooltip content={<VolumeTooltip />} cursor={{ fill: theme.gridLine }} />
          <Bar dataKey="count" name="news" fill={theme.accent[1]} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 3. Source breakdown — donut by source
// ---------------------------------------------------------------------------

interface SourceTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: SourceSlice; percent?: number }>;
}

function SourceTooltip({ active, payload }: SourceTooltipProps): JSX.Element | null {
  if (active !== true || !payload || payload.length === 0) return null;
  const slice = payload[0]?.payload;
  if (!slice) return null;
  const pct = payload[0]?.percent;
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">{slice.source}</div>
      <div className="tabular-nums text-slate-600 dark:text-slate-300">
        {slice.count} item{slice.count === 1 ? "" : "s"}
        {pct !== undefined ? ` · ${(pct * 100).toFixed(0)}%` : ""}
      </div>
    </ChartTooltip>
  );
}

function sliceColor(theme: ChartTheme, i: number): string {
  return theme.accent[i % theme.accent.length] ?? theme.primaryLine;
}

export function SourceBreakdownPie({ slices }: { slices: readonly SourceSlice[] }): JSX.Element {
  const theme = useChartTheme();
  if (slices.length === 0) {
    return <NoNews message="No sources in the window." />;
  }
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={[...slices]}
            dataKey="count"
            nameKey="source"
            cx="50%"
            cy="50%"
            innerRadius={50}
            outerRadius={95}
            paddingAngle={1}
            isAnimationActive={false}
          >
            {slices.map((s, i) => (
              <Cell key={s.source} fill={sliceColor(theme, i)} />
            ))}
          </Pie>
          <Tooltip content={<SourceTooltip />} />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}
