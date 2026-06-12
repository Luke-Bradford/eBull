/**
 * §4.2 Performance vs benchmark — indexed line chart (both series =
 * 100 at window start) over the trailing snapshot window, assembled
 * client-side from immutable stamped points (spec §3.1).
 *
 * Sparse-series treatment is deliberate (§6.3): always-on point
 * markers, ReferenceLine y=100, and the "N periods" count — 2–5
 * points must read as stated fact, not rendering failure.
 */
import {
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { ChartTooltip } from "@/components/charts/ChartTooltip";
import { NilLine, ScopeCaveat, Fn, type NoteIndex } from "@/components/reports/StatementChrome";
import type { TrailingPoint } from "@/components/reports/snapshotMath";
import { formatDate } from "@/lib/format";
import { useChartTheme } from "@/lib/useChartTheme";

interface TooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: TrailingPoint }>;
}

function PerformanceTooltip({ active, payload }: TooltipProps) {
  if (active !== true || !payload || payload.length === 0) return null;
  const point = payload[0]?.payload;
  if (!point) return null;
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">
        {formatDate(point.period_end)}
      </div>
      <div className="tabular-nums text-slate-600 dark:text-slate-300">
        Portfolio {point.portfolio.toFixed(1)}
      </div>
      {point.benchmark !== null ? (
        <div className="tabular-nums text-slate-500 dark:text-slate-400">
          Benchmark {point.benchmark.toFixed(1)}
        </div>
      ) : null}
    </ChartTooltip>
  );
}

export function PerformanceChart({
  points,
  benchmarkLabel,
  marker,
}: {
  points: TrailingPoint[];
  benchmarkLabel: string;
  marker: NoteIndex;
}) {
  const theme = useChartTheme();
  const hasBenchmark = points.some((p) => p.benchmark !== null);

  if (points.length === 0) {
    return <NilLine>No stamped performance points yet — the line begins with the next v2 snapshot.</NilLine>;
  }

  return (
    <div className="space-y-1">
      <div className="h-56 w-full">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={points} margin={{ top: 8, right: 16, left: 0, bottom: 4 }}>
            <XAxis
              dataKey="period_end"
              tickFormatter={(v: string) => formatDate(v)}
              stroke={theme.textSecondary}
              tick={{ fill: theme.textMuted, fontSize: 10 }}
            />
            <YAxis
              domain={["auto", "auto"]}
              stroke={theme.textSecondary}
              tick={{ fill: theme.textMuted, fontSize: 10 }}
              width={44}
            />
            <ReferenceLine y={100} stroke={theme.gridLine} />
            <Tooltip content={<PerformanceTooltip />} cursor={{ stroke: theme.crosshair }} />
            <Line
              type="monotone"
              dataKey="portfolio"
              stroke={theme.primaryLine}
              strokeWidth={1.5}
              dot={{ r: 3 }}
              isAnimationActive={false}
              name="Portfolio"
            />
            {hasBenchmark ? (
              <Line
                type="monotone"
                dataKey="benchmark"
                stroke={theme.accent[1]}
                strokeWidth={1.5}
                strokeDasharray="4 3"
                dot={{ r: 3 }}
                isAnimationActive={false}
                name={benchmarkLabel}
                connectNulls
              />
            ) : null}
          </LineChart>
        </ResponsiveContainer>
      </div>
      {/* Theme-aware legend — the recharts default breaks dark mode. */}
      <div className="flex items-center gap-4 text-[11px] text-slate-500">
        <span className="flex items-center gap-1.5">
          <span className="inline-block h-0.5 w-4" style={{ backgroundColor: theme.primaryLine }} />
          Portfolio
        </span>
        {hasBenchmark ? (
          <span className="flex items-center gap-1.5">
            <span
              className="inline-block h-0 w-4 border-t-2 border-dashed"
              style={{ borderColor: theme.accent[1] }}
            />
            {benchmarkLabel}
            <Fn n={marker.benchmark} />
          </span>
        ) : (
          <span>Benchmark unavailable for this window</span>
        )}
      </div>
      {points.length === 1 ? (
        <ScopeCaveat>One stamped period so far — the line begins with the next snapshot.</ScopeCaveat>
      ) : null}
      <ScopeCaveat>
        Indexed to 100 at window start · converted at each report&apos;s generation-date FX
        <Fn n={marker.fx} />
      </ScopeCaveat>
    </div>
  );
}
