/**
 * Charts for the filings-analytics drill (#592): a stacked filing-density
 * timeline + a form-type heatmap. Both read the server's per-(quarter,
 * filing_type) counts and EXCLUDE insider Forms (3/4/5/144) — routine,
 * high-volume, and owned by the #588 insider drill.
 */
import { Fragment, type CSSProperties } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { FilingQuarterCount } from "@/api/types";
import { ChartTooltip } from "@/components/charts/ChartTooltip";
import {
  buildDensity,
  buildHeatmap,
  DENSITY_CATEGORIES,
  type DensityCategory,
  type DensityRow,
} from "@/lib/filingsAnalytics";
import { type ChartTheme } from "@/lib/chartTheme";
import { useChartTheme } from "@/lib/useChartTheme";

const CHART_HEIGHT = 280;

/** Category → its accent slot (6 categories ↔ the 6-tuple `theme.accent`). */
function categoryColor(theme: ChartTheme, category: DensityCategory): string {
  const i = DENSITY_CATEGORIES.indexOf(category);
  return theme.accent[i % theme.accent.length] ?? theme.primaryLine;
}

function hexToRgb(hex: string): { r: number; g: number; b: number } {
  const h = hex.replace("#", "");
  const n = parseInt(h.length === 3 ? h.replace(/./g, "$&$&") : h, 16);
  return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 };
}

function NoFilings({ message }: { message: string }): JSX.Element {
  return <p className="px-2 py-6 text-xs text-slate-500">{message}</p>;
}

// ---------------------------------------------------------------------------
// 1. Filing density timeline — stacked bars, count/quarter by material category
// ---------------------------------------------------------------------------

interface DensityTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: DensityRow }>;
}

function DensityTooltip({ active, payload }: DensityTooltipProps): JSX.Element | null {
  if (active !== true || !payload || payload.length === 0) return null;
  const row = payload[0]?.payload;
  if (!row || row.total === 0) return null;
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">{row.quarter}</div>
      {DENSITY_CATEGORIES.filter((cat) => row[cat] > 0).map((cat) => (
        <div key={cat} className="tabular-nums text-slate-600 dark:text-slate-300">
          {cat} {row[cat]}
        </div>
      ))}
      <div className="mt-0.5 tabular-nums font-medium text-slate-700 dark:text-slate-200">
        Total {row.total}
      </div>
    </ChartTooltip>
  );
}

export function FilingDensityChart({
  counts,
}: {
  readonly counts: ReadonlyArray<FilingQuarterCount>;
}): JSX.Element {
  const theme = useChartTheme();
  const data = buildDensity(counts);
  if (data.length === 0 || data.every((r) => r.total === 0)) {
    return <NoFilings message="No material company filings in the window." />;
  }
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 4 }}>
          <CartesianGrid stroke={theme.gridLine} vertical={false} />
          <XAxis
            dataKey="quarter"
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
          <Tooltip content={<DensityTooltip />} cursor={{ fill: theme.gridLine }} />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          {DENSITY_CATEGORIES.map((cat) => (
            <Bar
              key={cat}
              dataKey={cat}
              name={cat}
              stackId="filings"
              fill={categoryColor(theme, cat)}
              isAnimationActive={false}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 2. Form-type heatmap — categories × quarters, cell intensity by count
// ---------------------------------------------------------------------------

export function FilingHeatmapChart({
  counts,
}: {
  readonly counts: ReadonlyArray<FilingQuarterCount>;
}): JSX.Element {
  const theme = useChartTheme();
  const h = buildHeatmap(counts);
  if (h.quarters.length === 0 || h.max === 0) {
    return <NoFilings message="No material company filings to map." />;
  }
  // Per-category hue, opacity scaled by count / global-max so an unusual
  // cluster (e.g. a hot 8-K quarter) stands out across the whole grid.
  const cellStyle = (category: DensityCategory, count: number): CSSProperties => {
    if (count === 0) return { backgroundColor: "transparent" };
    const { r, g, b } = hexToRgb(categoryColor(theme, category));
    const alpha = 0.15 + 0.85 * (count / h.max);
    return { backgroundColor: `rgba(${r}, ${g}, ${b}, ${alpha})` };
  };
  return (
    <div className="overflow-x-auto">
      <div
        className="grid gap-px"
        style={{ gridTemplateColumns: `auto repeat(${h.quarters.length}, minmax(20px, 1fr))` }}
      >
        {/* header row: corner + quarter labels */}
        <div />
        {h.quarters.map((q) => (
          <div
            key={q}
            className="text-center text-[9px] tabular-nums text-slate-500"
            title={q}
          >
            {q.slice(2)}
          </div>
        ))}
        {/* one row per category */}
        {h.categories.map((cat) => (
          <Fragment key={cat}>
            <div className="pr-2 text-right text-[11px] text-slate-600 dark:text-slate-300">
              {cat}
            </div>
            {h.quarters.map((q) => {
              const n = h.get(cat, q);
              return (
                <div
                  key={`${cat}|${q}`}
                  className="h-5 rounded-sm border border-slate-100 dark:border-slate-800"
                  style={cellStyle(cat, n)}
                  title={`${cat} · ${q}: ${n}`}
                />
              );
            })}
          </Fragment>
        ))}
      </div>
      <p className="mt-2 text-[10px] text-slate-400">
        Cell shade ∝ filing count (peak {h.max}). Routine insider Form 3/4/5 excluded.
      </p>
    </div>
  );
}
