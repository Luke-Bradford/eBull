/**
 * Recharts subcomponents for the dividends drill page (#590).
 *
 * Each chart consumes pre-computed series from `lib/dividendsMetrics`
 * — no fetching here. The page owns the API calls and the empty-
 * state branching for the page-wide "no dividend history" case;
 * each chart still emits its own per-pane "no data" hint when its
 * specific series is empty (e.g. payout ratio when FCF is missing
 * everywhere).
 */

import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { DividendPeriod } from "@/api/instruments";
import type { InstrumentFinancialRow } from "@/api/types";
import { chartTheme } from "@/lib/chartTheme";
import {
  buildCumulativeDps,
  buildDpsSeries,
  buildPayoutRatio,
  buildYieldOnCost,
} from "@/lib/dividendsMetrics";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const CHART_HEIGHT = 240;

function formatPeriod(date: string): string {
  // SEC + provider feeds use `YYYY-MM-DD`. Render as `Mar '26` for
  // axis ticks (single source of truth for both quarterly and FY
  // rows since a fiscal-year-end IS a date).
  const y = date.slice(2, 4);
  const m = Number(date.slice(5, 7));
  const months = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];
  if (m >= 1 && m <= 12) return `${months[m - 1]} '${y}`;
  return date;
}

function formatDps(n: number | null, currency: string | null): string {
  if (n === null) return "—";
  const ccy = currency ?? "USD";
  // Dividends are typically <$10/share — show 4 decimal places so a
  // £0.0125 micro-divvy does not collapse to "£0.01".
  return n.toLocaleString(undefined, {
    style: "currency",
    currency: ccy,
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  });
}

function formatPct(n: number | null, digits: number = 1): string {
  if (n === null) return "—";
  return `${n.toFixed(digits)}%`;
}

function NoData({ message }: { readonly message: string }) {
  return <p className="px-2 py-3 text-xs text-slate-500">{message}</p>;
}

const SHARED_AXIS = {
  stroke: chartTheme.textSecondary,
  tick: { fill: chartTheme.textMuted, fontSize: 10 } as const,
};

const SHARED_GRID = (
  <CartesianGrid stroke={chartTheme.gridLine} vertical={false} />
);

// ---------------------------------------------------------------------------
// 1. DPS line chart
// ---------------------------------------------------------------------------

export interface HistoryProps {
  readonly history: ReadonlyArray<DividendPeriod>;
}

export function DpsLineChart({ history }: HistoryProps): JSX.Element {
  const series = buildDpsSeries(history);
  const hasData = series.some((p) => p.dps !== null);
  if (!hasData) {
    return <NoData message="No declared DPS rows on file." />;
  }
  const currency = series.find((p) => p.currency !== null)?.currency ?? "USD";
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={series} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          {SHARED_GRID}
          <XAxis
            dataKey="period_end_date"
            tickFormatter={formatPeriod}
            interval="preserveStartEnd"
            minTickGap={20}
            {...SHARED_AXIS}
          />
          <YAxis
            tickFormatter={(v: number) => formatDps(v, currency)}
            width={64}
            {...SHARED_AXIS}
          />
          <Tooltip
            formatter={(value: number) => formatDps(value, currency)}
            labelFormatter={formatPeriod}
            contentStyle={{ fontSize: "11px" }}
          />
          <Line
            type="monotone"
            dataKey="dps"
            name="DPS declared"
            stroke={chartTheme.accent[0]}
            strokeWidth={2.5}
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 2. Cumulative dividends paid (area chart — visually conveys "running total")
// ---------------------------------------------------------------------------

export function CumulativeDpsChart({ history }: HistoryProps): JSX.Element {
  const series = buildCumulativeDps(history);
  // Empty hint fires when:
  //   - the helper returned no rows at all, or
  //   - every row's cumulative_dps is null (every source dps was
  //     missing — the round-2 fix to emit nulls for gaps means the
  //     all-null case now passes the rendered-axes guard otherwise).
  // Without the second branch, an issuer whose `dps_declared` column
  // is empty everywhere renders an AreaChart with no visible series
  // instead of the inline "no data" hint (PR #673 review).
  const lastWithValue = [...series]
    .reverse()
    .find((p) => p.cumulative_dps !== null);
  if (lastWithValue === undefined || lastWithValue.cumulative_dps === 0) {
    return <NoData message="No declared DPS history to accumulate." />;
  }
  const currency = lastWithValue.currency ?? "USD";
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={series} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          {SHARED_GRID}
          <XAxis
            dataKey="period_end_date"
            tickFormatter={formatPeriod}
            interval="preserveStartEnd"
            minTickGap={20}
            {...SHARED_AXIS}
          />
          <YAxis
            tickFormatter={(v: number) => formatDps(v, currency)}
            width={64}
            {...SHARED_AXIS}
          />
          <Tooltip
            formatter={(value: number) => formatDps(value, currency)}
            labelFormatter={formatPeriod}
            contentStyle={{ fontSize: "11px" }}
          />
          <Area
            type="monotone"
            dataKey="cumulative_dps"
            name="Cumulative DPS"
            stroke={chartTheme.accent[1]}
            fill={chartTheme.accent[1]}
            fillOpacity={0.15}
            strokeWidth={2.5}
            dot={false}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 3. Payout-ratio line (annual cashflow input)
// ---------------------------------------------------------------------------

export interface PayoutRatioProps {
  readonly cashflowRows: ReadonlyArray<InstrumentFinancialRow>;
}

export function PayoutRatioChart({ cashflowRows }: PayoutRatioProps): JSX.Element {
  const series = buildPayoutRatio(cashflowRows);
  const hasData = series.some((p) => p.payout_pct !== null);
  if (!hasData) {
    return (
      <NoData message="Payout ratio needs annual operating cash flow, capex, and dividends paid on the cash-flow statement." />
    );
  }
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={series} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          {SHARED_GRID}
          <XAxis
            dataKey="period_end_date"
            tickFormatter={formatPeriod}
            interval="preserveStartEnd"
            minTickGap={20}
            {...SHARED_AXIS}
          />
          <YAxis
            tickFormatter={(v: number) => `${v.toFixed(0)}%`}
            width={48}
            {...SHARED_AXIS}
          />
          <ReferenceLine
            y={100}
            stroke={chartTheme.accent[3]}
            strokeDasharray="4 4"
            label={{
              value: "100%",
              position: "right",
              fill: chartTheme.textMuted,
              fontSize: 10,
            }}
          />
          <Tooltip
            formatter={(value: number) => formatPct(value)}
            labelFormatter={formatPeriod}
            contentStyle={{ fontSize: "11px" }}
          />
          <Line
            type="monotone"
            dataKey="payout_pct"
            name="Payout / FCF"
            stroke={chartTheme.accent[2]}
            strokeWidth={2.5}
            dot={{ r: 3, fill: chartTheme.accent[2] }}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 4. Yield-on-cost (bar chart — discrete fiscal years)
// ---------------------------------------------------------------------------

export interface YieldOnCostProps {
  readonly history: ReadonlyArray<DividendPeriod>;
  /** Operator's average entry price (per share, native currency).
   *  When null the chart returns null upstream and the page hides
   *  the entire pane. */
  readonly avgEntry: number | null;
}

export function YieldOnCostChart({
  history,
  avgEntry,
}: YieldOnCostProps): JSX.Element {
  const series = buildYieldOnCost(history, avgEntry);
  if (series === null) {
    return <NoData message="Yield-on-cost is shown only when this instrument is held." />;
  }
  if (series.length === 0) {
    return <NoData message="No declared DPS rows to compute yield-on-cost." />;
  }
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={series} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          {SHARED_GRID}
          <XAxis dataKey="fiscal_year" {...SHARED_AXIS} />
          <YAxis
            tickFormatter={(v: number) => `${v.toFixed(1)}%`}
            width={48}
            {...SHARED_AXIS}
          />
          <Tooltip
            formatter={(value: number, name: string) =>
              name === "Yield-on-cost"
                ? [formatPct(value, 2), name]
                : [value.toFixed(4), name]
            }
            labelFormatter={(fy: number) => `FY${fy}`}
            contentStyle={{ fontSize: "11px" }}
          />
          <Bar dataKey="yoc_pct" name="Yield-on-cost" isAnimationActive={false}>
            {series.map((s) => (
              <Cell key={s.fiscal_year} fill={chartTheme.up} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
