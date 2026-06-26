/**
 * Recharts subcomponents for the fundamentals drill page (#589).
 *
 * Every chart consumes pre-computed data from `lib/fundamentalsMetrics`
 * — no fetching, no derivation here. The page owns the API calls and
 * passes the joined period array down; each chart picks the slice
 * it needs through one of the `build*` helpers and renders.
 *
 * Empty / sparse handling: when all values for a metric are null,
 * the component renders a small "No data" line instead of a recharts
 * frame with no bars. Mixed-presence series (some periods null, some
 * present) render as gaps rather than zero — recharts' `connectNulls`
 * is left at its default `false` so a missing quarter isn't visually
 * smoothed over.
 */

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { FcfYieldSeries } from "@/api/types";
import { ChartTooltip } from "@/components/charts/ChartTooltip";
import { type ChartTheme, lightTheme } from "@/lib/chartTheme";
import { useChartTheme } from "@/lib/useChartTheme";
import {
  buildCashflowWaterfall,
  buildDebtStructure,
  buildDupont,
  buildFcf,
  buildMargins,
  buildPnlBuckets,
  buildRoic,
  buildYoyGrowth,
  latestBalanceStructure,
  type JoinedPeriod,
} from "@/lib/fundamentalsMetrics";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const CHART_HEIGHT = 280;

function formatPeriod(period_end: string): string {
  // SEC XBRL reports always use `YYYY-MM-DD` so the slice is safe.
  const y = period_end.slice(2, 4);
  const m = Number(period_end.slice(5, 7));
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
  return period_end;
}

function formatBigNumber(n: number | null): string {
  if (n === null) return "—";
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(1)}K`;
  return n.toFixed(0);
}

function formatPct(n: number | null, digits: number = 1): string {
  if (n === null) return "—";
  return `${n.toFixed(digits)}%`;
}

function formatRatio(n: number | null, digits: number = 2): string {
  if (n === null) return "—";
  return n.toFixed(digits);
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
// 1. Quarterly P&L stacked bar
// ---------------------------------------------------------------------------

interface PnlChartProps {
  readonly periods: ReadonlyArray<JoinedPeriod>;
}

interface YoyChartProps extends PnlChartProps {
  /** Page-level period selection — passed through to `buildYoyGrowth`
   *  so the lag is computed from the requested view, not by guessing
   *  at the row's `period_type` (which the backend emits as `FY` /
   *  `Q1`…`Q4`, never the literal `"annual"`). */
  readonly period: "quarterly" | "annual";
}

export function PnlStackedChart({ periods }: PnlChartProps): JSX.Element {
  const theme = useChartTheme();
  const buckets = buildPnlBuckets(periods);
  const hasData = buckets.some(
    (b) => b.cogs !== null || b.opex !== null || b.op_income !== null,
  );
  if (!hasData) return <NoData message="No income statement data on file." />;
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={buckets} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis
            dataKey="period_end"
            tickFormatter={formatPeriod}
            interval="preserveStartEnd"
            minTickGap={20}
            {...sharedAxis(theme)}
          />
          <YAxis tickFormatter={(v: number) => formatBigNumber(v)} width={60} {...sharedAxis(theme)} />
          <Tooltip
            cursor={{ fill: theme.gridLine }}
            formatter={(value: number) => formatBigNumber(value)}
            labelFormatter={formatPeriod}
            contentStyle={{ fontSize: "11px" }}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Bar dataKey="cogs" name="COGS" stackId="a" fill={lightTheme.accent[3]} isAnimationActive={false} />
          <Bar dataKey="opex" name="Opex (R&D + SG&A)" stackId="a" fill={lightTheme.accent[2]} isAnimationActive={false} />
          <Bar dataKey="op_income" name="Operating income" stackId="a" fill={lightTheme.accent[0]} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 2. Margin trends multi-line
// ---------------------------------------------------------------------------

export function MarginTrendsChart({ periods }: PnlChartProps): JSX.Element {
  const theme = useChartTheme();
  const margins = buildMargins(periods);
  const hasData = margins.some(
    (m) => m.gross_pct !== null || m.operating_pct !== null || m.net_pct !== null,
  );
  if (!hasData) return <NoData message="Margins need both revenue and profit fields." />;
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={margins} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis dataKey="period_end" tickFormatter={formatPeriod} interval="preserveStartEnd" minTickGap={20} {...sharedAxis(theme)} />
          <YAxis tickFormatter={(v: number) => `${v.toFixed(0)}%`} width={48} {...sharedAxis(theme)} />
          <ReferenceLine y={0} stroke={theme.borderColor} />
          <Tooltip
            formatter={(value: number) => formatPct(value)}
            labelFormatter={formatPeriod}
            contentStyle={{ fontSize: "11px" }}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Line type="monotone" dataKey="gross_pct" name="Gross" stroke={lightTheme.accent[1]} strokeWidth={2} dot={false} isAnimationActive={false} />
          <Line type="monotone" dataKey="operating_pct" name="Operating" stroke={lightTheme.accent[2]} strokeWidth={2} dot={false} isAnimationActive={false} />
          <Line type="monotone" dataKey="net_pct" name="Net" stroke={lightTheme.accent[0]} strokeWidth={2} dot={false} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 3. YoY growth grouped bars
// ---------------------------------------------------------------------------

export function YoyGrowthChart({
  periods,
  period,
}: YoyChartProps): JSX.Element {
  const theme = useChartTheme();
  const yoy = buildYoyGrowth(periods, period);
  const hasData = yoy.some(
    (r) =>
      r.revenue_yoy_pct !== null ||
      r.eps_yoy_pct !== null ||
      r.fcf_yoy_pct !== null,
  );
  if (!hasData) {
    return (
      <NoData message="YoY growth needs at least one prior-year comparator (4 quarters or 1 fiscal year back)." />
    );
  }
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={yoy} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis dataKey="period_end" tickFormatter={formatPeriod} interval="preserveStartEnd" minTickGap={20} {...sharedAxis(theme)} />
          <YAxis tickFormatter={(v: number) => `${v.toFixed(0)}%`} width={48} {...sharedAxis(theme)} />
          <ReferenceLine y={0} stroke={theme.borderColor} />
          <Tooltip formatter={(value: number) => formatPct(value)} labelFormatter={formatPeriod} contentStyle={{ fontSize: "11px" }} />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Bar dataKey="revenue_yoy_pct" name="Revenue" fill={lightTheme.accent[1]} isAnimationActive={false} />
          <Bar dataKey="eps_yoy_pct" name="EPS (diluted)" fill={lightTheme.accent[2]} isAnimationActive={false} />
          <Bar dataKey="fcf_yoy_pct" name="FCF" fill={lightTheme.accent[3]} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 4. Cash-flow waterfall
// ---------------------------------------------------------------------------

interface WaterfallProps {
  readonly period: JoinedPeriod | null;
}

interface WaterfallBar {
  readonly label: string;
  readonly base: number;
  readonly delta: number;
  readonly value: number;
  readonly is_total: boolean;
}

export function CashflowWaterfallChart({ period }: WaterfallProps): JSX.Element {
  const theme = useChartTheme();
  if (period === null) return <NoData message="No cash-flow statement on file for the latest period." />;
  const steps = buildCashflowWaterfall(period);
  if (steps === null) {
    return <NoData message="Cash-flow statement is missing every flow for the latest period." />;
  }
  // Recharts pattern: render two stacked bars per row — a transparent
  // "base" representing where the bar starts, plus the visible
  // "delta". For totals (Net change) the base is 0 so the column
  // fills from the axis. For step bars the base is the cumulative
  // running total minus this step's signed value.
  const data: WaterfallBar[] = steps.map((s) => {
    if (s.is_total) {
      return {
        label: s.label,
        base: 0,
        delta: s.value,
        value: s.value,
        is_total: true,
      };
    }
    const base = s.cumulative - s.value;
    return {
      label: s.label,
      base,
      delta: s.value,
      value: s.value,
      is_total: false,
    };
  });
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis dataKey="label" {...sharedAxis(theme)} />
          <YAxis tickFormatter={(v: number) => formatBigNumber(v)} width={60} {...sharedAxis(theme)} />
          <ReferenceLine y={0} stroke={theme.borderColor} />
          <Tooltip
            cursor={{ fill: theme.gridLine }}
            formatter={(_value, _name, item) => {
              const payload = item.payload as WaterfallBar | undefined;
              if (payload === undefined) return ["—", ""];
              return [formatBigNumber(payload.value), payload.label];
            }}
            labelFormatter={() => ""}
            contentStyle={{ fontSize: "11px" }}
          />
          <Bar dataKey="base" stackId="a" fill="transparent" isAnimationActive={false} />
          <Bar dataKey="delta" stackId="a" isAnimationActive={false}>
            {data.map((d) => (
              <Cell
                key={d.label}
                fill={
                  d.is_total
                    ? lightTheme.accent[1]
                    : d.delta >= 0
                      ? lightTheme.up
                      : lightTheme.down
                }
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 5. Balance-sheet structure (latest snapshot)
// ---------------------------------------------------------------------------

export function BalanceStructureChart({
  periods,
}: PnlChartProps): JSX.Element {
  const theme = useChartTheme();
  const snap = latestBalanceStructure(periods);
  if (snap === null) {
    return <NoData message="No complete balance sheet (assets + liabilities + equity) on file." />;
  }
  const data = [
    {
      side: "Assets",
      assets: snap.assets,
      liabilities: 0,
      equity: 0,
    },
    {
      side: "Liabilities + Equity",
      assets: 0,
      liabilities: snap.liabilities,
      equity: snap.equity,
    },
  ];
  return (
    <div className="space-y-2">
      <p className="text-[10px] text-slate-500">
        Snapshot as of {formatPeriod(snap.period_end)}
      </p>
      <div style={{ height: 160 }} className="w-full">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart layout="vertical" data={data} margin={{ top: 4, right: 24, left: 8, bottom: 4 }}>
            <XAxis type="number" tickFormatter={(v: number) => formatBigNumber(v)} {...sharedAxis(theme)} />
            <YAxis type="category" dataKey="side" width={140} {...sharedAxis(theme)} />
            <Tooltip
              formatter={(value: number, name: string) => [formatBigNumber(value), name]}
              contentStyle={{ fontSize: "11px" }}
            />
            <Legend wrapperStyle={{ fontSize: "11px" }} />
            <Bar dataKey="assets" name="Assets" stackId="a" fill={lightTheme.accent[1]} isAnimationActive={false} />
            <Bar dataKey="liabilities" name="Liabilities" stackId="a" fill={lightTheme.down} isAnimationActive={false} />
            <Bar dataKey="equity" name="Equity" stackId="a" fill={lightTheme.up} isAnimationActive={false} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 6. Debt structure with interest coverage line
// ---------------------------------------------------------------------------

export function DebtStructureChart({ periods }: PnlChartProps): JSX.Element {
  const theme = useChartTheme();
  const rows = buildDebtStructure(periods);
  const hasData = rows.some(
    (r) => r.long_term !== null || r.short_term !== null,
  );
  if (!hasData) return <NoData message="No debt fields on file." />;
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={rows} margin={{ top: 8, right: 32, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis dataKey="period_end" tickFormatter={formatPeriod} interval="preserveStartEnd" minTickGap={20} {...sharedAxis(theme)} />
          <YAxis yAxisId="left" tickFormatter={(v: number) => formatBigNumber(v)} width={60} {...sharedAxis(theme)} />
          <YAxis yAxisId="right" orientation="right" tickFormatter={(v: number) => `${v.toFixed(0)}×`} width={48} {...sharedAxis(theme)} />
          <Tooltip
            formatter={(value: number, name: string) =>
              name === "Interest coverage"
                ? [`${value.toFixed(2)}×`, name]
                : [formatBigNumber(value), name]
            }
            labelFormatter={formatPeriod}
            contentStyle={{ fontSize: "11px" }}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Bar yAxisId="left" dataKey="long_term" name="Long-term debt" stackId="d" fill={lightTheme.accent[3]} isAnimationActive={false} />
          <Bar yAxisId="left" dataKey="short_term" name="Short-term debt" stackId="d" fill={lightTheme.accent[4]} isAnimationActive={false} />
          <Line yAxisId="right" type="monotone" dataKey="interest_coverage" name="Interest coverage" stroke={lightTheme.accent[0]} strokeWidth={2} dot={false} isAnimationActive={false} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 7. DuPont decomposition
// ---------------------------------------------------------------------------

export function DupontChart({ periods }: PnlChartProps): JSX.Element {
  const theme = useChartTheme();
  const dp = buildDupont(periods);
  const hasData = dp.some(
    (r) =>
      r.net_margin !== null ||
      r.asset_turnover !== null ||
      r.equity_multiplier !== null ||
      r.roe !== null,
  );
  if (!hasData) {
    return <NoData message="DuPont decomposition needs revenue, net income, total assets and equity." />;
  }
  // Display ROE on a left axis as a percent, the three components on
  // a shared right axis as ratios. Different scales necessitate the
  // dual axes — recharts' ComposedChart handles it cleanly.
  const displayed = dp.map((r) => ({
    period_end: r.period_end,
    roe_pct: r.roe !== null ? r.roe * 100 : null,
    npm_pct: r.net_margin !== null ? r.net_margin * 100 : null,
    asset_turnover: r.asset_turnover,
    equity_multiplier: r.equity_multiplier,
  }));
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={displayed} margin={{ top: 8, right: 32, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis dataKey="period_end" tickFormatter={formatPeriod} interval="preserveStartEnd" minTickGap={20} {...sharedAxis(theme)} />
          <YAxis yAxisId="pct" tickFormatter={(v: number) => `${v.toFixed(0)}%`} width={48} {...sharedAxis(theme)} />
          <YAxis yAxisId="x" orientation="right" tickFormatter={(v: number) => `${v.toFixed(1)}×`} width={48} {...sharedAxis(theme)} />
          <ReferenceLine yAxisId="pct" y={0} stroke={theme.borderColor} />
          <Tooltip
            formatter={(value: number, name: string) =>
              name === "ROE" || name === "Net margin"
                ? [formatPct(value), name]
                : [formatRatio(value), name]
            }
            labelFormatter={formatPeriod}
            contentStyle={{ fontSize: "11px" }}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Line yAxisId="pct" type="monotone" dataKey="roe_pct" name="ROE" stroke={lightTheme.accent[0]} strokeWidth={2.5} dot={false} isAnimationActive={false} />
          <Line yAxisId="pct" type="monotone" dataKey="npm_pct" name="Net margin" stroke={lightTheme.accent[1]} strokeWidth={1.5} dot={false} isAnimationActive={false} strokeDasharray="4 4" />
          <Line yAxisId="x" type="monotone" dataKey="asset_turnover" name="Asset turnover" stroke={lightTheme.accent[2]} strokeWidth={1.5} dot={false} isAnimationActive={false} strokeDasharray="4 4" />
          <Line yAxisId="x" type="monotone" dataKey="equity_multiplier" name="Equity multiplier" stroke={lightTheme.accent[3]} strokeWidth={1.5} dot={false} isAnimationActive={false} strokeDasharray="4 4" />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 8. ROIC trend
// ---------------------------------------------------------------------------

export function RoicChart({ periods }: PnlChartProps): JSX.Element {
  const theme = useChartTheme();
  const r = buildRoic(periods);
  const hasData = r.some((row) => row.roic !== null);
  if (!hasData) {
    return <NoData message="ROIC needs operating income, debt and equity." />;
  }
  const data = r.map((row) => ({
    period_end: row.period_end,
    roic_pct: row.roic !== null ? row.roic * 100 : null,
  }));
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis dataKey="period_end" tickFormatter={formatPeriod} interval="preserveStartEnd" minTickGap={20} {...sharedAxis(theme)} />
          <YAxis tickFormatter={(v: number) => `${v.toFixed(0)}%`} width={48} {...sharedAxis(theme)} />
          <ReferenceLine y={0} stroke={theme.borderColor} />
          <Tooltip formatter={(value: number) => formatPct(value)} labelFormatter={formatPeriod} contentStyle={{ fontSize: "11px" }} />
          <Line type="monotone" dataKey="roic_pct" name="ROIC" stroke={lightTheme.accent[2]} strokeWidth={2.5} dot={false} isAnimationActive={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 9. Free cash flow trend
// ---------------------------------------------------------------------------

interface FcfChartRow {
  readonly period_end: string;
  readonly fcf: number | null;
  readonly fcf_yield_pct: number | null;
}

interface FcfTooltipProps {
  active?: boolean;
  payload?: ReadonlyArray<{ payload?: FcfChartRow }>;
}

function FcfTooltip({ active, payload }: FcfTooltipProps): JSX.Element | null {
  if (active !== true || !payload || payload.length === 0) return null;
  const row = payload[0]?.payload;
  if (!row) return null;
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">{formatPeriod(row.period_end)}</div>
      <div className="tabular-nums text-slate-600 dark:text-slate-300">
        FCF (quarter) {row.fcf !== null ? formatBigNumber(row.fcf) : "—"}
      </div>
      {row.fcf_yield_pct !== null ? (
        <div className="tabular-nums text-slate-500 dark:text-slate-400">
          FCF yield (TTM) {row.fcf_yield_pct.toFixed(2)}%
        </div>
      ) : null}
    </ChartTooltip>
  );
}

/**
 * FCF (absolute, quarterly bars) + FCF yield (TTM, %) overlay (#671). The
 * yield denominator (market cap) is a fail-closed server policy
 * (`/instruments/{symbol}/fcf-yield`): multi-class (the retired dual-class
 * distortion #1662) and cross-currency issuers come back `suppressed`, so the
 * absolute line shows alone with a caveat. `yieldSeries` null = yield fetch in
 * flight / errored — the absolute line still renders (supplementary signal,
 * never blocks the FCF line).
 */
export function FcfChart({
  periods,
  yieldSeries,
}: {
  readonly periods: ReadonlyArray<JoinedPeriod>;
  readonly yieldSeries: FcfYieldSeries | null;
}): JSX.Element {
  const theme = useChartTheme();
  const f = buildFcf(periods);
  const hasData = f.some((r) => r.fcf !== null);
  if (!hasData) {
    return <NoData message="FCF needs operating cash flow and capex on the cash-flow statement." />;
  }
  // Decimal arrives as a string on the wire (#671 / types.ts) — coerce to
  // number at this chart boundary only.
  const yieldByPeriod = new Map<string, number | null>();
  for (const p of yieldSeries?.points ?? []) {
    yieldByPeriod.set(p.period_end, p.fcf_yield_pct === null ? null : Number(p.fcf_yield_pct));
  }
  const data: FcfChartRow[] = f.map((r) => ({
    period_end: r.period_end,
    fcf: r.fcf,
    fcf_yield_pct: yieldByPeriod.get(r.period_end) ?? null,
  }));
  const suppressed = yieldSeries?.suppressed_reason ?? null;
  const hasYield = suppressed === null && data.some((r) => r.fcf_yield_pct !== null);
  const caveat =
    suppressed === "multiclass"
      ? "FCF yield unavailable for multi-class issuers."
      : suppressed === "currency_mismatch"
        ? "FCF yield unavailable when reporting and trading currencies differ."
        : null;
  return (
    <div className="space-y-1">
      <div style={{ height: CHART_HEIGHT }} className="w-full">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 8, right: hasYield ? 32 : 8, left: 8, bottom: 4 }}>
            <SharedGrid theme={theme} />
            <XAxis dataKey="period_end" tickFormatter={formatPeriod} interval="preserveStartEnd" minTickGap={20} {...sharedAxis(theme)} />
            <YAxis yAxisId="fcf" tickFormatter={(v: number) => formatBigNumber(v)} width={60} {...sharedAxis(theme)} />
            {hasYield ? (
              <YAxis yAxisId="yield" orientation="right" tickFormatter={(v: number) => `${v.toFixed(1)}%`} width={48} {...sharedAxis(theme)} />
            ) : null}
            <ReferenceLine yAxisId="fcf" y={0} stroke={theme.borderColor} />
            <Tooltip content={<FcfTooltip />} cursor={{ stroke: theme.crosshair }} />
            <Line yAxisId="fcf" type="monotone" dataKey="fcf" name="FCF" stroke={theme.accent[1]} strokeWidth={2.5} dot={false} isAnimationActive={false} />
            {hasYield ? (
              <Line
                yAxisId="yield"
                type="monotone"
                dataKey="fcf_yield_pct"
                name="FCF yield (TTM)"
                stroke={theme.accent[0]}
                strokeWidth={1.5}
                strokeDasharray="4 3"
                dot={false}
                isAnimationActive={false}
                connectNulls={false}
              />
            ) : null}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      {caveat ? <p className="text-xs text-slate-500">{caveat}</p> : null}
    </div>
  );
}
