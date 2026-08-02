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
import { type ChartTheme, defaultTooltipStyle } from "@/lib/chartTheme";
import { formatBigMoney, formatBigNumber } from "@/lib/format";
import { useChartTheme } from "@/lib/useChartTheme";
import {
  buildCashflowWaterfall,
  buildDebtStructure,
  buildDupont,
  buildFcf,
  buildMargins,
  buildNetDebt,
  buildPnlBuckets,
  buildRoic,
  buildYoyGrowth,
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

/**
 * Money magnitude bound to the period's reported currency (#2185 §1.4).
 *
 * `financial_periods.reported_currency` is already surfaced by
 * `GET /instruments/{symbol}/financials` (selected at
 * `app/api/instruments.py:819`, extracted at `:838` as the response's
 * `currency`), so a bare `380.00B` does not say what it is denominated in.
 * Same class as #2129: a per-item currency field is a contract on every money
 * field.
 *
 * **Measured, not assumed.** The dev corpus is currently 100% USD —
 * `SELECT COALESCE(reported_currency,'<NULL>'), count(*) FROM
 * financial_periods WHERE superseded_at IS NULL GROUP BY 1` returns exactly
 * one row, `('USD', 223715)`, with zero NULLs and zero instruments carrying
 * more than one code. So this labels rather than corrects: today every money
 * axis simply gains the `US$` prefix (en-GB `Intl` renders USD as `US$`, not
 * `$`). It is a contract for the first non-USD reporter #2182 / international
 * ingest admits, not a fix for a wrong figure on screen now.
 *
 * A null currency renders UNPREFIXED rather than falling back to
 * `formatBigMoney`'s GBP default, which would assert a denomination the row
 * never reported. That branch is unreachable against the corpus above and is
 * covered by unit test rather than by live data. Both helpers are the shared
 * ones in `lib/format`; this file previously carried a private variant of
 * `formatBigNumber` that differed in the sub-million branch (`.toFixed(1)` vs
 * the shared `.toFixed(2)`), so K-magnitude labels gain one decimal.
 */
function formatMoneyAxis(n: number | null, currency: string | null): string {
  return currency === null ? formatBigNumber(n) : formatBigMoney(n, currency);
}

/**
 * Dot visibility for a discrete financial series (#2185 §1.2).
 *
 * Quarterly / annual financials are discrete observations, not a sampled
 * continuum: with `dot={false}` there is no cue at all for where a reported
 * value sits versus where the line is merely drawn between two of them. Show
 * the markers while the series is short enough for them to read as markers;
 * above ~40 points they merge into a band and become noise.
 *
 * Today every series is well under the threshold — the endpoint caps history
 * at `LIMIT 20` (`app/api/instruments.py:828`) — so the guard is for when
 * #2182 deepens the FY history.
 */
const DOT_VISIBILITY_MAX_POINTS = 40;

/** Gutter for a money `YAxis`. Wider than the percent/ratio axes because
 *  `formatMoneyAxis` prefixes the ISO currency (`US$450.00B`, not
 *  `450.00B`) — under en-GB, `Intl.NumberFormat` renders USD as `US$`,
 *  so the widest tick is ~8 characters. One constant: five money axes
 *  had this inline, and a divergent value shows up as misaligned chart
 *  left edges down the page (review NITPICK on PR #2188). */
const MONEY_AXIS_WIDTH = 72;

function seriesDot(pointCount: number): { readonly r: number } | false {
  return pointCount < DOT_VISIBILITY_MAX_POINTS ? { r: 2 } : false;
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

/** Charts whose axis is denominated in money carry the statement's
 *  `reported_currency` (#2185 §1.4). Ratio / percentage charts do not. */
interface MoneyChartProps extends PnlChartProps {
  readonly currency: string | null;
}

interface YoyChartProps extends PnlChartProps {
  /** Page-level period selection — passed through to `buildYoyGrowth`
   *  so the lag is computed from the requested view, not by guessing
   *  at the row's `period_type` (which the backend emits as `FY` /
   *  `Q1`…`Q4`, never the literal `"annual"`). */
  readonly period: "quarterly" | "annual";
}

export function PnlStackedChart({ periods, currency }: MoneyChartProps): JSX.Element {
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
          <YAxis tickFormatter={(v: number) => formatMoneyAxis(v, currency)} width={MONEY_AXIS_WIDTH} {...sharedAxis(theme)} />
          <Tooltip
            cursor={{ fill: theme.gridLine }}
            formatter={(value: number) => formatMoneyAxis(value, currency)}
            labelFormatter={formatPeriod}
            contentStyle={defaultTooltipStyle(theme)}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Bar dataKey="cogs" name="COGS" stackId="a" fill={theme.accent[3]} isAnimationActive={false} />
          <Bar dataKey="opex" name="Opex (R&D + SG&A)" stackId="a" fill={theme.accent[2]} isAnimationActive={false} />
          <Bar dataKey="op_income" name="Operating income" stackId="a" fill={theme.accent[0]} isAnimationActive={false} />
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
            contentStyle={defaultTooltipStyle(theme)}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Line type="linear" dataKey="gross_pct" name="Gross" stroke={theme.accent[1]} strokeWidth={2} dot={seriesDot(margins.length)} isAnimationActive={false} />
          <Line type="linear" dataKey="operating_pct" name="Operating" stroke={theme.accent[2]} strokeWidth={2} dot={seriesDot(margins.length)} isAnimationActive={false} />
          <Line type="linear" dataKey="net_pct" name="Net" stroke={theme.accent[0]} strokeWidth={2} dot={seriesDot(margins.length)} isAnimationActive={false} />
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
          <Tooltip formatter={(value: number) => formatPct(value)} labelFormatter={formatPeriod} contentStyle={defaultTooltipStyle(theme)} />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Bar dataKey="revenue_yoy_pct" name="Revenue" fill={theme.accent[1]} isAnimationActive={false} />
          <Bar dataKey="eps_yoy_pct" name="EPS (diluted)" fill={theme.accent[2]} isAnimationActive={false} />
          <Bar dataKey="fcf_yoy_pct" name="FCF" fill={theme.accent[3]} isAnimationActive={false} />
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
  readonly currency: string | null;
}

interface WaterfallBar {
  readonly label: string;
  readonly base: number;
  readonly delta: number;
  readonly value: number;
  readonly is_total: boolean;
}

export function CashflowWaterfallChart({ period, currency }: WaterfallProps): JSX.Element {
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
          <YAxis tickFormatter={(v: number) => formatMoneyAxis(v, currency)} width={MONEY_AXIS_WIDTH} {...sharedAxis(theme)} />
          <ReferenceLine y={0} stroke={theme.borderColor} />
          <Tooltip
            cursor={{ fill: theme.gridLine }}
            formatter={(_value, _name, item) => {
              const payload = item.payload as WaterfallBar | undefined;
              if (payload === undefined) return ["—", ""];
              return [formatMoneyAxis(payload.value, currency), payload.label];
            }}
            labelFormatter={() => ""}
            contentStyle={defaultTooltipStyle(theme)}
          />
          <Bar dataKey="base" stackId="a" fill="transparent" isAnimationActive={false} />
          <Bar dataKey="delta" stackId="a" isAnimationActive={false}>
            {data.map((d) => (
              <Cell
                key={d.label}
                fill={
                  d.is_total
                    ? theme.accent[1]
                    : d.delta >= 0
                      ? theme.up
                      : theme.down
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
// 5. Net-debt trend (replaces the balance-sheet identity chart)
// ---------------------------------------------------------------------------

/**
 * Net debt over time — "is the balance sheet getting safer?".
 *
 * Replaces the previous Assets vs (Liabilities + Equity) snapshot, which was
 * an *accounting identity*: the two bars are equal by construction, so the
 * chart could never vary and carried no signal (#2185 / spec §1.6, §3.5).
 *
 * The gross-debt and net-debt treatment is the repo's documented one, cited in
 * `buildNetDebt` (`lib/fundamentalsMetrics.ts`) — total debt is null only when
 * both components are null; a missing `cash` is a real gap, not a degrade path.
 *
 * Computability — stated because §0's standing rule requires it, and measured
 * per instrument on the full population (dev `financial_periods`,
 * `superseded_at IS NULL`), NOT sampled:
 *
 * | basis | rendered the deleted identity chart | renders this pane | net-debt LINE at ≥3 periods |
 * |---|---|---|---|
 * | FY | 4,173 / 4,760 (87.7%) | 3,338 (70.1%) | 2,549 (53.6%) |
 * | Q1 | 3,732 / 4,236 (88.1%) | 2,905 (68.6%) | 2,137 (50.4%) |
 *
 * So ~835 FY instruments (~827 quarterly) that previously saw a rendered pane
 * now see the empty state, and at the spec's own ≥3-usable-periods bar this is
 * the second-most-gated chart on the page after the 47% P&L stack. That is a
 * real cost and it is accepted knowingly: the chart it replaces was an
 * accounting identity, so its 88% was 88% of a pane that could not carry
 * signal. A blank pane beats a chart that cannot vary.
 *
 * §3.5's interest-coverage overlay is NOT rendered here — it already has its
 * own right-hand axis in the Debt-structure pane immediately below
 * (`DebtStructureChart`), so repeating it would put one series on two adjacent
 * panes. Note this is a deviation from an approved spec decided in code: the
 * earlier justification here also cited §3.7, which is scoped to the DuPont
 * chart and does not govern this pane. Gross debt and cash render as context
 * bars instead so the reader can see WHICH side moved the net line.
 */
export function NetDebtChart({ periods, currency }: MoneyChartProps): JSX.Element {
  const theme = useChartTheme();
  const rows = buildNetDebt(periods);
  // `net_debt !== null` implies `debt !== null`, so this guard is exactly
  // "some period reports a debt component". Cash is NOT required to render —
  // it only gates the net-debt LINE — so the message must not claim it is
  // missing to a debt-free issuer that does report cash.
  const hasData = rows.some((r) => r.debt !== null);
  if (!hasData) {
    return <NoData message="Net debt needs a reported debt component (long- or short-term) on the balance sheet." />;
  }
  return (
    <div style={{ height: CHART_HEIGHT }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={rows} margin={{ top: 8, right: 8, left: 8, bottom: 4 }}>
          <SharedGrid theme={theme} />
          <XAxis dataKey="period_end" tickFormatter={formatPeriod} interval="preserveStartEnd" minTickGap={20} {...sharedAxis(theme)} />
          <YAxis tickFormatter={(v: number) => formatMoneyAxis(v, currency)} width={MONEY_AXIS_WIDTH} {...sharedAxis(theme)} />
          {/* Net debt crossing zero is the readable event — below it the
              issuer holds more cash than debt. */}
          <ReferenceLine y={0} stroke={theme.borderColor} />
          <Tooltip
            cursor={{ fill: theme.gridLine }}
            formatter={(value: number, name: string) => [formatMoneyAxis(value, currency), name]}
            labelFormatter={formatPeriod}
            contentStyle={defaultTooltipStyle(theme)}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Bar dataKey="debt" name="Gross debt" fill={theme.accent[3]} isAnimationActive={false} />
          <Bar dataKey="cash" name="Cash" fill={theme.accent[0]} isAnimationActive={false} />
          <Line type="linear" dataKey="net_debt" name="Net debt" stroke={theme.accent[2]} strokeWidth={2.5} dot={seriesDot(rows.length)} isAnimationActive={false} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 6. Debt structure with interest coverage line
// ---------------------------------------------------------------------------

export function DebtStructureChart({ periods, currency }: MoneyChartProps): JSX.Element {
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
          <YAxis yAxisId="left" tickFormatter={(v: number) => formatMoneyAxis(v, currency)} width={MONEY_AXIS_WIDTH} {...sharedAxis(theme)} />
          <YAxis yAxisId="right" orientation="right" tickFormatter={(v: number) => `${v.toFixed(0)}×`} width={48} {...sharedAxis(theme)} />
          <Tooltip
            formatter={(value: number, name: string) =>
              name === "Interest coverage"
                ? [`${value.toFixed(2)}×`, name]
                : [formatMoneyAxis(value, currency), name]
            }
            labelFormatter={formatPeriod}
            contentStyle={defaultTooltipStyle(theme)}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Bar yAxisId="left" dataKey="long_term" name="Long-term debt" stackId="d" fill={theme.accent[3]} isAnimationActive={false} />
          <Bar yAxisId="left" dataKey="short_term" name="Short-term debt" stackId="d" fill={theme.accent[4]} isAnimationActive={false} />
          <Line yAxisId="right" type="linear" dataKey="interest_coverage" name="Interest coverage" stroke={theme.accent[0]} strokeWidth={2} dot={seriesDot(rows.length)} isAnimationActive={false} />
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
            contentStyle={defaultTooltipStyle(theme)}
          />
          <Legend wrapperStyle={{ fontSize: "11px" }} />
          <Line yAxisId="pct" type="linear" dataKey="roe_pct" name="ROE" stroke={theme.accent[0]} strokeWidth={2.5} dot={seriesDot(displayed.length)} isAnimationActive={false} />
          <Line yAxisId="pct" type="linear" dataKey="npm_pct" name="Net margin" stroke={theme.accent[1]} strokeWidth={1.5} dot={seriesDot(displayed.length)} isAnimationActive={false} strokeDasharray="4 4" />
          <Line yAxisId="x" type="linear" dataKey="asset_turnover" name="Asset turnover" stroke={theme.accent[2]} strokeWidth={1.5} dot={seriesDot(displayed.length)} isAnimationActive={false} strokeDasharray="4 4" />
          <Line yAxisId="x" type="linear" dataKey="equity_multiplier" name="Equity multiplier" stroke={theme.accent[3]} strokeWidth={1.5} dot={seriesDot(displayed.length)} isAnimationActive={false} strokeDasharray="4 4" />
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
          <Tooltip formatter={(value: number) => formatPct(value)} labelFormatter={formatPeriod} contentStyle={defaultTooltipStyle(theme)} />
          <Line type="linear" dataKey="roic_pct" name="ROIC" stroke={theme.accent[2]} strokeWidth={2.5} dot={seriesDot(data.length)} isAnimationActive={false} />
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
  /** Passed explicitly by the chart; recharts clones the element and injects
   *  `active` / `payload` around it, so an author-supplied prop survives. */
  currency?: string | null;
}

function FcfTooltip({ active, payload, currency }: FcfTooltipProps): JSX.Element | null {
  if (active !== true || !payload || payload.length === 0) return null;
  const row = payload[0]?.payload;
  if (!row) return null;
  return (
    <ChartTooltip>
      <div className="font-medium text-slate-700 dark:text-slate-200">{formatPeriod(row.period_end)}</div>
      <div className="tabular-nums text-slate-600 dark:text-slate-300">
        FCF (quarter) {formatMoneyAxis(row.fcf, currency ?? null)}
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
  currency,
}: {
  readonly periods: ReadonlyArray<JoinedPeriod>;
  readonly yieldSeries: FcfYieldSeries | null;
  readonly currency: string | null;
}): JSX.Element {
  const theme = useChartTheme();
  const f = buildFcf(periods);
  const hasData = f.some((r) => r.fcf !== null);
  if (!hasData) {
    // Capex is NOT required — the settled rule COALESCEs it to 0
    // (`fcf_yield.py:111,132`), so operating cash flow is the only input the
    // operator can be missing. Saying "and capex" told 1,142 FY instruments
    // that a field they simply have none of was the blocker (#2185).
    return <NoData message="FCF needs operating cash flow on the cash-flow statement." />;
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
            <YAxis yAxisId="fcf" tickFormatter={(v: number) => formatMoneyAxis(v, currency)} width={MONEY_AXIS_WIDTH} {...sharedAxis(theme)} />
            {hasYield ? (
              <YAxis yAxisId="yield" orientation="right" tickFormatter={(v: number) => `${v.toFixed(1)}%`} width={48} {...sharedAxis(theme)} />
            ) : null}
            <ReferenceLine yAxisId="fcf" y={0} stroke={theme.borderColor} />
            <Tooltip content={<FcfTooltip currency={currency} />} cursor={{ stroke: theme.crosshair }} />
            <Line yAxisId="fcf" type="linear" dataKey="fcf" name="FCF" stroke={theme.accent[1]} strokeWidth={2.5} dot={seriesDot(data.length)} isAnimationActive={false} />
            {hasYield ? (
              <Line
                yAxisId="yield"
                type="linear"
                dataKey="fcf_yield_pct"
                name="FCF yield (TTM)"
                stroke={theme.accent[0]}
                strokeWidth={1.5}
                strokeDasharray="4 3"
                dot={seriesDot(data.length)}
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
