/**
 * FundamentalsPane — 2×2 editorial grid of real charts (Revenue,
 * Op income, Net income, Total debt) over the latest 8 quarters from
 * SEC XBRL fundamentals (#567).
 *
 * Design-system v1 redesign (operator review): replaced the four
 * 120×36 sparklines with full Recharts AreaCharts (~h-24, gradient
 * fill, hover tooltip, period x-axis ticks). Asymmetric "lead +
 * supporting" was rejected because financial scanning is comparison-
 * driven — operators read 4 metrics side-by-side to spot trajectory
 * divergence (revenue up + FCF flat → working-capital problem). Equal
 * visual weight is correct for that scan.
 *
 * Per-cell coverage caption preserved (PR #684 review): when one
 * cell's period count diverges from siblings, the cell footer notes
 * `n/N periods` so the operator sees the time-axis asymmetry.
 *
 * Gating: `summary.capabilities.fundamentals.providers` includes
 * "sec_xbrl" with `data_present.sec_xbrl === true`. Hooks fire
 * unconditionally; the active-flag check happens after the data is
 * fetched (or while loading shows a skeleton).
 *
 * MLP/partnership issuers (e.g. IEP) file
 * `IncomeLossFromContinuingOperations` instead of `OperatingIncomeLoss`.
 * Per-cell null filtering means revenue + net_income still render
 * even when operating_income is null on every row — pre-fix the
 * strict joinPeriods gate dropped every row and hid the entire pane.
 */

import {
  Area,
  AreaChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
} from "recharts";

import { fetchInstrumentFinancials } from "@/api/instruments";
import type { InstrumentFinancialRow, InstrumentSummary } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Pane } from "@/components/instrument/Pane";
import { chartTheme } from "@/lib/chartTheme";
import { useAsync } from "@/lib/useAsync";
import { useCallback, useMemo } from "react";
import { useNavigate } from "react-router-dom";

const SLICE = 8;
const CELL_HEIGHT = 96;

interface SeriesRow {
  readonly period_end: string;
  // Each metric is independently nullable. Per-cell render filters its
  // own column rather than the whole row dropping when one column is
  // missing — partnership/MLP issuers like IEP file
  // ``IncomeLossFromContinuingOperations`` instead of the standard
  // ``OperatingIncomeLoss``, leaving operating_income null on every row,
  // which previously hid the entire pane (#684 operator report).
  readonly revenue: number | null;
  readonly operatingIncome: number | null;
  readonly netIncome: number | null;
  readonly totalDebt: number | null;
}

function num(v: string | null | undefined): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function joinPeriods(
  income: ReadonlyArray<InstrumentFinancialRow>,
  balance: ReadonlyArray<InstrumentFinancialRow>,
): SeriesRow[] {
  const bMap = new Map(
    balance.map((r) => [`${r.period_end}|${r.period_type}`, r]),
  );
  const joined: SeriesRow[] = [];
  for (const i of income) {
    const key = `${i.period_end}|${i.period_type}`;
    const b = bMap.get(key);
    const revenue = num(i.values["revenue"] ?? null);
    const operatingIncome = num(i.values["operating_income"] ?? null);
    const netIncome = num(i.values["net_income"] ?? null);
    const lt = b !== undefined ? num(b.values["long_term_debt"] ?? null) : null;
    const st = b !== undefined ? num(b.values["short_term_debt"] ?? null) : null;
    if (revenue === null && operatingIncome === null && netIncome === null) {
      continue;
    }
    const totalDebt =
      lt === null && st === null ? null : (lt ?? 0) + (st ?? 0);
    joined.push({
      period_end: i.period_end,
      revenue,
      operatingIncome,
      netIncome,
      totalDebt,
    });
  }
  joined.sort((a, b) => (a.period_end < b.period_end ? 1 : -1));
  const latest = joined.slice(0, SLICE);
  latest.reverse();
  return latest;
}

interface CellPoint {
  readonly period_end: string;
  readonly value: number;
}

/** Filter the per-period series down to non-null cell points for one
 *  metric, preserving period_end so the chart x-axis is honest. */
function buildCellPoints(
  series: ReadonlyArray<SeriesRow>,
  pick: (row: SeriesRow) => number | null,
): CellPoint[] {
  const out: CellPoint[] = [];
  for (const row of series) {
    const v = pick(row);
    if (v !== null) out.push({ period_end: row.period_end, value: v });
  }
  return out;
}

function formatBigNumber(n: number | null): string {
  if (n === null) return "—";
  const abs = Math.abs(n);
  const sign = n < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(2)}K`;
  return n.toFixed(0);
}

function formatPeriodTick(period_end: string): string {
  // SEC XBRL emits YYYY-MM-DD — slice safe.
  const months = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
  ];
  const m = Number(period_end.slice(5, 7));
  const y = period_end.slice(2, 4);
  if (m >= 1 && m <= 12) return `${months[m - 1]} '${y}`;
  return period_end;
}

/** Year-over-year delta for the latest period. Uses index-4 lookback
 *  when 4 prior quarters are available (true YoY); falls back to
 *  period-over-period when the series is too short for YoY. Returns
 *  null when prior is zero (undefined growth). */
function yoyDelta(points: ReadonlyArray<CellPoint>): {
  readonly pct: number;
  readonly label: "YoY" | "QoQ";
} | null {
  if (points.length < 2) return null;
  const last = points[points.length - 1]!.value;
  // YoY: lookback 4 quarters when available — financially meaningful
  // for quarterly fundamentals where seasonality dominates QoQ.
  if (points.length >= 5) {
    const prior = points[points.length - 5]!.value;
    if (prior === 0) return null;
    return { pct: ((last - prior) / Math.abs(prior)) * 100, label: "YoY" };
  }
  const prev = points[points.length - 2]!.value;
  if (prev === 0) return null;
  return { pct: ((last - prev) / Math.abs(prev)) * 100, label: "QoQ" };
}

export interface FundamentalsPaneProps {
  readonly summary: InstrumentSummary;
}

export function FundamentalsPane({ summary }: FundamentalsPaneProps): JSX.Element | null {
  const symbol = summary.identity.symbol;
  const navigate = useNavigate();
  const fundCell = summary.capabilities["fundamentals"];
  const active =
    fundCell !== undefined &&
    fundCell.providers.includes("sec_xbrl") &&
    fundCell.data_present["sec_xbrl"] === true;

  // Hooks must be called unconditionally — gating via `active` happens
  // after data is fetched (or while loading shows a skeleton).
  const income = useAsync(
    useCallback(
      () =>
        fetchInstrumentFinancials(symbol, {
          statement: "income",
          period: "quarterly",
        }),
      [symbol],
    ),
    [symbol],
  );
  const balance = useAsync(
    useCallback(
      () =>
        fetchInstrumentFinancials(symbol, {
          statement: "balance",
          period: "quarterly",
        }),
      [symbol],
    ),
    [symbol],
  );

  const series = useMemo(() => {
    if (income.data === null || balance.data === null) return [];
    return joinPeriods(income.data.rows, balance.data.rows);
  }, [income.data, balance.data]);

  if (!active) return null;

  // Capability active but the joined series is too short to plot.
  // Return null — four-state empty rule: capability active + zero
  // data is a SEC-ingest-running-but-no-quarters-yet state, not an
  // operator-actionable empty case worth its own chrome.
  const insufficient =
    !income.loading &&
    !balance.loading &&
    income.error === null &&
    balance.error === null &&
    series.length < 2;
  if (insufficient) return null;

  return (
    <Pane
      title="Fundamentals"
      scope="last 8 quarters"
      source={{ providers: ["sec_xbrl"] }}
      onExpand={() => navigate(`/instrument/${encodeURIComponent(symbol)}/fundamentals`)}
    >
      {income.loading || balance.loading ? (
        <SectionSkeleton rows={3} />
      ) : income.error !== null || balance.error !== null ? (
        <SectionError onRetry={() => { income.refetch(); balance.refetch(); }} />
      ) : (
        <FundamentalsGrid series={series} />
      )}
    </Pane>
  );
}

function FundamentalsGrid({
  series,
}: {
  readonly series: ReadonlyArray<SeriesRow>;
}): JSX.Element {
  const revenue = buildCellPoints(series, (r) => r.revenue);
  const opIncome = buildCellPoints(series, (r) => r.operatingIncome);
  const netIncome = buildCellPoints(series, (r) => r.netIncome);
  const totalDebt = buildCellPoints(series, (r) => r.totalDebt);
  // Cells share an x-axis only visually — when one cell has fewer
  // periods than siblings (e.g. an MLP with operating_income null on
  // every quarter), shapes can't be compared directly. Surface a
  // ``n/N periods`` caption on cells whose coverage diverges from
  // the maximum so the operator notices the asymmetry. PR #684 review.
  const maxLen = Math.max(
    revenue.length,
    opIncome.length,
    netIncome.length,
    totalDebt.length,
  );
  return (
    <div className="grid grid-cols-1 gap-x-6 gap-y-4 sm:grid-cols-2">
      <FundamentalCell
        label="Revenue"
        points={revenue}
        maxLen={maxLen}
        fillColor={chartTheme.accent[1]}
      />
      <FundamentalCell
        label="Op income"
        points={opIncome}
        maxLen={maxLen}
        fillColor={chartTheme.up}
      />
      <FundamentalCell
        label="Net income"
        points={netIncome}
        maxLen={maxLen}
        fillColor={chartTheme.up}
      />
      <FundamentalCell
        label="Total debt"
        points={totalDebt}
        maxLen={maxLen}
        fillColor={chartTheme.accent[3]}
      />
    </div>
  );
}

function FundamentalCell({
  label,
  points,
  maxLen,
  fillColor,
}: {
  readonly label: string;
  readonly points: ReadonlyArray<CellPoint>;
  /** Largest period count across sibling cells. When this cell's
   *  ``points.length`` is smaller, the chart x-axis can't be compared
   *  directly to siblings — surface a coverage caption. */
  readonly maxLen: number;
  readonly fillColor: string;
}) {
  const showCoverage = points.length > 0 && points.length < maxLen;
  const delta = yoyDelta(points);
  const latestVal = points.length > 0 ? points[points.length - 1]!.value : null;
  const deltaClass =
    delta === null
      ? "text-slate-400"
      : delta.pct > 0
        ? "text-emerald-600"
        : delta.pct < 0
          ? "text-rose-600"
          : "text-slate-500";
  // Recharts gradient id must be unique per chart instance — collisions
  // pollute the gradient defs and cells render with wrong fills.
  const gradId = `fund-grad-${label.replace(/[^a-z]/gi, "")}`;
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-[10px] uppercase tracking-[0.08em] text-slate-500">
          {label}
        </span>
        {showCoverage ? (
          <span
            className="text-[9px] uppercase tracking-wider text-amber-600"
            title={`This cell covers ${points.length} of the ${maxLen} periods rendered by sibling cells.`}
          >
            {points.length}/{maxLen}
          </span>
        ) : null}
      </div>
      <div className="flex items-baseline gap-2">
        <span className="text-xl font-semibold tabular-nums text-slate-900">
          {formatBigNumber(latestVal)}
        </span>
        {delta !== null ? (
          <span className={`text-[11px] font-medium tabular-nums ${deltaClass}`}>
            {delta.pct > 0 ? "▲" : delta.pct < 0 ? "▼" : "·"}
            {Math.abs(delta.pct).toFixed(1)}%
            <span className="ml-1 text-[9px] uppercase tracking-wider text-slate-400">
              {delta.label}
            </span>
          </span>
        ) : null}
      </div>
      {points.length >= 2 ? (
        <div style={{ height: CELL_HEIGHT }} className="w-full">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart
              data={points as CellPoint[]}
              margin={{ top: 4, right: 4, left: 0, bottom: 0 }}
            >
              <defs>
                <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={fillColor} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={fillColor} stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <XAxis
                dataKey="period_end"
                tickFormatter={formatPeriodTick}
                interval="preserveStartEnd"
                minTickGap={28}
                tick={{ fill: chartTheme.textMuted, fontSize: 9 }}
                stroke={chartTheme.borderColor}
                tickLine={false}
                axisLine={false}
              />
              <Tooltip
                cursor={{ stroke: chartTheme.crosshair, strokeWidth: 1, strokeDasharray: "3 3" }}
                formatter={(value: number) => [formatBigNumber(value), label]}
                labelFormatter={formatPeriodTick}
                contentStyle={{
                  fontSize: "11px",
                  borderColor: chartTheme.borderColor,
                  borderRadius: 4,
                }}
              />
              <Area
                type="monotone"
                dataKey="value"
                stroke={fillColor}
                strokeWidth={1.75}
                fill={`url(#${gradId})`}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div
          style={{ height: CELL_HEIGHT }}
          className="flex w-full items-center justify-center text-[10px] uppercase tracking-wider text-slate-400"
        >
          insufficient history
        </div>
      )}
    </div>
  );
}
