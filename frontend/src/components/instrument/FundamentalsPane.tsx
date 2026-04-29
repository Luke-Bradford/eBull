/**
 * FundamentalsPane — 2×2 editorial grid of real charts (Revenue,
 * Op income, Net income, Total debt) over the latest 8 quarters from
 * SEC XBRL fundamentals (#567).
 *
 * Design-system v1 redesign (operator review): replaced four 120×36
 * sparklines with full Recharts AreaCharts (~h-24, gradient fill,
 * hover tooltip, period x-axis ticks). Asymmetric "lead + supporting"
 * was rejected because financial scanning is comparison-driven —
 * operators read 4 metrics side-by-side to spot trajectory divergence
 * (revenue up + FCF flat → working-capital problem). Equal visual
 * weight is correct for that scan.
 *
 * Sparse-period honesty (Codex review): each cell keeps every row
 * from `series` with `value: null` for missing quarters. AreaChart
 * uses `connectNulls={false}` so the line breaks at gaps, and the
 * x-axis spacing reflects actual reporting cadence rather than
 * collapsing missing quarters into adjacent ones.
 *
 * YoY / QoQ delta: lookback uses period_end dates (~365 days back
 * with ±45-day tolerance) rather than array-index, so a missing
 * quarter mid-history doesn't get silently labelled YoY.
 *
 * Per-cell coverage caption preserved (PR #684 review): when this
 * metric has fewer non-null values than the joined series length,
 * the cell footer notes `n/N quarters`.
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
 *
 * Insufficient-data empty state (Codex review): when the joined
 * series has <2 rows we still render the Pane chrome with an
 * empty-state line. Returning null left an empty `lg:col-span-6`
 * wrapper in the Health row of the bento grid (operator-visible
 * dead space). Same rationale as DividendsPanel/RecentNewsPane.
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
import { useCallback, useId, useMemo } from "react";
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
  readonly value: number | null;
}

/** Map the joined series to per-cell points keeping every period
 *  (including those with null values for this metric) so the chart
 *  x-axis spacing reflects actual reporting cadence. Recharts will
 *  skip null values when ``connectNulls={false}``. */
function buildCellPoints(
  series: ReadonlyArray<SeriesRow>,
  pick: (row: SeriesRow) => number | null,
): CellPoint[] {
  return series.map((row) => ({
    period_end: row.period_end,
    value: pick(row),
  }));
}

function nonNullCount(points: ReadonlyArray<CellPoint>): number {
  let n = 0;
  for (const p of points) if (p.value !== null) n += 1;
  return n;
}

function lastNonNullIndex(points: ReadonlyArray<CellPoint>): number {
  for (let i = points.length - 1; i >= 0; i--) {
    if (points[i]!.value !== null) return i;
  }
  return -1;
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

const ONE_DAY_MS = 86400000;

/** Year-over-year delta against the row whose period_end is ~365 days
 *  earlier (±45-day window — covers fiscal-year ending day shifts and
 *  thirteen-week quarters). Falls back to QoQ when no YoY-distance
 *  prior row exists. Returns null when fewer than 2 non-null points
 *  or when the prior is zero (undefined growth).
 *
 *  Codex review: array-index lookback was wrong because
 *  buildCellPoints used to drop null rows — index 5 back could mean
 *  any number of calendar quarters. Date-based lookup is robust to
 *  sparse coverage. */
function yoyDelta(points: ReadonlyArray<CellPoint>): {
  readonly pct: number;
  readonly label: "YoY" | "QoQ";
} | null {
  const lastIdx = lastNonNullIndex(points);
  if (lastIdx < 0) return null;
  const lastPoint = points[lastIdx]!;
  const lastVal = lastPoint.value!;
  const lastTs = Date.parse(lastPoint.period_end);
  if (!Number.isFinite(lastTs)) return null;
  // Look for a prior non-null row whose period_end is roughly one
  // year before. Walk backward from lastIdx so the closest match wins.
  for (let i = lastIdx - 1; i >= 0; i--) {
    const p = points[i]!;
    if (p.value === null) continue;
    const ts = Date.parse(p.period_end);
    if (!Number.isFinite(ts)) continue;
    const diffDays = (lastTs - ts) / ONE_DAY_MS;
    if (diffDays >= 320 && diffDays <= 410) {
      if (p.value === 0) return null;
      return {
        pct: ((lastVal - p.value) / Math.abs(p.value)) * 100,
        label: "YoY",
      };
    }
  }
  // QoQ fallback: immediate previous non-null point.
  for (let i = lastIdx - 1; i >= 0; i--) {
    const p = points[i]!;
    if (p.value !== null) {
      if (p.value === 0) return null;
      return {
        pct: ((lastVal - p.value) / Math.abs(p.value)) * 100,
        label: "QoQ",
      };
    }
  }
  return null;
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

  const onExpand = () => navigate(`/instrument/${encodeURIComponent(symbol)}/fundamentals`);
  const settled =
    !income.loading &&
    !balance.loading &&
    income.error === null &&
    balance.error === null;

  // Capability active but the joined series is too short to plot —
  // render the Pane chrome with empty-state copy rather than null.
  // Returning null left an empty col-span-6 wrapper in the bento
  // Health row (operator-visible dead space). #684 round-3 policy.
  if (settled && series.length < 2) {
    return (
      <Pane
        title="Fundamentals"
        scope="last 8 quarters"
        source={{ providers: ["sec_xbrl"] }}
        onExpand={onExpand}
      >
        <p className="text-xs text-slate-500">
          Insufficient quarterly history yet for this instrument.
        </p>
      </Pane>
    );
  }

  return (
    <Pane
      title="Fundamentals"
      scope="last 8 quarters"
      source={{ providers: ["sec_xbrl"] }}
      onExpand={onExpand}
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
  // Coverage = non-null entries against the full series length. When
  // a metric is missing in some quarters (e.g. MLP issuer with
  // operating_income null on every row) the cell footer notes the
  // shortfall so the operator notices the time-axis asymmetry.
  // PR #684 review.
  const totalLen = series.length;
  // useId() per-instance hash so that two FundamentalsPane components
  // mounted in the same document don't collide on gradient ids in
  // <defs>. Codex review caught the prior label-only id was global.
  const idPrefix = useId().replace(/[^a-z0-9]/gi, "");
  return (
    <div className="grid grid-cols-1 gap-x-6 gap-y-4 sm:grid-cols-2">
      <FundamentalCell
        label="Revenue"
        idPrefix={idPrefix}
        points={revenue}
        nonNull={nonNullCount(revenue)}
        totalLen={totalLen}
        fillColor={chartTheme.accent[1]}
      />
      <FundamentalCell
        label="Op income"
        idPrefix={idPrefix}
        points={opIncome}
        nonNull={nonNullCount(opIncome)}
        totalLen={totalLen}
        fillColor={chartTheme.up}
      />
      <FundamentalCell
        label="Net income"
        idPrefix={idPrefix}
        points={netIncome}
        nonNull={nonNullCount(netIncome)}
        totalLen={totalLen}
        fillColor={chartTheme.up}
      />
      <FundamentalCell
        label="Total debt"
        idPrefix={idPrefix}
        points={totalDebt}
        nonNull={nonNullCount(totalDebt)}
        totalLen={totalLen}
        fillColor={chartTheme.accent[3]}
      />
    </div>
  );
}

function FundamentalCell({
  label,
  idPrefix,
  points,
  nonNull,
  totalLen,
  fillColor,
}: {
  readonly label: string;
  readonly idPrefix: string;
  readonly points: ReadonlyArray<CellPoint>;
  readonly nonNull: number;
  readonly totalLen: number;
  readonly fillColor: string;
}) {
  const showCoverage = nonNull > 0 && nonNull < totalLen;
  const delta = yoyDelta(points);
  const lastIdx = lastNonNullIndex(points);
  const latestVal = lastIdx >= 0 ? points[lastIdx]!.value : null;
  const deltaClass =
    delta === null
      ? "text-slate-400"
      : delta.pct > 0
        ? "text-emerald-600"
        : delta.pct < 0
          ? "text-rose-600"
          : "text-slate-500";
  // Per-instance + per-label gradient id (Codex review): two
  // FundamentalsPane instances in the same DOM previously collided.
  const gradId = `fund-grad-${idPrefix}-${label.replace(/[^a-z]/gi, "")}`;
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-[10px] uppercase tracking-[0.08em] text-slate-500">
          {label}
        </span>
        {showCoverage ? (
          <span
            className="text-[9px] uppercase tracking-wider text-amber-600"
            title={`This metric reported ${nonNull} of the ${totalLen} quarters in view.`}
          >
            {nonNull}/{totalLen}
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
      {nonNull >= 2 ? (
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
                formatter={(value) => {
                  // Recharts ValueType is string | number | (string|number)[].
                  // For null-valued points we expect Recharts to skip the
                  // tooltip row, but coerce defensively so the formatter
                  // signature is honest about every value Recharts can pass.
                  const n =
                    typeof value === "number"
                      ? value
                      : typeof value === "string" && value !== ""
                        ? Number(value)
                        : null;
                  return [formatBigNumber(Number.isFinite(n) ? n : null), label];
                }}
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
                connectNulls={false}
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
