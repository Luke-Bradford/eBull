/**
 * FundamentalsPane — 4 sparklines (Revenue / Op income / Net income /
 * Total debt) over the latest 8 quarters from SEC XBRL fundamentals
 * (#567). Gated on `summary.capabilities.fundamentals.providers` including
 * "sec_xbrl" with `data_present.sec_xbrl === true` so non-SEC instruments
 * don't render a dead pane.
 *
 * Data path: 2 parallel calls to /instruments/{symbol}/financials —
 * one for income, one for balance — joined per (period_end, period_type)
 * to keep all four sparklines on the same quarter set.
 */

import { fetchInstrumentFinancials } from "@/api/instruments";
import type { InstrumentFinancialRow, InstrumentSummary } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Pane } from "@/components/instrument/Pane";
import { Sparkline } from "@/components/instrument/Sparkline";
import { useAsync } from "@/lib/useAsync";
import { useCallback, useMemo } from "react";
import { useNavigate } from "react-router-dom";

const SLICE = 8;

interface SeriesRow {
  readonly period_end: string;
  // Each metric is independently nullable. Per-cell render filters its
  // own column rather than the whole row dropping when one column is
  // missing — partnership/MLP issuers like IEP file
  // `IncomeLossFromContinuingOperations` instead of the standard
  // `OperatingIncomeLoss`, leaving operating_income null on every row,
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
    // Drop a row only when every income-side flagship metric is null
    // — otherwise the pane has nothing to plot. Total debt is a
    // best-effort sum (if either component is non-null we surface
    // what we have; balance-side gaps don't kill the income-side
    // sparklines).
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
  // Sort newest first then take the latest SLICE; reverse so the
  // sparklines plot oldest → newest left → right.
  joined.sort((a, b) => (a.period_end < b.period_end ? 1 : -1));
  const latest = joined.slice(0, SLICE);
  latest.reverse();
  return latest;
}

/** Filter the per-period series down to non-null values for one
 *  metric. Empty array means "this issuer doesn't report this metric"
 *  — the cell renders an em dash + a small "no data" hint. */
function nonNullValues(
  series: ReadonlyArray<SeriesRow>,
  pick: (row: SeriesRow) => number | null,
): number[] {
  const out: number[] = [];
  for (const row of series) {
    const v = pick(row);
    if (v !== null) out.push(v);
  }
  return out;
}

function formatLatest(values: ReadonlyArray<number>): string {
  if (values.length === 0) return "—";
  // length > 0 is guaranteed above; cast away the possible-undefined
  // that TypeScript infers from array index access in strict mode.
  const v = values[values.length - 1] as number;
  if (Math.abs(v) >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(2)}K`;
  return v.toFixed(0);
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

  // Capability active but the joined series is too short to plot — return
  // null to follow the polish round-2 four-state empty rule (no full
  // empty-state cards on the instrument page). Loading + error states
  // still render the Pane so the operator sees the chrome.
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
  const revenueValues = nonNullValues(series, (r) => r.revenue);
  const opIncomeValues = nonNullValues(series, (r) => r.operatingIncome);
  const netIncomeValues = nonNullValues(series, (r) => r.netIncome);
  const totalDebtValues = nonNullValues(series, (r) => r.totalDebt);
  // Sparklines are side-by-side and share an x-axis only visually —
  // when one cell has fewer periods than the siblings (e.g. an MLP
  // with operating_income null on every quarter), shapes can't be
  // compared directly. Surface a "n/N periods" caption on cells
  // whose coverage diverges from the maximum so the operator notices
  // the asymmetry. PR #684 review.
  const maxLen = Math.max(
    revenueValues.length,
    opIncomeValues.length,
    netIncomeValues.length,
    totalDebtValues.length,
  );
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      <FundamentalCell
        label="Revenue"
        values={revenueValues}
        maxLen={maxLen}
        stroke="text-sky-500"
      />
      <FundamentalCell
        label="Op income"
        values={opIncomeValues}
        maxLen={maxLen}
        stroke="text-emerald-500"
      />
      <FundamentalCell
        label="Net income"
        values={netIncomeValues}
        maxLen={maxLen}
        stroke="text-emerald-500"
      />
      <FundamentalCell
        label="Total debt"
        values={totalDebtValues}
        maxLen={maxLen}
        stroke="text-amber-500"
      />
    </div>
  );
}

/** Period-over-period delta as a percentage. Sign carried by the
 *  display layer; null when fewer than 2 points OR when prior is
 *  zero (undefined growth). */
function periodDelta(values: ReadonlyArray<number>): number | null {
  if (values.length < 2) return null;
  const prev = values[values.length - 2]!;
  const last = values[values.length - 1]!;
  if (prev === 0) return null;
  return ((last - prev) / Math.abs(prev)) * 100;
}

function FundamentalCell({
  label,
  values,
  maxLen,
  stroke,
}: {
  readonly label: string;
  readonly values: ReadonlyArray<number>;
  /** Largest period count across sibling cells. When this cell's
   *  ``values.length`` is smaller, the shapes between sparklines
   *  can't be compared directly — surface a coverage caption. */
  readonly maxLen: number;
  readonly stroke: string;
}) {
  const showCoverage = values.length > 0 && values.length < maxLen;
  const delta = periodDelta(values);
  const deltaClass =
    delta === null
      ? "text-slate-400"
      : delta > 0
        ? "text-emerald-600"
        : delta < 0
          ? "text-red-600"
          : "text-slate-500";
  return (
    <div className="flex flex-col items-start gap-0.5">
      <span className="flex w-full items-baseline justify-between gap-2">
        <span className="text-[10px] uppercase tracking-wider text-slate-500">
          {label}
        </span>
        {showCoverage ? (
          <span
            className="text-[9px] uppercase tracking-wider text-amber-600"
            title={`This cell covers ${values.length} of the ${maxLen} periods rendered by sibling cells.`}
          >
            {values.length}/{maxLen}
          </span>
        ) : null}
      </span>
      <Sparkline values={values} width={120} height={36} className={stroke} />
      <span className="flex items-baseline gap-1.5">
        <span className="text-sm font-semibold tabular-nums text-slate-800">
          {formatLatest(values)}
        </span>
        {delta !== null ? (
          <span className={`text-[10px] font-medium tabular-nums ${deltaClass}`}>
            {delta > 0 ? "▲" : delta < 0 ? "▼" : "·"}
            {Math.abs(delta).toFixed(1)}%
          </span>
        ) : null}
      </span>
    </div>
  );
}
