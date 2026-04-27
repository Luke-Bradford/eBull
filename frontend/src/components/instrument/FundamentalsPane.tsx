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
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { Sparkline } from "@/components/instrument/Sparkline";
import { useAsync } from "@/lib/useAsync";
import { useCallback, useMemo } from "react";
import { Link } from "react-router-dom";

const SLICE = 8;

interface SeriesRow {
  readonly period_end: string;
  readonly revenue: number;
  readonly operatingIncome: number;
  readonly netIncome: number;
  readonly totalDebt: number;
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
    if (b === undefined) continue;
    const revenue = num(i.values["revenue"] ?? null);
    const operatingIncome = num(i.values["operating_income"] ?? null);
    const netIncome = num(i.values["net_income"] ?? null);
    const lt = num(b.values["long_term_debt"] ?? null) ?? 0;
    const st = num(b.values["short_term_debt"] ?? null) ?? 0;
    if (revenue === null || operatingIncome === null || netIncome === null) {
      continue;
    }
    joined.push({
      period_end: i.period_end,
      revenue,
      operatingIncome,
      netIncome,
      totalDebt: lt + st,
    });
  }
  // Sort newest first then take the latest SLICE; reverse so the
  // sparklines plot oldest → newest left → right.
  joined.sort((a, b) => (a.period_end < b.period_end ? 1 : -1));
  const latest = joined.slice(0, SLICE);
  latest.reverse();
  return latest;
}

function formatLatest(values: ReadonlyArray<number>): string {
  if (values.length === 0) return "—";
  const v = values[values.length - 1];
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

  return (
    <Section title="Fundamentals">
      {income.loading || balance.loading ? (
        <SectionSkeleton rows={3} />
      ) : income.error !== null || balance.error !== null ? (
        <SectionError onRetry={() => { income.refetch(); balance.refetch(); }} />
      ) : series.length < 2 ? (
        <EmptyState
          title="Not enough fundamentals history"
          description="Need at least 2 quarters with both income + balance data."
        />
      ) : (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <FundamentalCell
            label="Revenue"
            values={series.map((r) => r.revenue)}
            stroke="text-sky-500"
          />
          <FundamentalCell
            label="Op income"
            values={series.map((r) => r.operatingIncome)}
            stroke="text-emerald-500"
          />
          <FundamentalCell
            label="Net income"
            values={series.map((r) => r.netIncome)}
            stroke="text-emerald-500"
          />
          <FundamentalCell
            label="Total debt"
            values={series.map((r) => r.totalDebt)}
            stroke="text-amber-500"
          />
        </div>
      )}
      <div className="mt-2 border-t border-slate-100 pt-1.5 text-right">
        <Link
          to={`/instrument/${encodeURIComponent(symbol)}?tab=financials`}
          className="text-[11px] text-sky-700 hover:underline"
        >
          View statements →
        </Link>
      </div>
    </Section>
  );
}

function FundamentalCell({
  label,
  values,
  stroke,
}: {
  readonly label: string;
  readonly values: ReadonlyArray<number>;
  readonly stroke: string;
}) {
  return (
    <div className="flex flex-col items-start">
      <span className="text-[10px] uppercase tracking-wider text-slate-500">
        {label}
      </span>
      <Sparkline values={values} className={stroke} />
      <span className="text-xs font-medium tabular-nums text-slate-800">
        {formatLatest(values)}
      </span>
    </div>
  );
}
