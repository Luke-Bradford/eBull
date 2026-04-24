/**
 * DividendsPanel — TTM yield + per-quarter history for the instrument
 * page. Backed by GET /instruments/{symbol}/dividends.
 *
 * Never-paid instruments render an explicit empty state rather than a
 * 404 or a zero row.
 */

import { fetchInstrumentDividends } from "@/api/instruments";
import type { DividendPeriod, InstrumentDividends } from "@/api/instruments";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";

export interface DividendsPanelProps {
  readonly symbol: string;
}

function formatDps(raw: string | null, currency: string | null): string {
  if (raw === null) return "—";
  const num = Number(raw);
  if (!Number.isFinite(num)) return "—";
  return `${currency ?? ""}${currency ? " " : ""}${num.toFixed(4).replace(/\.?0+$/, "")}`.trim();
}

function formatYieldPct(raw: string | null): string {
  if (raw === null) return "—";
  const num = Number(raw);
  if (!Number.isFinite(num)) return "—";
  return `${num.toFixed(2)}%`;
}

function HistoryBar({ period, max }: { period: DividendPeriod; max: number }) {
  const num = period.dps_declared !== null ? Number(period.dps_declared) : 0;
  const pct = max > 0 ? Math.min(100, (num / max) * 100) : 0;
  const label = `FY${period.fiscal_year} ${period.period_type}`;
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="w-20 shrink-0 text-slate-500">{label}</span>
      <div className="h-3 flex-1 rounded-sm bg-slate-100">
        <div
          className="h-full rounded-sm bg-sky-500"
          style={{ width: `${pct}%` }}
          role="progressbar"
          aria-valuenow={num}
          aria-valuemin={0}
          aria-valuemax={max}
          aria-label={`DPS ${num} in ${label}`}
        />
      </div>
      <span className="w-16 shrink-0 text-right font-mono tabular-nums text-slate-700">
        {formatDps(period.dps_declared, period.reported_currency)}
      </span>
    </div>
  );
}

export function DividendsPanel({ symbol }: DividendsPanelProps) {
  const state = useAsync<InstrumentDividends>(
    useCallback(() => fetchInstrumentDividends(symbol), [symbol]),
    [symbol],
  );

  return (
    <Section title="Dividends">
      {state.loading ? (
        <SectionSkeleton rows={3} />
      ) : state.error !== null || state.data === null ? (
        <SectionError onRetry={state.refetch} />
      ) : !state.data.summary.has_dividend || state.data.history.length === 0 ? (
        <EmptyState
          title="No dividend history on file"
          description="This instrument has not reported a positive dividend in its SEC filings."
        />
      ) : (
        <DividendsBody data={state.data} />
      )}
    </Section>
  );
}

function DividendsBody({ data }: { data: InstrumentDividends }) {
  const { summary, history } = data;
  const max = history.reduce((acc, p) => {
    if (p.dps_declared === null) return acc;
    const n = Number(p.dps_declared);
    return Number.isFinite(n) && n > acc ? n : acc;
  }, 0);

  return (
    <div className="space-y-4">
      {/* Summary row */}
      <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
        <dt className="text-slate-500">TTM yield</dt>
        <dd className="font-semibold text-emerald-700">
          {formatYieldPct(summary.ttm_yield_pct)}
        </dd>
        <dt className="text-slate-500">TTM DPS</dt>
        <dd>{formatDps(summary.ttm_dps, summary.dividend_currency)}</dd>
        <dt className="text-slate-500">Latest DPS</dt>
        <dd>
          {formatDps(summary.latest_dps, summary.dividend_currency)}
          {summary.latest_dividend_at !== null && (
            <span className="ml-2 text-xs text-slate-500">
              period end {summary.latest_dividend_at}
            </span>
          )}
        </dd>
        <dt className="text-slate-500">Consecutive quarters</dt>
        <dd>{summary.dividend_streak_q}</dd>
      </dl>

      {/* Per-quarter history */}
      <div>
        <div className="mb-1 text-xs font-medium uppercase tracking-wider text-slate-500">
          Per-quarter DPS
        </div>
        <div className="space-y-1">
          {history.map((p) => (
            <HistoryBar
              key={`${p.period_end_date}-${p.period_type}`}
              period={p}
              max={max}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
