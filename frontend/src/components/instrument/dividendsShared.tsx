/**
 * Shared helpers for DividendsPanel (compact summary) and DividendsPage
 * (full drill-through). Extracted as part of #578.
 *
 * Exports:
 *   formatDps            — numeric string → localised "USD 0.25"
 *   formatYieldPct       — numeric string → "0.52%"
 *   HistoryBar           — single-row bar chart for a DividendPeriod
 *   NextDividendBanner   — amber card for UpcomingDividend
 *   DividendsSummaryBlock — 4-row summary dl (TTM yield / TTM DPS / Latest / Streak)
 */

import type { DividendPeriod, DividendSummary, UpcomingDividend } from "@/api/instruments";
import { Term } from "@/components/Term";

// ---------------------------------------------------------------------------
// Formatters
// ---------------------------------------------------------------------------

export function formatDps(raw: string | null, currency: string | null): string {
  if (raw === null) return "—";
  const num = Number(raw);
  if (!Number.isFinite(num)) return "—";
  return `${currency ?? ""}${currency ? " " : ""}${num.toFixed(4).replace(/\.?0+$/, "")}`.trim();
}

export function formatYieldPct(raw: string | null): string {
  if (raw === null) return "—";
  const num = Number(raw);
  if (!Number.isFinite(num)) return "—";
  return `${num.toFixed(2)}%`;
}

// ---------------------------------------------------------------------------
// HistoryBar
// ---------------------------------------------------------------------------

export function HistoryBar({
  period,
  max,
}: {
  period: DividendPeriod;
  max: number;
}) {
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

// ---------------------------------------------------------------------------
// NextDividendBanner
// ---------------------------------------------------------------------------

export function NextDividendBanner({ upcoming }: { upcoming: UpcomingDividend }) {
  // Banner shows whichever calendar dates survived the 8-K regex parse.
  // A row with only dps_declared (no dates yet) still renders — the
  // banner's job is "operator awareness", not a filled-in calendar.
  const exOrPay = upcoming.ex_date ?? upcoming.pay_date;
  return (
    <div className="mb-4 rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm">
      <div className="flex items-baseline justify-between gap-3">
        <span className="font-semibold text-amber-900">Next dividend</span>
        {upcoming.dps_declared !== null && (
          <span className="font-mono tabular-nums text-amber-900">
            {formatDps(upcoming.dps_declared, upcoming.currency)}
          </span>
        )}
      </div>
      <dl className="mt-1 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-xs text-amber-800">
        {upcoming.ex_date !== null && (
          <>
            <dt className="text-amber-700">Ex-date</dt>
            <dd>{upcoming.ex_date}</dd>
          </>
        )}
        {upcoming.record_date !== null && (
          <>
            <dt className="text-amber-700">Record</dt>
            <dd>{upcoming.record_date}</dd>
          </>
        )}
        {upcoming.pay_date !== null && (
          <>
            <dt className="text-amber-700">Pay</dt>
            <dd>{upcoming.pay_date}</dd>
          </>
        )}
        {exOrPay === null && upcoming.declaration_date !== null && (
          <>
            <dt className="text-amber-700">Declared</dt>
            <dd>{upcoming.declaration_date} (calendar TBD)</dd>
          </>
        )}
      </dl>
    </div>
  );
}

// ---------------------------------------------------------------------------
// DividendsSummaryBlock
// ---------------------------------------------------------------------------

export function DividendsSummaryBlock({ summary }: { summary: DividendSummary }) {
  return (
    <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
      <dt className="text-slate-500">
        <Term term="TTM" /> yield
      </dt>
      <dd className="font-semibold text-emerald-700">
        {formatYieldPct(summary.ttm_yield_pct)}
      </dd>
      <dt className="text-slate-500">
        <Term term="TTM" /> <Term term="DPS" />
      </dt>
      <dd>{formatDps(summary.ttm_dps, summary.dividend_currency)}</dd>
      <dt className="text-slate-500">
        Latest <Term term="DPS" />
      </dt>
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
  );
}
