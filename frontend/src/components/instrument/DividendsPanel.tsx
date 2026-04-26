/**
 * DividendsPanel — provider-agnostic shell for the per-instrument
 * dividends capability (#515 PR 3b). Backed by GET
 * /instruments/{symbol}/dividends?provider=<provider>.
 *
 * The shell renders the normalised dividend shape returned by the
 * endpoint regardless of which provider populated it. Today only
 * ``sec_dividend_summary`` is wired; per-region integration PRs
 * (Companies House dividends, KRX dividends, …) reuse the same shell
 * + endpoint contract — no panel code changes required.
 *
 * Never-paid instruments render an explicit empty state rather than a
 * 404 or a zero row.
 */

import { fetchInstrumentDividends } from "@/api/instruments";
import type {
  DividendPeriod,
  InstrumentDividends,
  UpcomingDividend,
} from "@/api/instruments";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { providerLabel } from "@/lib/capabilityProviders";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";

export interface DividendsPanelProps {
  readonly symbol: string;
  /** Capability provider tag, resolved via
   *  ``summary.capabilities.dividends.providers`` upstream. The
   *  shell forwards it to the endpoint as ``?provider=<tag>``. */
  readonly provider: string;
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

export function DividendsPanel({ symbol, provider }: DividendsPanelProps) {
  const state = useAsync<InstrumentDividends>(
    useCallback(
      () => fetchInstrumentDividends(symbol, provider),
      [symbol, provider],
    ),
    [symbol, provider],
  );
  const title = `Dividends · ${providerLabel(provider)}`;

  return (
    <Section title={title}>
      {state.loading ? (
        <SectionSkeleton rows={3} />
      ) : state.error !== null || state.data === null ? (
        <SectionError onRetry={state.refetch} />
      ) : (
        <>
          {/* Upcoming banner renders OUTSIDE the has_dividend gate so a
              company announcing its first-ever dividend via 8-K (with
              zero XBRL history yet) still shows the calendar instead
              of the "never paid" empty state. */}
          {state.data.upcoming[0] !== undefined && (
            <NextDividendBanner upcoming={state.data.upcoming[0]} />
          )}
          {!state.data.summary.has_dividend ||
          state.data.history.length === 0 ? (
            <EmptyState
              title="No dividend history on file"
              description="This instrument has not reported a positive dividend in this provider's data."
            />
          ) : (
            <DividendsBody data={state.data} />
          )}
        </>
      )}
    </Section>
  );
}

function NextDividendBanner({ upcoming }: { upcoming: UpcomingDividend }) {
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
            <dd>
              {upcoming.declaration_date} (calendar TBD)
            </dd>
          </>
        )}
      </dl>
    </div>
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
