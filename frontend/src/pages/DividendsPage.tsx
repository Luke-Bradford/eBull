/**
 * /instrument/:symbol/dividends — full dividend drill-through page (#578).
 *
 * Sections:
 *   1. Summary  — DividendsSummaryBlock
 *   2. Upcoming — NextDividendBanner (when present)
 *   3. Per-quarter history — full HistoryBar list, grouped by FY DESC
 *   4. Per-FY totals — summed dps_declared per fiscal_year
 *
 * URL: /instrument/:symbol/dividends
 * Optional ?provider= query param forwarded to the API; defaults to the
 * provider embedded in the data (any row's reported_currency shares the
 * same provider — we don't need to inspect capabilities here because this
 * page is only reachable from DividendsPanel which already resolved the
 * provider).
 */

import { fetchInstrumentDividends } from "@/api/instruments";
import type { DividendPeriod, InstrumentDividends } from "@/api/instruments";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { Pane } from "@/components/instrument/Pane";
import {
  DividendsSummaryBlock,
  formatDps,
  HistoryBar,
  NextDividendBanner,
} from "@/components/instrument/dividendsShared";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

// ---------------------------------------------------------------------------
// Per-FY totals helper
// ---------------------------------------------------------------------------

interface FyTotal {
  fiscal_year: number;
  total_dps: number;
  currency: string | null;
}

function buildFyTotals(history: ReadonlyArray<DividendPeriod>): FyTotal[] {
  const map = new Map<number, { sum: number; currency: string | null }>();
  for (const p of history) {
    const existing = map.get(p.fiscal_year);
    const dps = p.dps_declared !== null ? Number(p.dps_declared) : 0;
    const num = Number.isFinite(dps) ? dps : 0;
    if (existing === undefined) {
      map.set(p.fiscal_year, {
        sum: num,
        currency: p.reported_currency,
      });
    } else {
      existing.sum += num;
    }
  }
  // Sort descending by fiscal year.
  return [...map.entries()]
    .sort(([a], [b]) => b - a)
    .map(([fy, { sum, currency }]) => ({
      fiscal_year: fy,
      total_dps: sum,
      currency,
    }));
}

// ---------------------------------------------------------------------------
// Full history sorted newest-first
// ---------------------------------------------------------------------------

function sortedHistory(history: ReadonlyArray<DividendPeriod>): DividendPeriod[] {
  return [...history].sort((a, b) => {
    if (b.fiscal_year !== a.fiscal_year) return b.fiscal_year - a.fiscal_year;
    // Within a FY, sort by period_end_date DESC.
    return b.period_end_date.localeCompare(a.period_end_date);
  });
}

// ---------------------------------------------------------------------------
// Page component
// ---------------------------------------------------------------------------

export function DividendsPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams] = useSearchParams();
  const provider = searchParams.get("provider") ?? undefined;

  const state = useAsync<InstrumentDividends>(
    useCallback(
      () => fetchInstrumentDividends(symbol, provider),
      [symbol, provider],
    ),
    [symbol, provider],
  );

  const backHref = `/instrument/${encodeURIComponent(symbol)}`;

  return (
    <div className="mx-auto max-w-screen-xl space-y-4 p-4">
      <header className="border-b border-slate-200 pb-3">
        <Link
          to={backHref}
          className="text-xs text-sky-700 hover:underline"
        >
          ← Back to {symbol}
        </Link>
        <h1 className="mt-1 text-lg font-semibold text-slate-900">
          Dividends — {symbol}
        </h1>
      </header>

      {state.loading ? (
        <SectionSkeleton rows={6} />
      ) : state.error !== null || state.data === null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data.history.length === 0 && state.data.upcoming.length === 0 ? (
        <EmptyState
          title="No dividend data"
          description="No dividend history or upcoming dividends on file for this instrument."
        >
          <Link
            to={backHref}
            className="text-sm text-sky-700 hover:underline"
          >
            ← Back to {symbol}
          </Link>
        </EmptyState>
      ) : (
        <div className="space-y-4">
          {/* 1. Summary */}
          <Pane title="Summary">
            <DividendsSummaryBlock summary={state.data.summary} />
          </Pane>

          {/* 2. Upcoming dividend (optional) */}
          {state.data.upcoming[0] !== undefined && (
            <Pane title="Upcoming dividend">
              <NextDividendBanner upcoming={state.data.upcoming[0]} />
            </Pane>
          )}

          {/* 3. Per-quarter history */}
          {state.data.history.length > 0 && (
            <Pane title="Per-quarter history">
              <PerQuarterHistory history={state.data.history} />
            </Pane>
          )}

          {/* 4. Per-FY totals */}
          {state.data.history.length > 0 && (
            <Pane title="Per-FY totals">
              <FyTotalsTable history={state.data.history} />
            </Pane>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Per-quarter history
// ---------------------------------------------------------------------------

function PerQuarterHistory({
  history,
}: {
  readonly history: ReadonlyArray<DividendPeriod>;
}) {
  const sorted = sortedHistory(history);
  const max = sorted.reduce((acc, p) => {
    if (p.dps_declared === null) return acc;
    const n = Number(p.dps_declared);
    return Number.isFinite(n) && n > acc ? n : acc;
  }, 0);
  return (
    <div className="space-y-1">
      {sorted.map((p) => (
        <HistoryBar
          key={`${p.period_end_date}-${p.period_type}`}
          period={p}
          max={max}
        />
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Per-FY totals table
// ---------------------------------------------------------------------------

function FyTotalsTable({
  history,
}: {
  readonly history: ReadonlyArray<DividendPeriod>;
}) {
  const totals = buildFyTotals(history);
  return (
    <table className="min-w-full text-xs">
      <thead>
        <tr className="border-b border-slate-200 text-left text-slate-500">
          <th className="px-2 py-1">Fiscal year</th>
          <th className="px-2 py-1 text-right">Total DPS</th>
        </tr>
      </thead>
      <tbody>
        {totals.map((row) => (
          <tr
            key={row.fiscal_year}
            className="border-b border-slate-100 last:border-0"
          >
            <td className="px-2 py-1 font-medium text-slate-700">
              FY{row.fiscal_year}
            </td>
            <td className="px-2 py-1 text-right font-mono tabular-nums text-slate-700">
              {formatDps(row.total_dps.toString(), row.currency)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
