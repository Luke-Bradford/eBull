/**
 * /instrument/:symbol/dividends — full dividend drill-through page (#578).
 *
 * #590 upgrade: four recharts panes layered above the existing raw
 * tables — DPS line, cumulative DPS area, payout-ratio line (annual
 * cashflow input), and yield-on-cost bar (only when the operator
 * holds the instrument). The original Summary / Upcoming / per-quarter
 * history bars / per-FY totals stay below the charts so the analyst
 * view and the raw audit-trail view live side by side.
 *
 * Fetches:
 *   - GET /instruments/{symbol}/dividends                       (always)
 *   - GET /instruments/{symbol}/summary                         (always — gets instrument_id for the position lookup)
 *   - GET /instruments/{symbol}/financials?statement=cashflow&period=annual (for payout ratio; degrades the pane only)
 *   - GET /portfolio/instruments/{instrument_id}                (after summary lands; degrades to "not held" on 404 / zero units)
 *
 * URL: /instrument/:symbol/dividends
 * Optional ?provider= forwarded to the dividends endpoint.
 */

import { useCallback, useMemo } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import {
  fetchInstrumentDividends,
  fetchInstrumentFinancials,
  fetchInstrumentSummary,
} from "@/api/instruments";
import type {
  DividendPeriod,
  InstrumentDividends,
} from "@/api/instruments";
import type {
  InstrumentFinancials,
  InstrumentPositionDetail,
  InstrumentSummary,
} from "@/api/types";
import { ApiError } from "@/api/client";
import { fetchInstrumentPositions } from "@/api/portfolio";
import {
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import {
  CumulativeDpsChart,
  DpsLineChart,
  PayoutRatioChart,
  YieldOnCostChart,
} from "@/components/dividends/dividendsCharts";
import { Pane } from "@/components/instrument/Pane";
import { Term } from "@/components/Term";
import {
  DividendsSummaryBlock,
  formatDps,
  HistoryBar,
  NextDividendBanner,
} from "@/components/instrument/dividendsShared";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

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
// Position fetch (degrades to null on 404 — instrument exists but
// is not in the operator's portfolio. Other failures rethrow so the
// error surfaces in the SectionError path.)
// ---------------------------------------------------------------------------

async function fetchPositionOrNull(
  instrumentId: number,
): Promise<InstrumentPositionDetail | null> {
  try {
    return await fetchInstrumentPositions(instrumentId);
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Page component
// ---------------------------------------------------------------------------

export function DividendsPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams] = useSearchParams();
  const provider = searchParams.get("provider") ?? undefined;

  const dividends = useAsync<InstrumentDividends>(
    useCallback(
      () => fetchInstrumentDividends(symbol, provider),
      [symbol, provider],
    ),
    [symbol, provider],
  );

  const summary = useAsync<InstrumentSummary>(
    useCallback(() => fetchInstrumentSummary(symbol), [symbol]),
    [symbol],
  );

  // Annual cashflow rows feed the payout-ratio pane. Quarterly is too
  // noisy (capex spikes throw the ratio); spec asks for annual.
  const cashflow = useAsync<InstrumentFinancials>(
    useCallback(
      () =>
        fetchInstrumentFinancials(symbol, {
          statement: "cashflow",
          period: "annual",
        }),
      [symbol],
    ),
    [symbol],
  );

  // Position fetch keys on the instrument_id once summary resolves.
  // useAsync re-runs when deps change, so we're safe to feed it the
  // resolved id without an effect dance.
  const instrumentId = summary.data?.instrument_id ?? null;
  const position = useAsync<InstrumentPositionDetail | null>(
    useCallback(
      () =>
        instrumentId === null
          ? Promise.resolve(null)
          : fetchPositionOrNull(instrumentId),
      [instrumentId],
    ),
    [instrumentId],
  );

  const avgEntry = useMemo<number | null>(() => {
    const pos = position.data;
    if (pos === null) return null;
    if (pos.total_units <= 0) return null;
    // `avg_entry` is the per-share weighted entry price. Yield-on-
    // cost divides by it, so a null / zero / negative value is
    // mathematically meaningless; treat the position as effectively
    // unheld for YoC purposes and let the page hide the pane.
    if (pos.avg_entry === null || pos.avg_entry <= 0) return null;
    return pos.avg_entry;
  }, [position.data]);

  const backHref = `/instrument/${encodeURIComponent(symbol)}`;

  return (
    <div className="mx-auto max-w-screen-xl space-y-4 p-4">
      <header className="border-b border-slate-200 dark:border-slate-800 pb-3">
        <Link to={backHref} className="text-xs text-sky-700 hover:underline">
          ← Back to {symbol}
        </Link>
        <h1 className="mt-1 text-lg font-semibold text-slate-900 dark:text-slate-100">
          Dividends — {symbol}
        </h1>
        <p className="mt-1 text-xs text-slate-500">
          SEC XBRL declared per-share / per-unit history. Charts read from
          the per-quarter <Term term="DPS" /> stream; <Term term="TTM" />{" "}
          summary uses the latest 4 quarters. <Term term="Payout ratio" />{" "}
          and <Term term="Yield-on-cost" /> render only when their inputs
          (annual cashflow / your entry price) are available.
        </p>
      </header>

      {dividends.loading ? (
        <SectionSkeleton rows={6} />
      ) : dividends.error !== null || dividends.data === null ? (
        <SectionError onRetry={dividends.refetch} />
      ) : dividends.data.history.length === 0 &&
        dividends.data.upcoming.length === 0 ? (
        <EmptyState
          title="No dividend data"
          description="No dividend history or upcoming dividends on file for this instrument."
        >
          <Link to={backHref} className="text-sm text-sky-700 hover:underline">
            ← Back to {symbol}
          </Link>
        </EmptyState>
      ) : (
        <div className="space-y-4">
          {/* 1. Summary */}
          <Pane title="Summary">
            <DividendsSummaryBlock summary={dividends.data.summary} />
          </Pane>

          {/* 2. Upcoming dividend (optional) */}
          {dividends.data.upcoming[0] !== undefined && (
            <Pane title="Upcoming dividend">
              <NextDividendBanner upcoming={dividends.data.upcoming[0]} />
            </Pane>
          )}

          {/* 3. DPS line */}
          {dividends.data.history.length > 0 && (
            <Pane
              title="DPS over time"
              scope="declared per period"
              source={{ providers: ["sec_xbrl"] }}
            >
              <DpsLineChart history={dividends.data.history} />
            </Pane>
          )}

          {/* 4. Cumulative DPS */}
          {dividends.data.history.length > 0 && (
            <Pane
              title="Cumulative DPS"
              scope="running total since first reported"
              source={{ providers: ["sec_xbrl"] }}
            >
              <CumulativeDpsChart history={dividends.data.history} />
            </Pane>
          )}

          {/* 5. Payout ratio (annual). Degrades inside the pane when
               the cashflow endpoint fails or has no data — the rest
               of the page keeps rendering. */}
          <Pane
            title="Payout ratio"
            scope="annual · dividends paid / FCF"
            source={{ providers: ["sec_xbrl"] }}
          >
            <PayoutRatioPaneBody state={cashflow} />
          </Pane>

          {/* 6. Yield-on-cost — only when held. The chart itself
               renders the not-held empty hint, but we hide the pane
               entirely so unheld instruments don't carry an awkward
               gray block. */}
          {avgEntry !== null && dividends.data.history.length > 0 && (
            <Pane
              title="Yield-on-cost"
              scope={`vs avg entry ${avgEntry.toFixed(2)}`}
              source={{ providers: ["sec_xbrl", "etoro"] }}
            >
              <YieldOnCostChart
                history={dividends.data.history}
                avgEntry={avgEntry}
              />
            </Pane>
          )}

          {/* 7. Per-quarter history (raw audit) */}
          {dividends.data.history.length > 0 && (
            <Pane title="Per-quarter history">
              <PerQuarterHistory history={dividends.data.history} />
            </Pane>
          )}

          {/* 8. Per-FY totals (raw audit) */}
          {dividends.data.history.length > 0 && (
            <Pane title="Per-FY totals">
              <FyTotalsTable history={dividends.data.history} />
            </Pane>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Per-pane payout-ratio body — separated so its loading / error /
// no-data branches don't bloat the page render. The cashflow endpoint
// can legitimately fail or be empty without taking down the rest of
// the page.
// ---------------------------------------------------------------------------

function PayoutRatioPaneBody({
  state,
}: {
  readonly state: ReturnType<typeof useAsync<InstrumentFinancials>>;
}): JSX.Element {
  if (state.loading) return <SectionSkeleton rows={3} />;
  if (state.error !== null) {
    return (
      <p className="text-xs text-slate-500">
        Cash-flow data unavailable.
        <button
          type="button"
          onClick={state.refetch}
          className="ml-2 text-sky-700 hover:underline"
        >
          Retry
        </button>
      </p>
    );
  }
  return <PayoutRatioChart cashflowRows={state.data?.rows ?? []} />;
}

// ---------------------------------------------------------------------------
// Per-quarter history (existing component, unchanged)
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
// Per-FY totals table (existing component, unchanged)
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
        <tr className="border-b border-slate-200 dark:border-slate-800 text-left text-slate-500">
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
