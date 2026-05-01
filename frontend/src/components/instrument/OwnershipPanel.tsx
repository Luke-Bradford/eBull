/**
 * Ownership reporting card (#729).
 *
 * Renders Institutions / ETFs / Insiders / Unallocated slices with
 * percentages computed against the free-float denominator
 * (shares_outstanding − treasury_shares). Treasury appears as a
 * memo line because it IS the gap between outstanding and float.
 *
 * Coverage gating:
 *   * No shares_outstanding on file → whole-card empty state.
 *   * Per-slice missing → "—" for that row, NOT 0%.
 *
 * Effective slice coverage depends on:
 *   * #731 — shares_outstanding + treasury_shares from financial_periods
 *   * #730 — institutional_holdings via the new reader endpoint
 *   * #740 — CUSIP backfill so the ingester actually resolves
 *     holdings to instrument_ids (currently most are dropped)
 *
 * Until #740 lands, the Institutions + ETFs slices return zero or
 * "—" for most US instruments. Operator-side ownership of the
 * coverage-gap copy is via the per-slice ``source_label`` link.
 */

import { useCallback } from "react";

import { fetchInstitutionalHoldings } from "@/api/institutionalHoldings";
import type { InstitutionalHoldingsResponse } from "@/api/institutionalHoldings";
import {
  fetchInsiderTransactions,
  fetchInstrumentFinancials,
} from "@/api/instruments";
import type { InsiderTransactionsList } from "@/api/instruments";
import type { InstrumentFinancials } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { Pane } from "@/components/instrument/Pane";
import {
  aggregateInsiderHoldings,
  computeOwnership,
  formatPct,
  formatShares,
  parseShareCount,
} from "@/components/instrument/ownershipMetrics";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

export interface OwnershipPanelProps {
  readonly symbol: string;
}

interface OwnershipData {
  readonly outstanding: number | null;
  readonly treasury: number | null;
  readonly institutions: number | null;
  readonly etfs: number | null;
  readonly insiders: number | null;
  readonly as_of_period: string | null;
  readonly institutional_period: string | null;
}

function pickLatestBalance(
  financials: InstrumentFinancials,
  column: string,
): number | null {
  for (const row of financials.rows) {
    const raw = row.values[column];
    const parsed = parseShareCount(raw ?? null);
    if (parsed !== null) return parsed;
  }
  return null;
}

export function OwnershipPanel({ symbol }: OwnershipPanelProps): JSX.Element {
  // Three parallel fetches: balance sheet (outstanding + treasury),
  // institutional holdings (institutions + ETFs), insider
  // transactions (insiders aggregate). Each fetch fails
  // independently — a missing input degrades the corresponding
  // slice rather than the whole card.
  const balanceState = useAsync<InstrumentFinancials>(
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

  const institutionalState = useAsync<InstitutionalHoldingsResponse>(
    useCallback(() => fetchInstitutionalHoldings(symbol, 50), [symbol]),
    [symbol],
  );

  const insidersState = useAsync<InsiderTransactionsList>(
    useCallback(() => fetchInsiderTransactions(symbol, 200), [symbol]),
    [symbol],
  );

  const isLoading =
    balanceState.loading ||
    institutionalState.loading ||
    insidersState.loading;
  const allErrored =
    balanceState.error !== null &&
    institutionalState.error !== null &&
    insidersState.error !== null;

  return (
    <Pane title="Ownership" source={{ providers: ["sec_13f", "sec_form4", "sec_xbrl"] }}>
      {isLoading ? (
        <SectionSkeleton rows={4} />
      ) : allErrored ? (
        <SectionError
          onRetry={() => {
            balanceState.refetch();
            institutionalState.refetch();
            insidersState.refetch();
          }}
        />
      ) : (
        renderBody(extractData(balanceState.data, institutionalState.data, insidersState.data))
      )}
    </Pane>
  );
}

function extractData(
  balance: InstrumentFinancials | null,
  institutional: InstitutionalHoldingsResponse | null,
  insiders: InsiderTransactionsList | null,
): OwnershipData {
  const outstanding =
    balance !== null ? pickLatestBalance(balance, "shares_outstanding") : null;
  const treasury =
    balance !== null ? pickLatestBalance(balance, "treasury_shares") : null;

  const inst_totals = institutional?.totals ?? null;
  const institutions = inst_totals
    ? parseShareCount(inst_totals.institutions_shares)
    : null;
  const etfs = inst_totals ? parseShareCount(inst_totals.etfs_shares) : null;

  const insiderShares = insiders ? aggregateInsiderHoldings(insiders.rows) : null;

  // The "as-of" surfaced in the header is the latest signal we
  // have — institutional period when present, else the balance row.
  const as_of_period =
    inst_totals?.period_of_report ??
    (balance?.rows[0]?.period_end ?? null);

  return {
    outstanding,
    treasury,
    institutions,
    etfs,
    insiders: insiderShares,
    as_of_period,
    institutional_period: inst_totals?.period_of_report ?? null,
  };
}

function renderBody(data: OwnershipData): JSX.Element {
  const breakdown = computeOwnership({
    shares_outstanding: data.outstanding,
    treasury_shares: data.treasury,
    institutions: { shares: data.institutions, source_label: "13F filers" },
    etfs: { shares: data.etfs, source_label: "13F filers (ETF)" },
    insiders: { shares: data.insiders, source_label: "Form 4" },
  });

  if (breakdown === null) {
    return (
      <EmptyState
        title="No ownership data"
        description="Shares outstanding is not on file for this instrument yet — the ownership breakdown needs SEC XBRL coverage to compute the float denominator."
      />
    );
  }

  return (
    <div className="space-y-3">
      {data.as_of_period !== null && (
        <p className="text-xs text-slate-500 dark:text-slate-400">
          As of {data.as_of_period}. Free float ={" "}
          {formatShares(breakdown.denominator)} shares.
        </p>
      )}
      {breakdown.has_overflow && (
        <p className="text-xs text-amber-700 dark:text-amber-400">
          Reported slice shares exceed free float — likely a 13F-HR
          filing that lags the latest XBRL share count. Unallocated
          clamped to 0%.
        </p>
      )}
      <table className="w-full text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          <tr>
            <th className="pb-1 text-left">Slice</th>
            <th className="pb-1 text-right">Shares</th>
            <th className="pb-1 text-right">% of float</th>
          </tr>
        </thead>
        <tbody>
          {breakdown.slices.map((slice) => (
            <tr
              key={slice.label}
              className="border-t border-slate-100 dark:border-slate-800"
            >
              <td className="py-1.5 text-slate-700 dark:text-slate-200">
                {slice.label}
                {slice.source_label !== undefined && (
                  <span className="ml-2 text-xs text-slate-400 dark:text-slate-500">
                    {slice.source_label}
                  </span>
                )}
              </td>
              <td className="py-1.5 text-right font-mono text-slate-700 dark:text-slate-200">
                {formatShares(slice.shares)}
              </td>
              <td className="py-1.5 text-right font-mono text-slate-900 dark:text-slate-100">
                {formatPct(slice.pct)}
              </td>
            </tr>
          ))}
          <tr className="border-t border-slate-200 dark:border-slate-700 text-xs text-slate-500 dark:text-slate-400">
            <td className="py-1.5 italic">Treasury (memo)</td>
            <td className="py-1.5 text-right font-mono">
              {formatShares(breakdown.treasury.shares)}
            </td>
            <td className="py-1.5 text-right font-mono">
              {formatPct(breakdown.treasury.pct)}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}
