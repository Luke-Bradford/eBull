/**
 * Ownership reporting card (#729).
 *
 * Three-ring sunburst keyed on ``shares_outstanding`` as the
 * denominator. Treasury is one of the categories — the denominator
 * includes it, not free float.
 *
 *   ring 1 (inner)  — total shares outstanding (label in center hole)
 *   ring 2 (middle) — Institutions / ETFs / Insiders / Treasury,
 *                     plus a transparent residual for the unaccounted
 *                     portion.
 *   ring 3 (outer)  — per-filer / per-officer wedges, plus a
 *                     transparent within-category gap when the filer
 *                     detail is incomplete (e.g. Institutions reports
 *                     a 50% aggregate but the #740 CUSIP backfill
 *                     hasn't resolved every filer to an instrument).
 *
 * Effective coverage depends on:
 *   * #731 — shares_outstanding + treasury_shares from financial_periods (merged)
 *   * #730 — institutional_holdings via the new reader endpoint (merged)
 *   * #740 — CUSIP backfill so the ingester resolves holdings to instrument_ids (open)
 *   * #735 — DEI projection so XBRL ownership columns (treasury, public float) flow (open)
 *
 * Click on any colored wedge → drill to the L2 ownership page with
 * the corresponding filter pre-applied.
 */

import { useCallback } from "react";
import { useNavigate } from "react-router-dom";

import { fetchInstitutionalHoldings } from "@/api/institutionalHoldings";
import type {
  InstitutionalFilerHolding,
  InstitutionalHoldingsResponse,
} from "@/api/institutionalHoldings";
import {
  fetchInsiderTransactions,
  fetchInstrumentFinancials,
} from "@/api/instruments";
import type { InsiderTransactionsList } from "@/api/instruments";
import type { InstrumentFinancials } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { OwnershipFreshnessChips } from "@/components/instrument/OwnershipFreshnessChips";
import {
  OwnershipLegend,
  OwnershipSunburst,
} from "@/components/instrument/OwnershipSunburst";
import type { WedgeClick } from "@/components/instrument/OwnershipSunburst";
import { Pane } from "@/components/instrument/Pane";
import {
  formatPct,
  formatShares,
  parseShareCount,
} from "@/components/instrument/ownershipMetrics";
import {
  type SunburstHolder,
  type SunburstInputs,
  buildSunburstRings,
} from "@/components/instrument/ownershipRings";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

export interface OwnershipPanelProps {
  readonly symbol: string;
}

interface OwnershipData {
  readonly outstanding: number | null;
  readonly treasury: number | null;
  readonly institutions_total: number | null;
  readonly etfs_total: number | null;
  readonly insiders_total: number | null;
  readonly institutional_holders: readonly SunburstHolder[];
  readonly etf_holders: readonly SunburstHolder[];
  readonly insider_holders: readonly SunburstHolder[];
  /** 13F snapshot date — applies to both Institutions and ETFs (same
   *  ``period_of_report`` cohort across filer types). */
  readonly thirteen_f_as_of: string | null;
  /** Latest Form 4 transaction date for any insider on this issuer. */
  readonly insiders_as_of: string | null;
  /** XBRL period_end of the row that produced ``treasury``. */
  readonly treasury_as_of: string | null;
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
    useCallback(() => fetchInstitutionalHoldings(symbol, 500), [symbol]),
    [symbol],
  );

  const insidersState = useAsync<InsiderTransactionsList>(
    useCallback(() => fetchInsiderTransactions(symbol, 200), [symbol]),
    [symbol],
  );

  const navigate = useNavigate();
  const handleWedgeClick = useCallback(
    (target: WedgeClick) => {
      const params = new URLSearchParams();
      if (target.kind === "category") params.set("category", target.category_key);
      if (target.kind === "leaf") {
        params.set("category", target.category_key);
        params.set("filer", target.leaf_key);
      }
      const qs = params.toString();
      const suffix = qs.length > 0 ? `?${qs}` : "";
      navigate(`/instrument/${encodeURIComponent(symbol)}/ownership${suffix}`);
    },
    [navigate, symbol],
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
        renderBody(
          extractData(balanceState.data, institutionalState.data, insidersState.data),
          handleWedgeClick,
        )
      )}
    </Pane>
  );
}

export function extractData(
  balance: InstrumentFinancials | null,
  institutional: InstitutionalHoldingsResponse | null,
  insiders: InsiderTransactionsList | null,
): OwnershipData {
  const outstanding =
    balance !== null ? pickLatestBalance(balance, "shares_outstanding") : null;
  const treasury =
    balance !== null ? pickLatestBalance(balance, "treasury_shares") : null;

  const inst_totals = institutional?.totals ?? null;
  const filers = institutional?.filers ?? [];
  // Equity-only — option exposure double-counts the underlying.
  const equity_filers = filers.filter((f) => f.is_put_call === null);

  const institutional_holders: SunburstHolder[] = equity_filers
    .filter((f) => f.filer_type !== "ETF")
    .map(filerToHolder("institutions"));
  const etf_holders: SunburstHolder[] = equity_filers
    .filter((f) => f.filer_type === "ETF")
    .map(filerToHolder("etfs"));
  const insider_holders = aggregateInsiderHoldersForSunburst(insiders);

  const institutions_total = parseShareCount(inst_totals?.institutions_shares ?? null);
  const etfs_total = parseShareCount(inst_totals?.etfs_shares ?? null);
  // Form 4 has no aggregate-total endpoint — sum the per-officer
  // post-transaction balances. When no officer rows are on file the
  // total is null (category does not render).
  const insiders_total =
    insider_holders.length === 0
      ? null
      : insider_holders.reduce((s, h) => s + h.shares, 0);

  // Per-category freshness sources (#767):
  //   * 13F (Institutions + ETFs): one shared period_of_report from
  //     the totals row — both filer-type buckets are computed from
  //     the same ``MAX(period_of_report)`` cohort backend-side.
  //   * Insiders: latest txn_date observed across non-derivative
  //     post-transaction rows. The reader endpoint exposes a
  //     summary.latest_txn_date but the panel hits the per-row list;
  //     derive locally so we don't add a second round trip.
  //   * Treasury: balance-sheet row period_end that produced the
  //     value — already the latest non-null treasury_shares row from
  //     pickLatestBalance.
  const thirteen_f_as_of = inst_totals?.period_of_report ?? null;
  const insiders_as_of =
    insiders === null || insiders.rows.length === 0
      ? null
      : latestTxnDate(insiders.rows as readonly InsiderRowShape[]);
  const treasury_as_of =
    treasury !== null && balance !== null ? findRowDateFor(balance, "treasury_shares") : null;

  return {
    outstanding,
    treasury,
    institutions_total,
    etfs_total,
    insiders_total,
    institutional_holders,
    etf_holders,
    insider_holders,
    thirteen_f_as_of,
    insiders_as_of,
    treasury_as_of,
  };
}

/**
 * Single row-eligibility predicate shared by ``latestTxnDate`` and
 * ``aggregateInsiderHoldersForSunburst``. Codex (review of #767) caught
 * that the two had drifted: the date helper accepted any non-derivative
 * row, but the holders aggregator additionally requires a parseable
 * ``post_transaction_shares``. With separate predicates a Form 4 row
 * with a null share count would advance the freshness chip ahead of the
 * actual holdings snapshot the ring renders. Keep them in lockstep here.
 */
function isInsiderHoldingRow(row: InsiderRowShape): boolean {
  if (row.is_derivative) return false;
  return parseShareCount(row.post_transaction_shares) !== null;
}

function latestTxnDate(rows: readonly InsiderRowShape[]): string | null {
  let latest: string | null = null;
  for (const row of rows) {
    if (!isInsiderHoldingRow(row)) continue;
    if (latest === null || row.txn_date > latest) latest = row.txn_date;
  }
  return latest;
}

/** Find the period_end of the first balance-sheet row that has a
 *  non-null value for ``column``. Mirrors the iteration in
 *  ``pickLatestBalance`` so the date and value stay paired. */
function findRowDateFor(financials: InstrumentFinancials, column: string): string | null {
  for (const row of financials.rows) {
    const raw = row.values[column];
    const parsed = parseShareCount(raw ?? null);
    if (parsed !== null) return row.period_end;
  }
  return null;
}

function filerToHolder(
  category: SunburstHolder["category"],
): (f: InstitutionalFilerHolding) => SunburstHolder {
  return (f) => ({
    key: f.filer_cik,
    label: f.filer_name,
    shares: parseShareCount(f.shares) ?? 0,
    category,
  });
}

interface InsiderRowShape {
  readonly filer_cik: string | null;
  readonly filer_name: string;
  readonly txn_date: string;
  readonly post_transaction_shares: string | null;
  readonly is_derivative: boolean;
}

function aggregateInsiderHoldersForSunburst(
  insiders: InsiderTransactionsList | null,
): readonly SunburstHolder[] {
  if (insiders === null) return [];
  const latestByFiler = new Map<
    string,
    { txn_date: string; shares: number; label: string }
  >();
  for (const row of insiders.rows as readonly InsiderRowShape[]) {
    if (!isInsiderHoldingRow(row)) continue;
    // Predicate above guarantees parseShareCount is non-null.
    const shares = parseShareCount(row.post_transaction_shares)!;
    const key = row.filer_cik ?? `name:${row.filer_name}`;
    const existing = latestByFiler.get(key);
    if (existing === undefined || row.txn_date > existing.txn_date) {
      latestByFiler.set(key, {
        txn_date: row.txn_date,
        shares,
        label: row.filer_name,
      });
    }
  }
  const holders: SunburstHolder[] = [];
  for (const [key, entry] of latestByFiler.entries()) {
    holders.push({
      key,
      label: entry.label,
      shares: entry.shares,
      category: "insiders",
    });
  }
  return holders;
}

function renderBody(
  data: OwnershipData,
  onWedgeClick: (target: WedgeClick) => void,
): JSX.Element {
  if (data.outstanding === null || data.outstanding <= 0) {
    return (
      <EmptyState
        title="No ownership data"
        description="Shares outstanding is not on file for this instrument yet — the ownership breakdown needs SEC XBRL coverage to compute the denominator."
      />
    );
  }

  // Denominator = outstanding + treasury. Operator's mental model:
  // "100% of issued / allotted shares — some held in market, some
  // held back in vault." Treasury renders as a category wedge.
  const total_shares = data.outstanding + (data.treasury ?? 0);
  const inputs: SunburstInputs = {
    total_shares,
    holders: [
      ...data.institutional_holders,
      ...data.etf_holders,
      ...data.insider_holders,
    ],
    institutions_total: data.institutions_total,
    etfs_total: data.etfs_total,
    insiders_total: data.insiders_total,
    treasury_shares: data.treasury,
    institutions_as_of: data.thirteen_f_as_of,
    etfs_as_of: data.thirteen_f_as_of,
    insiders_as_of: data.insiders_as_of,
    treasury_as_of: data.treasury_as_of,
  };
  const rings = buildSunburstRings(inputs);
  if (rings === null) {
    return (
      <EmptyState
        title="No ownership data"
        description="Sunburst rings could not be derived — shares outstanding resolved to zero or the input snapshot is malformed."
      />
    );
  }

  const denom = rings.total_shares;
  const accountedFor = rings.categories.reduce((s, c) => s + c.shares, 0);
  const accountedPct = accountedFor / denom;
  const oversubscribed = rings.total_shares > rings.reported_total;

  return (
    <div className="flex flex-col gap-4 lg:flex-row lg:items-start">
      <div className="flex flex-col items-center gap-3">
        <OwnershipSunburst inputs={inputs} onWedgeClick={onWedgeClick} />
        <OwnershipLegend rings={rings} />
      </div>
      <div className="min-w-0 flex-1">
        <div className="mb-2">
          <OwnershipFreshnessChips rings={rings} today={new Date()} />
        </div>
        <p className="mb-1 text-xs text-slate-500 dark:text-slate-400">
          {formatShares(data.outstanding)} outstanding
          {data.treasury !== null && data.treasury > 0 && (
            <> + {formatShares(data.treasury)} treasury</>
          )}
          .
        </p>
        <p className="mb-2 text-xs">
          <span className="font-medium text-slate-700 dark:text-slate-200">
            {formatPct(accountedPct)} accounted for
          </span>
          {accountedPct < 0.999 && (
            <span className="ml-1.5 text-slate-500 dark:text-slate-400">
              · remainder is unallocated public float (gated on
              {" "}
              <span className="font-mono">#740</span> CUSIP backfill +{" "}
              <span className="font-mono">#735</span> DEI projection).
            </span>
          )}
          {oversubscribed && (
            <span className="ml-1.5 text-amber-700 dark:text-amber-400">
              · category totals exceed reported total shares by{" "}
              {formatShares(rings.total_shares - rings.reported_total)} (snapshot lag).
            </span>
          )}
        </p>
        <table className="w-full text-sm">
          <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
            <tr>
              <th className="pb-1 text-left">Category</th>
              <th className="pb-1 text-right">Shares</th>
              <th className="pb-1 text-right">% of total</th>
              <th className="pb-1 text-right">Resolved filers</th>
            </tr>
          </thead>
          <tbody>
            {rings.categories.map((cat) => {
              const resolvedPct =
                cat.shares > 0 ? cat.resolved_leaf_shares / cat.shares : 0;
              return (
                <tr
                  key={cat.key}
                  className="border-t border-t-slate-100 dark:border-t-slate-800"
                >
                  <td className="py-1.5 text-slate-700 dark:text-slate-200">
                    {cat.label}
                  </td>
                  <td className="py-1.5 text-right font-mono text-slate-700 dark:text-slate-200">
                    {formatShares(cat.shares)}
                  </td>
                  <td className="py-1.5 text-right font-mono text-slate-900 dark:text-slate-100">
                    {formatPct(cat.shares / denom)}
                  </td>
                  <td className="py-1.5 text-right font-mono text-slate-500 dark:text-slate-400">
                    {cat.leaves.length === 0 && cat.shares > 0
                      ? "—"
                      : formatPct(resolvedPct)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        <p className="mt-2 text-xs text-slate-400 dark:text-slate-500">
          Click any colored wedge for the per-filer drilldown.
        </p>
      </div>
    </div>
  );
}
