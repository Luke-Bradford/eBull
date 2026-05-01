/**
 * Ownership reporting card (#729).
 *
 * Renders a three-ring sunburst:
 *   ring 1 — Held (free-float total in the center hole)
 *   ring 2 — Institutions / ETFs / Insiders / Treasury / Unallocated
 *   ring 3 — per-filer / per-officer wedges + "Other [Category]" tail
 *
 * Coverage gating:
 *   * No shares_outstanding on file → whole-card empty state.
 *   * Per-category data missing → wedge renders as a desaturated
 *     "coverage gap" arc rather than vanishing or rendering 0%.
 *
 * Effective slice coverage depends on:
 *   * #731 — shares_outstanding + treasury_shares from financial_periods (merged)
 *   * #730 — institutional_holdings via the new reader endpoint (merged)
 *   * #740 — CUSIP backfill so the ingester resolves holdings to instrument_ids (open)
 *
 * Click on any wedge → drill to the L2 ownership page (route lands
 * in the next PR; for now the click handler is a no-op surfaced via
 * ``onWedgeClick`` so the integration test can pin the wiring).
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
import { OwnershipSunburst } from "@/components/instrument/OwnershipSunburst";
import type { WedgeClick } from "@/components/instrument/OwnershipSunburst";
import { Pane } from "@/components/instrument/Pane";
import {
  formatPct,
  formatShares,
  parseShareCount,
} from "@/components/instrument/ownershipMetrics";
import {
  type SunburstCategoryStatus,
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
  readonly free_float: number | null;
  readonly institutional_holders: readonly SunburstHolder[];
  readonly etf_holders: readonly SunburstHolder[];
  readonly insider_holders: readonly SunburstHolder[];
  readonly institutions_status: SunburstCategoryStatus;
  readonly etfs_status: SunburstCategoryStatus;
  readonly insiders_status: SunburstCategoryStatus;
  readonly as_of_period: string | null;
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
  // institutional holdings (institutions + ETFs + per-filer rows),
  // insider transactions (insiders aggregate). Each fetch fails
  // independently — a missing input degrades the corresponding
  // category rather than the whole card.
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
      // L2 drill page lands in the next PR; stub the navigation now
      // so the integration is wired and the route handler can be
      // added without touching this component.
      const params = new URLSearchParams();
      if (target.kind === "category") params.set("category", target.category_key);
      if (target.kind === "leaf") {
        params.set("category", target.category_key);
        // Synthetic gap leaves (key ``${category}-unknown`` or
        // similar) carry no real filer identity — clicking them
        // should drill to the category, not filter to a non-
        // existent filer key. Detect via the ``-unknown`` suffix
        // emitted by ``buildSunburstRings`` for unknown-category
        // sentinel leaves.
        if (!target.leaf_key.endsWith("-unknown")) {
          params.set("filer", target.leaf_key);
        }
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

  const free_float =
    outstanding !== null ? Math.max(0, outstanding - (treasury ?? 0)) : null;

  // Institutional + ETF wedges. Split the ``filers`` list by
  // filer_type — equity-only rows feed the sunburst since option
  // exposure double-counts the underlying.
  const inst_totals = institutional?.totals ?? null;
  const filers = institutional?.filers ?? [];
  const equity_filers = filers.filter((f) => f.is_put_call === null);

  const institutional_holders: SunburstHolder[] = equity_filers
    .filter((f) => f.filer_type !== "ETF")
    .map(filerToHolder("institutions"));
  const etf_holders: SunburstHolder[] = equity_filers
    .filter((f) => f.filer_type === "ETF")
    .map(filerToHolder("etfs"));

  // Insider holders — aggregate latest non-derivative
  // post-transaction shares per officer. The list is short enough
  // (~10-30 officers) that we render every officer as their own
  // wedge; the sunburst transformer bypasses the visibility
  // threshold for this category.
  const insider_holders = aggregateInsiderHoldersForSunburst(insiders);

  const institutions_status: SunburstCategoryStatus = deriveCategoryStatus(
    institutional,
    institutional_holders,
    inst_totals?.institutions_shares,
  );
  const etfs_status: SunburstCategoryStatus = deriveCategoryStatus(
    institutional,
    etf_holders,
    inst_totals?.etfs_shares,
  );
  const insiders_status: SunburstCategoryStatus =
    insiders === null
      ? "unknown"
      : insider_holders.length > 0
        ? "ok"
        : "empty";

  const as_of_period =
    inst_totals?.period_of_report ?? balance?.rows[0]?.period_end ?? null;

  return {
    outstanding,
    treasury,
    free_float,
    institutional_holders,
    etf_holders,
    insider_holders,
    institutions_status,
    etfs_status,
    insiders_status,
    as_of_period,
  };
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
  // Latest non-derivative post-transaction-shares per officer.
  // Keyed on filer_cik when present, falls back to filer_name so a
  // filer with no CIK in the audit trail still gets distinct rows.
  const latestByFiler = new Map<
    string,
    { txn_date: string; shares: number; label: string }
  >();
  for (const row of insiders.rows as readonly InsiderRowShape[]) {
    if (row.is_derivative) continue;
    const shares = parseShareCount(row.post_transaction_shares);
    if (shares === null) continue;
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

function deriveCategoryStatus(
  institutional: InstitutionalHoldingsResponse | null,
  holders: readonly SunburstHolder[],
  raw_total: string | undefined,
): SunburstCategoryStatus {
  // No fetch result at all — likely API error or pre-coverage.
  if (institutional === null) return "unknown";
  // ``totals`` is null when no holdings on file. The card surfaces
  // this as a coverage gap so the operator sees that 13F ingest
  // hasn't run for this instrument.
  if (institutional.totals === null) return "unknown";
  // Total reported but every CUSIP unresolved (the #740 backfill
  // gap). The total may be 0 even though filers exist; render as
  // ``unknown`` rather than ``empty`` so the coverage-gap copy
  // shows.
  const total = parseShareCount(raw_total ?? "0") ?? 0;
  if (total <= 0 && holders.length === 0) return "empty";
  if (holders.length === 0 && total > 0) return "unknown";
  return "ok";
}

function renderBody(
  data: OwnershipData,
  onWedgeClick: (target: WedgeClick) => void,
): JSX.Element {
  if (data.free_float === null || data.free_float <= 0) {
    return (
      <EmptyState
        title="No ownership data"
        description="Shares outstanding is not on file for this instrument yet — the ownership breakdown needs SEC XBRL coverage to compute the float denominator."
      />
    );
  }

  const inputs: SunburstInputs = {
    free_float: data.free_float,
    holders: [
      ...data.institutional_holders,
      ...data.etf_holders,
      ...data.insider_holders,
    ],
    treasury_shares: data.treasury,
    institutions_status: data.institutions_status,
    etfs_status: data.etfs_status,
    insiders_status: data.insiders_status,
  };
  const rings = buildSunburstRings(inputs);
  if (rings === null) {
    return (
      <EmptyState
        title="No ownership data"
        description="Sunburst rings could not be derived — free float resolved to zero or the input snapshot is malformed."
      />
    );
  }

  const knownPct = rings.inner.known_pct;
  const gapPct = rings.inner.gap_pct;
  const hasMaterialGap = gapPct > 0.005; // > 0.5% counts as material to surface
  const gapReasons = collectGapReasons(rings.categories);

  return (
    <div className="flex flex-col gap-4 lg:flex-row lg:items-start">
      <div className="flex justify-center">
        <OwnershipSunburst inputs={inputs} onWedgeClick={onWedgeClick} />
      </div>
      <div className="min-w-0 flex-1">
        {data.as_of_period !== null && (
          <p className="mb-1 text-xs text-slate-500 dark:text-slate-400">
            As of {data.as_of_period}. Free float ={" "}
            {formatShares(data.free_float)} shares.
          </p>
        )}
        <p className="mb-2 text-xs">
          <span className="font-medium text-slate-700 dark:text-slate-200">
            {formatPct(knownPct)} known
          </span>
          {hasMaterialGap && (
            <>
              <span className="mx-1.5 text-slate-400">·</span>
              <span className="font-medium text-amber-700 dark:text-amber-400">
                {formatPct(gapPct)} coverage gap
              </span>
              {gapReasons.length > 0 && (
                <span className="ml-1 text-slate-500 dark:text-slate-400">
                  ({gapReasons.join(", ")})
                </span>
              )}
            </>
          )}
        </p>
        <table className="w-full text-sm">
          <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
            <tr>
              <th className="pb-1 text-left">Category</th>
              <th className="pb-1 text-right">Shares</th>
              <th className="pb-1 text-right">% of float</th>
            </tr>
          </thead>
          <tbody>
            {rings.categories.map((cat) => (
              <tr
                key={cat.key}
                className="border-t border-slate-100 dark:border-slate-800"
              >
                <td className="py-1.5 text-slate-700 dark:text-slate-200">
                  {cat.label}
                  {cat.status === "unknown" && (
                    <span className="ml-2 text-xs text-amber-600 dark:text-amber-400">
                      {unknownReasonShort(cat.unknown_reason)}
                    </span>
                  )}
                </td>
                <td className="py-1.5 text-right font-mono text-slate-700 dark:text-slate-200">
                  {cat.status === "unknown" ? "—" : formatShares(cat.shares)}
                </td>
                <td className="py-1.5 text-right font-mono text-slate-900 dark:text-slate-100">
                  {cat.status === "unknown" ? "—" : formatPct(cat.pct)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="mt-2 text-xs text-slate-400 dark:text-slate-500">
          Click any wedge for the per-filer drilldown.
        </p>
      </div>
    </div>
  );
}

function collectGapReasons(
  categories: readonly { unknown_reason?: string; status: string }[],
): string[] {
  // Dedupes across categories so a stock with both
  // institutions and ETFs gated on #740 doesn't repeat the
  // ticket. Generic 'no_data' / undefined reasons fall back to a
  // neutral label so the header parenthetical still surfaces
  // when an unknown category cannot be tied to a tracked
  // follow-up — "X% coverage gap" with no parenthetical was
  // ambiguous and dropped a real gap below the operator's
  // attention threshold.
  const reasons = new Set<string>();
  for (const cat of categories) {
    if (cat.status !== "unknown") continue;
    switch (cat.unknown_reason) {
      case "cusip_backfill":
        reasons.add("#740 CUSIP backfill");
        break;
      case "dei_projection":
        reasons.add("#735 DEI projection");
        break;
      default:
        reasons.add("data not on file");
    }
  }
  return Array.from(reasons);
}

function unknownReasonShort(reason: string | undefined): string {
  switch (reason) {
    case "cusip_backfill":
      return "needs CUSIPs (#740)";
    case "dei_projection":
      return "needs DEI tag (#735)";
    default:
      return "no data";
  }
}
