/**
 * Ownership L2 drill page (#729).
 *
 * Mirrors the L1 ``OwnershipPanel`` data model — same denominator
 * (``shares_outstanding``), same faithful-proportional rings, same
 * transparent gap arcs — at a larger size with a per-filer drilldown
 * table and three operator-side controls:
 *
 *   * ``?category=etf|institutions|insiders|treasury`` — filter the
 *     table to that category. Set by L1 / L2 middle-ring clicks.
 *   * ``?filer=<cik|name-fallback>`` — scroll to + highlight a
 *     specific filer row. Set by L1 / L2 outer-ring clicks.
 *   * ``?view=raw`` — emit the table as a downloadable CSV.
 */

import { useCallback, useMemo, useRef } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { fetchBlockholders } from "@/api/blockholders";
import type { BlockholdersResponse } from "@/api/blockholders";
import { fetchInstitutionalHoldings } from "@/api/institutionalHoldings";
import type {
  InstitutionalFilerHolding,
  InstitutionalHoldingsResponse,
} from "@/api/institutionalHoldings";
import {
  fetchInsiderBaseline,
  fetchInsiderTransactions,
  fetchInstrumentFinancials,
} from "@/api/instruments";
import type {
  InsiderBaselineList,
  InsiderTransactionsList,
} from "@/api/instruments";
import type { InstrumentFinancials } from "@/api/types";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { OwnershipFreshnessChips } from "@/components/instrument/OwnershipFreshnessChips";
import {
  type InsiderRowShape,
  isBaselineHoldingRow,
  isInsiderHoldingRow,
} from "@/components/instrument/ownershipInsiders";
import {
  OwnershipLegend,
  OwnershipSunburst,
} from "@/components/instrument/OwnershipSunburst";
import type { WedgeClick } from "@/components/instrument/OwnershipSunburst";
import {
  formatPct,
  formatShares,
  parseShareCount,
} from "@/components/instrument/ownershipMetrics";
import {
  type CategoryKey,
  type SunburstHolder,
  type SunburstInputs,
  buildSunburstRings,
} from "@/components/instrument/ownershipRings";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

export interface FilerRow {
  readonly key: string;
  readonly label: string;
  readonly category: CategoryKey;
  readonly category_label: string;
  readonly shares: number;
  readonly value_usd: number | null;
  readonly voting: string | null;
  readonly is_put_call: string | null;
  readonly accession: string | null;
  readonly period_of_report: string | null;
}

const CATEGORY_LABELS: Record<CategoryKey, string> = {
  institutions: "Institutions",
  etfs: "ETFs",
  insiders: "Insiders",
  treasury: "Treasury",
  blockholders: "Blockholders",
};

export function OwnershipPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams, setSearchParams] = useSearchParams();

  const categoryFilter = searchParams.get("category");
  const filerFilter = searchParams.get("filer");
  const viewMode = searchParams.get("view");

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
    useCallback(() => fetchInsiderTransactions(symbol, 500), [symbol]),
    [symbol],
  );

  const baselineState = useAsync<InsiderBaselineList>(
    useCallback(() => fetchInsiderBaseline(symbol), [symbol]),
    [symbol],
  );

  const blockholdersState = useAsync<BlockholdersResponse>(
    useCallback(() => fetchBlockholders(symbol, 500), [symbol]),
    [symbol],
  );

  const isLoading =
    balanceState.loading ||
    institutionalState.loading ||
    insidersState.loading ||
    baselineState.loading ||
    blockholdersState.loading;
  const allErrored =
    balanceState.error !== null &&
    institutionalState.error !== null &&
    insidersState.error !== null &&
    baselineState.error !== null &&
    blockholdersState.error !== null;

  const handleWedgeClick = useCallback(
    (target: WedgeClick) => {
      const next = new URLSearchParams(searchParams);
      if (target.kind === "category") {
        next.set("category", target.category_key);
        next.delete("filer");
      } else if (target.kind === "leaf") {
        next.set("category", target.category_key);
        next.set("filer", target.leaf_key);
      } else {
        next.delete("category");
        next.delete("filer");
      }
      setSearchParams(next, { replace: false });
    },
    [searchParams, setSearchParams],
  );

  const clearFilters = useCallback(() => {
    const next = new URLSearchParams(searchParams);
    next.delete("category");
    next.delete("filer");
    setSearchParams(next, { replace: false });
  }, [searchParams, setSearchParams]);

  const clearFiler = useCallback(() => {
    const next = new URLSearchParams(searchParams);
    next.delete("filer");
    setSearchParams(next, { replace: false });
  }, [searchParams, setSearchParams]);

  const backHref = `/instrument/${encodeURIComponent(symbol)}`;

  return (
    <div className="mx-auto max-w-screen-2xl space-y-4 p-4">
      <header className="border-b border-slate-200 pb-3 dark:border-slate-800">
        <Link to={backHref} className="text-xs text-sky-700 hover:underline">
          ← Back to {symbol}
        </Link>
        <h1 className="mt-1 text-lg font-semibold text-slate-900 dark:text-slate-100">
          Ownership — {symbol}
        </h1>
        <p className="mt-1 text-xs text-slate-500 dark:text-slate-400">
          Three-ring breakdown of shares outstanding by category, filer, and
          officer. SEC 13F-HR institutional + ETF holdings, Form 4 insider
          transactions, XBRL treasury share counts.
        </p>
      </header>

      {isLoading ? (
        <SectionSkeleton rows={8} />
      ) : allErrored ? (
        <SectionError
          onRetry={() => {
            balanceState.refetch();
            institutionalState.refetch();
            insidersState.refetch();
            baselineState.refetch();
            blockholdersState.refetch();
          }}
        />
      ) : (
        <OwnershipBody
          symbol={symbol}
          balance={balanceState.data}
          institutional={institutionalState.data}
          insiders={insidersState.data}
          baseline={baselineState.data}
          blockholders={blockholdersState.data}
          categoryFilter={categoryFilter}
          filerFilter={filerFilter}
          viewMode={viewMode}
          onWedgeClick={handleWedgeClick}
          onClearFilters={clearFilters}
          onClearFiler={clearFiler}
        />
      )}
    </div>
  );
}

interface OwnershipBodyProps {
  readonly symbol: string;
  readonly balance: InstrumentFinancials | null;
  readonly institutional: InstitutionalHoldingsResponse | null;
  readonly insiders: InsiderTransactionsList | null;
  readonly baseline: InsiderBaselineList | null;
  readonly blockholders: BlockholdersResponse | null;
  readonly categoryFilter: string | null;
  readonly filerFilter: string | null;
  readonly viewMode: string | null;
  readonly onWedgeClick: (target: WedgeClick) => void;
  readonly onClearFilters: () => void;
  /** Clear only the per-filer filter; keeps the category filter
   *  in place so the operator stays in the same drilldown view. */
  readonly onClearFiler: () => void;
}

function OwnershipBody({
  symbol,
  balance,
  institutional,
  insiders,
  baseline,
  blockholders,
  categoryFilter,
  filerFilter,
  viewMode,
  onWedgeClick,
  onClearFilters,
  onClearFiler,
}: OwnershipBodyProps): JSX.Element {
  const outstanding = balance !== null ? pickLatestBalance(balance, "shares_outstanding") : null;
  const treasury = balance !== null ? pickLatestBalance(balance, "treasury_shares") : null;

  if (outstanding === null || outstanding <= 0) {
    return (
      <EmptyState
        title="No ownership data"
        description={`Shares outstanding is not on file for ${symbol} yet — the ownership breakdown needs SEC XBRL coverage to compute the denominator.`}
      />
    );
  }

  const inst_totals = institutional?.totals ?? null;
  const filers = institutional?.filers ?? [];

  // Memoise every derived array so downstream useMemo deps land on
  // a stable identity. Pre-fix ``inputs.holders`` was a fresh array
  // literal on every render → ``allRows``'s useMemo never hit
  // cache and ``buildFilerRows`` re-ran on every keystroke into the
  // search params.
  const equity_filers = useMemo(
    () => filers.filter((f) => f.is_put_call === null),
    [filers],
  );
  const institutional_holders = useMemo<readonly SunburstHolder[]>(
    () =>
      equity_filers
        .filter((f) => f.filer_type !== "ETF")
        .map(filerToHolder("institutions")),
    [equity_filers],
  );
  const etf_holders = useMemo<readonly SunburstHolder[]>(
    () =>
      equity_filers
        .filter((f) => f.filer_type === "ETF")
        .map(filerToHolder("etfs")),
    [equity_filers],
  );
  const form4_insider_holders = useMemo(
    () => aggregateInsiderHoldersForSunburst(insiders),
    [insiders],
  );
  // Form-3-only filers (#768 PR4) — officers who hold a baseline
  // grant but never traded after appointment. Backend NOT EXISTS
  // gate guarantees no overlap with form4_insider_holders.
  const baseline_insider_holders = useMemo<readonly SunburstHolder[]>(
    () => baselineToInsiderHolders(baseline),
    [baseline],
  );
  const insider_holders = useMemo<readonly SunburstHolder[]>(
    () => [...form4_insider_holders, ...baseline_insider_holders],
    [form4_insider_holders, baseline_insider_holders],
  );
  const blockholder_holders = useMemo<readonly SunburstHolder[]>(
    () => blockholdersToHolders(blockholders),
    [blockholders],
  );
  const allHolders = useMemo<readonly SunburstHolder[]>(
    () => [
      ...institutional_holders,
      ...etf_holders,
      ...insider_holders,
      ...blockholder_holders,
    ],
    [institutional_holders, etf_holders, insider_holders, blockholder_holders],
  );

  const institutions_total = useMemo(
    () => parseShareCount(inst_totals?.institutions_shares ?? null),
    [inst_totals?.institutions_shares],
  );
  const etfs_total = useMemo(
    () => parseShareCount(inst_totals?.etfs_shares ?? null),
    [inst_totals?.etfs_shares],
  );
  const insiders_total = useMemo(
    () =>
      insider_holders.length === 0
        ? null
        : insider_holders.reduce((s, h) => s + h.shares, 0),
    [insider_holders],
  );

  // Denominator = ``shares_outstanding`` (XBRL DEI) only. Treasury is
  // an additive category wedge on top of the chart, NOT part of the
  // denominator. Codex audit 2026-05-03 caught the prior
  // ``outstanding + treasury`` math as a ship-blocker — treasury > 0
  // wedged every other category down by the treasury fraction. Spec:
  // docs/superpowers/specs/2026-05-03-ownership-tier0-and-cik-history-design.md
  const total_shares = useMemo(() => outstanding, [outstanding]);

  // Per-category freshness sources (#767).
  const thirteen_f_as_of = inst_totals?.period_of_report ?? null;
  // Stable ``today`` reference for the freshness chip strip — pre-fix
  // ``new Date()`` inline forced the chip to re-render on every parent
  // re-render even when its rings data was identical.
  const today = useMemo(() => new Date(), []);

  const insiders_as_of = useMemo(() => {
    // Insider freshness factors in BOTH sources: latest Form 4
    // txn_date AND latest Form 3 baseline as_of_date (#768 PR4).
    // An issuer where every observed insider holds via a Form 3
    // grant and never trades has no Form 4 rows but should still
    // surface the latest baseline date as the chip's "as of".
    let latest_form4: string | null = null;
    if (insiders !== null && insiders.rows.length > 0) {
      // Same eligibility predicate as the holders aggregator below
      // and ``buildFilerRows`` — Codex (review of #767) caught that
      // a divergent predicate would advance the freshness chip
      // ahead of the actual snapshot the ring renders.
      for (const row of insiders.rows as readonly InsiderRowShape[]) {
        if (!isInsiderHoldingRow(row)) continue;
        if (latest_form4 === null || row.txn_date > latest_form4) {
          latest_form4 = row.txn_date;
        }
      }
    }
    let latest_baseline: string | null = null;
    if (baseline !== null && baseline.rows.length > 0) {
      // Same eligibility predicate as ``baselineToInsiderHolders``
      // and ``buildFilerRows`` — without the guard a null/zero-
      // shares row would advance the chip past a wedge that never
      // renders (Codex / bot review of #768 PR4).
      for (const row of baseline.rows) {
        if (!isBaselineHoldingRow(row)) continue;
        if (latest_baseline === null || row.as_of_date > latest_baseline) {
          latest_baseline = row.as_of_date;
        }
      }
    }
    if (latest_form4 === null) return latest_baseline;
    if (latest_baseline === null) return latest_form4;
    return latest_form4 >= latest_baseline ? latest_form4 : latest_baseline;
  }, [insiders, baseline]);
  const treasury_as_of = useMemo(() => {
    if (balance === null || treasury === null) return null;
    for (const row of balance.rows) {
      const raw = row.values["treasury_shares"];
      const parsed = parseShareCount(raw ?? null);
      if (parsed !== null) return row.period_end;
    }
    return null;
  }, [balance, treasury]);

  const blockholders_total = useMemo(
    () => parseShareCount(blockholders?.totals?.blockholders_shares ?? null),
    [blockholders?.totals?.blockholders_shares],
  );
  const blockholders_as_of = useMemo(
    () => blockholders?.totals?.as_of_date ?? null,
    [blockholders?.totals?.as_of_date],
  );

  const inputs: SunburstInputs = useMemo(
    () => ({
      total_shares,
      holders: allHolders,
      institutions_total,
      etfs_total,
      insiders_total,
      blockholders_total,
      treasury_shares: treasury,
      institutions_as_of: thirteen_f_as_of,
      etfs_as_of: thirteen_f_as_of,
      insiders_as_of,
      treasury_as_of,
      blockholders_as_of,
    }),
    [
      total_shares,
      allHolders,
      institutions_total,
      etfs_total,
      insiders_total,
      blockholders_total,
      treasury,
      thirteen_f_as_of,
      insiders_as_of,
      treasury_as_of,
      blockholders_as_of,
    ],
  );

  const rings = useMemo(() => buildSunburstRings(inputs), [inputs]);

  const allRows = useMemo(
    () => buildFilerRows(filers, insiders, baseline, treasury, blockholders),
    [filers, insiders, baseline, treasury, blockholders],
  );

  const filteredRows = useMemo(() => {
    if (categoryFilter === null) return allRows;
    return allRows.filter((r) => r.category === categoryFilter);
  }, [allRows, categoryFilter]);

  const accountedFor = rings?.categories.reduce((s, c) => s + c.shares, 0) ?? 0;
  const accountedPct =
    rings !== null && rings.total_shares > 0
      ? accountedFor / rings.total_shares
      : 0;
  const oversubscribed = rings !== null && rings.total_shares > rings.reported_total;

  const filerRowRef = useRef<HTMLTableRowElement | null>(null);

  if (viewMode === "raw") {
    const csv = buildCsv(filteredRows);
    return (
      <div className="space-y-3">
        <p className="text-xs text-slate-500 dark:text-slate-400">
          Raw view: {filteredRows.length} rows
          {categoryFilter !== null && (
            <> · filtered to <strong>{labelFor(categoryFilter)}</strong></>
          )}
        </p>
        <pre className="overflow-x-auto rounded border border-slate-200 bg-slate-50 p-3 text-xs dark:border-slate-700 dark:bg-slate-950">
{csv}
        </pre>
        <a
          href={`data:text/csv;charset=utf-8,${encodeURIComponent(csv)}`}
          download={`${symbol}-ownership.csv`}
          className="inline-block rounded border border-slate-300 px-3 py-1.5 text-xs hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-800"
        >
          Download CSV
        </a>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-12 gap-6">
      <div className="col-span-12 lg:col-span-5">
        <div className="flex flex-col items-center gap-3">
          <OwnershipSunburst inputs={inputs} onWedgeClick={onWedgeClick} size={420} />
          {rings !== null && <OwnershipLegend rings={rings} />}
          {rings !== null && <OwnershipFreshnessChips rings={rings} today={today} />}
        </div>
        <p className="mt-3 text-center text-xs text-slate-500 dark:text-slate-400">
          {formatShares(outstanding)} outstanding
          {treasury !== null && treasury > 0 && (
            <> + {formatShares(treasury)} treasury</>
          )}
          {" = "}
          {formatShares(total_shares)} total shares
        </p>
        <p className="mt-1 text-center text-xs">
          <span className="font-medium text-slate-700 dark:text-slate-200">
            {formatPct(accountedPct)} accounted for
          </span>
          {accountedPct < 0.999 && (
            <span className="ml-1.5 text-slate-500 dark:text-slate-400">
              · remainder is unallocated public float
            </span>
          )}
          {oversubscribed && rings !== null && (
            <span className="ml-1.5 text-amber-700 dark:text-amber-400">
              · category totals exceed reported total shares by{" "}
              {formatShares(rings.total_shares - rings.reported_total)} (snapshot lag)
            </span>
          )}
        </p>
      </div>
      <div className="col-span-12 lg:col-span-7">
        <FilterStrip
          categoryFilter={categoryFilter}
          filerFilter={filerFilter}
          rowCount={filteredRows.length}
          totalCount={allRows.length}
          onClear={onClearFilters}
        />
        <FilerTable
          rows={filteredRows}
          highlightFiler={filerFilter}
          highlightRef={filerRowRef}
          onClearHighlight={onClearFiler}
        />
      </div>
    </div>
  );
}

interface FilterStripProps {
  readonly categoryFilter: string | null;
  readonly filerFilter: string | null;
  readonly rowCount: number;
  readonly totalCount: number;
  readonly onClear: () => void;
}

function FilterStrip({
  categoryFilter,
  filerFilter,
  rowCount,
  totalCount,
  onClear,
}: FilterStripProps): JSX.Element | null {
  if (categoryFilter === null && filerFilter === null) {
    return (
      <p className="mb-2 text-xs text-slate-500 dark:text-slate-400">
        Showing all {totalCount} filer rows. Click any colored wedge in the chart to filter.
      </p>
    );
  }
  return (
    <div className="mb-2 flex items-baseline justify-between text-xs">
      <p className="text-slate-600 dark:text-slate-300">
        Showing {rowCount} of {totalCount}
        {categoryFilter !== null && (
          <>
            {" "}· category <strong>{labelFor(categoryFilter)}</strong>
          </>
        )}
        {filerFilter !== null && (
          <>
            {" "}· filer <strong>{filerFilter}</strong>
          </>
        )}
      </p>
      <button
        type="button"
        onClick={onClear}
        className="rounded border border-slate-300 px-2 py-0.5 text-xs hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-800"
      >
        Clear filters
      </button>
    </div>
  );
}

interface FilerTableProps {
  readonly rows: readonly FilerRow[];
  readonly highlightFiler: string | null;
  readonly highlightRef: React.RefObject<HTMLTableRowElement>;
  readonly onClearHighlight?: () => void;
}

function FilerTable({
  rows,
  highlightFiler,
  highlightRef,
  onClearHighlight,
}: FilerTableProps): JSX.Element {
  if (rows.length === 0) {
    return (
      <EmptyState
        title="No filers match this filter"
        description="Try clearing the filter or clicking a different wedge."
      />
    );
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          <tr>
            <th className="pb-1 text-left">Filer</th>
            <th className="pb-1 text-left">Category</th>
            <th className="pb-1 text-right">Shares</th>
            <th className="pb-1 text-right">Value (USD)</th>
            <th className="pb-1 text-left">Voting</th>
            <th className="pb-1 text-left">P/C</th>
            <th className="pb-1 text-left">Period</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => {
            const isHighlight = highlightFiler !== null && row.key === highlightFiler;
            // Logical-side ``border-t-*`` instead of all-sides shorthand
            // so an isHighlight row's ``border-l-sky-500`` can't be
            // silently overridden if Tailwind emits the shorthand color
            // rule after the side-specific one.
            const baseCls = "border-t border-t-slate-100 dark:border-t-slate-800";
            const highlightCls = isHighlight
              ? "border-l-2 border-l-sky-500 bg-sky-50/40 dark:bg-sky-950/20 cursor-pointer"
              : "";
            return (
              <tr
                key={`${row.category}-${row.key}`}
                ref={isHighlight ? highlightRef : null}
                className={`${baseCls} ${highlightCls}`}
                onClick={isHighlight ? onClearHighlight : undefined}
                title={isHighlight ? "Click to clear the per-filer filter" : undefined}
              >
                <td className="py-1.5 text-slate-700 dark:text-slate-200">{row.label}</td>
                <td className="py-1.5 text-slate-500 dark:text-slate-400">
                  {row.category_label}
                </td>
                <td className="py-1.5 text-right font-mono text-slate-700 dark:text-slate-200">
                  {formatShares(row.shares)}
                </td>
                <td className="py-1.5 text-right font-mono text-slate-700 dark:text-slate-200">
                  {row.value_usd === null ? "—" : formatShares(Math.round(row.value_usd))}
                </td>
                <td className="py-1.5 text-slate-500 dark:text-slate-400">
                  {row.voting ?? "—"}
                </td>
                <td className="py-1.5 text-slate-500 dark:text-slate-400">
                  {row.is_put_call ?? "—"}
                </td>
                <td className="py-1.5 text-slate-500 dark:text-slate-400">
                  {row.period_of_report ?? "—"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function labelFor(key: string): string {
  return (CATEGORY_LABELS as Record<string, string>)[key] ?? key;
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
    // Predicate guarantees the share count is parseable.
    const shares = parseShareCount(row.post_transaction_shares)!;
    const key = row.filer_cik ?? `name:${row.filer_name}`;
    const existing = latestByFiler.get(key);
    if (existing === undefined || row.txn_date > existing.txn_date) {
      latestByFiler.set(key, { txn_date: row.txn_date, shares, label: row.filer_name });
    }
  }
  const holders: SunburstHolder[] = [];
  for (const [key, entry] of latestByFiler.entries()) {
    holders.push({ key, label: entry.label, shares: entry.shares, category: "insiders" });
  }
  return holders;
}

/** Map baseline-API rows to SunburstHolder shape (#768 PR4). Uses
 *  the shared ``isBaselineHoldingRow`` predicate so the holders set,
 *  the L2 ``buildFilerRows`` table writer, and the L2 freshness
 *  chip's ``as_of_date`` derivation cannot drift. The ``baseline:``
 *  key prefix prevents collisions with same-CIK Form 4 leaves. */
function baselineToInsiderHolders(
  baseline: InsiderBaselineList | null,
): readonly SunburstHolder[] {
  if (baseline === null || baseline.rows.length === 0) return [];
  const out: SunburstHolder[] = [];
  for (const row of baseline.rows) {
    if (!isBaselineHoldingRow(row)) continue;
    // Predicate guarantees parseShareCount > 0.
    const shares = parseShareCount(row.shares)!;
    out.push({
      key: `baseline:${row.filer_cik}:${row.is_derivative ? "d" : "n"}`,
      label: row.filer_name,
      shares,
      category: "insiders",
    });
  }
  return out;
}


/** Map blockholder API rows into SunburstHolder wedges, deduping
 *  by accession_number so joint-filing reporters collapse to one
 *  wedge per block. Mirrors the L1 implementation in
 *  ``OwnershipPanel.tsx`` so a wedge click on the L1 ring lands on
 *  the corresponding wedge on the L2 ring. Codex pre-push review
 *  caught the prior version that surfaced one wedge per per-reporter
 *  row, which double-counted joint filings via the snapshot-lag
 *  leaf-sum bump in ``buildSunburstRings``. */
function blockholdersToHolders(
  blockholders: BlockholdersResponse | null,
): readonly SunburstHolder[] {
  if (blockholders === null) return [];
  const seen_accessions = new Set<string>();
  const out: SunburstHolder[] = [];
  for (const row of blockholders.blockholders) {
    if (seen_accessions.has(row.accession_number)) continue;
    seen_accessions.add(row.accession_number);
    const shares = parseShareCount(row.aggregate_amount_owned);
    if (shares === null || shares <= 0) continue;
    const reporter_identity = row.reporter_cik ?? `name:${row.reporter_name}`;
    out.push({
      key: `block:${reporter_identity}`,
      label: row.filer_name,
      shares,
      category: "blockholders",
    });
  }
  return out;
}


function buildFilerRows(
  filers: readonly InstitutionalFilerHolding[],
  insiders: InsiderTransactionsList | null,
  baseline: InsiderBaselineList | null,
  treasury: number | null,
  blockholders: BlockholdersResponse | null,
): FilerRow[] {
  const rows: FilerRow[] = [];

  for (const f of filers) {
    const cat: CategoryKey = f.filer_type === "ETF" ? "etfs" : "institutions";
    rows.push({
      key: f.filer_cik,
      label: f.filer_name,
      category: cat,
      category_label: CATEGORY_LABELS[cat],
      shares: parseShareCount(f.shares) ?? 0,
      value_usd: parseShareCount(f.market_value_usd ?? null),
      voting: f.voting_authority,
      is_put_call: f.is_put_call,
      accession: f.accession_number,
      period_of_report: f.period_of_report,
    });
  }

  if (insiders !== null) {
    const latestByFiler = new Map<
      string,
      { row: InsiderRowShape; shares: number }
    >();
    for (const row of insiders.rows as readonly InsiderRowShape[]) {
      if (!isInsiderHoldingRow(row)) continue;
      const shares = parseShareCount(row.post_transaction_shares)!;
      const key = row.filer_cik ?? `name:${row.filer_name}`;
      const existing = latestByFiler.get(key);
      if (existing === undefined || row.txn_date > existing.row.txn_date) {
        latestByFiler.set(key, { row, shares });
      }
    }
    for (const [key, entry] of latestByFiler.entries()) {
      rows.push({
        key,
        label: entry.row.filer_name,
        category: "insiders",
        category_label: CATEGORY_LABELS.insiders,
        shares: entry.shares,
        value_usd: null,
        voting: null,
        is_put_call: null,
        accession: null,
        period_of_report: entry.row.txn_date,
      });
    }
  }

  // Form 3 baseline-only insiders (#768 PR4). Backend gate
  // guarantees no CIK overlap with the Form 4 set above; the
  // ``baseline:`` key prefix matches the SunburstHolder key so a
  // wedge click on the L1 ring lands on the right L2 row.
  if (baseline !== null) {
    for (const row of baseline.rows) {
      if (!isBaselineHoldingRow(row)) continue;
      // Predicate guarantees parseShareCount > 0.
      const shares = parseShareCount(row.shares)!;
      rows.push({
        key: `baseline:${row.filer_cik}:${row.is_derivative ? "d" : "n"}`,
        label: row.filer_name,
        category: "insiders",
        category_label: CATEGORY_LABELS.insiders,
        shares,
        value_usd: parseShareCount(row.value_owned ?? null),
        voting: null,
        is_put_call: null,
        accession: null,
        period_of_report: row.as_of_date,
      });
    }
  }

  if (treasury !== null && treasury > 0) {
    rows.push({
      key: "treasury",
      label: "Treasury (memo)",
      category: "treasury",
      category_label: CATEGORY_LABELS.treasury,
      shares: treasury,
      value_usd: null,
      voting: null,
      is_put_call: null,
      accession: null,
      period_of_report: null,
    });
  }

  // 13D / 13G blockholders (#766). The drilldown table keeps every
  // per-reporter chain row so the operator can see joint filings in
  // detail. Wedges, by contrast, are deduped per-accession in
  // ``blockholdersToHolders`` so the chart's leaf-sum matches the
  // backend totals.
  //
  // Row key matches the wedge leaf key (``block:${reporter_identity}``)
  // verbatim so a wedge click that navigates to ``?filer=block:CIK``
  // highlights the corresponding L2 row. The backend's
  // ``per_reporter_chain`` DISTINCT ON guarantees one row per
  // reporter per issuer, so reporter_identity alone is unique
  // within the table without an accession suffix. Codex pre-push
  // review caught the wedge↔table key mismatch from a prior fix.
  if (blockholders !== null) {
    for (const row of blockholders.blockholders) {
      const shares = parseShareCount(row.aggregate_amount_owned);
      if (shares === null || shares <= 0) continue;
      const reporter_identity = row.reporter_cik ?? `name:${row.reporter_name}`;
      rows.push({
        key: `block:${reporter_identity}`,
        // Label includes the joint-filing depth so the operator can
        // see at a glance which blocks are held by groups.
        label:
          row.additional_reporters > 0
            ? `${row.reporter_name} (+${row.additional_reporters} co-reporter${row.additional_reporters === 1 ? "" : "s"})`
            : row.reporter_name,
        category: "blockholders",
        category_label: CATEGORY_LABELS.blockholders,
        shares,
        value_usd: null,
        voting: row.submission_type, // surface 13D / 13G as the "voting" column
        is_put_call: null,
        accession: row.accession_number,
        period_of_report: row.date_of_event ?? null,
      });
    }
  }

  const categoryOrder: CategoryKey[] = [
    "institutions",
    "etfs",
    "blockholders",
    "insiders",
    "treasury",
  ];
  rows.sort((a, b) => {
    const ai = categoryOrder.indexOf(a.category);
    const bi = categoryOrder.indexOf(b.category);
    if (ai !== bi) return ai - bi;
    return b.shares - a.shares;
  });

  return rows;
}

export function buildCsv(rows: readonly FilerRow[]): string {
  const header = [
    "filer_key",
    "filer_label",
    "category",
    "shares",
    "value_usd",
    "voting_authority",
    "put_call",
    "accession",
    "period_of_report",
  ].join(",");
  const lines = rows.map((r) =>
    [
      csvEscape(r.key),
      csvEscape(r.label),
      csvEscape(r.category),
      r.shares.toString(),
      r.value_usd === null ? "" : r.value_usd.toString(),
      csvEscape(r.voting ?? ""),
      csvEscape(r.is_put_call ?? ""),
      csvEscape(r.accession ?? ""),
      csvEscape(r.period_of_report ?? ""),
    ].join(","),
  );
  return [header, ...lines].join("\n");
}

function csvEscape(value: string): string {
  // RFC 4180 escaping + formula-injection guard (mirrors
  // app.api.instruments).
  let v = value;
  if (v !== "" && /^[=+\-@]/.test(v)) v = `'${v}`;
  if (/[",\n]/.test(v)) {
    return `"${v.replace(/"/g, '""')}"`;
  }
  return v;
}
