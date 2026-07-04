/**
 * Ownership L2 drill page (#729, re-pointed onto the canonical
 * rollup by #1589).
 *
 * Reads the SAME single fetch as the L1 ``OwnershipPanel``
 * (``/instruments/{symbol}/ownership-rollup``, the #1233 two-layer
 * model) so the L1 wedge an operator clicks and the L2 page it lands
 * on can never disagree — denominator, per-category totals, and
 * per-filer rows all come from one server-side ``snapshot_read``
 * snapshot. The prior version composed five legacy per-source
 * fetches (13F / Form 4 / Form 3 baseline / 13D-G / XBRL balance)
 * and contradicted L1 wherever those feeds lagged the rollup
 * (GME: 0.01% vs 31.70% known coverage).
 *
 * Operator-side controls:
 *
 *   * ``?category=institutions|etfs|insiders|def14a|blockholders|treasury``
 *     — filter the table to that category. Set by L1 / L2
 *     middle-ring clicks.
 *   * ``?filer=<cik|name-fallback>`` — scroll to + highlight a
 *     specific filer row. Set by L1 / L2 outer-ring clicks.
 *   * ``?view=raw`` — server-side CSV export of the same rollup.
 */

import { useCallback, useMemo, useRef } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { fetchOwnershipRollup } from "@/api/ownership";
import type {
  OwnershipRollupResponse,
  OwnershipSliceCategory,
  OwnershipSourceTag,
} from "@/api/ownership";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { OwnershipCoverageBanner } from "@/components/instrument/OwnershipCoverageBanner";
import { DualClassDenominatorCallout } from "@/components/instrument/DualClassDenominatorCallout";
import { PerClassDenominatorCallout } from "@/components/instrument/PerClassDenominatorCallout";
import { OwnershipFreshnessChips } from "@/components/instrument/OwnershipFreshnessChips";
import { OwnershipHistoryChart } from "@/components/instrument/OwnershipHistoryChart";
import { rollupToSunburstInputs } from "@/components/instrument/OwnershipPanel";
import {
  OwnershipLegend,
  OwnershipSunburst,
  openWedgeSource,
} from "@/components/instrument/OwnershipSunburst";
import type { WedgeClick } from "@/components/instrument/OwnershipSunburst";
import {
  formatPct,
  formatShares,
  ownershipStaleDenominatorCopy,
  parseShareCount,
} from "@/components/instrument/ownershipMetrics";
import {
  type CategoryKey,
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
  /** Fraction of shares outstanding (``0.0822`` = 8.22%); null when
   *  the rollup row carried an unparseable percentage. */
  readonly pct_outstanding: number | null;
  /** Dedup-winning source tag; null only for the treasury memo row,
   *  which is XBRL-derived rather than a filing survivor. */
  readonly source: OwnershipSourceTag | null;
  /** SEC archive index URL for the winning accession. */
  readonly source_url: string | null;
  readonly as_of_date: string | null;
  /** Constituent 13F sub-CIK rows when this row is a collapsed
   *  institutional family (#1644 / #1649). Display-only breakdown —
   *  already counted once in ``shares``. Empty/absent for ordinary rows. */
  readonly family_members?: readonly FilerMemberRow[];
  /** Per-lot direct/indirect breakdown when this owner's additive Section-16
   *  lots were collapsed to one line (#1942). Display-only — already summed
   *  into ``shares``. Empty/absent for single-lot rows. */
  readonly lots?: readonly FilerLotRow[];
}

/** One sub-CIK breakdown row under a collapsed family (#1644 / #1649). */
export interface FilerMemberRow {
  readonly label: string;
  readonly shares: number;
  readonly source: OwnershipSourceTag;
  readonly source_url: string | null;
  readonly as_of_date: string | null;
}

/** One direct/indirect lot under a collapsed owner (#1942). */
export interface FilerLotRow {
  /** ``direct`` / ``indirect`` (or null when nature is absent). */
  readonly nature: string | null;
  readonly shares: number;
  readonly source: OwnershipSourceTag;
  readonly source_url: string | null;
  readonly as_of_date: string | null;
}

const CATEGORY_LABELS: Record<CategoryKey, string> = {
  institutions: "Institutions",
  etfs: "ETFs",
  insiders: "Insiders",
  // Non-additive cross-check overlay (#1659) — labelled (memo) so the filer
  // table doesn't read as an additive pie category.
  def14a: "DEF 14A (memo)",
  treasury: "Treasury",
  blockholders: "Blockholders",
};

/** Rollup slice category → chart/table category. ``def14a_unmatched``
 *  is its own ``def14a`` category (#1627 — un-folded from insiders so
 *  the L2 filer rows match the un-folded wedges 1:1); ``funds`` and
 *  ``esop`` (#961) are non-additive memo overlays and are excluded
 *  from the filer table entirely. */
const SLICE_TO_TABLE_CATEGORY: Record<
  OwnershipSliceCategory,
  CategoryKey | null
> = {
  institutions: "institutions",
  etfs: "etfs",
  insiders: "insiders",
  blockholders: "blockholders",
  def14a_unmatched: "def14a",
  funds: null,
  esop: null,
};

export function OwnershipPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams, setSearchParams] = useSearchParams();

  const categoryFilter = searchParams.get("category");
  const filerFilter = searchParams.get("filer");
  const viewMode = searchParams.get("view");

  const rollupState = useAsync<OwnershipRollupResponse>(
    useCallback(() => fetchOwnershipRollup(symbol), [symbol]),
    [symbol],
  );

  const handleWedgeClick = useCallback(
    (target: WedgeClick) => {
      // #921 split model: a per-filer leaf wedge with a known source
      // filing opens SEC EDGAR in a new tab; everything else
      // (categories, center, URL-less leaves, popup-blocked opens)
      // falls through to the in-page drill below. The rollup holders
      // carry ``winning_edgar_url``, so L2 leaves click through just
      // like L1 leaves since #1589.
      if (openWedgeSource(target)) return;
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
          officer. Cross-source deduped rollup — one winning filing per
          holder across Form 3/4 insiders, 13D/G blockholders, 13F-HR
          institutions + ETFs, DEF 14A, and XBRL treasury.
        </p>
      </header>

      {rollupState.loading ? (
        <SectionSkeleton rows={8} />
      ) : rollupState.error !== null || rollupState.data === null ? (
        // ``data === null`` is defence-in-depth only: the rollup
        // endpoint always returns at least the ``no_data`` payload
        // shape (200 OK + empty slices), so a null body would only
        // arise from a future middleware that unwraps the response.
        // Surface it as an error rather than a perma-skeleton —
        // same rationale as the L1 panel (PR 798 round 2).
        <SectionError onRetry={rollupState.refetch} />
      ) : (
        <OwnershipBody
          symbol={symbol}
          rollup={rollupState.data}
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
  readonly rollup: OwnershipRollupResponse;
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
  rollup,
  categoryFilter,
  filerFilter,
  viewMode,
  onWedgeClick,
  onClearFilters,
  onClearFiler,
}: OwnershipBodyProps): JSX.Element {
  // Same mapping the L1 panel uses — single source of truth for the
  // rollup → rings transform (denominator = shares_outstanding only;
  // treasury additive on top; def14a_unmatched is its own wedge #1627).
  const inputs = useMemo(() => rollupToSunburstInputs(rollup), [rollup]);
  const rings = useMemo(
    () => (inputs === null ? null : buildSunburstRings(inputs)),
    [inputs],
  );
  const allRows = useMemo(() => rollupToFilerRows(rollup), [rollup]);
  const filteredRows = useMemo(() => {
    if (categoryFilter === null) return allRows;
    return allRows.filter((r) => r.category === categoryFilter);
  }, [allRows, categoryFilter]);

  // Stable ``today`` reference for the freshness chip strip — an
  // inline ``new Date()`` would force the chips to re-render on
  // every parent re-render even when the rings are identical.
  const today = useMemo(() => new Date(), []);

  const filerRowRef = useRef<HTMLTableRowElement | null>(null);

  const outstanding = parseShareCount(rollup.shares_outstanding);
  const treasury = parseShareCount(rollup.treasury_shares);

  if (rollup.banner.state === "no_data" || inputs === null || rings === null) {
    return (
      <div className="space-y-3">
        <OwnershipCoverageBanner banner={rollup.banner} />
        <EmptyState
          title="No ownership data"
          description={
            ownershipStaleDenominatorCopy(
              rollup.banner.state,
              rollup.shares_outstanding_as_of,
            ) ??
            `Shares outstanding is not on file for ${symbol} yet — the ownership breakdown needs SEC XBRL coverage to compute the denominator. Trigger a fundamentals sync, or wait for the next scheduled run.`
          }
        />
      </div>
    );
  }

  if (viewMode === "raw") {
    // Server-side CSV export (Chain 2.8 of #788). The CSV is built
    // from the same canonical deduped rollup this page renders, so
    // the operator's spreadsheet matches the L1/L2 chart 1:1.
    //
    // ``categoryFilter`` flows through to the backend's ``?category=``
    // filter so a drilled view (e.g. ``?category=institutions&view=raw``)
    // exports only that slice. Codex pre-push review (Chain 2.8
    // follow-up) caught the regression when the filter wasn't
    // forwarded.
    // The chart category key in ``?category=`` matches the backend's
    // CSV slice-filter param for every category EXCEPT ``def14a``,
    // whose backend slice category is ``def14a_unmatched`` (#1627). Map
    // it so a drilled DEF 14A export hits the right slice instead of
    // 400ing on an unknown category.
    const exportCategory =
      categoryFilter === "def14a" ? "def14a_unmatched" : categoryFilter;
    const exportPath = `/api/instruments/${encodeURIComponent(symbol)}/ownership-rollup/export.csv`;
    const exportHref =
      exportCategory !== null
        ? `${exportPath}?category=${encodeURIComponent(exportCategory)}`
        : exportPath;
    return (
      <div className="space-y-3">
        <p className="text-xs text-slate-500 dark:text-slate-400">
          Canonical deduped CSV — one row per surviving holder across
          insiders, blockholders, institutions, ETFs, plus treasury
          memo + residual rows. Server-built from the same dedup
          priority (form4 &gt; form3 &gt; 13D/G &gt; def14a &gt; 13f)
          the L1/L2 chart uses.
          {categoryFilter !== null && (
            <> · scoped to <strong>{labelFor(categoryFilter)}</strong></>
          )}
        </p>
        <a
          href={exportHref}
          // Explicit filename — bare ``download`` collapses to the
          // URL's last path segment (``export.csv``) for every symbol
          // in browsers that don't honor the server's
          // ``Content-Disposition`` header. Match the backend's
          // ``${symbol}_ownership_rollup.csv`` pattern. Claude PR
          // review (#835 round 1) flagged the regression.
          download={`${symbol}_ownership_rollup.csv`}
          className="inline-block rounded border border-slate-300 px-3 py-1.5 text-xs hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-800"
        >
          Download CSV
        </a>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <OwnershipCoverageBanner banner={rollup.banner} />
      <div className="grid grid-cols-12 gap-6">
        <div className="col-span-12 lg:col-span-5">
          <div className="flex flex-col items-center gap-3">
            <OwnershipSunburst inputs={inputs} onWedgeClick={onWedgeClick} size={420} />
            <OwnershipLegend rings={rings} />
            <OwnershipFreshnessChips rings={rings} today={today} />
          </div>
          <p className="mt-3 text-center text-xs text-slate-500 dark:text-slate-400">
            {formatShares(outstanding ?? 0)} outstanding
            {treasury !== null && treasury > 0 && (
              <> + {formatShares(treasury)} treasury (memo)</>
            )}
            {rollup.shares_outstanding_as_of !== null && (
              <> · as of {rollup.shares_outstanding_as_of}</>
            )}
            {rollup.shares_outstanding_source.edgar_url !== null && (
              <>
                {" · "}
                <a
                  className="underline decoration-dotted hover:text-slate-700 dark:hover:text-slate-300"
                  href={rollup.shares_outstanding_source.edgar_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  data-test="shares-outstanding-source"
                >
                  {rollup.shares_outstanding_source.form_type ?? "SEC filing"}
                </a>
              </>
            )}
          </p>
          <p className="mt-1 text-center text-xs">
            <span className="font-medium text-slate-700 dark:text-slate-200">
              {rollup.concentration.info_chip}
            </span>
            <span className="ml-1.5 text-slate-500 dark:text-slate-400">
              · {rollup.residual.label}:{" "}
              {formatPct(parseShareCount(rollup.residual.pct_outstanding) ?? 0)}
            </span>
            {rollup.residual.oversubscribed && (
              <span className="ml-1.5 text-amber-700 dark:text-amber-400">
                · category totals exceed shares outstanding (snapshot lag)
              </span>
            )}
          </p>
          {rollup.dual_class_denominator !== null && (
            <div className="mt-2">
              <DualClassDenominatorCallout
                note={rollup.dual_class_denominator.note}
              />
            </div>
          )}
          {rollup.per_class_denominator != null && (
            <div className="mt-2">
              <PerClassDenominatorCallout
                note={rollup.per_class_denominator.note}
              />
            </div>
          )}
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
        {/* History pane (#922) — trend companion to the pie. Reads the
            same ?category=/?filer= selection the pie + table use. */}
        <div className="col-span-12">
          <OwnershipHistoryChart
            symbol={symbol}
            categoryFilter={categoryFilter}
            filerFilter={filerFilter}
            filerLabel={
              filerFilter !== null
                ? (allRows.find(
                    (r) =>
                      r.key === filerFilter &&
                      (categoryFilter === null || r.category === categoryFilter),
                  )?.label ?? null)
                : null
            }
            outstanding={outstanding}
          />
        </div>
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
            <th className="pb-1 text-right">% outstanding</th>
            <th className="pb-1 text-left">Source</th>
            <th className="pb-1 text-left">As of</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
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
                // ``row.key`` (the bare ``filer_cik``/``name:`` drill token,
                // matched against ``?filer=`` for the highlight above) is NOT
                // unique — an insider can recur with the same CIK (and even a
                // byte-identical row) within a category (#1800, verified on GME
                // insiders). Append the array index so the React row key is
                // unique while ``row.key`` stays the clean ``?filer=`` token.
                key={`${row.category}-${row.key}-${i}`}
                ref={isHighlight ? highlightRef : null}
                className={`${baseCls} ${highlightCls}`}
                onClick={isHighlight ? onClearHighlight : undefined}
                title={isHighlight ? "Click to clear the per-filer filter" : undefined}
              >
                <td className="py-1.5 text-slate-700 dark:text-slate-200">
                  {row.family_members && row.family_members.length > 0 ? (
                    <details>
                      <summary className="cursor-pointer list-none">
                        <span className="select-none text-slate-400">▸ </span>
                        {row.label}
                        <span className="ml-1 text-xs text-slate-400">
                          ({row.family_members.length} entities)
                        </span>
                      </summary>
                      <ul className="mt-1 ml-4 space-y-0.5 border-l border-slate-200 pl-3 text-xs text-slate-500 dark:border-slate-700 dark:text-slate-400">
                        {row.family_members.map((m) => (
                          <li key={m.label} className="flex justify-between gap-4">
                            <span>{m.label}</span>
                            <span className="font-mono">{formatShares(m.shares)}</span>
                          </li>
                        ))}
                      </ul>
                    </details>
                  ) : row.lots && row.lots.length > 0 ? (
                    // Collapsed owner (#1942): one line at total beneficial
                    // ownership (Item 403); the direct/indirect lots (Form 4
                    // General Instruction 4(b)) live in the drilldown.
                    <details>
                      <summary className="cursor-pointer list-none">
                        <span className="select-none text-slate-400">▸ </span>
                        {row.label}
                        <span className="ml-1 text-xs text-slate-400">
                          ({row.lots.length} lots)
                        </span>
                      </summary>
                      <ul className="mt-1 ml-4 space-y-0.5 border-l border-slate-200 pl-3 text-xs text-slate-500 dark:border-slate-700 dark:text-slate-400">
                        {row.lots.map((l, li) => (
                          <li
                            key={`${l.nature ?? "lot"}-${l.source}-${li}`}
                            className="flex justify-between gap-4"
                          >
                            <span>
                              {l.nature ?? "—"}
                              <span className="ml-1 text-slate-400">({l.source})</span>
                            </span>
                            <span className="font-mono">{formatShares(l.shares)}</span>
                          </li>
                        ))}
                      </ul>
                    </details>
                  ) : (
                    row.label
                  )}
                </td>
                <td className="py-1.5 text-slate-500 dark:text-slate-400">
                  {row.category_label}
                </td>
                <td className="py-1.5 text-right font-mono text-slate-700 dark:text-slate-200">
                  {formatShares(row.shares)}
                </td>
                <td className="py-1.5 text-right font-mono text-slate-700 dark:text-slate-200">
                  {row.pct_outstanding === null ? "—" : formatPct(row.pct_outstanding)}
                </td>
                <td className="py-1.5 text-slate-500 dark:text-slate-400">
                  {row.source === null ? (
                    "—"
                  ) : row.source_url !== null ? (
                    <a
                      className="underline decoration-dotted hover:text-slate-700 dark:hover:text-slate-300"
                      href={row.source_url}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {row.source}
                    </a>
                  ) : (
                    row.source
                  )}
                </td>
                <td className="py-1.5 text-slate-500 dark:text-slate-400">
                  {row.as_of_date ?? "—"}
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

const _CATEGORY_ORDER: readonly CategoryKey[] = [
  "institutions",
  "etfs",
  "blockholders",
  "insiders",
  "def14a",
  "treasury",
];

/**
 * Map the rollup's per-slice ``holders`` arrays into the drilldown
 * table rows.
 *
 * Row keys MUST match the sunburst leaf keys produced by
 * ``rollupToSunburstInputs`` (``filer_cik ?? name:`` fallback) —
 * an L1/L2 wedge click navigates to ``?filer=<leaf key>`` and the
 * highlight + scroll-to behavior resolves rows by that key. The
 * share-parse skip predicate is identical for the same reason: a
 * wedge that renders must have a row, and vice versa.
 *
 * Treasury appends as a memo row (additive category on top of the
 * pie, not a filing survivor — no source/accession).
 */
export function rollupToFilerRows(
  rollup: OwnershipRollupResponse,
): FilerRow[] {
  const rows: FilerRow[] = [];

  for (const slice of rollup.slices) {
    const cat = SLICE_TO_TABLE_CATEGORY[slice.category];
    if (cat === null) continue; // funds memo overlay (#919) — non-additive
    for (const h of slice.holders) {
      const shares = parseShareCount(h.shares);
      if (shares === null || shares <= 0) continue;
      const members = (h.family_members ?? [])
        .map((m): FilerMemberRow | null => {
          const mShares = parseShareCount(m.shares);
          if (mShares === null || mShares <= 0) return null;
          return {
            label: m.filer_name,
            shares: mShares,
            source: m.source,
            source_url: m.edgar_url,
            as_of_date: m.as_of_date,
          };
        })
        .filter((m): m is FilerMemberRow => m !== null)
        .sort((a, b) => b.shares - a.shares);
      const lots = (h.lots ?? [])
        .map((l): FilerLotRow | null => {
          const lShares = parseShareCount(l.shares);
          if (lShares === null || lShares <= 0) return null;
          return {
            nature: l.ownership_nature,
            shares: lShares,
            source: l.source,
            source_url: l.edgar_url,
            as_of_date: l.as_of_date,
          };
        })
        .filter((l): l is FilerLotRow => l !== null)
        .sort((a, b) => b.shares - a.shares);
      rows.push({
        key: h.filer_cik ?? `name:${h.filer_name}`,
        label: h.filer_name,
        category: cat,
        category_label: CATEGORY_LABELS[cat],
        shares,
        pct_outstanding: parseShareCount(h.pct_outstanding),
        source: h.winning_source,
        source_url: h.winning_edgar_url,
        as_of_date: h.as_of_date,
        ...(members.length > 0 ? { family_members: members } : {}),
        ...(lots.length > 0 ? { lots } : {}),
      });
    }
  }

  const treasury = parseShareCount(rollup.treasury_shares);
  const outstanding = parseShareCount(rollup.shares_outstanding);
  if (treasury !== null && treasury > 0) {
    rows.push({
      key: "treasury",
      label: "Treasury (memo)",
      category: "treasury",
      category_label: CATEGORY_LABELS.treasury,
      shares: treasury,
      pct_outstanding:
        outstanding !== null && outstanding > 0 ? treasury / outstanding : null,
      source: null,
      source_url: null,
      as_of_date: rollup.treasury_as_of,
    });
  }

  rows.sort((a, b) => {
    const ai = _CATEGORY_ORDER.indexOf(a.category);
    const bi = _CATEGORY_ORDER.indexOf(b.category);
    if (ai !== bi) return ai - bi;
    return b.shares - a.shares;
  });

  return rows;
}
