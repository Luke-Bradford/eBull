/**
 * Ownership card (#789, parent #788).
 *
 * Cross-channel deduped ownership snapshot rendered as a three-ring
 * sunburst against the canonical ``shares_outstanding`` denominator
 * (XBRL DEI). Treasury renders as an additive top wedge — NOT in the
 * denominator. Treasury-in-denominator math (the prior version's
 * ``shares_outstanding + treasury_shares``) systematically wedged
 * every other category down by the treasury fraction; codex audit
 * 2026-05-03 flagged this as a ship-blocker.
 *
 * The panel issues exactly one fetch (``fetchOwnershipRollup``) so
 * every slice, the residual, and the coverage banner all reconcile
 * against a single server-side ``snapshot_read`` snapshot. The five-
 * fetch race in the prior version is gone.
 *
 * Banner state machine (server-driven):
 *
 *   * ``no_data`` — XBRL ``shares_outstanding`` not on file. Red
 *     banner with a "trigger fundamentals sync" CTA copy.
 *   * ``unknown_universe`` — outstanding present but per-category
 *     universe estimates aren't yet seeded (Tier 0 default until
 *     #790 lands per-instrument 13F filer counts). Yellow banner
 *     with explicit "estimate not available" copy.
 *   * ``red`` / ``amber`` / ``green`` — universe coverage thresholds.
 *     Red ranks worst, then ``unknown_universe``, then amber, then
 *     green; the worst-of fold across the four tracked categories
 *     decides the banner. Codex v2 review pinned this ordering.
 *
 * Concentration (sum of slices / outstanding) ships as a separate
 * info chip — Codex v2 review caught the prior conflation that
 * would have permanently red-banned retail-heavy names.
 */

import { useCallback, useMemo } from "react";
import { useNavigate } from "react-router-dom";

import { fetchOwnershipRollup } from "@/api/ownership";
import type {
  OwnershipBannerVariant,
  OwnershipCoverageState,
  OwnershipRollupResponse,
  OwnershipSliceCategory,
} from "@/api/ownership";
import { SectionError, SectionSkeleton } from "@/components/dashboard/Section";
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
import type {
  SunburstHolder,
  SunburstInputs,
} from "@/components/instrument/ownershipRings";
import { buildSunburstRings } from "@/components/instrument/ownershipRings";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";

export interface OwnershipPanelProps {
  readonly symbol: string;
}

export function OwnershipPanel({ symbol }: OwnershipPanelProps): JSX.Element {
  const rollupState = useAsync<OwnershipRollupResponse>(
    useCallback(() => fetchOwnershipRollup(symbol), [symbol]),
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

  return (
    <Pane
      title="Ownership"
      source={{
        providers: [
          "sec_13f",
          "sec_form3",
          "sec_form4",
          "sec_13dg",
          "sec_def14a",
          "sec_xbrl",
        ],
      }}
    >
      {rollupState.loading ? (
        <SectionSkeleton rows={4} />
      ) : rollupState.error !== null || rollupState.data === null ? (
        // ``data === null`` should only be reachable as a defense-
        // in-depth path: the rollup endpoint always returns at least
        // the ``no_data`` payload shape (200 OK + empty slices), so a
        // null body would only arise from a future middleware that
        // unwraps the response. Surface it as an error rather than
        // hanging on a perma-skeleton. Claude PR review (PR 798)
        // round 2 caught the prior skeleton fallback.
        <SectionError onRetry={rollupState.refetch} />
      ) : (
        <PanelBody rollup={rollupState.data} onWedgeClick={handleWedgeClick} />
      )}
    </Pane>
  );
}

/**
 * Map the rollup response to the existing ``SunburstInputs`` shape so
 * the chart code can render unchanged. ``shares_outstanding`` is the
 * denominator (treasury-on-top model); per-slice totals come from
 * the deduped rollup; per-filer holders feed the outer ring.
 *
 * ``def14a_unmatched`` rows fold into the ``insiders`` chart category
 * (they are by definition named officers in the proxy with no Form 4
 * filing on record). The slice table keeps them as a separate
 * category for transparency, but the chart treats them as insiders so
 * the sunburst doesn't need a fifth named category and the chart-vs-
 * table totals stay reconciled. Codex pre-push review (Batch 1 of
 * #788) caught the prior version that dropped them from the chart.
 */
export function rollupToSunburstInputs(
  rollup: OwnershipRollupResponse,
): SunburstInputs | null {
  const outstanding = parseShareCount(rollup.shares_outstanding);
  if (outstanding === null || outstanding <= 0) return null;
  const sliceTotal = (cat: OwnershipSliceCategory): number | null => {
    const found = rollup.slices.find((s) => s.category === cat);
    return found === undefined ? null : parseShareCount(found.total_shares);
  };
  const sliceAsOf = (cat: OwnershipSliceCategory): string | null => {
    const found = rollup.slices.find((s) => s.category === cat);
    if (found === undefined || found.holders.length === 0) return null;
    let latest: string | null = null;
    for (const h of found.holders) {
      if (h.as_of_date === null) continue;
      if (latest === null || h.as_of_date > latest) latest = h.as_of_date;
    }
    return latest;
  };
  const flattenHolders = (
    cat: OwnershipSliceCategory,
    target: SunburstHolder["category"],
  ): SunburstHolder[] => {
    const found = rollup.slices.find((s) => s.category === cat);
    if (found === undefined) return [];
    const out: SunburstHolder[] = [];
    for (const h of found.holders) {
      const shares = parseShareCount(h.shares);
      if (shares === null || shares <= 0) continue;
      const key = h.filer_cik ?? `name:${h.filer_name}`;
      out.push({ key, label: h.filer_name, shares, category: target });
    }
    return out;
  };

  const treasury = parseShareCount(rollup.treasury_shares);

  // Insiders bucket folds in def14a_unmatched holders. Both totals
  // and as_of dates take the max across the two slices so the chart
  // category sums match the slice table's insiders + def14a_unmatched
  // rows.
  const insiders_slice_total = sliceTotal("insiders");
  const def14a_unmatched_total = sliceTotal("def14a_unmatched");
  const combined_insiders_total =
    insiders_slice_total === null && def14a_unmatched_total === null
      ? null
      : (insiders_slice_total ?? 0) + (def14a_unmatched_total ?? 0);
  // Latest as_of across {insiders, def14a_unmatched}. Sort surfaces
  // the most recent date last; ``at(-1)`` returns it; ``?? null``
  // handles the both-empty case. Claude PR review (PR 798) round 2
  // pinned this idiom over nested ternaries — extends cleanly when a
  // third source enters the fold.
  const combined_insiders_as_of =
    [sliceAsOf("insiders"), sliceAsOf("def14a_unmatched")]
      .filter((d): d is string => d !== null)
      .sort()
      .at(-1) ?? null;

  return {
    // Canonical denominator: shares_outstanding only. Treasury is
    // additive on top, NOT part of the denominator.
    total_shares: outstanding,
    holders: [
      ...flattenHolders("institutions", "institutions"),
      ...flattenHolders("etfs", "etfs"),
      ...flattenHolders("insiders", "insiders"),
      ...flattenHolders("def14a_unmatched", "insiders"),
      ...flattenHolders("blockholders", "blockholders"),
    ],
    institutions_total: sliceTotal("institutions"),
    etfs_total: sliceTotal("etfs"),
    insiders_total: combined_insiders_total,
    blockholders_total: sliceTotal("blockholders"),
    treasury_shares: treasury,
    institutions_as_of: sliceAsOf("institutions"),
    etfs_as_of: sliceAsOf("etfs"),
    insiders_as_of: combined_insiders_as_of,
    treasury_as_of: rollup.treasury_as_of,
    blockholders_as_of: sliceAsOf("blockholders"),
  };
}

const _BANNER_VARIANT_CLASS: Record<OwnershipBannerVariant, string> = {
  error:
    "border-red-200 bg-red-50 text-red-900 dark:border-red-900/60 dark:bg-red-900/20 dark:text-red-200",
  warning:
    "border-amber-200 bg-amber-50 text-amber-900 dark:border-amber-900/60 dark:bg-amber-900/20 dark:text-amber-200",
  info: "border-slate-200 bg-slate-50 text-slate-700 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-200",
  success:
    "border-emerald-200 bg-emerald-50 text-emerald-900 dark:border-emerald-900/60 dark:bg-emerald-900/20 dark:text-emerald-200",
};

interface PanelBodyProps {
  readonly rollup: OwnershipRollupResponse;
  readonly onWedgeClick: (target: WedgeClick) => void;
}

function PanelBody({ rollup, onWedgeClick }: PanelBodyProps): JSX.Element {
  const inputs = useMemo(() => rollupToSunburstInputs(rollup), [rollup]);
  if (rollup.banner.state === "no_data" || inputs === null) {
    return (
      <div className="flex flex-col gap-3">
        <Banner banner={rollup.banner} />
        <EmptyState
          title="No ownership data"
          description="XBRL shares-outstanding not yet on file for this instrument. Trigger a fundamentals sync, or wait for the next scheduled run."
        />
      </div>
    );
  }
  const rings = buildSunburstRings(inputs);
  if (rings === null) {
    return (
      <div className="flex flex-col gap-3">
        <Banner banner={rollup.banner} />
        <EmptyState
          title="No ownership data"
          description="Sunburst rings could not be derived — shares outstanding resolved to zero or the input snapshot is malformed."
        />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <Banner banner={rollup.banner} />
      <ConcentrationChip rollup={rollup} />
      {rollup.residual.oversubscribed && <OversubscribedWarning />}
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start">
        <div className="flex flex-col items-center gap-3">
          <OwnershipSunburst inputs={inputs} onWedgeClick={onWedgeClick} />
          <OwnershipLegend rings={rings} />
        </div>
        <div className="min-w-0 flex-1">
          <p className="mb-1 text-xs text-slate-500 dark:text-slate-400">
            {rollup.shares_outstanding !== null
              ? `${formatShares(parseShareCount(rollup.shares_outstanding) ?? 0)} outstanding`
              : "outstanding unknown"}
            {rollup.treasury_shares !== null
              && parseShareCount(rollup.treasury_shares) !== null
              && (parseShareCount(rollup.treasury_shares) ?? 0) > 0 && (
                <> + {formatShares(parseShareCount(rollup.treasury_shares) ?? 0)} treasury</>
              )}
            {rollup.shares_outstanding_as_of !== null && (
              <> · as of {rollup.shares_outstanding_as_of}</>
            )}
            {rollup.shares_outstanding_source.accession_number !== null && (
              <>
                {" · "}
                <a
                  className="underline decoration-dotted hover:text-slate-700 dark:hover:text-slate-300"
                  href={`https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum=${encodeURIComponent(rollup.shares_outstanding_source.accession_number)}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  data-test="shares-outstanding-source"
                >
                  {rollup.shares_outstanding_source.form_type ?? "SEC filing"}
                </a>
              </>
            )}
            .
          </p>
          <SliceTable rollup={rollup} />
          <ResidualLine rollup={rollup} />
          <p className="mt-2 text-xs text-slate-400 dark:text-slate-500">
            Click any colored wedge for the per-filer drilldown.
          </p>
        </div>
      </div>
    </div>
  );
}

interface BannerProps {
  readonly banner: OwnershipRollupResponse["banner"];
}

function Banner({ banner }: BannerProps): JSX.Element | null {
  if (banner.state === "green") {
    return (
      <div
        className={`rounded-md border px-3 py-2 text-xs ${_BANNER_VARIANT_CLASS.success}`}
        role="status"
        data-banner-state={banner.state}
      >
        <span className="font-medium">{banner.headline}</span>
        <span className="ml-1.5">{banner.body}</span>
      </div>
    );
  }
  return (
    <div
      className={`rounded-md border px-3 py-2 text-xs ${_BANNER_VARIANT_CLASS[banner.variant]}`}
      role="status"
      data-banner-state={banner.state}
    >
      <p className="font-medium">{banner.headline}</p>
      <p className="mt-0.5">{banner.body}</p>
    </div>
  );
}

interface ConcentrationChipProps {
  readonly rollup: OwnershipRollupResponse;
}

function ConcentrationChip({ rollup }: ConcentrationChipProps): JSX.Element {
  return (
    <p className="text-xs text-slate-500 dark:text-slate-400" data-test="concentration-chip">
      {rollup.concentration.info_chip}
    </p>
  );
}

function OversubscribedWarning(): JSX.Element {
  // The server clamped the residual to 0; surface the diagnostic so
  // the operator knows a stale 13F + fresh Form 4/13D mix is in play.
  return (
    <div
      className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900 dark:border-amber-900/60 dark:bg-amber-900/20 dark:text-amber-200"
      role="status"
      data-test="oversubscribed-warning"
    >
      Category totals exceed shares outstanding (likely cause: stale 13F
      quarter combined with fresh Form 4 / 13D filings). Awaiting next
      13F cycle for the snapshots to align.
    </div>
  );
}

function ResidualLine({ rollup }: { rollup: OwnershipRollupResponse }): JSX.Element {
  const pct = parseShareCount(rollup.residual.pct_outstanding) ?? 0;
  return (
    <p className="mt-2 text-xs text-slate-500 dark:text-slate-400" data-test="residual-line">
      {rollup.residual.label}: {formatPct(pct)}
    </p>
  );
}

interface SliceTableProps {
  readonly rollup: OwnershipRollupResponse;
}

const _CATEGORY_ORDER_TABLE: readonly OwnershipSliceCategory[] = [
  "insiders",
  "blockholders",
  "institutions",
  "etfs",
  "def14a_unmatched",
];

function SliceTable({ rollup }: SliceTableProps): JSX.Element {
  const slicesByCategory = new Map(rollup.slices.map((s) => [s.category, s]));
  const visible = _CATEGORY_ORDER_TABLE.filter((cat) => slicesByCategory.has(cat));
  if (visible.length === 0) {
    return (
      <p className="text-xs text-slate-500 dark:text-slate-400">
        No filings ingested yet.
      </p>
    );
  }
  return (
    <table className="w-full text-sm">
      <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
        <tr>
          <th className="pb-1 text-left">Category</th>
          <th className="pb-1 text-right">Shares</th>
          <th className="pb-1 text-right">% of outstanding</th>
          <th className="pb-1 text-right">Filers</th>
        </tr>
      </thead>
      <tbody>
        {visible.map((cat) => {
          const slc = slicesByCategory.get(cat)!;
          const shares = parseShareCount(slc.total_shares) ?? 0;
          const pct = parseShareCount(slc.pct_outstanding) ?? 0;
          return (
            <tr key={cat} className="border-t border-t-slate-100 dark:border-t-slate-800">
              <td className="py-1.5 text-slate-700 dark:text-slate-200">
                {slc.label}
              </td>
              <td className="py-1.5 text-right font-mono text-slate-700 dark:text-slate-200">
                {formatShares(shares)}
              </td>
              <td className="py-1.5 text-right font-mono text-slate-900 dark:text-slate-100">
                {formatPct(pct)}
              </td>
              <td className="py-1.5 text-right font-mono text-slate-500 dark:text-slate-400">
                {slc.filer_count}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// Internal banner-state union exported solely so the test file can
// drive snapshot fixtures without re-importing from the api module
// (keeps the test imports tight). Codex caught a similar pattern on
// #767's freshness chip extraction.
export type _BannerStateForTest = OwnershipCoverageState;
