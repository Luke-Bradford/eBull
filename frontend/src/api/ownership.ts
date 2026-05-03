/**
 * Client for /instruments/{symbol}/ownership-rollup (#789, parent
 * #788).
 *
 * Cross-channel deduped ownership snapshot: insiders + blockholders +
 * institutions + ETFs + def14a-unmatched, all keyed on a single
 * ``shares_outstanding`` denominator. The single endpoint replaces
 * three of the five fetches the prior ``OwnershipPanel`` made
 * (institutional / blockholders / insiders) so the panel can render
 * one consistent snapshot without race-induced double-counting.
 *
 * Banner state machine (driven server-side from per-category
 * universe coverage, NOT float concentration):
 *
 *   * ``no_data`` — XBRL ``shares_outstanding`` not on file.
 *   * ``unknown_universe`` — outstanding present but per-category
 *     universe estimates are NULL (Tier 0 default until #790).
 *   * ``red`` / ``amber`` / ``green`` — only after #790 seeds
 *     per-category estimates.
 *
 * Concentration (sum of slices / outstanding) ships as a separate
 * info chip — Codex review of the v2 spec caught the prior banner
 * design that conflated concentration with coverage.
 */

import { apiFetch } from "@/api/client";

export type OwnershipSourceTag =
  | "form4"
  | "form3"
  | "13d"
  | "13g"
  | "def14a"
  | "13f";

export type OwnershipSliceCategory =
  | "insiders"
  | "blockholders"
  | "institutions"
  | "etfs"
  | "def14a_unmatched";

export type OwnershipCoverageState =
  | "no_data"
  | "red"
  | "unknown_universe"
  | "amber"
  | "green";

export type OwnershipBannerVariant = "error" | "warning" | "info" | "success";

export interface OwnershipDroppedSource {
  readonly source: OwnershipSourceTag;
  readonly accession_number: string;
  /** Decimal-as-string. */
  readonly shares: string;
  readonly as_of_date: string | null;
}

export interface OwnershipHolder {
  readonly filer_cik: string | null;
  readonly filer_name: string;
  /** Decimal-as-string. */
  readonly shares: string;
  /** Decimal-as-string fraction (e.g. ``"0.0822"`` = 8.22%). */
  readonly pct_outstanding: string;
  readonly winning_source: OwnershipSourceTag;
  readonly winning_accession: string;
  readonly as_of_date: string | null;
  /** 13F filer-type tag — only populated for 13F survivors. */
  readonly filer_type: string | null;
  /**
   * One row per losing source for this canonical holder. Surfaces
   * in the Batch 3 provenance footer so the operator can see the
   * full set of accessions that referred to this holder, even
   * though only one supplied the share count used.
   */
  readonly dropped_sources: readonly OwnershipDroppedSource[];
}

export interface OwnershipSlice {
  readonly category: OwnershipSliceCategory;
  readonly label: string;
  /** Decimal-as-string. */
  readonly total_shares: string;
  readonly pct_outstanding: string;
  readonly filer_count: number;
  readonly dominant_source: OwnershipSourceTag | null;
  readonly holders: readonly OwnershipHolder[];
}

export interface OwnershipResidual {
  readonly shares: string;
  readonly pct_outstanding: string;
  readonly label: string;
  readonly tooltip: string;
  /** True when slice totals + treasury exceed shares_outstanding
   *  (stale 13F + fresh Form 4/13D mix). The renderer shows a
   *  warning bar above the chart in this case; residual itself is
   *  clamped to 0. */
  readonly oversubscribed: boolean;
}

export interface OwnershipCategoryCoverage {
  readonly known_filers: number;
  readonly estimated_universe: number | null;
  readonly pct_universe: string | null;
  readonly state: OwnershipCoverageState;
}

export interface OwnershipCoverage {
  /** Worst-of fold across the four tracked categories (insiders,
   *  blockholders, institutions, etfs). ``unknown_universe`` ranks
   *  worse than ``amber`` so a single well-seeded category cannot
   *  mask blind spots in the others. */
  readonly state: OwnershipCoverageState;
  readonly categories: Readonly<Record<string, OwnershipCategoryCoverage>>;
}

export interface OwnershipConcentration {
  readonly pct_outstanding_known: string;
  readonly info_chip: string;
}

export interface OwnershipBanner {
  readonly state: OwnershipCoverageState;
  readonly variant: OwnershipBannerVariant;
  readonly headline: string;
  readonly body: string;
}

export interface OwnershipSharesOutstandingSource {
  readonly accession_number: string | null;
  readonly concept: string | null;
  readonly form_type: string | null;
}

export interface OwnershipRollupResponse {
  readonly symbol: string;
  readonly instrument_id: number;
  /** Decimal-as-string; null when XBRL has no figure on file (= the
   *  ``no_data`` banner state). */
  readonly shares_outstanding: string | null;
  readonly shares_outstanding_as_of: string | null;
  readonly shares_outstanding_source: OwnershipSharesOutstandingSource;
  readonly treasury_shares: string | null;
  readonly treasury_as_of: string | null;
  readonly slices: readonly OwnershipSlice[];
  readonly residual: OwnershipResidual;
  readonly concentration: OwnershipConcentration;
  readonly coverage: OwnershipCoverage;
  readonly banner: OwnershipBanner;
  readonly computed_at: string;
}

export function fetchOwnershipRollup(
  symbol: string,
): Promise<OwnershipRollupResponse> {
  return apiFetch<OwnershipRollupResponse>(
    `/instruments/${encodeURIComponent(symbol)}/ownership-rollup`,
  );
}
