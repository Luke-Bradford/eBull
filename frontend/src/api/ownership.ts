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
 *     universe estimates are NULL (Tier 0 default; #790 closed
 *     no-code).
 *   * ``red`` / ``amber`` / ``green`` — only once per-category
 *     estimates are seeded.
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
  | "13f"
  | "nport";

export type OwnershipSliceCategory =
  | "insiders"
  | "blockholders"
  | "institutions"
  | "etfs"
  | "def14a_unmatched"
  | "funds";

/**
 * Tags whether a slice contributes to the pie wedges that sum to
 * ``shares_outstanding`` (``pie_wedge``) or is a memo overlay rendered
 * alongside without affecting the pie math (``institution_subset``).
 *
 * Funds slice (#919) is the first ``institution_subset`` overlay:
 * N-PORT rows are fund-level detail INSIDE the 13F-HR institutional
 * aggregate, so additive accounting would double-count.
 */
export type OwnershipDenominatorBasis = "pie_wedge" | "institution_subset";

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
  /** SEC archive index URL for the accession. ``null`` only when the
   *  accession is malformed (defensive). */
  readonly edgar_url: string | null;
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
  /** SEC archive index URL for the winning accession — surfaces in
   *  the L2 holder table as a click-through to the source filing. */
  readonly winning_edgar_url: string | null;
  readonly as_of_date: string | null;
  /** 13F filer-type tag — only populated for 13F survivors. */
  readonly filer_type: string | null;
  /**
   * One row per losing source for this canonical holder. Surfaces
   * in the provenance footer so the operator can see the full set
   * of accessions that referred to this holder, even though only
   * one supplied the share count used.
   */
  readonly dropped_sources: readonly OwnershipDroppedSource[];
  /**
   * Constituent 13F sub-CIK rows of a collapsed institutional family
   * (#1644 / #1649) — e.g. the 10 Vanguard sub-entities under one
   * "The Vanguard Group" row. Display-only breakdown; their shares are
   * already counted once in this holder's ``shares``. Empty for ordinary
   * holders.
   */
  readonly family_members?: readonly OwnershipFamilyMember[];
}

/** One constituent row inside a collapsed institutional family
 *  (#1644 / #1649). Display-only; NOT additive. */
export interface OwnershipFamilyMember {
  readonly filer_cik: string | null;
  readonly filer_name: string;
  /** Decimal-as-string. */
  readonly shares: string;
  readonly source: OwnershipSourceTag;
  readonly accession_number: string;
  readonly edgar_url: string | null;
  readonly as_of_date: string | null;
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
  /** Pie-wedge slices contribute to residual / concentration math;
   *  ``institution_subset`` slices (e.g. funds) render as memo
   *  overlays. Defaults to ``pie_wedge`` when absent for backwards
   *  compatibility with older payloads. */
  readonly denominator_basis?: OwnershipDenominatorBasis;
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

/** One row of ``instrument_symbol_history``, oldest-first. Frontend
 *  renders a "Filed as X" callout when the chain includes any symbol
 *  other than the current one (Batch 7 of #788). */
export interface OwnershipHistoricalSymbol {
  readonly symbol: string;
  /** ISO ``YYYY-MM-DD`` — when this symbol started representing the
   *  instrument. */
  readonly effective_from: string;
  /** ISO ``YYYY-MM-DD`` — when this symbol stopped representing the
   *  instrument. ``null`` marks the current symbol row. */
  readonly effective_to: string | null;
  /** ``imported`` | ``rebrand`` | ``delisting`` | ``relisting`` |
   *  ``manual`` — see migration 103. */
  readonly source_event: string;
}

export interface OwnershipSharesOutstandingSource {
  readonly accession_number: string | null;
  readonly concept: string | null;
  readonly form_type: string | null;
  /** Pre-computed SEC archive index URL — null when the accession is
   *  null. Backend-computed (Batch 3 of #788) so the frontend can't
   *  drift to the wrong EDGAR endpoint. */
  readonly edgar_url: string | null;
}

/** A figure-changing correction applied at read time (#1639 / #1647).
 *  First-class structured record so the UI / a machine consumer sees WHY the
 *  institutions total changed, not just the corrected number. ``kind`` is a
 *  closed vocabulary; today only ``suppressed_by_13f_nt`` (a filer's stale
 *  13F-HR removed because the filer filed a 13F-NT for a later quarter). */
export type OwnershipCorrectionKind =
  | "suppressed_by_13f_nt"
  | "def14a_restates_institution"
  | "institutional_family_collapse";

export interface OwnershipCorrectionApplied {
  readonly kind: OwnershipCorrectionKind;
  /** ``null`` for a proxy-name-only family fold (no CIK). */
  readonly filer_cik: string | null;
  readonly filer_name: string;
  /** Decimal-as-string — shares removed from the institutions slice (they flow
   *  into the residual). */
  readonly shares_removed: string;
  /** ISO ``YYYY-MM-DD`` — the superseded 13F-HR quarter (NT kind only). */
  readonly superseded_period: string | null;
  /** ISO ``YYYY-MM-DD`` — the 13F-NT quarter that superseded it (NT kind only). */
  readonly winning_nt_period: string | null;
  readonly winning_nt_accession: string | null;
  /** Institutional-family fold provenance (#1644 / #1649). */
  readonly family_id: string | null;
  readonly source_channel: OwnershipSourceTag | null;
  readonly winning_source: OwnershipSourceTag | null;
  readonly winning_accession: string | null;
  readonly detail: string;
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
  readonly historical_symbols: readonly OwnershipHistoricalSymbol[];
  /** Figure-changing corrections applied at read time (#1639 / #1647). Empty
   *  when none fired. ``suppressed_by_notice`` is the convenience count of the
   *  ``suppressed_by_13f_nt`` kind. */
  readonly corrections_applied: readonly OwnershipCorrectionApplied[];
  readonly suppressed_by_notice: number;
  readonly computed_at: string;
}

export function fetchOwnershipRollup(
  symbol: string,
): Promise<OwnershipRollupResponse> {
  return apiFetch<OwnershipRollupResponse>(
    `/instruments/${encodeURIComponent(symbol)}/ownership-rollup`,
  );
}
