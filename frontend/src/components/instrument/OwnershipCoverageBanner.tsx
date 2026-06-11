/**
 * Ownership coverage banner (#923, extracted from OwnershipPanel).
 *
 * Renders the SERVER-driven 5-state coverage machine (#840:
 * ``no_data`` / ``red`` / ``unknown_universe`` / ``amber`` /
 * ``green``). Copy (headline/body) and color (``variant``) are
 * backend-owned and rendered verbatim — the FE must not fork copy or
 * re-derive variant from state. The 6-state Phase-1 vocabulary the
 * #923 issue cites was superseded by #840; see
 * ``docs/specs/ui/2026-06-11-ownership-coverage-banner-v2.md`` and
 * the settled-decisions entry.
 *
 * What the FE adds: a per-STATE glyph, because ``no_data`` and
 * ``red`` share ``variant="error"`` and were visually identical —
 * the operator must tell "nothing on file" from "coverage
 * dangerously incomplete" at a glance.
 */

import type {
  OwnershipBannerVariant,
  OwnershipCoverageState,
  OwnershipRollupResponse,
} from "@/api/ownership";

const BANNER_VARIANT_CLASS: Record<OwnershipBannerVariant, string> = {
  error:
    "border-red-200 bg-red-50 text-red-900 dark:border-red-900/60 dark:bg-red-900/20 dark:text-red-200",
  warning:
    "border-amber-200 bg-amber-50 text-amber-900 dark:border-amber-900/60 dark:bg-amber-900/20 dark:text-amber-200",
  info: "border-slate-200 bg-slate-50 text-slate-700 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-200",
  success:
    "border-emerald-200 bg-emerald-50 text-emerald-900 dark:border-emerald-900/60 dark:bg-emerald-900/20 dark:text-emerald-200",
};

/** Exhaustive by construction — a new backend/type state fails the
 *  typecheck here instead of silently rendering glyph-less. */
export const BANNER_STATE_GLYPH: Record<OwnershipCoverageState, string> = {
  no_data: "⊘",
  red: "✕",
  unknown_universe: "?",
  amber: "▲",
  green: "✓",
};

export interface OwnershipCoverageBannerProps {
  readonly banner: OwnershipRollupResponse["banner"];
}

export function OwnershipCoverageBanner({
  banner,
}: OwnershipCoverageBannerProps): JSX.Element {
  const glyph = (
    // aria-hidden glyph only — headline/body remain the entire
    // accessible content of the role="status" region.
    <span aria-hidden="true" className="mr-1.5 font-semibold">
      {BANNER_STATE_GLYPH[banner.state]}
    </span>
  );
  if (banner.state === "green") {
    // Compact single-line form for the healthy state.
    return (
      <div
        className={`rounded-md border px-3 py-2 text-xs ${BANNER_VARIANT_CLASS[banner.variant]}`}
        role="status"
        data-banner-state={banner.state}
      >
        {glyph}
        <span className="font-medium">{banner.headline}</span>
        <span className="ml-1.5">{banner.body}</span>
      </div>
    );
  }
  return (
    <div
      className={`rounded-md border px-3 py-2 text-xs ${BANNER_VARIANT_CLASS[banner.variant]}`}
      role="status"
      data-banner-state={banner.state}
    >
      <p className="font-medium">
        {glyph}
        {banner.headline}
      </p>
      <p className="mt-0.5">{banner.body}</p>
    </div>
  );
}
