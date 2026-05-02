/**
 * Sunburst data transformer for the ownership card (#729).
 *
 * Three concentric rings:
 *
 *   ring 1 (inner)  : single arc — "Held" (sum of all categories below)
 *   ring 2 (middle) : per-category — Institutions / ETFs / Insiders / Treasury / Unallocated
 *   ring 3 (outer)  : per-filer / per-officer wedges within each category, plus
 *                     "Other [Category]" tail-aggregate when individual filers
 *                     fall below the visibility threshold.
 *
 * Threshold-based grouping (vs top-N):
 *   * 0.5% of float OR 10,000 shares — whichever is larger
 *   * The float-relative floor keeps mega-caps with hundreds of small
 *     13F filers from drowning the canvas in confetti while still
 *     promoting any holder large enough to move the thesis.
 *   * The 10,000-share absolute floor prevents thinly-floated
 *     micro-caps from setting an effectively-zero threshold that
 *     would render every legitimate holder.
 *   * Insiders bypass the threshold — the officer set is small and
 *     every officer's holding is signal.
 *
 * Coverage gating: when a category's total is unknown (Institutions /
 * ETFs gated on #740 CUSIP backfill), the transformer emits a
 * sentinel ``status='unknown'`` middle wedge sized to the residual
 * (free-float minus known categories) so the operator sees the gap
 * visually rather than as missing slices.
 */

export type SunburstCategoryStatus = "ok" | "unknown" | "empty";

/**
 * Why a category resolves to ``status='unknown'``. Drives the
 * tooltip + summary copy so the operator distinguishes a CUSIP
 * backfill gap (#740) from a missing DEI cover-page projection
 * (#735) etc. Today only ``cusip_backfill`` is wired through;
 * ``dei_projection`` is reserved for treasury_shares specifically
 * and lights up once #735 surfaces the column.
 */
export type SunburstUnknownReason =
  | "cusip_backfill"
  | "dei_projection"
  | "no_data";

export interface SunburstCategory {
  readonly key: string; // 'institutions' | 'etfs' | 'insiders' | 'treasury' | 'unallocated'
  readonly label: string;
  /** Arithmetic share count — 0 when status is ``unknown``. The
   *  renderer sizes wedges by ``display_shares`` so unknown
   *  categories get a visible arc proportional to the float gap. */
  readonly shares: number;
  /** Renderer-facing wedge size. Equals ``shares`` for known
   *  categories. For unknown categories, set to the proportional
   *  share of the aggregate float gap so the wedge has a visible
   *  arc instead of collapsing to a 0-degree sliver. */
  readonly display_shares: number;
  readonly pct: number; // share / float
  readonly status: SunburstCategoryStatus;
  /** Set only when ``status === 'unknown'``. */
  readonly unknown_reason?: SunburstUnknownReason;
  /** Outer-ring wedges within this category. */
  readonly leaves: readonly SunburstLeaf[];
}

export interface SunburstLeaf {
  readonly key: string; // stable id for click-drill (filer cik, officer name, "other-etfs")
  readonly label: string;
  readonly shares: number;
  /** Renderer-facing wedge size — see ``SunburstCategory.display_shares``. */
  readonly display_shares: number;
  readonly pct: number; // share / float
  /** True for the aggregated tail wedge. */
  readonly is_other: boolean;
  /** Counts only meaningful on the "Other" tail wedge. */
  readonly tail_meta?: SunburstTailMeta;
}

export interface SunburstTailMeta {
  readonly count: number;
  readonly aggregate_shares: number;
  readonly aggregate_pct: number;
  readonly largest_label: string;
  readonly largest_pct: number;
}

export interface SunburstHolder {
  /** Stable identifier — filer CIK, officer CIK, or fallback name. */
  readonly key: string;
  readonly label: string;
  readonly shares: number;
  readonly category: "institutions" | "etfs" | "insiders";
}

export interface SunburstInputs {
  readonly free_float: number;
  /** Holders contributing to Institutions / ETFs / Insiders. */
  readonly holders: readonly SunburstHolder[];
  /** Treasury memo line — single wedge under its own middle category. */
  readonly treasury_shares: number | null;
  /**
   * Per-category status flags. ``unknown`` short-circuits the
   * transformer for that category so ungated CUSIP coverage doesn't
   * silently appear as 0%.
   */
  readonly institutions_status: SunburstCategoryStatus;
  readonly etfs_status: SunburstCategoryStatus;
  readonly insiders_status: SunburstCategoryStatus;
}

/**
 * Inner-ring split. Pre-#746 the inner ring reported "Held" as a
 * single arc summing every category including the synthetic
 * ``unknown`` placeholders — so an instrument with 95% of its
 * float in coverage-gap categories rendered a 100% Held arc,
 * which lied. Now the inner ring carries explicit ``known`` and
 * ``gap`` segments and the renderer draws them as two arcs.
 */
export interface InnerRing {
  /** Sum of every category whose status is ``ok``. */
  readonly known_shares: number;
  /** Sum of every category whose status is ``unknown`` — i.e. the
   *  free-float residual that we cannot account for today. */
  readonly gap_shares: number;
  readonly known_pct: number;
  readonly gap_pct: number;
}

export interface SunburstRings {
  readonly free_float: number;
  readonly inner: InnerRing;
  /** ring 2 — per-category wedges. */
  readonly categories: readonly SunburstCategory[];
}

const SHARES_FLOOR = 10_000;
const FLOAT_PCT_FLOOR = 0.005; // 0.5%

/**
 * Compute the per-category visibility threshold for outer-ring wedges.
 *
 * Insiders ignore this — every officer surfaces.
 */
export function visibilityThreshold(free_float: number): number {
  if (free_float <= 0) return SHARES_FLOOR;
  return Math.max(SHARES_FLOOR, free_float * FLOAT_PCT_FLOOR);
}

interface CategorySpec {
  readonly key: "institutions" | "etfs" | "insiders" | "treasury" | "unallocated";
  readonly label: string;
  readonly status: SunburstCategoryStatus;
  /** Reason an unknown-status category resolved unknown — drives
   *  the operator-facing tooltip + summary copy. */
  readonly unknown_reason?: SunburstUnknownReason;
  /** True = bypass the visibility threshold (insiders, treasury, unallocated). */
  readonly bypass_threshold: boolean;
}

const CATEGORY_LABEL: Record<string, string> = {
  institutions: "Institutions",
  etfs: "ETFs",
  insiders: "Insiders",
  treasury: "Treasury",
  unallocated: "Unallocated",
};

/**
 * Transform raw inputs into the three-ring sunburst data model.
 *
 * Returns ``null`` when ``free_float`` is missing / zero — the caller
 * renders the card empty state (no denominator → no rings).
 */
export function buildSunburstRings(input: SunburstInputs): SunburstRings | null {
  if (input.free_float <= 0 || !Number.isFinite(input.free_float)) return null;

  const float = input.free_float;
  const threshold = visibilityThreshold(float);

  const inst_holders = input.holders.filter((h) => h.category === "institutions");
  const etf_holders = input.holders.filter((h) => h.category === "etfs");
  const insider_holders = input.holders.filter((h) => h.category === "insiders");

  const institutions = buildCategory(
    {
      key: "institutions",
      label: CATEGORY_LABEL.institutions!,
      status: input.institutions_status,
      unknown_reason: "cusip_backfill",
      bypass_threshold: false,
    },
    inst_holders,
    float,
    threshold,
  );
  const etfs = buildCategory(
    {
      key: "etfs",
      label: CATEGORY_LABEL.etfs!,
      status: input.etfs_status,
      unknown_reason: "cusip_backfill",
      bypass_threshold: false,
    },
    etf_holders,
    float,
    threshold,
  );
  const insiders = buildCategory(
    {
      key: "insiders",
      label: CATEGORY_LABEL.insiders!,
      status: input.insiders_status,
      unknown_reason: "no_data",
      bypass_threshold: true,
    },
    insider_holders,
    float,
    threshold,
  );

  // Treasury renders as a single leaf wedge.
  // Distinguish "treasury reported as zero" from "DEI projection
  // missing" — treasury_shares=null is the #735 / #731 follow-up,
  // not the CUSIP-backfill #740 gap.
  const treasury_shares = input.treasury_shares ?? 0;
  const treasury_status: SunburstCategoryStatus =
    input.treasury_shares === null
      ? "unknown"
      : treasury_shares > 0
        ? "ok"
        : "empty";
  const treasury: SunburstCategory = {
    key: "treasury",
    label: CATEGORY_LABEL.treasury!,
    // Arithmetic shares = the actual treasury count (0 when unknown).
    // The renderer-facing wedge size is patched further down by
    // ``distributeGapShares`` so unknown wedges have visible arcs
    // proportional to the float gap.
    shares: treasury_shares,
    display_shares: treasury_shares,
    pct: treasury_shares / float,
    status: treasury_status,
    ...(treasury_status === "unknown"
      ? { unknown_reason: "dei_projection" as const }
      : {}),
    leaves:
      treasury_status === "unknown"
        ? [
            {
              key: "treasury-unknown",
              label: "Treasury — DEI projection pending",
              shares: 0,
              display_shares: 0,
              pct: 0,
              is_other: false,
            },
          ]
        : treasury_shares > 0
          ? [
              {
                key: "treasury",
                label: "Treasury",
                shares: treasury_shares,
                display_shares: treasury_shares,
                pct: treasury_shares / float,
                is_other: false,
              },
            ]
          : [],
  };

  // Unallocated absorbs whatever's left after every known category. When
  // any category is ``unknown`` we cannot derive Unallocated cleanly —
  // emit it as ``empty`` so the operator's eye lands on the
  // upstream-unknown wedges as the source of the gap, not on a
  // fabricated "Unallocated coverage gap" downstream wedge.
  const known_shares =
    (institutions.status === "ok" ? institutions.shares : 0) +
    (etfs.status === "ok" ? etfs.shares : 0) +
    (insiders.status === "ok" ? insiders.shares : 0) +
    (treasury.status === "ok" ? treasury.shares : 0);

  const has_unknown =
    institutions.status === "unknown" ||
    etfs.status === "unknown" ||
    insiders.status === "unknown" ||
    treasury.status === "unknown";

  const residual_shares = Math.max(0, float - known_shares);
  // When every category is ``ok``, residual is the genuine
  // Unallocated slice (retail + small holders below the 13F threshold).
  // When any category is ``unknown``, the residual is contaminated
  // by the upstream gap and would mislead — collapse to ``empty``.
  const unallocated: SunburstCategory = {
    key: "unallocated",
    label: CATEGORY_LABEL.unallocated!,
    shares: has_unknown ? 0 : residual_shares,
    display_shares: has_unknown ? 0 : residual_shares,
    pct: has_unknown ? 0 : residual_shares / float,
    status: has_unknown ? "empty" : residual_shares > 0 ? "ok" : "empty",
    leaves: has_unknown
      ? []
      : residual_shares > 0
        ? [
            {
              key: "unallocated",
              label: "Unallocated",
              shares: residual_shares,
              display_shares: residual_shares,
              pct: residual_shares / float,
              is_other: false,
            },
          ]
        : [],
  };

  const draft_categories: SunburstCategory[] = [
    institutions,
    etfs,
    insiders,
    treasury,
    unallocated,
  ];

  // Inner-ring split: known (sum of OK categories + the
  // ok-status Unallocated residual) vs gap (everything else =
  // float minus known). Pre-fix the inner ring carried the synthetic
  // unknown-padding through unaltered, lighting up as a 100% Held
  // arc even when 95% of the float was in coverage gaps.
  const inner_known = known_shares + (unallocated.status === "ok" ? unallocated.shares : 0);
  const inner_gap = Math.max(0, float - inner_known);

  // Wedge-size policy:
  //
  // 1. Equally distribute the float gap across unknown categories
  //    so each unknown wedge has a visible arc.
  // 2. Floor every renderable wedge at a minimum proportional size
  //    so a small-but-known category (e.g. Insiders 0.06% on AAPL)
  //    doesn't collapse to a sub-pixel sliver against the huge
  //    unknown wedges. Without this floor, faithful proportional
  //    sizing makes the small wedge invisible — operator clicked
  //    where Insiders should be and hit the gap wedge instead.
  //
  // Floor = ``MIN_WEDGE_FRACTION`` of the total ring per wedge.
  // The cost is that the ring is no longer strictly
  // proportional — small wedges read slightly larger than their
  // actual share. The header summary line ("X% known · Y% gap")
  // and the legend table carry the precise numbers; the wedge
  // arcs are visual hints not measurement instruments.
  const MIN_WEDGE_FRACTION = 0.05; // each visible wedge ≥ 5% of the ring

  const unknown_count = draft_categories.filter((c) => c.status === "unknown").length;
  const gap_per_unknown = unknown_count > 0 ? inner_gap / unknown_count : 0;

  // Compute floor in absolute share-equivalent units.
  const min_display_shares = float * MIN_WEDGE_FRACTION;

  const categories_with_gap: SunburstCategory[] = draft_categories.map((cat) => {
    if (cat.status !== "unknown") return cat;
    return {
      ...cat,
      display_shares: gap_per_unknown,
      leaves: cat.leaves.map((leaf) => ({
        ...leaf,
        display_shares: gap_per_unknown,
      })),
    };
  });

  // Apply the minimum-wedge floor only to categories that will
  // actually render (skip ``empty``-status wedges which are
  // filtered out by the renderer).
  //
  // Invariant: ``sum(leaf.display_shares for leaf in cat.leaves)``
  // must equal ``cat.display_shares`` for every non-empty category,
  // otherwise the middle and outer rings draw arcs of different
  // widths for the same category. The pre-floor data already
  // satisfies it (OK leaves sum to the category total via the
  // "Other" tail; unknown leaves carry the gap allocation as a
  // single placeholder). When the floor patches the category's
  // display_shares, every leaf must be rescaled by the same factor
  // so the invariant survives. Independently flooring each leaf
  // (the original bug, PR #754 round 2) inflates the outer ring by
  // a factor of ``leaves.length`` for any sub-floor multi-leaf
  // category.
  const categories: SunburstCategory[] = categories_with_gap.map((cat) => {
    const renders = !(cat.status === "empty" && cat.shares <= 0);
    if (!renders) return cat;
    if (cat.display_shares >= min_display_shares) return cat;
    return {
      ...cat,
      display_shares: min_display_shares,
      leaves: rescaleLeaves(cat.leaves, min_display_shares),
    };
  });

  return {
    free_float: float,
    inner: {
      known_shares: inner_known,
      gap_shares: inner_gap,
      known_pct: inner_known / float,
      gap_pct: inner_gap / float,
    },
    categories,
  };
}

/**
 * Rescale leaves so they sum to ``target_total`` while preserving
 * each leaf's relative weight. Used by the minimum-wedge floor in
 * ``buildSunburstRings`` to keep the middle and outer rings
 * consistent for sub-floor categories.
 *
 * When the leaves' current display_shares total is zero (e.g. an
 * unknown category whose single placeholder leaf carries
 * ``display_shares=0`` because the float gap is zero), distribute
 * the target evenly across leaves so every leaf still gets a
 * visible arc.
 */
function rescaleLeaves(
  leaves: readonly SunburstLeaf[],
  target_total: number,
): readonly SunburstLeaf[] {
  if (leaves.length === 0) return leaves;
  const current_total = leaves.reduce((sum, l) => sum + l.display_shares, 0);
  if (current_total <= 0) {
    const even = target_total / leaves.length;
    return leaves.map((leaf) => ({ ...leaf, display_shares: even }));
  }
  const scale = target_total / current_total;
  return leaves.map((leaf) => ({
    ...leaf,
    display_shares: leaf.display_shares * scale,
  }));
}

function buildCategory(
  spec: CategorySpec,
  holders: readonly SunburstHolder[],
  float: number,
  threshold: number,
): SunburstCategory {
  if (spec.status === "unknown") {
    return {
      key: spec.key,
      label: spec.label,
      shares: 0,
      // ``display_shares`` patched in by the caller via
      // ``distributeGapShares``; placeholder here.
      display_shares: 0,
      pct: 0,
      status: "unknown",
      ...(spec.unknown_reason !== undefined ? { unknown_reason: spec.unknown_reason } : {}),
      leaves: [
        {
          key: `${spec.key}-unknown`,
          label: unknownLeafLabel(spec.label, spec.unknown_reason),
          shares: 0,
          display_shares: 0,
          pct: 0,
          is_other: false,
        },
      ],
    };
  }

  if (holders.length === 0) {
    return {
      key: spec.key,
      label: spec.label,
      shares: 0,
      display_shares: 0,
      pct: 0,
      status: "empty",
      leaves: [],
    };
  }

  const total_shares = holders.reduce((sum, h) => sum + h.shares, 0);
  if (total_shares <= 0) {
    return {
      key: spec.key,
      label: spec.label,
      shares: 0,
      display_shares: 0,
      pct: 0,
      status: "empty",
      leaves: [],
    };
  }

  // Sort largest-first so the canvas reads counter-clockwise from
  // 12 o'clock with the dominant holders most visually prominent.
  const sorted = [...holders].sort((a, b) => b.shares - a.shares);

  const visible: SunburstLeaf[] = [];
  const tail: SunburstHolder[] = [];

  for (const h of sorted) {
    const passes = spec.bypass_threshold || h.shares >= threshold;
    if (passes) {
      visible.push({
        key: h.key,
        label: h.label,
        shares: h.shares,
        display_shares: h.shares,
        pct: h.shares / float,
        is_other: false,
      });
    } else {
      tail.push(h);
    }
  }

  const leaves: SunburstLeaf[] = [...visible];
  if (tail.length > 0) {
    const aggregate_shares = tail.reduce((sum, h) => sum + h.shares, 0);
    const largest = tail.reduce((biggest, h) => (h.shares > biggest.shares ? h : biggest), tail[0]!);
    leaves.push({
      key: `${spec.key}-other`,
      label: `Other ${spec.label.toLowerCase()}`,
      shares: aggregate_shares,
      display_shares: aggregate_shares,
      pct: aggregate_shares / float,
      is_other: true,
      tail_meta: {
        count: tail.length,
        aggregate_shares,
        aggregate_pct: aggregate_shares / float,
        largest_label: largest.label,
        largest_pct: largest.shares / float,
      },
    });
  }

  return {
    key: spec.key,
    label: spec.label,
    shares: total_shares,
    display_shares: total_shares,
    pct: total_shares / float,
    status: "ok",
    leaves,
  };
}

function unknownLeafLabel(
  category_label: string,
  reason: SunburstUnknownReason | undefined,
): string {
  switch (reason) {
    case "cusip_backfill":
      return `${category_label} — needs CUSIP backfill (#740)`;
    case "dei_projection":
      return `${category_label} — DEI projection pending (#735)`;
    default:
      return `${category_label} — data not available`;
  }
}
