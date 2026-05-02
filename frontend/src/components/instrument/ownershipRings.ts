/**
 * Ownership sunburst data model (#729).
 *
 * Three concentric rings keyed on a single denominator:
 * ``total_shares`` (= ``shares_outstanding + treasury_shares``).
 * Treasury counts toward the denominator because the operator's
 * mental model is "100% of issued / allotted shares — some held in
 * the market, some held back in the company's vault". Treasury
 * appears as one of the categories.
 *
 *   ring 1 (inner)  : center hole shows ``total_shares``.
 *   ring 2 (middle) : per-category wedges sized faithfully against
 *                     ``total_shares``. Categories with shares=0 are
 *                     not rendered. After every known category, a
 *                     single transparent wedge soaks up the residual
 *                     so the visible arcs stop short and the operator
 *                     sees a literal empty arc for the unaccounted
 *                     portion of the float.
 *   ring 3 (outer)  : per-filer / per-officer wedges within each
 *                     category, sized faithfully. If a category's
 *                     known leaves sum to less than the category
 *                     total (e.g. Institutions reports 50% of
 *                     outstanding via 13F totals but only resolves
 *                     47% to named filers), the outer ring shows
 *                     a transparent arc for the within-category
 *                     gap.
 *
 * Threshold-based grouping for outer-ring leaves: every filer that
 * meets ``max(0.5% of outstanding, 10,000 shares)`` gets its own
 * wedge. Sub-threshold filers aggregate into "Other [Category]"
 * with tail metadata.
 *
 * Snapshot-lag / oversubscription handling — both can occur because
 * totals (13F aggregate snapshot, XBRL period-end balance) and
 * per-filer detail (13F filings) carry independent snapshot dates:
 *
 *   * Per-category leaf cap: when filer detail sums to MORE than the
 *     category total (snapshot lag), the category's reported total
 *     is bumped to ``max(reported, sum_of_leaves)``. The leaves are
 *     more recent ground truth — using their sum as the wedge size
 *     keeps ring 3 inside ring 2 geometrically.
 *   * Cross-category oversubscription: when category totals sum to
 *     more than the input ``total_shares`` (XBRL outstanding +
 *     treasury can lag), the effective denominator is bumped to the
 *     sum so wedges aren't silently renormalised by Recharts.
 *
 * No synthetic placeholders. No "unknown" status hack. The chart
 * faithfully reports what we know and literally leaves the rest
 * empty.
 */

export interface SunburstHolder {
  /** Stable identifier — filer CIK, officer CIK, or fallback name. */
  readonly key: string;
  readonly label: string;
  readonly shares: number;
  readonly category:
    | "institutions"
    | "etfs"
    | "insiders"
    | "treasury"
    | "blockholders";
}

export interface SunburstInputs {
  /** Denominator. Every category + leaf is sized as a proportion of
   *  this number; the visible arcs sum to ≤100%. Callers typically
   *  compute this as ``shares_outstanding + (treasury_shares ?? 0)``. */
  readonly total_shares: number;

  /** Per-filer detail for institutional / ETF / insider categories.
   *  May be incomplete (e.g. CUSIP-backfill #740 means many filers
   *  resolve to no instrument and are dropped). */
  readonly holders: readonly SunburstHolder[];

  /** Aggregate share counts per category from the upstream API.
   *  These can exceed ``sum(holders.shares)`` when the per-filer
   *  detail is incomplete — the ring 3 transparent arc visualises
   *  that gap. ``null`` = the API returned no data; the category
   *  does not render at all (its share of the ring stays empty). */
  readonly institutions_total: number | null;
  readonly etfs_total: number | null;
  readonly insiders_total: number | null;
  /** 13D/G blockholders aggregate (#766). One block per primary
   *  filer per issuer (joint reporters collapse upstream so this
   *  count does NOT double-count co-reporters of the same filing).
   *  Null = no 13D/G blocks on file. */
  readonly blockholders_total: number | null;

  /** Treasury (issuer-held) shares from XBRL. ``null`` = not on
   *  file; treasury wedge does not render. Counted in the
   *  denominator when present. */
  readonly treasury_shares: number | null;

  /** Source-row date that produced each category's totals. Drives
   *  the per-category freshness chip strip on the card header so the
   *  operator can tell which slice is the stalest at a glance — 13F
   *  lags 45-135d, Form 4 lags 0-2d, XBRL treasury lags 0-90d.
   *  ``null`` = caller has no date for that category (e.g. before
   *  the first ingest landed); the chip renders without an age
   *  delta. ISO ``YYYY-MM-DD``. Optional so existing call sites that
   *  pre-date #767 keep compiling — they'll render no chips until
   *  threaded through. */
  readonly institutions_as_of?: string | null;
  readonly etfs_as_of?: string | null;
  readonly insiders_as_of?: string | null;
  readonly treasury_as_of?: string | null;
  /** Blockholders as_of_date — latest filed_at across the included
   *  blocks (the reader endpoint returns this directly). */
  readonly blockholders_as_of?: string | null;
}

export type CategoryKey =
  | "institutions"
  | "etfs"
  | "insiders"
  | "treasury"
  | "blockholders";

export interface SunburstLeaf {
  readonly key: string;
  readonly label: string;
  readonly shares: number;
  /** True for the aggregated tail wedge from threshold grouping. */
  readonly is_other: boolean;
  readonly tail_meta?: SunburstTailMeta;
}

export interface SunburstTailMeta {
  readonly count: number;
  readonly aggregate_shares: number;
  readonly largest_label: string;
  readonly largest_shares: number;
}

export interface SunburstCategory {
  readonly key: CategoryKey;
  readonly label: string;
  /** Aggregate share count for the category. Drives the middle-ring
   *  wedge size. Bumped to ``sum(leaves)`` when filer detail
   *  oversubscribes the upstream-reported total (snapshot lag). */
  readonly shares: number;
  /** Upstream-reported total before snapshot-lag bump. Surfaced for
   *  diagnostics — operator-facing copy can flag when ``shares !=
   *  reported_total``. */
  readonly reported_total: number;
  /** Sum of named-filer shares we have detail on. Equals
   *  ``shares`` when detail is complete; less when incomplete. */
  readonly resolved_leaf_shares: number;
  /** Per-filer wedges. */
  readonly leaves: readonly SunburstLeaf[];
  /** Outer-ring residual = ``shares - resolved_leaf_shares``. The
   *  renderer paints a transparent wedge of this size so the named
   *  leaves don't get inflated to fill the parent arc. */
  readonly within_category_gap: number;
  /** Source-row date that produced ``shares`` for this category. ISO
   *  ``YYYY-MM-DD``. ``null`` when the caller didn't supply one. The
   *  freshness chip renders without an age delta in that case. */
  readonly as_of_date: string | null;
}

export interface SunburstRings {
  /** Effective denominator. Equals input ``total_shares`` unless
   *  category totals oversubscribe — in that case bumped to the
   *  sum so wedges aren't silently renormalised by Recharts. */
  readonly total_shares: number;
  /** Input ``total_shares`` before oversubscription bump. Surfaced
   *  for diagnostic copy when the two diverge. */
  readonly reported_total: number;
  /** Categories that render. Any category whose total is null/0 is
   *  omitted; its proportion of the ring stays empty. */
  readonly categories: readonly SunburstCategory[];
  /** ``total_shares - sum(categories.shares)``. The renderer paints
   *  a transparent wedge of this size on ring 2 so the visible arcs
   *  faithfully report only what we know. */
  readonly category_residual: number;
}

const SHARES_FLOOR = 10_000;
const OUTSTANDING_PCT_FLOOR = 0.005; // 0.5%

/**
 * Per-category visibility threshold for outer-ring leaves. Filers
 * below this size aggregate into the category's "Other" wedge.
 *
 * Insiders ignore the threshold — every officer surfaces.
 */
export function visibilityThreshold(total_shares: number): number {
  if (total_shares <= 0) return SHARES_FLOOR;
  return Math.max(SHARES_FLOOR, total_shares * OUTSTANDING_PCT_FLOOR);
}

const CATEGORY_LABEL: Record<CategoryKey, string> = {
  institutions: "Institutions",
  etfs: "ETFs",
  insiders: "Insiders",
  treasury: "Treasury",
  blockholders: "Blockholders",
};

/**
 * Build the ring data. Returns ``null`` when ``total_shares`` is
 * missing / zero — caller renders the empty state.
 */
export function buildSunburstRings(input: SunburstInputs): SunburstRings | null {
  const reported_total = input.total_shares;
  if (reported_total <= 0 || !Number.isFinite(reported_total)) return null;

  const inst_holders = input.holders.filter((h) => h.category === "institutions");
  const etf_holders = input.holders.filter((h) => h.category === "etfs");
  const insider_holders = input.holders.filter((h) => h.category === "insiders");
  const blockholder_holders = input.holders.filter(
    (h) => h.category === "blockholders",
  );

  // Threshold should align with the effective denominator the chart
  // ends up rendering against. When category totals oversubscribe
  // ``reported_total`` (snapshot lag), the renderer bumps the
  // denominator to ``sum_known`` — base the visibility threshold on
  // a pessimistic upper bound (max of reported_total and the sum of
  // every input holder's shares) so the 0.5% rule applies to the
  // denom the operator actually sees.
  const sum_holders = input.holders.reduce((s, h) => s + h.shares, 0);
  const threshold = visibilityThreshold(Math.max(reported_total, sum_holders));

  const categories: SunburstCategory[] = [];

  if (input.institutions_total !== null && input.institutions_total > 0) {
    categories.push(
      buildCategoryFromTotal(
        "institutions",
        input.institutions_total,
        inst_holders,
        threshold,
        false,
        input.institutions_as_of ?? null,
      ),
    );
  }
  if (input.etfs_total !== null && input.etfs_total > 0) {
    categories.push(
      buildCategoryFromTotal(
        "etfs",
        input.etfs_total,
        etf_holders,
        threshold,
        false,
        input.etfs_as_of ?? null,
      ),
    );
  }
  if (input.insiders_total !== null && input.insiders_total > 0) {
    categories.push(
      buildCategoryFromTotal(
        "insiders",
        input.insiders_total,
        insider_holders,
        threshold,
        true, // bypass threshold — every officer surfaces
        input.insiders_as_of ?? null,
      ),
    );
  }
  if (input.blockholders_total !== null && input.blockholders_total > 0) {
    categories.push(
      buildCategoryFromTotal(
        "blockholders",
        input.blockholders_total,
        blockholder_holders,
        threshold,
        true, // bypass threshold — every ≥5% block is, by definition, large enough
        input.blockholders_as_of ?? null,
      ),
    );
  }
  if (input.treasury_shares !== null && input.treasury_shares > 0) {
    categories.push({
      key: "treasury",
      label: CATEGORY_LABEL.treasury,
      shares: input.treasury_shares,
      reported_total: input.treasury_shares,
      resolved_leaf_shares: input.treasury_shares,
      leaves: [
        {
          key: "treasury",
          label: "Treasury",
          shares: input.treasury_shares,
          is_other: false,
        },
      ],
      within_category_gap: 0,
      as_of_date: input.treasury_as_of ?? null,
    });
  }

  // Cross-category oversubscription guard. If reported category
  // totals sum to MORE than input total_shares, bump the effective
  // denominator to the sum so Recharts doesn't silently renormalise
  // ring 2 to occupy 360° at the wrong proportions.
  const sum_known = categories.reduce((s, c) => s + c.shares, 0);
  const total_shares = Math.max(reported_total, sum_known);
  const category_residual = total_shares - sum_known;

  return {
    total_shares,
    reported_total,
    categories,
    category_residual,
  };
}

function buildCategoryFromTotal(
  key: CategoryKey,
  reported_total: number,
  holders: readonly SunburstHolder[],
  threshold: number,
  bypass_threshold: boolean,
  as_of_date: string | null,
): SunburstCategory {
  if (holders.length === 0) {
    // Total from upstream API but zero per-filer detail (e.g.
    // CUSIP-backfill #740 dropped every filer at ingest). Middle
    // ring renders the category total; outer ring is one big
    // transparent within-category gap.
    return {
      key,
      label: CATEGORY_LABEL[key],
      shares: reported_total,
      reported_total,
      resolved_leaf_shares: 0,
      leaves: [],
      within_category_gap: reported_total,
      as_of_date,
    };
  }

  // Sort largest-first so wedges read counter-clockwise from
  // 12 o'clock with the dominant holders most visually prominent.
  const sorted = [...holders].sort((a, b) => b.shares - a.shares);

  const visible: SunburstLeaf[] = [];
  const tail: SunburstHolder[] = [];

  for (const h of sorted) {
    const passes = bypass_threshold || h.shares >= threshold;
    if (passes) {
      visible.push({
        key: h.key,
        label: h.label,
        shares: h.shares,
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
      key: `${key}-other`,
      label: `Other ${CATEGORY_LABEL[key].toLowerCase()}`,
      shares: aggregate_shares,
      is_other: true,
      tail_meta: {
        count: tail.length,
        aggregate_shares,
        largest_label: largest.label,
        largest_shares: largest.shares,
      },
    });
  }

  const resolved_leaf_shares = leaves.reduce((s, l) => s + l.shares, 0);

  // Snapshot-lag bump: if filer detail sums to MORE than the
  // upstream-reported category total, the leaves are more recent
  // ground truth (13F filings post-date the aggregate snapshot).
  // Use ``max(reported, sum_of_leaves)`` so ring 3 fits inside
  // ring 2 geometrically; the ``reported_total`` field is preserved
  // for diagnostic copy.
  const shares = Math.max(reported_total, resolved_leaf_shares);
  const within_category_gap = shares - resolved_leaf_shares;

  return {
    key,
    label: CATEGORY_LABEL[key],
    shares,
    reported_total,
    resolved_leaf_shares,
    leaves,
    within_category_gap,
    as_of_date,
  };
}
