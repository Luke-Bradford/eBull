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

export interface SunburstCategory {
  readonly key: string; // 'institutions' | 'etfs' | 'insiders' | 'treasury' | 'unallocated'
  readonly label: string;
  readonly shares: number;
  readonly pct: number; // share / float
  readonly status: SunburstCategoryStatus;
  /** Outer-ring wedges within this category. */
  readonly leaves: readonly SunburstLeaf[];
}

export interface SunburstLeaf {
  readonly key: string; // stable id for click-drill (filer cik, officer name, "other-etfs")
  readonly label: string;
  readonly shares: number;
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

export interface SunburstRings {
  readonly free_float: number;
  /** ring 1 — single inner-ring arc representing "Held" total. */
  readonly inner: { readonly shares: number; readonly pct: number };
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
      bypass_threshold: true,
    },
    insider_holders,
    float,
    threshold,
  );

  // Treasury renders as a single leaf wedge.
  const treasury_shares = input.treasury_shares ?? 0;
  const treasury: SunburstCategory = {
    key: "treasury",
    label: CATEGORY_LABEL.treasury!,
    shares: treasury_shares,
    pct: treasury_shares / float,
    status: input.treasury_shares === null ? "unknown" : treasury_shares > 0 ? "ok" : "empty",
    leaves:
      treasury_shares > 0
        ? [
            {
              key: "treasury",
              label: "Treasury",
              shares: treasury_shares,
              pct: treasury_shares / float,
              is_other: false,
            },
          ]
        : [],
  };

  // Unallocated absorbs whatever's left after every known category. When
  // any category is ``unknown`` we cannot derive Unallocated cleanly —
  // emit it as ``unknown`` so the visual signals "we don't know what
  // sits here" rather than reporting a fabricated residual.
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
  const unallocated: SunburstCategory = {
    key: "unallocated",
    label: CATEGORY_LABEL.unallocated!,
    shares: residual_shares,
    pct: residual_shares / float,
    status: has_unknown ? "unknown" : residual_shares > 0 ? "ok" : "empty",
    leaves: [
      {
        key: "unallocated",
        label: has_unknown ? "Coverage gap (#740)" : "Unallocated",
        shares: residual_shares,
        pct: residual_shares / float,
        is_other: false,
      },
    ],
  };

  const categories = [institutions, etfs, insiders, treasury, unallocated];

  // Inner-ring "Held" = sum of every known category. When any
  // category is ``unknown``, ``inner`` reports the known portion only;
  // the visible gap on ring 1 implicitly conveys "not all held shares
  // are accounted for".
  const inner_shares = known_shares + residual_shares;
  const inner_pct = inner_shares / float;

  return {
    free_float: float,
    inner: { shares: inner_shares, pct: inner_pct },
    categories,
  };
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
      pct: 0,
      status: "unknown",
      leaves: [
        {
          key: `${spec.key}-unknown`,
          label: "Coverage gap (#740)",
          shares: 0,
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
    pct: total_shares / float,
    status: "ok",
    leaves,
  };
}
