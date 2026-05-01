/**
 * Pure-math helpers for the ownership reporting card (#729).
 *
 * Slice computation contract:
 *
 *   denominator = shares_outstanding − treasury_shares  (free float)
 *
 *   institutions_pct = institutions_shares / denominator
 *   etfs_pct         = etfs_shares         / denominator
 *   insiders_pct     = insiders_shares     / denominator
 *   treasury_pct     = treasury_shares     / shares_outstanding   (memo line — uses TOTAL outstanding, not float, because treasury IS the diff between outstanding and float)
 *   unallocated_pct  = 1 − (institutions_pct + etfs_pct + insiders_pct)
 *
 * Empty-state semantics:
 *   * shares_outstanding missing → return null (the card renders the
 *     no-coverage fallback for the whole panel).
 *   * Per-slice input missing (e.g. no insider data) → that slice
 *     reports ``shares=null`` and ``pct=null``; the card renders
 *     "—" for that slice rather than fabricating a 0%.
 *   * Negative residual (slices sum > 100%) is clamped to 0 in
 *     ``unallocated_pct`` and surfaced via ``has_overflow``.
 *
 * All inputs are absolute share counts (never percentages). Decimal
 * inputs may arrive as either ``number`` or ``string`` (the API
 * returns NUMERICs as strings); :func:`parseShareCount` normalises.
 */

export interface OwnershipSliceInput {
  /** Sum of long-equity shares for this slice. ``null`` = no data. */
  readonly shares: number | null;
  /** Optional metadata for the per-slice link / empty-state copy. */
  readonly source_label?: string;
}

export interface OwnershipInputs {
  /** Total shares outstanding (denominator base). ``null`` = no data. */
  readonly shares_outstanding: number | null;
  /** Shares held in treasury — subtracted from outstanding for the float denominator. */
  readonly treasury_shares: number | null;
  readonly institutions: OwnershipSliceInput;
  readonly etfs: OwnershipSliceInput;
  readonly insiders: OwnershipSliceInput;
}

export interface OwnershipSlice {
  readonly label: string;
  readonly shares: number | null;
  readonly pct: number | null;
  readonly source_label: string | undefined;
}

export interface OwnershipBreakdown {
  /** Free-float denominator: shares_outstanding − treasury_shares. */
  readonly denominator: number;
  readonly slices: readonly OwnershipSlice[];
  /** Treasury memo line — uses TOTAL outstanding as denominator. */
  readonly treasury: OwnershipSlice;
  /** True when slices sum past 100% (rounding or stale data). */
  readonly has_overflow: boolean;
}

/**
 * Coerce a Decimal-as-string or number to a ``number`` share count.
 * Returns ``null`` for null / undefined / blank / non-numeric input.
 *
 * Uses ``Number()`` rather than ``parseFloat`` so input like
 * ``"1234abc"`` returns ``NaN`` (caught + mapped to null) rather than
 * silently parsing to ``1234``.
 */
export function parseShareCount(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  const trimmed = value.trim();
  if (trimmed === "") return null;
  const n = Number(trimmed);
  return Number.isFinite(n) ? n : null;
}

/**
 * Compute the ownership breakdown.
 *
 * Returns ``null`` when ``shares_outstanding`` is missing — the card
 * cannot derive any slice without a denominator.
 */
export function computeOwnership(input: OwnershipInputs): OwnershipBreakdown | null {
  if (input.shares_outstanding === null || input.shares_outstanding <= 0) return null;

  const treasury = input.treasury_shares ?? 0;
  const denominator = Math.max(0, input.shares_outstanding - treasury);
  if (denominator <= 0) return null;

  const sliceFrom = (label: string, slice: OwnershipSliceInput): OwnershipSlice => ({
    label,
    shares: slice.shares,
    pct: slice.shares === null ? null : slice.shares / denominator,
    source_label: slice.source_label,
  });

  const institutions = sliceFrom("Institutions", input.institutions);
  const etfs = sliceFrom("ETFs", input.etfs);
  const insiders = sliceFrom("Insiders", input.insiders);

  // Unallocated is residual = (1 − Σ slice_pct). Only meaningful
  // when every slice has a known share count; otherwise the
  // residual silently absorbs the unknown slices and overstates
  // genuine "unallocated equity". Render as null/"—" instead of
  // coercing null inputs to 0 — Codex caught this on PR review.
  const has_unknown_slice =
    institutions.shares === null || etfs.shares === null || insiders.shares === null;
  const allocated_pct =
    (institutions.pct ?? 0) + (etfs.pct ?? 0) + (insiders.pct ?? 0);
  const has_overflow = !has_unknown_slice && allocated_pct > 1;

  const unallocated_pct = has_unknown_slice ? null : Math.max(0, 1 - allocated_pct);
  const unallocated_shares =
    unallocated_pct === null ? null : Math.round(unallocated_pct * denominator);

  const unallocated: OwnershipSlice = {
    label: "Unallocated",
    shares: unallocated_shares,
    pct: unallocated_pct,
    source_label: has_unknown_slice ? "needs full slice coverage" : "residual",
  };

  return {
    denominator,
    slices: [institutions, etfs, insiders, unallocated],
    treasury: {
      label: "Treasury",
      shares: input.treasury_shares,
      pct: input.treasury_shares === null ? null : treasury / input.shares_outstanding,
      source_label: "10-K cover",
    },
    has_overflow,
  };
}

/**
 * Sum the latest post-transaction shares across distinct insider
 * filers. Insider data ships per-transaction; the slice contract
 * needs the most recent state per filer summed across the operator
 * group.
 *
 * Per the ownership-card spec: "Insiders (most-recent
 * post-transaction holdings from insider_transactions)". One row
 * per filer; pick the row with the latest ``txn_date`` per
 * ``filer_cik`` (or ``filer_name`` when CIK is null).
 */
export interface InsiderHoldingRow {
  readonly filer_cik: string | null;
  readonly filer_name: string;
  readonly txn_date: string;
  readonly post_transaction_shares: string | null;
  readonly is_derivative: boolean;
}

export function aggregateInsiderHoldings(
  rows: readonly InsiderHoldingRow[],
): number | null {
  // Latest non-derivative post-transaction-shares per filer.
  // Derivative positions (options) are excluded — same posture as
  // the institutional slice's PUT/CALL exclusion.
  const latestByFiler = new Map<string, { txn_date: string; shares: number }>();

  for (const row of rows) {
    if (row.is_derivative) continue;
    const shares = parseShareCount(row.post_transaction_shares);
    if (shares === null) continue;
    const key = row.filer_cik ?? `name:${row.filer_name}`;
    const existing = latestByFiler.get(key);
    if (existing === undefined || row.txn_date > existing.txn_date) {
      latestByFiler.set(key, { txn_date: row.txn_date, shares });
    }
  }

  if (latestByFiler.size === 0) return null;
  let total = 0;
  for (const entry of latestByFiler.values()) total += entry.shares;
  return total;
}

/**
 * Format a fractional ratio (0..1) as ``XX.XX%`` for display.
 * ``null`` -> ``"—"``. Used by the card.
 */
export function formatPct(pct: number | null): string {
  if (pct === null || !Number.isFinite(pct)) return "—";
  return `${(pct * 100).toFixed(2)}%`;
}

/**
 * Format an absolute share count with thousands separators.
 * ``null`` -> ``"—"``.
 */
export function formatShares(shares: number | null): string {
  if (shares === null || !Number.isFinite(shares)) return "—";
  return shares.toLocaleString("en-US", { maximumFractionDigits: 0 });
}
