/**
 * Insider Form 4 row eligibility for ownership-card consumers.
 *
 * Single source of truth for "is this Form 4 row part of the holdings
 * snapshot the ring renders?". Used by:
 *
 *   * the per-filer holders aggregator on L1 (``OwnershipPanel``)
 *     and L2 (``OwnershipPage``) — drives ring 3 wedges.
 *   * the L2 ``buildFilerRows`` table writer — drives the per-filer
 *     drilldown rows.
 *   * the freshness chip's ``insiders_as_of`` derivation (#767) —
 *     drives the chip's age delta.
 *
 * Codex (review of #767 round 1) flagged that the chip's date predicate
 * had drifted to "any non-derivative row" while the aggregators required
 * a parseable ``post_transaction_shares``. Fix landed two independent
 * copies of the predicate — the review bot then caught that the
 * "structural fix" itself reintroduced the same drift class. Extracting
 * here so the predicate lives in exactly one place.
 */

import { parseShareCount } from "@/components/instrument/ownershipMetrics";

/** Shape every consumer of the insider transactions endpoint reads. The
 *  full row carries more fields; consumers TypeScript-narrow to this
 *  subset. */
export interface InsiderRowShape {
  readonly filer_cik: string | null;
  readonly filer_name: string;
  readonly txn_date: string;
  readonly post_transaction_shares: string | null;
  readonly is_derivative: boolean;
}

/**
 * True when a Form 4 row should count toward the holdings snapshot.
 *
 * Excludes derivative-table rows (option grants / RSUs aren't held
 * shares of the underlying) and rows with no parseable
 * ``post_transaction_shares`` (the value the aggregator would attribute
 * to the filer is missing — counting the row as fresh-but-zero would
 * advance the freshness chip without contributing to the ring).
 */
export function isInsiderHoldingRow(row: InsiderRowShape): boolean {
  if (row.is_derivative) return false;
  return parseShareCount(row.post_transaction_shares) !== null;
}
