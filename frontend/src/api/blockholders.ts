/**
 * Client for /instruments/{symbol}/blockholders (#766 PR 3).
 *
 * Surfaces the latest non-superseded 13D / 13G filing per primary
 * filer per issuer — one row per ≥5% block on the cap table. The
 * ownership card (#729) renders this as the 5th sunburst category
 * alongside Institutions / ETFs / Insiders / Treasury.
 *
 * Joint-filing reporters (e.g. a fund + its principals all claiming
 * the same block) collapse to one row per primary filer; the
 * ``additional_reporters`` count surfaces the joint-filing depth so
 * the operator can drill in for detail without the totals slice
 * double-counting them.
 *
 * `totals` is `null` when no 13D/G filings on file (non-covered or
 * pre-ingest instrument); the consumer renders the no-coverage
 * empty-state.
 */

import { apiFetch } from "@/api/client";

export interface BlockholdersTotals {
  /** Sum of per-block aggregate_amount_owned across every primary
   *  filer's latest non-superseded filing on this issuer. Decimal
   *  as string. */
  readonly blockholders_shares: string;
  /** 13D-only subtotal. Decimal as string. */
  readonly active_shares: string;
  /** 13G-only subtotal. Decimal as string. */
  readonly passive_shares: string;
  /** Distinct primary-filer count = block count. */
  readonly total_filers: number;
  /** Latest filed_at across the included blocks (ISO yyyy-mm-dd).
   *  Null when every included filing has a NULL signature date —
   *  rare, but the parser leaves filed_at unset on malformed
   *  signature blocks. The freshness chip renders neutrally in that
   *  case. */
  readonly as_of_date: string | null;
}

export interface BlockholderRow {
  /** Primary filer's CIK — the EDGAR submitter for the accession. */
  readonly filer_cik: string;
  /** Primary filer's name (operator-facing label). */
  readonly filer_name: string;
  /** Representative reporter's CIK; null for natural persons /
   *  family trusts that have no EDGAR CIK. */
  readonly reporter_cik: string | null;
  /** Representative reporter's name (largest-aggregate of the joint
   *  filing). */
  readonly reporter_name: string;
  /** 'SCHEDULE 13D' | 'SCHEDULE 13D/A' | 'SCHEDULE 13G' | 'SCHEDULE 13G/A'. */
  readonly submission_type: string;
  /** 'active' (13D / 13D/A) | 'passive' (13G / 13G/A). */
  readonly status: string;
  readonly accession_number: string;
  /** Decimal as string. May be null if the filing references the
   *  prior cover page rather than restating numbers. */
  readonly aggregate_amount_owned: string | null;
  /** Decimal as string (e.g. "5.5000"). Null under the same
   *  defer-to-prior rule. */
  readonly percent_of_class: string | null;
  /** Joint-filing co-reporters omitted from this row. ``0`` when the
   *  filing has only one reporting person. */
  readonly additional_reporters: number;
  /** SEC item 4 / 13G event date (ISO yyyy-mm-dd). */
  readonly date_of_event: string | null;
  /** Filing signature date coerced to UTC midnight (ISO datetime). */
  readonly filed_at: string | null;
}

export interface BlockholdersResponse {
  readonly symbol: string;
  /** Null when no 13D/G blocks on file for this instrument. */
  readonly totals: BlockholdersTotals | null;
  /** Top-N blocks by aggregate_amount_owned DESC; capped at the
   *  request limit. */
  readonly blockholders: readonly BlockholderRow[];
}

export function fetchBlockholders(
  symbol: string,
  limit: number = 50,
): Promise<BlockholdersResponse> {
  const params = new URLSearchParams({ limit: String(limit) });
  return apiFetch<BlockholdersResponse>(
    `/instruments/${encodeURIComponent(symbol)}/blockholders?${params.toString()}`,
  );
}
