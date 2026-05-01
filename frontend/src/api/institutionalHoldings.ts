/**
 * Client for /instruments/{symbol}/institutional-holdings (#730 PR 4).
 *
 * Surfaces the most recent quarter's 13F-HR holdings + per-slice
 * rollups so the ownership card (#729) can render Institutions vs
 * ETFs vs Insiders vs Treasury vs Unallocated. `totals` is `null`
 * when no holdings are on file (non-covered or pre-ingest
 * instrument); the consumer renders the no-coverage empty-state.
 */

import { apiFetch } from "@/api/client";

export interface InstitutionalHoldingsTotals {
  /** Latest period_of_report on file (ISO-8601 yyyy-mm-dd). */
  readonly period_of_report: string;
  /** Sum of equity shares held by INV/INS/BD/OTHER filers. Decimal as string. */
  readonly institutions_shares: string;
  /** Sum of equity shares held by ETF filers. Decimal as string. */
  readonly etfs_shares: string;
  /** Distinct filer count across the full drilldown (equity + PUT + CALL). */
  readonly total_filers: number;
  readonly total_institutions_filers: number;
  readonly total_etfs_filers: number;
}

export interface InstitutionalFilerHolding {
  readonly filer_cik: string;
  readonly filer_name: string;
  /** 'ETF' | 'INV' | 'INS' | 'BD' | 'OTHER'. */
  readonly filer_type: string;
  readonly accession_number: string;
  readonly period_of_report: string;
  readonly shares: string;
  readonly market_value_usd: string | null;
  /** 'SOLE' | 'SHARED' | 'NONE' | null. */
  readonly voting_authority: string | null;
  /** 'PUT' | 'CALL' | null (null = underlying equity). */
  readonly is_put_call: string | null;
}

export interface InstitutionalHoldingsResponse {
  readonly symbol: string;
  /** Null when no 13F-HR holdings on file for this instrument. */
  readonly totals: InstitutionalHoldingsTotals | null;
  /** Top-N filers by shares DESC; capped at the request limit. */
  readonly filers: readonly InstitutionalFilerHolding[];
}

export function fetchInstitutionalHoldings(
  symbol: string,
  limit: number = 50,
): Promise<InstitutionalHoldingsResponse> {
  const params = new URLSearchParams({ limit: String(limit) });
  return apiFetch<InstitutionalHoldingsResponse>(
    `/instruments/${encodeURIComponent(symbol)}/institutional-holdings?${params.toString()}`,
  );
}
