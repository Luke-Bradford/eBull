/**
 * Fetcher + response mirror for ``GET
 * /instruments/{symbol}/ownership-history`` (#840.F, aggregate mode
 * #922). Mirrors ``OwnershipHistoryResponse`` /
 * ``OwnershipHistoryPointResponse`` in ``app/api/instruments.py``
 * field-for-field — update both sides in the same PR.
 */
import { apiFetch } from "@/api/client";

export type OwnershipHistoryCategory =
  | "insiders"
  | "blockholders"
  | "institutions"
  | "treasury"
  | "def14a";

export interface OwnershipHistoryPoint {
  /** ISO ``YYYY-MM-DD`` valid-time bucket end. */
  readonly period_end: string;
  readonly ownership_nature: string;
  /** Decimal-as-string. */
  readonly shares: string | null;
  readonly source: string;
  /** ``null`` on aggregate points — a category total has no single
   *  accession. */
  readonly source_accession: string | null;
  readonly filed_at: string | null;
  /** Filers contributing to an aggregate bucket (#922); ``null`` on
   *  per-holder series and issuer-level treasury points. */
  readonly holder_count: number | null;
}

export interface OwnershipHistoryResponse {
  readonly symbol: string;
  readonly instrument_id: number;
  readonly category: string;
  readonly holder_id: string | null;
  readonly points: readonly OwnershipHistoryPoint[];
}

export interface FetchOwnershipHistoryParams {
  readonly category: OwnershipHistoryCategory;
  readonly holderId?: string;
  readonly aggregate?: boolean;
  /** Inclusive ``period_end`` lower bound, ISO ``YYYY-MM-DD``. */
  readonly fromDate?: string;
}

export function fetchOwnershipHistory(
  symbol: string,
  params: FetchOwnershipHistoryParams,
): Promise<OwnershipHistoryResponse> {
  const qs = new URLSearchParams({ category: params.category });
  if (params.holderId !== undefined) qs.set("holder_id", params.holderId);
  if (params.aggregate === true) qs.set("aggregate", "true");
  if (params.fromDate !== undefined) qs.set("from_date", params.fromDate);
  return apiFetch<OwnershipHistoryResponse>(
    `/instruments/${encodeURIComponent(symbol)}/ownership-history?${qs.toString()}`,
  );
}
