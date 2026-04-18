/**
 * Coverage admin API client (#268 Chunk H).
 *
 * Surfaces:
 *   - fetchCoverageSummary() — counts by filings_status across all
 *     tradable instruments. Powers the AdminPage "Filings coverage"
 *     card. Wraps GET /coverage/summary.
 *   - fetchCoverageInsufficient() — drill-down list of instruments
 *     stuck in insufficient / structurally_young states. Powers the
 *     /admin/coverage/insufficient route. Wraps GET /coverage/insufficient.
 */

import { apiFetch } from "@/api/client";
import type {
  CoverageSummaryResponse,
  InsufficientListResponse,
} from "@/api/types";

export function fetchCoverageSummary(): Promise<CoverageSummaryResponse> {
  return apiFetch<CoverageSummaryResponse>("/coverage/summary");
}

export function fetchCoverageInsufficient(): Promise<InsufficientListResponse> {
  return apiFetch<InsufficientListResponse>("/coverage/insufficient");
}
