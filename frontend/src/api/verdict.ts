import { apiFetch } from "@/api/client";
import type { VerdictResponse } from "@/api/types";

/**
 * Latest per-instrument score + Instrument Analytical Record (#1824, P3 of
 * #1815). Returns `score: null` when the instrument has never been scored
 * (200 + null). Pre-#1823 rows return `score` with `analytics_json: null`.
 */
export function fetchScoreVerdict(instrumentId: number): Promise<VerdictResponse> {
  return apiFetch<VerdictResponse>(`/rankings/verdict/${instrumentId}`);
}
