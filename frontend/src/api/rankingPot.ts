import { apiFetch } from "@/api/client";
import type { RankingPotReadoutResponse, RankingPotStatusResponse } from "@/api/types";

/** #2842 slice 7: the ranking pot's state, jobs, rebalances, looks and positions with tickets. */
export function fetchRankingPotStatus(): Promise<RankingPotStatusResponse> {
  return apiFetch("/ranking-pot/status");
}

/** #2842 slice 7: the §9.4 readout at the latest stepped session (its own call: it walks every step row). */
export function fetchRankingPotReadout(): Promise<RankingPotReadoutResponse> {
  return apiFetch("/ranking-pot/readout");
}
