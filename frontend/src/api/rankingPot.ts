import { apiFetch } from "@/api/client";
import type {
  RankingPotReadoutResponse,
  RankingPotStatusResponse,
  RankingPotV2ReadoutResponse,
  RankingPotV2StatusResponse,
} from "@/api/types";

/** #3592 slice 4c: ranking-pot-v2 (shadow only) — state, jobs, rebalances, looks, shadow holdings with reasons. */
export function fetchRankingPotV2Status(): Promise<RankingPotV2StatusResponse> {
  return apiFetch("/ranking-pot/v2/status");
}

/** #3592 slice 4c: v2's §7 readout at the latest stepped session. */
export function fetchRankingPotV2Readout(): Promise<RankingPotV2ReadoutResponse> {
  return apiFetch("/ranking-pot/v2/readout");
}

/** #2842 slice 7: the ranking pot's state, jobs, rebalances, looks and positions with tickets. */
export function fetchRankingPotStatus(): Promise<RankingPotStatusResponse> {
  return apiFetch("/ranking-pot/status");
}

/** #2842 slice 7: the §9.4 readout at the latest stepped session (its own call: it walks every step row). */
export function fetchRankingPotReadout(): Promise<RankingPotReadoutResponse> {
  return apiFetch("/ranking-pot/readout");
}
