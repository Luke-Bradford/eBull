import { apiFetch } from "@/api/client";
import type {
  AllocationUpdateRequest,
  AllocationUpdateResponse,
  FiredSignalsResponse,
  StrategyOverviewResponse,
} from "@/api/types";

export function fetchStrategyOverview(): Promise<StrategyOverviewResponse> {
  return apiFetch("/strategies/overview");
}

export function fetchFiredSignals(cursor: number | null): Promise<FiredSignalsResponse> {
  const params = new URLSearchParams();
  if (cursor !== null) params.set("cursor", String(cursor));
  const query = params.size === 0 ? "" : `?${params.toString()}`;
  return apiFetch(`/strategies/signals${query}`);
}

export function updateStrategyAllocation(
  strategyId: string,
  request: AllocationUpdateRequest,
): Promise<AllocationUpdateResponse> {
  return apiFetch(`/strategies/${encodeURIComponent(strategyId)}/allocation`, {
    method: "PUT",
    body: JSON.stringify(request),
  });
}
