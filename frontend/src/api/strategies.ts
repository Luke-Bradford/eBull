import { apiFetch } from "@/api/client";
import type {
  AllocationUpdateRequest,
  AllocationUpdateResponse,
  FiredSignalsResponse,
  StrategyOverviewResponse,
  StrategyPaperPool,
  StrategyPnlHistoryResponse,
} from "@/api/types";

export function fetchStrategyOverview(): Promise<StrategyOverviewResponse> {
  return apiFetch("/strategies/overview");
}

export function fetchFiredSignals(cursor: number | null, strategyId?: string): Promise<FiredSignalsResponse> {
  const params = new URLSearchParams();
  if (cursor !== null) params.set("cursor", String(cursor));
  if (strategyId) params.set("strategy_id", strategyId);
  const query = params.size === 0 ? "" : `?${params.toString()}`;
  return apiFetch(`/strategies/signals${query}`);
}

export function fetchStrategyPnlHistory(): Promise<StrategyPnlHistoryResponse> {
  return apiFetch("/strategies/pnl-history");
}

export function updateStrategyPaperPool(body: {
  enabled: boolean;
  capital_limit: string;
  reason: string;
}): Promise<StrategyPaperPool> {
  return apiFetch("/strategies/paper-pool", {
    method: "PUT",
    body: JSON.stringify(body),
  });
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
