import { apiFetch } from "@/api/client";
import type { FiredSignalsResponse, StrategyOverviewResponse } from "@/api/types";

export function fetchStrategyOverview(): Promise<StrategyOverviewResponse> {
  return apiFetch("/strategies/overview");
}

export function fetchFiredSignals(cursor: number | null): Promise<FiredSignalsResponse> {
  const params = new URLSearchParams();
  if (cursor !== null) params.set("cursor", String(cursor));
  const query = params.size === 0 ? "" : `?${params.toString()}`;
  return apiFetch(`/strategies/signals${query}`);
}
