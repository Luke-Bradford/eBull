import { apiFetch } from "@/api/client";
import type {
  AllocationUpdateRequest,
  AllocationUpdateResponse,
  FiredSignalsResponse,
  StrategyAdvanceResponse,
  StrategyOperatorAction,
  StrategyOverviewResponse,
  StrategyEvidenceRefreshResponse,
  StrategyOwnedPositionsResponse,
  StrategyPaperPool,
  StrategyPnlHistoryResponse,
  StrategyPositionCloseResponse,
  StrategySizingUpdateResponse,
} from "@/api/types";

export function fetchStrategyOverview(): Promise<StrategyOverviewResponse> {
  return apiFetch("/strategies/overview");
}

export function requestStrategyEvidenceRefresh(): Promise<StrategyEvidenceRefreshResponse> {
  return apiFetch("/strategies/evidence-refresh", { method: "POST" });
}

export function fetchFiredSignals(cursor: number | null, strategyId?: string): Promise<FiredSignalsResponse> {
  const params = new URLSearchParams();
  params.set("limit", "15");
  if (cursor !== null) params.set("cursor", String(cursor));
  if (strategyId) params.set("strategy_id", strategyId);
  const query = params.size === 0 ? "" : `?${params.toString()}`;
  return apiFetch(`/strategies/signals${query}`);
}

export function fetchStrategyPnlHistory(): Promise<StrategyPnlHistoryResponse> {
  return apiFetch("/strategies/wealth-history");
}

export function fetchStrategyOwnedPositions(): Promise<StrategyOwnedPositionsResponse> {
  return apiFetch("/strategies/positions");
}

export function closeStrategyOwnedPosition(
  strategyTradeId: number,
  brokerPositionId: number,
): Promise<StrategyPositionCloseResponse> {
  return apiFetch(
    `/strategies/positions/${strategyTradeId}/${brokerPositionId}/close`,
    { method: "POST" },
  );
}

export function updateStrategyPaperPool(body: {
  enabled: boolean;
  capital_limit: string;
  capital_mode: "fixed" | "compound";
  risk_profile: "unconfigured" | "cautious" | "balanced" | "growth";
  reason: string;
}): Promise<StrategyPaperPool> {
  return apiFetch("/strategies/paper-pool", {
    method: "PUT",
    body: JSON.stringify(body),
  });
}

/** Advance one strategy by one ordered step (#2770).
 *
 * ⚠ Sends the ACTION and a reason, and nothing else. No `strategy_version`, no
 * `to_stage`, no `result_ids` — those are the three inputs that would let this
 * client choose its own promotion denominator, and the endpoint does not accept
 * them. The evidence is assembled server-side inside the locked transaction.
 */
export function advanceStrategyStage(
  strategyId: string,
  body: { action: StrategyOperatorAction; reason: string },
): Promise<StrategyAdvanceResponse> {
  return apiFetch(`/strategies/${encodeURIComponent(strategyId)}/advance`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function updateStrategySizing(
  strategyId: string,
  body: {
    strategy_version: string;
    ticket_sizing_mode: "percent" | "fixed";
    ticket_value: string;
    max_ticket_amount: string;
    reason: string;
  },
): Promise<StrategySizingUpdateResponse> {
  return apiFetch(`/strategies/${encodeURIComponent(strategyId)}/sizing`, {
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
