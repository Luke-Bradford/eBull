import { apiFetch } from "@/api/client";
import type {
  BudgetConfigResponse,
  BudgetStateResponse,
  CapitalEventResponse,
} from "@/api/types";

export function fetchBudget(): Promise<BudgetStateResponse> {
  return apiFetch<BudgetStateResponse>("/budget");
}

export function fetchBudgetConfig(): Promise<BudgetConfigResponse> {
  return apiFetch<BudgetConfigResponse>("/budget/config");
}

export function fetchCapitalEvents(
  limit = 50,
  offset = 0,
): Promise<CapitalEventResponse[]> {
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  return apiFetch<CapitalEventResponse[]>(`/budget/events?${params}`);
}

export function createCapitalEvent(body: {
  event_type: "injection" | "withdrawal";
  amount: number;
  currency?: "USD" | "GBP";
  note?: string;
}): Promise<CapitalEventResponse> {
  return apiFetch<CapitalEventResponse>("/budget/events", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function updateBudgetConfig(body: {
  cash_buffer_pct?: number;
  cgt_scenario?: "basic" | "higher";
  updated_by: string;
  reason: string;
}): Promise<BudgetConfigResponse> {
  return apiFetch<BudgetConfigResponse>("/budget/config", {
    method: "PATCH",
    body: JSON.stringify(body),
  });
}
