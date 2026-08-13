import { apiFetch } from "@/api/client";
import type {
  ActivityResponse,
  InstrumentPositionDetail,
  PortfolioResponse,
  RollingPnlResponse,
  ValueHistoryRange,
  ValueHistoryResponse,
} from "@/api/types";

export function fetchPortfolio(): Promise<PortfolioResponse> {
  return apiFetch<PortfolioResponse>("/portfolio");
}

export function fetchInstrumentPositions(instrumentId: number): Promise<InstrumentPositionDetail> {
  return apiFetch<InstrumentPositionDetail>(`/portfolio/instruments/${instrumentId}`);
}

export function fetchRollingPnl(): Promise<RollingPnlResponse> {
  return apiFetch<RollingPnlResponse>("/portfolio/rolling-pnl");
}

export function fetchValueHistory(
  range: ValueHistoryRange,
): Promise<ValueHistoryResponse> {
  return apiFetch<ValueHistoryResponse>(
    `/portfolio/value-history?range=${encodeURIComponent(range)}`,
  );
}

export function fetchActivity(
  includeMirrors: boolean,
  instrumentId?: number,
  limit?: number,
): Promise<ActivityResponse> {
  const params = new URLSearchParams({ include_mirrors: String(includeMirrors) });
  // Scope the ledger to one instrument for the per-instrument Positions tab
  // trade-history section (#1926).
  if (instrumentId !== undefined) params.set("instrument_id", String(instrumentId));
  if (limit !== undefined) params.set("limit", String(limit));
  return apiFetch<ActivityResponse>(`/portfolio/activity?${params}`);
}
