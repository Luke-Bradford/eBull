import { apiFetch } from "@/api/client";
import type { InstrumentPositionDetail, PortfolioResponse } from "@/api/types";

export function fetchPortfolio(): Promise<PortfolioResponse> {
  return apiFetch<PortfolioResponse>("/portfolio");
}

export function fetchInstrumentPositions(instrumentId: number): Promise<InstrumentPositionDetail> {
  return apiFetch<InstrumentPositionDetail>(`/portfolio/instruments/${instrumentId}`);
}
