import { apiFetch } from "@/api/client";
import type { PortfolioResponse } from "@/api/types";

export function fetchPortfolio(): Promise<PortfolioResponse> {
  return apiFetch<PortfolioResponse>("/portfolio");
}
