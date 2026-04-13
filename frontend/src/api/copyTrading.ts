import { apiFetch } from "@/api/client";
import type { CopyTradingResponse } from "@/api/types";

export function fetchCopyTrading(): Promise<CopyTradingResponse> {
  return apiFetch<CopyTradingResponse>("/portfolio/copy-trading");
}
