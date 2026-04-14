import { apiFetch } from "@/api/client";
import type { CopyTradingResponse, MirrorDetailResponse } from "@/api/types";

export function fetchCopyTrading(): Promise<CopyTradingResponse> {
  return apiFetch<CopyTradingResponse>("/portfolio/copy-trading");
}

export function fetchMirrorDetail(mirrorId: number): Promise<MirrorDetailResponse> {
  return apiFetch<MirrorDetailResponse>(`/portfolio/copy-trading/${mirrorId}`);
}
