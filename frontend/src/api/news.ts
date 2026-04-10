import { apiFetch } from "@/api/client";
import type { NewsListResponse } from "@/api/types";

export function fetchNews(
  instrumentId: number,
  offset = 0,
  limit = 10,
): Promise<NewsListResponse> {
  const params = new URLSearchParams({
    offset: String(offset),
    limit: String(limit),
  });
  return apiFetch<NewsListResponse>(
    `/news/${instrumentId}?${params.toString()}`,
  );
}
