import { apiFetch } from "@/api/client";
import type { NewsItem, NewsListResponse } from "@/api/types";

export function fetchNews(
  instrumentId: number,
  offset = 0,
  limit = 10,
  since?: string,
): Promise<NewsListResponse> {
  const params = new URLSearchParams({
    offset: String(offset),
    limit: String(limit),
  });
  if (since !== undefined) params.set("since", since);
  return apiFetch<NewsListResponse>(
    `/news/${instrumentId}?${params.toString()}`,
  );
}

// Endpoint page cap (`app/api/news.py` MAX_PAGE_LIMIT) + a hard page bound so
// a hot name can't spin an unbounded loop. 10 × 200 = 2000 rows max.
const PAGE_LIMIT = 200;
const MAX_PAGES = 10;
const DAY_MS = 86_400_000;

export interface AllNews {
  readonly items: NewsItem[];
  readonly total: number;
  readonly symbol: string | null;
  /** True when MAX_PAGES was hit before draining `total` — the charts then
   *  show the newest `items.length` of `total` and the page captions it. */
  readonly capped: boolean;
}

/**
 * Fetch the full news window for the analytics drill, paginating the
 * 200-row-capped endpoint until drained (Codex ckpt-1 #1 — a single fetch
 * would silently truncate a high-volume name to its newest 200 rows).
 */
export async function fetchAllNews(
  instrumentId: number,
  sinceDays = 365,
): Promise<AllNews> {
  const since = new Date(Date.now() - sinceDays * DAY_MS).toISOString();
  const items: NewsItem[] = [];
  let total = 0;
  let symbol: string | null = null;
  let capped = false;
  for (let page = 0; page < MAX_PAGES; page++) {
    const res = await fetchNews(instrumentId, page * PAGE_LIMIT, PAGE_LIMIT, since);
    total = res.total;
    symbol = res.symbol;
    items.push(...res.items);
    if (items.length >= res.total || res.items.length === 0) break;
    if (page === MAX_PAGES - 1) capped = items.length < res.total;
  }
  return { items, total, symbol, capped };
}
