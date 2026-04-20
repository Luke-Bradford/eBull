import { apiFetch } from "@/api/client";

export interface WatchlistItem {
  instrument_id: number;
  symbol: string;
  company_name: string;
  exchange: string | null;
  currency: string | null;
  sector: string | null;
  added_at: string;
  notes: string | null;
}

export interface WatchlistListResponse {
  items: WatchlistItem[];
  total: number;
}

export function fetchWatchlist(): Promise<WatchlistListResponse> {
  return apiFetch<WatchlistListResponse>("/watchlist");
}

export function addToWatchlist(
  symbol: string,
  notes?: string | null,
): Promise<WatchlistItem> {
  return apiFetch<WatchlistItem>("/watchlist", {
    method: "POST",
    body: JSON.stringify({ symbol, notes }),
    headers: { "Content-Type": "application/json" },
  });
}

export function removeFromWatchlist(symbol: string): Promise<void> {
  return apiFetch<void>(`/watchlist/${encodeURIComponent(symbol)}`, {
    method: "DELETE",
  });
}
