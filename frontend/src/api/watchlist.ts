import { apiFetch } from "@/api/client";

export interface WatchlistItem {
  instrument_id: number;
  symbol: string;
  company_name: string;
  exchange: string | null;
  currency: string | null;
  sector: string | null; // eToro numeric industry id (provider contract)
  sector_name: string | null; // resolved eToro industry name — non-SEC fallback label
  gics_sector: string | null; // #1851: SEC-SIC-derived GICS sector — preferred label
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
