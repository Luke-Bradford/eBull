import { apiFetch } from "@/api/client";

/** Response shape of `GET /market-calendar/us/{year}` (#609). Dates are
 *  `America/New_York` civil dates (`YYYY-MM-DD`). */
export interface UsMarketCalendar {
  year: number;
  full_closures: string[];
  half_days: string[];
}

export function fetchUsMarketCalendar(year: number): Promise<UsMarketCalendar> {
  return apiFetch<UsMarketCalendar>(`/market-calendar/us/${year}`);
}
