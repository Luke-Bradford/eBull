import { apiFetch } from "@/api/client";
import type { CalendarEvents, CalendarScope } from "@/api/types";

export function fetchCalendarEvents(
  scope: CalendarScope = "portfolio",
  days = 7,
): Promise<CalendarEvents> {
  const params = new URLSearchParams({ scope, days: String(days) });
  return apiFetch<CalendarEvents>(`/calendar/events?${params.toString()}`);
}
