import { apiFetch } from "@/api/client";
import type { CalendarEvents, CalendarScope } from "@/api/types";

export function fetchCalendarEvents(scope: CalendarScope = "portfolio"): Promise<CalendarEvents> {
  const params = new URLSearchParams({ scope });
  return apiFetch<CalendarEvents>(`/calendar/events?${params.toString()}`);
}
