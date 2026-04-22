import { apiFetch } from "@/api/client";
import type {
  CoverageStatusDropsResponse,
  GuardRejectionsResponse,
  PositionAlertsResponse,
} from "@/api/types";

/**
 * Fetchers for the alerts endpoints.
 *
 * Mirrors:
 *   GET /alerts/guard-rejections              -> app/api/alerts.py
 *   POST /alerts/seen                         -> app/api/alerts.py
 *   POST /alerts/dismiss-all                  -> app/api/alerts.py
 *
 * All endpoints are protected by require_session_or_service_token
 * (cookie auth; apiFetch passes credentials: include). Errors bubble
 * as ApiError(status, detail) — the backend's `detail` string is a
 * fixed phrase so callers may surface it verbatim via `error.message`.
 *
 * Contract: no business logic here. Typed wrapper only. Anything
 * resembling validation, retry, caching, or analytics belongs in
 * the calling component.
 */

export function fetchGuardRejections(): Promise<GuardRejectionsResponse> {
  return apiFetch<GuardRejectionsResponse>("/alerts/guard-rejections");
}

export function markAlertsSeen(seenThroughDecisionId: number): Promise<void> {
  return apiFetch<void>("/alerts/seen", {
    method: "POST",
    body: JSON.stringify({ seen_through_decision_id: seenThroughDecisionId }),
  });
}

export function dismissAllAlerts(): Promise<void> {
  return apiFetch<void>("/alerts/dismiss-all", { method: "POST" });
}

// --- #396/#401 position-alert endpoints -------------------------------------

export function fetchPositionAlerts(): Promise<PositionAlertsResponse> {
  return apiFetch<PositionAlertsResponse>("/alerts/position-alerts");
}

export function markPositionAlertsSeen(
  seenThroughPositionAlertId: number,
): Promise<void> {
  return apiFetch<void>("/alerts/position-alerts/seen", {
    method: "POST",
    body: JSON.stringify({
      seen_through_position_alert_id: seenThroughPositionAlertId,
    }),
  });
}

export function dismissAllPositionAlerts(): Promise<void> {
  return apiFetch<void>("/alerts/position-alerts/dismiss-all", {
    method: "POST",
  });
}

// --- #397/#402 coverage-status-drops endpoints ------------------------------

export function fetchCoverageStatusDrops(): Promise<CoverageStatusDropsResponse> {
  return apiFetch<CoverageStatusDropsResponse>("/alerts/coverage-status-drops");
}

export function markCoverageStatusDropsSeen(
  seenThroughEventId: number,
): Promise<void> {
  return apiFetch<void>("/alerts/coverage-status-drops/seen", {
    method: "POST",
    body: JSON.stringify({ seen_through_event_id: seenThroughEventId }),
  });
}

export function dismissAllCoverageStatusDrops(): Promise<void> {
  return apiFetch<void>("/alerts/coverage-status-drops/dismiss-all", {
    method: "POST",
  });
}
