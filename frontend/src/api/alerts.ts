import { apiFetch } from "@/api/client";
import type {
  CoverageStatusDropsResponse,
  GuardRejectionsResponse,
  PositionAlertsResponse,
  RankMovesResponse,
  ThesisChangesResponse,
  ThesisStalenessResponse,
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

// --- #1922 rank-move endpoints ----------------------------------------------

export function fetchRankMoves(): Promise<RankMovesResponse> {
  return apiFetch<RankMovesResponse>("/alerts/rank-moves");
}

export function markRankMovesSeen(
  seenThroughRankEventId: number,
): Promise<void> {
  return apiFetch<void>("/alerts/rank-moves/seen", {
    method: "POST",
    body: JSON.stringify({ seen_through_rank_event_id: seenThroughRankEventId }),
  });
}

export function dismissAllRankMoves(): Promise<void> {
  return apiFetch<void>("/alerts/rank-moves/dismiss-all", { method: "POST" });
}

// --- #2013 thesis-change endpoints -------------------------------------------
// Cursor feed on theses.thesis_id; a dismiss with the newest listed id clears
// everything older, so there is no separate dismiss-all endpoint.

export function fetchThesisChanges(): Promise<ThesisChangesResponse> {
  return apiFetch<ThesisChangesResponse>("/alerts/thesis-changes");
}

export function markThesisChangesSeen(seenThroughThesisId: number): Promise<void> {
  return apiFetch<void>("/alerts/thesis-changes/seen", {
    method: "POST",
    body: JSON.stringify({ seen_through_thesis_id: seenThroughThesisId }),
  });
}

// --- #1902 thesis-staleness snapshot ----------------------------------------
// Standing condition, no cursor endpoints — clears when the thesis
// regenerates, not when acknowledged.

export function fetchThesisStaleness(): Promise<ThesisStalenessResponse> {
  return apiFetch<ThesisStalenessResponse>("/alerts/thesis-staleness");
}
