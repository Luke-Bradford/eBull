import { apiFetch } from "@/api/client";
import type {
  ClosePositionRequest,
  OrderResponse,
  PlaceOrderRequest,
} from "@/api/types";

/**
 * Fetchers for the operator-facing order endpoints.
 *
 * Mirrors:
 *   POST /portfolio/orders                      -> app/api/orders.py:405
 *   POST /portfolio/positions/{id}/close        -> app/api/orders.py:473
 *
 * Both endpoints are protected by require_session_or_service_token
 * (cookie auth; apiFetch passes credentials: include). Errors bubble
 * as ApiError(status, detail) — the backend's `detail` string is a
 * fixed phrase (prevention log #86 / #89) so callers may surface it
 * verbatim via `error.message`.
 *
 * Contract: no business logic here. Typed wrapper only. Anything
 * resembling validation, retry, caching, or analytics belongs in
 * the calling component.
 */

export function placeOrder(body: PlaceOrderRequest): Promise<OrderResponse> {
  return apiFetch<OrderResponse>("/portfolio/orders", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function closePosition(
  positionId: number,
  body: ClosePositionRequest,
): Promise<OrderResponse> {
  return apiFetch<OrderResponse>(
    `/portfolio/positions/${positionId}/close`,
    { method: "POST", body: JSON.stringify(body) },
  );
}
