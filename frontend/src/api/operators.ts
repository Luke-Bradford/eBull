/**
 * Operators API client (issue #106 / Ticket G).
 *
 * Wraps the operator-management endpoints. All routes are session-only
 * on the backend; the cookie carries the auth, so callers do not need
 * to pass anything beyond the field values.
 */

import { apiFetch } from "@/api/client";

export interface OperatorView {
  id: string;
  username: string;
  created_at: string;
  last_login_at: string | null;
  is_self: boolean;
}

export interface CreateOperatorResponse {
  operator: OperatorView;
}

export function listOperators(): Promise<OperatorView[]> {
  return apiFetch<OperatorView[]>("/operators");
}

export function createOperator(
  username: string,
  password: string,
): Promise<CreateOperatorResponse> {
  return apiFetch<CreateOperatorResponse>("/operators", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
}

export function deleteOperator(operatorId: string): Promise<void> {
  return apiFetch<void>(`/operators/${operatorId}`, { method: "DELETE" });
}
