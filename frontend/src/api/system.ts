import { apiFetch } from "@/api/client";
import type { SystemStatusResponse } from "@/api/types";

export function fetchSystemStatus(): Promise<SystemStatusResponse> {
  return apiFetch<SystemStatusResponse>("/system/status");
}
