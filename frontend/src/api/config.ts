import { apiFetch } from "@/api/client";
import type {
  ConfigResponse,
  KillSwitchRequest,
  KillSwitchResponse,
} from "@/api/types";

export function fetchConfig(): Promise<ConfigResponse> {
  return apiFetch<ConfigResponse>("/config");
}

/**
 * Activate / deactivate the system-wide kill switch (POST
 * /config/kill-switch). `reason` and `activated_by` are required
 * non-empty by the backend; callers must supply both. A 503 ApiError
 * means the singleton row is missing (env fault), not a generic failure.
 */
export function postKillSwitch(
  body: KillSwitchRequest,
): Promise<KillSwitchResponse> {
  return apiFetch<KillSwitchResponse>("/config/kill-switch", {
    method: "POST",
    body: JSON.stringify(body),
  });
}
