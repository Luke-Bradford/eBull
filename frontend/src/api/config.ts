import { apiFetch } from "@/api/client";
import type {
  ConfigPatchRequest,
  ConfigResponse,
  KillSwitchRequest,
  KillSwitchResponse,
  RuntimeFlagsResponse,
} from "@/api/types";

export function fetchConfig(): Promise<ConfigResponse> {
  return apiFetch<ConfigResponse>("/config");
}

/**
 * Partial update of runtime config (PATCH /config). Send only changed
 * fields — the backend rejects no-op patches with 422. Returns the
 * post-update runtime flags (not the full ConfigResponse).
 */
export function patchConfig(body: ConfigPatchRequest): Promise<RuntimeFlagsResponse> {
  return apiFetch<RuntimeFlagsResponse>("/config", {
    method: "PATCH",
    body: JSON.stringify(body),
  });
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
