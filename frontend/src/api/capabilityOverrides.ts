/**
 * Capability-override admin API client (#531).
 *
 * fetchCapabilityOverrides() — exchange rows whose ``capabilities`` JSONB
 * diverges from the migration-071 seed default for their asset_class.
 * Powers the AdminPage "Capability overrides" card + the
 * /admin/capability-overrides drill page. Wraps
 * GET /admin/capability-overrides. Read-only; revert-to-seed is a
 * future write endpoint (out of scope for #531).
 */

import { apiFetch } from "@/api/client";
import type { OverridesListResponse } from "@/api/types";

export function fetchCapabilityOverrides(): Promise<OverridesListResponse> {
  return apiFetch<OverridesListResponse>("/admin/capability-overrides");
}
