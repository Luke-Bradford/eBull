/**
 * Sync orchestrator API client (issue #260 Phase 3).
 *
 * Surfaces the three read endpoints + the POST trigger:
 *   - fetchSyncLayers()  — 15-layer freshness table (GET /sync/layers)
 *   - fetchSyncStatus()  — current running sync + active layer (GET /sync/status)
 *   - fetchSyncRuns(n)   — recent sync runs (GET /sync/runs?limit=n)
 *   - triggerSync(scope) — POST /sync; 202 on success, 409 on collision,
 *                          503 if ORCHESTRATOR_ENABLED=false
 */

import { apiFetch } from "@/api/client";
import type { LayerEnabledResponse, SyncLayersV2Response } from "@/api/types";

export type LayerTier = 0 | 1 | 2 | 3;

export interface SyncLayer {
  name: string;
  display_name: string;
  tier: LayerTier;
  is_fresh: boolean;
  freshness_detail: string;
  last_success_at: string | null;
  last_duration_seconds: number | null;
  last_error_category: string | null;
  consecutive_failures: number;
  dependencies: string[];
  is_blocking: boolean;
}

export interface SyncLayersResponse {
  layers: SyncLayer[];
}

export type SyncStatus = "running" | "complete" | "partial" | "failed";

export interface SyncRun {
  sync_run_id: number;
  scope: "full" | "layer" | "high_frequency" | "job" | "behind";
  scope_detail: string | null;
  trigger: "manual" | "scheduled" | "catch_up";
  started_at: string;
  finished_at: string | null;
  status: SyncStatus;
  layers_planned: number;
  layers_done: number;
  layers_failed: number;
  layers_skipped: number;
}

export interface SyncRunsResponse {
  runs: SyncRun[];
}

export interface SyncStatusResponse {
  is_running: boolean;
  current_run: {
    sync_run_id: number;
    scope: string;
    trigger: string;
    started_at: string;
    layers_planned: number;
    layers_done: number;
    layers_failed: number;
    layers_skipped: number;
  } | null;
  active_layer: {
    name: string;
    started_at: string | null;
    items_total: number | null;
    items_done: number | null;
  } | null;
}

export type SyncScopeKind = "full" | "layer" | "high_frequency" | "job" | "behind";

export interface SyncTriggerRequest {
  scope: SyncScopeKind;
  layer?: string;
  job?: string;
}

export interface SyncTriggerResponse {
  sync_run_id: number;
  plan: {
    layers_to_refresh: Array<{
      name: string;
      emits: string[];
      reason: string;
      dependencies: string[];
      is_blocking: boolean;
      estimated_items: number;
    }>;
    layers_skipped: Array<{ name: string; reason: string }>;
  };
}

export function fetchSyncLayers(): Promise<SyncLayersResponse> {
  return apiFetch<SyncLayersResponse>("/sync/layers");
}

export function fetchSyncStatus(): Promise<SyncStatusResponse> {
  return apiFetch<SyncStatusResponse>("/sync/status");
}

export function fetchSyncRuns(limit: number = 20): Promise<SyncRunsResponse> {
  // Fail-closed on invalid input: NaN / non-integer / out-of-range
  // would otherwise interpolate into the URL and hit the server as
  // an obviously-bad request. Backend caps at 100; match the cap.
  if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
    return Promise.reject(
      new RangeError(`fetchSyncRuns limit must be an integer in [1, 100], got ${limit}`),
    );
  }
  return apiFetch<SyncRunsResponse>(`/sync/runs?limit=${limit}`);
}

export function triggerSync(
  body: SyncTriggerRequest,
): Promise<SyncTriggerResponse> {
  return apiFetch<SyncTriggerResponse>("/sync", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export function fetchSyncLayersV2(): Promise<SyncLayersV2Response> {
  return apiFetch<SyncLayersV2Response>("/sync/layers/v2");
}

export function setLayerEnabled(
  layerName: string,
  enabled: boolean,
): Promise<LayerEnabledResponse> {
  return apiFetch<LayerEnabledResponse>(
    `/sync/layers/${encodeURIComponent(layerName)}/enabled`,
    {
      method: "POST",
      body: JSON.stringify({ enabled }),
    },
  );
}
