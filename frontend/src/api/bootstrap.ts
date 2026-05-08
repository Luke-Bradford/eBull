import { apiFetch } from "@/api/client";

/**
 * First-install bootstrap API client (#997).
 *
 * Spec: docs/superpowers/specs/2026-05-07-first-install-bootstrap.md.
 * Backend: app/api/bootstrap.py.
 */

export type BootstrapStatus =
  | "pending"
  | "running"
  | "complete"
  | "partial_error";

export type BootstrapStageStatus =
  | "pending"
  | "running"
  | "success"
  | "error"
  | "skipped"
  // ``blocked`` (#1020): orchestrator never invoked the stage because
  // a `requires` upstream stage finished error/blocked.
  | "blocked";

export type BootstrapLane =
  | "init"
  | "etoro"
  | "sec"
  | "sec_rate"
  | "sec_bulk_download"
  | "db";

export interface BootstrapArchiveResultResponse {
  archive_name: string;
  rows_written: number;
  rows_skipped: Record<string, number>;
  completed_at: string | null;
}

export interface BootstrapStageResponse {
  stage_key: string;
  stage_order: number;
  lane: BootstrapLane;
  job_name: string;
  status: BootstrapStageStatus;
  started_at: string | null;
  completed_at: string | null;
  rows_processed: number | null;
  expected_units: number | null;
  units_done: number | null;
  last_error: string | null;
  attempt_count: number;
  // #1046: per-archive ingest progress for C-stages.
  archive_results: BootstrapArchiveResultResponse[];
}

export type BulkManifestMode = "bulk" | "fallback";

export interface BulkManifestResponse {
  present: boolean;
  mode: BulkManifestMode | null;
  bootstrap_run_id: number | null;
  archive_count: number;
}

export interface BootstrapStatusResponse {
  status: BootstrapStatus;
  current_run_id: number | null;
  last_completed_at: string | null;
  stages: BootstrapStageResponse[];
  bulk_manifest: BulkManifestResponse | null;
}

export interface BootstrapRunQueuedResponse {
  run_id: number;
  request_id: number;
}

export interface BootstrapMarkCompleteResponse {
  status: BootstrapStatus;
}

export function fetchBootstrapStatus(): Promise<BootstrapStatusResponse> {
  return apiFetch<BootstrapStatusResponse>("/system/bootstrap/status");
}

export function runBootstrap(): Promise<BootstrapRunQueuedResponse> {
  return apiFetch<BootstrapRunQueuedResponse>("/system/bootstrap/run", {
    method: "POST",
  });
}

export function retryFailedBootstrap(): Promise<BootstrapRunQueuedResponse> {
  return apiFetch<BootstrapRunQueuedResponse>(
    "/system/bootstrap/retry-failed",
    { method: "POST" },
  );
}

export function markBootstrapComplete(): Promise<BootstrapMarkCompleteResponse> {
  return apiFetch<BootstrapMarkCompleteResponse>(
    "/system/bootstrap/mark-complete",
    { method: "POST" },
  );
}
