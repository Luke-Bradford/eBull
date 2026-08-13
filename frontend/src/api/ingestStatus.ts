/**
 * Client for /operator/ingest-* (#793, Batch 4 of #788).
 *
 * Drives the ``/admin/ingest-health`` operator page. Three GETs +
 * one POST give the page full visibility into provider state +
 * recent failures + the backfill queue, with an idempotent enqueue
 * trigger for operator-driven re-runs.
 *
 * Per the user's product intent (2026-05-03): "we need to be
 * mindful of the first start up... so they know how far the
 * updates are, how long it will take or anything." The state +
 * failure + queue endpoints are the building blocks; the page
 * groups providers so the operator scans in 5 seconds rather
 * than parsing per-source rows by hand.
 */

import { apiFetch } from "@/api/client";

export type IngestProviderGroupKey =
  | "sec_fundamentals"
  | "sec_ownership"
  | "etoro"
  | "fundamentals_other"
  | "other";

export type IngestGroupState = "never_run" | "green" | "amber" | "red";

export interface IngestSourceSummary {
  readonly source: string;
  readonly last_success_at: string | null;
  readonly last_attempt_at: string | null;
  readonly last_attempt_status: string | null;
  readonly failures_24h: number;
  readonly rows_upserted_total: number;
}

export interface IngestProviderGroup {
  readonly key: IngestProviderGroupKey;
  readonly label: string;
  readonly description: string;
  readonly state: IngestGroupState;
  readonly sources: readonly IngestSourceSummary[];
  readonly backlog_pending: number;
  readonly backlog_running: number;
  readonly backlog_failed: number;
}

export interface IngestStatusResponse {
  readonly groups: readonly IngestProviderGroup[];
  readonly queue_total: number;
  readonly queue_running: number;
  readonly queue_failed: number;
  readonly computed_at: string;
}

export interface IngestFailure {
  readonly source: string;
  readonly started_at: string;
  readonly finished_at: string | null;
  readonly error: string | null;
  readonly rows_upserted: number;
}

export interface IngestFailuresResponse {
  readonly failures: readonly IngestFailure[];
}

export type BackfillQueueStatus =
  | "pending"
  | "running"
  | "complete"
  | "failed";

export interface BackfillQueueRow {
  readonly instrument_id: number;
  readonly symbol: string | null;
  readonly pipeline_name: string;
  readonly priority: number;
  readonly status: BackfillQueueStatus;
  readonly queued_at: string;
  readonly started_at: string | null;
  readonly completed_at: string | null;
  readonly attempts: number;
  readonly last_error: string | null;
  readonly triggered_by: "system" | "operator" | "migration" | "consumer";
}

export interface BackfillQueueResponse {
  readonly rows: readonly BackfillQueueRow[];
}

export function fetchIngestStatus(): Promise<IngestStatusResponse> {
  return apiFetch<IngestStatusResponse>("/operator/ingest-status");
}

export function fetchIngestFailures(
  limit: number = 50,
): Promise<IngestFailuresResponse> {
  return apiFetch<IngestFailuresResponse>(
    `/operator/ingest-failures?limit=${limit}`,
  );
}

export function fetchBackfillQueue(
  options: { status?: BackfillQueueStatus | "all"; limit?: number } = {},
): Promise<BackfillQueueResponse> {
  const params = new URLSearchParams();
  if (options.status !== undefined && options.status !== null) {
    params.set("status", options.status);
  }
  if (options.limit !== undefined) {
    params.set("limit", String(options.limit));
  }
  const qs = params.toString();
  return apiFetch<BackfillQueueResponse>(
    `/operator/ingest-backfill-queue${qs ? `?${qs}` : ""}`,
  );
}

export interface EnqueueBackfillRequest {
  readonly instrument_id: number;
  readonly pipeline_name: string;
  readonly priority?: number;
}

export interface EnqueueBackfillResponse {
  readonly instrument_id: number;
  readonly pipeline_name: string;
  readonly status: "queued";
}

export function enqueueBackfill(
  request: EnqueueBackfillRequest,
): Promise<EnqueueBackfillResponse> {
  return apiFetch<EnqueueBackfillResponse>("/operator/ingest-backfill", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      instrument_id: request.instrument_id,
      pipeline_name: request.pipeline_name,
      priority: request.priority ?? 100,
      triggered_by: "operator",
    }),
  });
}
