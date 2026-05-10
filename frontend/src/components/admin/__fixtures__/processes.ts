/**
 * ProcessRowResponse + ProcessListResponse fixture builders for the
 * admin control hub FE tests (#1076 / #1064).
 */

import type {
  ErrorClassSummaryResponse,
  ProcessLane,
  ProcessListResponse,
  ProcessRowResponse,
  ProcessStatus,
  ProcessWatermarkResponse,
} from "@/api/types";

let nextRunId = 1;

export function makeWatermark(
  overrides: Partial<ProcessWatermarkResponse> = {},
): ProcessWatermarkResponse {
  return {
    cursor_kind: "filed_at",
    cursor_value: "2026-05-08T13:00:00+00:00",
    human:
      "Resume from filings filed after 2026-05-08T13:00Z (12 of 1547 subjects awaiting next poll)",
    last_advanced_at: "2026-05-08T13:00:00+00:00",
    ...overrides,
  };
}

export function makeError(
  overrides: Partial<ErrorClassSummaryResponse> = {},
): ErrorClassSummaryResponse {
  return {
    error_class: "ConnectionTimeout",
    count: 3,
    last_seen_at: "2026-05-08T13:55:00+00:00",
    sample_message: "fetch timed out after 30s",
    sample_subject: "CIK 320193 / accession 0000320193-25-000001",
    ...overrides,
  };
}

export function makeProcessRow(
  overrides: Partial<ProcessRowResponse> = {},
): ProcessRowResponse {
  const status: ProcessStatus = overrides.status ?? "ok";
  const lane: ProcessLane = overrides.lane ?? "sec";
  return {
    process_id: "sec_form4_ingest",
    display_name: "Insider Form 4 ingest",
    lane,
    mechanism: "scheduled_job",
    status,
    last_run: {
      run_id: nextRunId++,
      started_at: "2026-05-08T13:00:00+00:00",
      finished_at: "2026-05-08T13:03:00+00:00",
      duration_seconds: 180,
      rows_processed: 4520,
      rows_skipped_by_reason: {},
      rows_errored: 0,
      status: "success",
      cancelled_by_operator_id: null,
    },
    active_run: null,
    cadence_human: "every 5m",
    cadence_cron: "*/5 * * * *",
    next_fire_at: "2026-05-08T14:00:00+00:00",
    watermark: makeWatermark(),
    can_iterate: true,
    can_full_wash: true,
    can_cancel: false,
    last_n_errors: [],
    stale_reasons: [],
    params_metadata: [],
    description: "Operator-facing description for the Insider Form 4 ingest.",
    ...overrides,
  };
}

export function makeProcessList(
  rows: ProcessRowResponse[],
  partial = false,
): ProcessListResponse {
  return { rows, partial };
}
