/**
 * ProcessRowResponse + ProcessListResponse fixture builders for the
 * admin control hub FE tests (#1076 / #1064).
 */

import type {
  ErrorClassSummaryResponse,
  HealthVerdict,
  ProcessLane,
  ProcessListResponse,
  ProcessRowResponse,
  ProcessStatus,
  ProcessWatermarkResponse,
  StaleReason,
} from "@/api/types";

let nextRunId = 1;

const REASON_LABEL: Record<StaleReason, string> = {
  schedule_missed: "schedule missed",
  watermark_gap: "ingest failing",
  queue_stuck: "queue stuck",
  mid_flight_stuck: "no progress",
};
const REASON_ORDER: StaleReason[] = [
  "schedule_missed",
  "watermark_gap",
  "queue_stuck",
  "mid_flight_stuck",
];

/**
 * Mirror of `app/services/processes/health_verdict.py::compute_verdict`
 * so fixtures built from `{status, stale_reasons}` carry a coherent
 * verdict without every test spelling it out. Keep in lock-step with
 * the BE precedence table (#1512).
 */
export function deriveVerdict(
  status: ProcessStatus,
  staleReasons: StaleReason[],
  lastRunFailed = false,
): { health_verdict: HealthVerdict; self_healing: boolean; verdict_reason: string } {
  const actionable = REASON_ORDER.filter((r) => staleReasons.includes(r));
  // #1831 — kill switch (disabled) is neutral `paused` (the halt is the loop's
  // normal state). Two exceptions stay red so nothing genuine is masked: a
  // WEDGE (queue_stuck / mid_flight_stuck — a halt does not un-stick a queue),
  // and a last terminal run that genuinely failed. Only the halt-expected
  // reasons (schedule_missed / watermark_gap) demote to paused.
  if (status === "disabled") {
    const wedge = actionable.find((r) => r === "queue_stuck" || r === "mid_flight_stuck");
    if (wedge !== undefined)
      return { health_verdict: "attention", self_healing: false, verdict_reason: REASON_LABEL[wedge] };
    return lastRunFailed
      ? { health_verdict: "attention", self_healing: false, verdict_reason: "last run failed" }
      : { health_verdict: "paused", self_healing: false, verdict_reason: "" };
  }
  const headline = actionable[0];
  if (headline !== undefined) {
    const reason =
      status === "failed"
        ? "last run failed"
        : status === "running" && actionable.includes("mid_flight_stuck")
          ? "running but no progress"
          : REASON_LABEL[headline];
    return { health_verdict: "attention", self_healing: false, verdict_reason: reason };
  }
  switch (status) {
    case "running":
      return { health_verdict: "working", self_healing: false, verdict_reason: "" };
    case "pending_retry":
      return { health_verdict: "self_healing", self_healing: true, verdict_reason: "retry scheduled" };
    case "failed":
      return { health_verdict: "attention", self_healing: false, verdict_reason: "last run failed" };
    case "cancelled":
      return { health_verdict: "attention", self_healing: false, verdict_reason: "last run cancelled" };
    case "pending_first_run":
      return { health_verdict: "working", self_healing: false, verdict_reason: "first run pending" };
    case "ok":
    case "idle":
      return { health_verdict: "current", self_healing: false, verdict_reason: "" };
    default:
      return { health_verdict: "attention", self_healing: false, verdict_reason: "unknown state" };
  }
}

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
  const derived = deriveVerdict(status, overrides.stale_reasons ?? []);
  return {
    process_id: "sec_form4_ingest",
    display_name: "Insider Form 4 ingest",
    lane,
    mechanism: "scheduled_job",
    role: "steady_state",
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
    ...derived,
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
