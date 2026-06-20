/**
 * Status semantics + reason → tooltip mappings (#1076 / #1064).
 *
 * Single source of truth for the FE — both ProcessRow and
 * ProcessDetailPage import from here so the operator sees the same
 * copy regardless of surface. Spec §"Status semantics — full enum"
 * + §"Trigger preconditions matrix".
 */

import type {
  HealthVerdict,
  ProcessStatus,
  StaleReason,
  TriggerConflictReason,
} from "@/api/types";

export interface StatusVisual {
  /** Short human label rendered inside the pill. */
  readonly label: string;
  /** Tailwind classes for the pill background / border / text. */
  readonly toneClass: string;
  /** True for statuses that should pulse. Pulse respects `motion-reduce`. */
  readonly pulse: boolean;
}

export const STATUS_VISUAL: Record<ProcessStatus, StatusVisual> = {
  idle: {
    label: "idle",
    toneClass:
      "border-slate-200 bg-slate-50 text-slate-600 dark:border-slate-700 dark:bg-slate-800/60 dark:text-slate-300",
    pulse: false,
  },
  pending_first_run: {
    label: "first run pending",
    toneClass:
      "border-slate-300 bg-slate-100 text-slate-700 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200",
    pulse: false,
  },
  running: {
    label: "running",
    toneClass:
      "border-sky-300 bg-sky-50 text-sky-800 dark:border-sky-800 dark:bg-sky-950/60 dark:text-sky-200",
    pulse: true,
  },
  ok: {
    label: "ok",
    toneClass:
      "border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950/60 dark:text-emerald-300",
    pulse: false,
  },
  failed: {
    label: "failed",
    toneClass:
      "border-red-300 bg-red-50 text-red-700 dark:border-red-800 dark:bg-red-950/60 dark:text-red-300",
    pulse: false,
  },
  pending_retry: {
    label: "pending retry",
    toneClass:
      "border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300",
    pulse: false,
  },
  cancelled: {
    label: "cancelled",
    toneClass:
      "border-slate-300 bg-slate-50 text-slate-500 line-through dark:border-slate-700 dark:bg-slate-800/40 dark:text-slate-400",
    pulse: false,
  },
  disabled: {
    label: "disabled",
    toneClass:
      "border-slate-200 bg-slate-50 text-slate-400 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-500",
    pulse: false,
  },
};

/**
 * Map a structured 409 trigger / cancel reason to operator-facing copy.
 *
 * Reasons emitted by `app/api/processes.py` PR4. Anything else falls
 * back to a generic phrase so we never render `error.message` to the
 * DOM (loading-error-empty-states.md rule).
 */
export const REASON_TOOLTIP: Record<TriggerConflictReason, string> = {
  kill_switch_active:
    "Kill switch is active — deactivate before triggering this process.",
  bootstrap_already_running:
    "Bootstrap is already running — wait or cancel first.",
  bootstrap_state_missing:
    "Bootstrap is not initialised. Apply the sql/129 migration before triggering.",
  bootstrap_not_resumable:
    "Nothing to iterate — bootstrap is not in a failed or cancelled state.",
  iterate_already_pending: "An iterate is already in flight for this process.",
  full_wash_already_pending:
    "A full-wash is already in flight — wait for it to complete.",
  active_run_in_progress:
    "A run is in progress — cancel first or wait for completion.",
  shared_source_active_run:
    "A sibling job sharing the same source is running. Cancel that run before full-wash.",
  shared_source_full_wash_pending:
    "A sibling job has an active full-wash. Wait for it to complete.",
  no_active_run: "Nothing to cancel — no active run.",
  stop_already_pending: "A cancel is already pending for this run.",
  trigger_not_supported:
    "Sweeps are read-only — trigger via the underlying scheduled job.",
  cancel_not_supported:
    "Sweeps have no in-flight state — cancel the underlying scheduled job.",
  bootstrap_not_complete:
    "First-install bootstrap is not complete — finish or override before triggering this job.",
};

const KNOWN_REASONS = new Set<string>(Object.keys(REASON_TOOLTIP));

export function reasonFromError(err: unknown): TriggerConflictReason | null {
  if (typeof err !== "object" || err === null) return null;
  const detail = (err as { detail?: unknown }).detail;
  if (typeof detail !== "object" || detail === null) return null;
  const reason = (detail as { reason?: unknown }).reason;
  if (typeof reason !== "string") return null;
  if (!KNOWN_REASONS.has(reason)) return null;
  return reason as TriggerConflictReason;
}

export function reasonTooltip(err: unknown): string {
  const reason = reasonFromError(err);
  if (reason !== null) return REASON_TOOLTIP[reason];
  return "Request rejected. Check the browser console for details.";
}

/**
 * Stale-reason chip labels (PR8 / #1083 — operator-amendment §A1).
 * The mid_flight_stuck chip is rendered with the elapsed-since-
 * heartbeat appended client-side ("no progress 7m"), computed from
 * `active_run.last_progress_at`; the label here is the prefix.
 */
export const STALE_REASON_LABEL: Record<StaleReason, string> = {
  schedule_missed: "schedule missed",
  // #1508 Task 2 (C2): watermark_gap now means the source's
  // data-freshness index is in `error` state — i.e. ingest is actually
  // failing — not "source has fresh data we haven't pulled". Label
  // matches the backend reason copy.
  watermark_gap: "ingest failing",
  queue_stuck: "queue stuck",
  mid_flight_stuck: "no progress",
};

/**
 * Visuals for the single computed health verdict (#1512). The main
 * Processes row AND the legacy Background Jobs table (#1689) both render
 * THIS pill instead of raw `status` / `last_status`, so the operator
 * never sees a transient / retrying / restart-reaped run painted red.
 *
 * #1689 three-state semaphore (supersedes the #1508 C3 two-colour fold):
 *   - green  (`current` / `working`)  — ok, system working as designed.
 *   - amber  (`self_healing`)         — recovering: an auto-scheduled retry
 *       is in flight. The operator should SEE it healing, not mistake it for
 *       done (green) or broken (red). This is the deliberate reversal of C3,
 *       which painted self_healing calm-green; the operator asked for amber.
 *   - red    (`attention`)            — act: operator must intervene.
 *   - muted  (`stale_manual`)         — aged history: an exhausted one-shot
 *       (bootstrap/backfill) failure that is no longer a live alarm (#1689).
 * Each tone is hoisted to a single const so verdicts cannot drift apart on a
 * future dark-mode tweak (single-source-of-truth).
 */
const CALM_TONE =
  "border-emerald-300 bg-emerald-50 text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950/60 dark:text-emerald-300";
const AMBER_TONE =
  "border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-800 dark:bg-amber-950/60 dark:text-amber-300";
const MUTED_TONE =
  "border-slate-300 bg-slate-50 text-slate-500 dark:border-slate-700 dark:bg-slate-900/60 dark:text-slate-400";

export const VERDICT_VISUAL: Record<HealthVerdict, StatusVisual> = {
  current: {
    label: "current",
    toneClass: CALM_TONE,
    pulse: false,
  },
  working: {
    // Distinct label, but the calm-green tone of `current`: a live run is
    // the system working as designed — not something to alarm on.
    label: "working",
    toneClass: CALM_TONE,
    pulse: false,
  },
  self_healing: {
    // #1689 — amber: a scheduled retry is auto-recovery in progress. Distinct
    // from green (done) and red (broken) so the operator SEES it healing.
    label: "retrying",
    toneClass: AMBER_TONE,
    pulse: false,
  },
  attention: {
    label: "needs attention",
    toneClass:
      "border-red-300 bg-red-50 text-red-700 dark:border-red-800 dark:bg-red-950/60 dark:text-red-300",
    pulse: false,
  },
  stale_manual: {
    // #1689 — muted: an aged, exhausted one-shot (bootstrap/backfill) failure.
    // No longer a live alarm; sits in the collapsed Manual & backfill section.
    label: "stale",
    toneClass: MUTED_TONE,
    pulse: false,
  },
};

/**
 * Sort priority: `attention` pins to the top (rank 0). The calm/recovering
 * verdicts (`current` / `working` / `self_healing`) share rank 1 — one quiet
 * group the table collapses behind a disclosure. `stale_manual` (#1689) sinks
 * to rank 2 so aged one-shot history settles below live jobs. Only `attention`
 * pins; lower number = higher.
 */
export const VERDICT_SORT_PRIORITY: Record<HealthVerdict, number> = {
  attention: 0,
  current: 1,
  working: 1,
  self_healing: 1,
  stale_manual: 2,
};

/**
 * #1514 — honest framing for the displayed next-fire time. It is computed
 * from the declared cadence (`compute_next_run`), NOT read from the live
 * scheduler: since #719 the scheduler runs in a separate process the API
 * does not query, so this is the *expected* next slot, not a confirmation
 * it will fire. A scheduler that has actually stopped firing surfaces as
 * "needs attention" via the liveness stall detection (#1510), so the
 * "expected" label plus the verdict together keep the page honest.
 */
export const NEXT_RUN_EXPECTED_TOOLTIP =
  "Expected next fire, computed from the declared cadence — not a live " +
  "scheduler confirmation (the scheduler runs in a separate process). " +
  "A job that has actually stopped firing is flagged as needs-attention.";
