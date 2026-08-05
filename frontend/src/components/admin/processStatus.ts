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
import type { BadgeTone } from "@/components/ui/Badge";

export interface StatusVisual {
  /** Short human label rendered inside the pill. */
  readonly label: string;
  /**
   * SEMANTIC tone, rendered by `components/ui/Badge` (#2148). Never a colour
   * class: a tone map holding raw Tailwind is how `eightKSeverity.ts` shipped
   * light-only chips past the dark gate (prevention-log → "A lint gate's
   * file-glob is part of its contract").
   */
  readonly tone: BadgeTone;
  /**
   * Non-colour decoration the tone cannot express — `line-through` on a
   * cancelled status, `italic`, or a dimming `opacity-*`. MUST NOT contain a
   * colour utility; colour belongs to the tone.
   */
  readonly extraClass?: string;
}

export const STATUS_VISUAL: Record<ProcessStatus, StatusVisual> = {
  idle: { label: "idle", tone: "neutral" },
  pending_first_run: { label: "first run pending", tone: "neutral" },
  // #2148 — `running` was `sky`, a family outside the operator colour table.
  // Folded onto `info` (blue), the table's "neutral interactive / in-progress"
  // slot. A deliberate small colour change, not a no-op.
  running: { label: "running", tone: "info" },
  ok: { label: "ok", tone: "ok" },
  // #2218 — `warn`, not `risk`: the job completed, so this is not an
  // incident, but it made no progress and needs the operator. Sits between
  // `ok` and `failed` visually because that is exactly where it sits
  // semantically.
  degraded: { label: "no progress", tone: "warn" },
  failed: { label: "failed", tone: "risk" },
  pending_retry: { label: "pending retry", tone: "warn" },
  cancelled: { label: "cancelled", tone: "neutral", extraClass: "line-through" },
  // Dimmed rather than given its own grey: `disabled` and `idle` are both
  // neutral, and the operator distinguishes them by label. Opacity is
  // theme-independent, so unlike the old dimmer slate it cannot drift in dark.
  disabled: { label: "disabled", tone: "neutral", extraClass: "opacity-70" },
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
  bootstrap_not_resettable:
    "Bootstrap is not in a re-runnable state — only a failed or cancelled run can be re-run.",
  bootstrap_no_failed_stages:
    "Nothing to re-run — the latest bootstrap run has no failed stages.",
};

/**
 * Short inline-visible label for a structured trigger / cancel reason
 * (#1230). Sibling to REASON_TOOLTIP: the tooltip carries the full
 * "what to do" sentence (hover), this carries the *category* so the
 * operator scanning the page sees WHY a row is blocked without hovering.
 * Keep keys in lock-step with REASON_TOOLTIP.
 */
export const REASON_SHORT_LABEL: Record<TriggerConflictReason, string> = {
  kill_switch_active: "kill switch active",
  bootstrap_already_running: "bootstrap already running",
  bootstrap_state_missing: "bootstrap not initialised",
  bootstrap_not_resumable: "nothing to iterate",
  iterate_already_pending: "iterate already pending",
  full_wash_already_pending: "full-wash already pending",
  active_run_in_progress: "run in progress",
  shared_source_active_run: "sibling job running",
  shared_source_full_wash_pending: "sibling full-wash pending",
  no_active_run: "no active run",
  stop_already_pending: "cancel already pending",
  trigger_not_supported: "trigger not supported",
  cancel_not_supported: "cancel not supported",
  bootstrap_not_complete: "bootstrap not complete",
  bootstrap_not_resettable: "bootstrap not re-runnable",
  bootstrap_no_failed_stages: "no failed stages",
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
 * Short inline category for a trigger / cancel rejection (#1230), or
 * null when the reason is unstructured/unknown — the caller then shows
 * the generic "rejected" label alone (the full hint stays in the
 * tooltip; we never render exception text inline).
 */
export function reasonShortLabel(err: unknown): string | null {
  const reason = reasonFromError(err);
  return reason !== null ? REASON_SHORT_LABEL[reason] : null;
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
 * Since #2148 each verdict names a semantic Badge tone rather than a hoisted
 * class-string const, so the three-state semaphore is stated in meaning and
 * cannot drift apart on a future dark-mode tweak — there is no colour here to
 * tweak.
 */
export const VERDICT_VISUAL: Record<HealthVerdict, StatusVisual> = {
  current: { label: "current", tone: "ok" },
  working: {
    // Distinct label, but the calm-green tone of `current`: a live run is
    // the system working as designed — not something to alarm on.
    label: "working",
    tone: "ok",
  },
  self_healing: {
    // #1689 — amber: a scheduled retry is auto-recovery in progress. Distinct
    // from green (done) and red (broken) so the operator SEES it healing.
    label: "retrying",
    tone: "warn",
  },
  attention: { label: "needs attention", tone: "risk" },
  stale_manual: {
    // #1689 — muted: an aged, exhausted one-shot (bootstrap/backfill) failure.
    // No longer a live alarm; sits in the collapsed Manual & backfill section.
    label: "stale",
    tone: "neutral",
  },
  paused: {
    // #1831 — grey: disabled by the global kill switch. The halt is the normal
    // unattended-loop state, so a paused job is neutral, not a red "problem".
    // The kill-switch banner conveys the halt; a genuinely-failed halted job
    // still reads `attention` (its verdict is computed server-side).
    label: "paused",
    tone: "neutral",
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
  // #1831 — paused (kill switch) is neutral, not attention: sinks below live
  // jobs alongside aged history so a halt does not crowd the actionable top.
  paused: 2,
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
