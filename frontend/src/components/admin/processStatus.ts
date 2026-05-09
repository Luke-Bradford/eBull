/**
 * Status semantics + reason → tooltip mappings (#1076 / #1064).
 *
 * Single source of truth for the FE — both ProcessRow and
 * ProcessDetailPage import from here so the operator sees the same
 * copy regardless of surface. Spec §"Status semantics — full enum"
 * + §"Trigger preconditions matrix".
 */

import type { ProcessStatus, TriggerConflictReason } from "@/api/types";

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
  stale: {
    label: "stale",
    toneClass:
      "border-amber-300 bg-amber-50 text-amber-800 dark:border-amber-800 dark:bg-amber-950/60 dark:text-amber-300",
    pulse: true,
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
 * Sort priority: failed/stale first, running next, terminal states by
 * lane order. Spec §"Error display rules" — failed processes float to
 * top so the operator sees them without scrolling.
 */
export const STATUS_SORT_PRIORITY: Record<ProcessStatus, number> = {
  failed: 0,
  stale: 1,
  pending_retry: 2,
  running: 3,
  cancelled: 4,
  pending_first_run: 5,
  idle: 6,
  ok: 7,
  disabled: 8,
};
