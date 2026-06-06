"""Pure-logic single health verdict for the admin Processes page.

Issue #1512 (umbrella #1508). Spec:
``docs/specs/ui/2026-06-06-process-health-verdict.md``.

The shipped model rendered two orthogonal axes per row — the
``ProcessStatus`` pill (*did the last terminal run succeed?*) and the
``stale_reasons`` chips (*is it overdue / behind right now?*) — and the
operator saw their product, producing contradictory rows like
``ok + schedule_missed`` or ``idle + schedule_missed``. This module
collapses both axes into ONE precedence-ordered verdict so a row can
never display two cells that disagree (contradiction-free *by
construction*).

Computed (not stored) at the API layer — see
``app/api/processes.py::_convert_row`` — which is the single choke point
all three adapters' rows flow through, so no adapter changes are needed.

**Load-bearing invariant (Codex ckpt-1):** an actionable stale reason
must NEVER be masked by a status. Only the global kill switch
(``status == "disabled"``) outranks a stale reason; every other status
is evaluated *after* the stale check. Without this, ``running +
queue_stuck`` would render blue "working" while a worker is wedged, and
``pending_retry + queue_stuck`` would render "self-healing" while a
dispatched request is stuck — re-introducing the masking the verdict
exists to kill.

**v1 scope:** T3 (retry/``next_retry_at``), T4 (watchdog re-enqueue) and
T5 (post-bootstrap seed + watermark look-through) are not yet landed, so
the only ``self_healing`` source is the existing ``pending_retry``
status. An overdue row with no recovery mechanism reads **attention** —
honest, because it currently IS stuck (the bug #1511 fixes). When
T3/T4/T5 land they extend this function (add ``next_retry_at`` /
liveness inputs) to reclassify *covered* overdue rows from ``attention``
to ``self_healing`` with a "will retry HH:MM" reason.
"""

from __future__ import annotations

from typing import Final

from app.services.processes import (
    HealthVerdict,
    ProcessStatus,
    StaleReason,
)

# All four stale reasons are actionable in v1 (none auto-recovers yet).
ACTIONABLE_STALE: Final[frozenset[StaleReason]] = frozenset(
    {"schedule_missed", "watermark_gap", "queue_stuck", "mid_flight_stuck"}
)

# Stable order for picking the headline reason when several fire at once.
_REASON_ORDER: Final[tuple[StaleReason, ...]] = (
    "schedule_missed",
    "watermark_gap",
    "queue_stuck",
    "mid_flight_stuck",
)

_REASON_LABEL: Final[dict[StaleReason, str]] = {
    "schedule_missed": "schedule missed",
    "watermark_gap": "source has fresh data",
    "queue_stuck": "queue stuck",
    "mid_flight_stuck": "no progress",
}


def compute_verdict(
    *,
    status: ProcessStatus,
    stale_reasons: tuple[StaleReason, ...],
) -> tuple[HealthVerdict, bool, str]:
    """Collapse ``status`` + ``stale_reasons`` into one verdict.

    Returns ``(verdict, self_healing, verdict_reason)``:

    * ``verdict`` — ``current`` (green, fresh) / ``working`` (blue,
      progressing) / ``self_healing`` (amber, auto-recovering, no action
      needed) / ``attention`` (red, operator must act).
    * ``self_healing`` — convenience boolean (``verdict == "self_healing"``
      today; kept distinct so T3/T4 can flag a row that is *both*
      surfaced and recovering).
    * ``verdict_reason`` — short inline copy (folds #1230). Empty string
      for ``current`` / plain ``working`` where no explanation helps.

    Precedence (first match wins) — see the spec's mapping table:
    """
    # ``_REASON_ORDER`` IS exactly the actionable set (all four reasons),
    # in fixed display order — so the headline is the first listed reason
    # that fired.
    actionable: list[StaleReason] = [r for r in _REASON_ORDER if r in stale_reasons]

    # 1. Kill switch — global, deliberate, outranks everything (incl. a
    #    stale reason that may still compute on a halted job).
    if status == "disabled":
        return ("attention", False, "kill switch active")

    # 2. Any actionable stale reason — surfaced before any non-disabled
    #    status so it can never be masked. Verdict is attention; only the
    #    headline reason text varies by usefulness.
    if actionable:
        if status == "failed":
            reason = "last run failed"
        elif status == "running" and "mid_flight_stuck" in actionable:
            reason = "running but no progress"
        else:
            reason = _REASON_LABEL[actionable[0]]
        return ("attention", False, reason)

    # 3-9. Status-only (no actionable stale).
    if status == "running":
        return ("working", False, "")
    if status == "pending_retry":
        return ("self_healing", True, "retry scheduled")
    if status == "failed":
        return ("attention", False, "last run failed")
    if status == "cancelled":
        return ("attention", False, "last run cancelled")
    if status == "pending_first_run":
        return ("working", False, "first run pending")
    if status == "ok":
        return ("current", False, "")
    if status == "idle":
        # Last terminal run was 'skipped' (prerequisite/gate not met) and
        # nothing is overdue — benign.
        return ("current", False, "")

    # Fallback — unreachable given the ProcessStatus Literal; guards
    # against silent drift if a new status is added without a mapping.
    return ("attention", False, "unknown state")


__all__ = ["ACTIONABLE_STALE", "compute_verdict"]
