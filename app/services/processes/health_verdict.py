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

**Scope:** all three recovery look-throughs are now wired —
``watermark_is_fresh`` promotes a bootstrap-covered, still-fresh
``pending_first_run`` job to Current (#1511 / T5); ``retry_in_flight`` +
``retry_at_display`` read a transiently-failed row as Self-healing
"will retry HH:MM" (#1509 / T3); and ``liveness_kick_in_flight`` reads a
watchdog-re-enqueued *stalled* job as Self-healing "re-enqueued,
recovering" (#1510 / T4). An overdue row with NO recovery mechanism in
flight still reads **attention** — honest, because it currently IS stuck.
In every case a genuine wedge (``queue_stuck`` / ``mid_flight_stuck`` /
``watermark_gap``) outranks the recovery signal and stays attention.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final

from app.services.processes import (
    HealthVerdict,
    ProcessRow,
    ProcessStatus,
    StaleReason,
)

# #1689 — a bootstrap/backfill (one-shot, non-steady) job whose latest run
# failed permanently (no retry in flight) and finished longer ago than this
# window is aged-out history, not a steady-state alarm: it reads
# ``stale_manual`` (muted, collapsed) instead of ``attention`` (red). A
# *recent* failure inside the window still reads attention so the operator
# sees their triggered job failed.
STALE_MANUAL_WINDOW: Final[timedelta] = timedelta(hours=24)

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
    "watermark_gap": "ingest failing",
    "queue_stuck": "queue stuck",
    "mid_flight_stuck": "no progress",
}


def compute_verdict(
    *,
    status: ProcessStatus,
    stale_reasons: tuple[StaleReason, ...],
    watermark_is_fresh: bool = False,
    retry_in_flight: bool = False,
    retry_at_display: str = "",
    liveness_kick_in_flight: bool = False,
    never_started: bool = False,
    cancel_was_operator_initiated: bool = False,
    manual_aged_exhausted: bool = False,
) -> tuple[HealthVerdict, bool, str]:
    """Collapse ``status`` + ``stale_reasons`` into one verdict.

    Returns ``(verdict, self_healing, verdict_reason)``:

    * ``verdict`` — ``current`` (green, fresh) / ``working`` (blue,
      progressing) / ``self_healing`` (amber, auto-recovering, no action
      needed) / ``attention`` (red, operator must act) / ``stale_manual``
      (muted, aged one-shot bootstrap/backfill failure — #1689).
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

    # T3 (#1509): a scheduled retry IS the fix for a missed schedule, so an
    # in-flight retry suppresses ONLY ``schedule_missed``. Genuine wedges
    # (``queue_stuck`` / ``mid_flight_stuck`` / ``watermark_gap``) still
    # outrank — a stuck queue means the retry itself may be wedged, so the row
    # stays attention rather than being painted self-healing (preserves the
    # ckpt-1 invariant: an actionable wedge is never masked).
    if retry_in_flight:
        actionable = [r for r in actionable if r != "schedule_missed"]

    # T4 (#1510): a liveness-watchdog re-enqueue IS the fix for a job that
    # silently stopped firing (the ``schedule_missed`` it surfaces as), so an
    # in-flight kick suppresses ONLY ``schedule_missed`` — exactly like a retry.
    # Genuine wedges (``queue_stuck`` / ``mid_flight_stuck`` / ``watermark_gap``)
    # are NOT dropped, so the actionable block below still returns attention for
    # them (ckpt-1 invariant: an actionable wedge is never masked — a kick into a
    # stuck queue does not make the queue un-stuck).
    #
    # ``kick_is_recovering`` gates the self_healing branch below on the stall
    # ACTUALLY being present (Codex ckpt-2): a kick request can linger
    # ``pending``/``claimed`` after a natural fire already cleared the stall, and
    # a recovered row (no ``schedule_missed``) must read its honest status, not be
    # repainted "re-enqueued, recovering".
    kick_is_recovering = liveness_kick_in_flight and "schedule_missed" in stale_reasons
    if liveness_kick_in_flight:
        actionable = [r for r in actionable if r != "schedule_missed"]

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

    # T4 (#1510): a fresh liveness-watchdog re-enqueue is in flight FOR A ROW THAT
    # IS STILL STALLED (``schedule_missed`` present — see ``kick_is_recovering``)
    # and no genuine wedge outranks it (the actionable block above already
    # returned for ``queue_stuck`` / ``mid_flight_stuck`` / ``watermark_gap``).
    # The system detected the silent stall and is auto-recovering it —
    # Self-healing, no operator action. Placed before the status-only branches
    # because a kick does NOT flip the adapter status to ``running`` unless the
    # last terminal was a failure (scheduled_adapter:211), so a stalled ``ok`` /
    # ``idle`` row would otherwise fall through to Current and hide the recovery.
    if kick_is_recovering:
        return ("self_healing", True, "re-enqueued, recovering")

    # 3-9. Status-only (no actionable stale).
    if status == "running":
        return ("working", False, "")
    # T3 (#1509): a failed/pending-retry row with a scheduled near-term retry
    # (``next_retry_at`` set — past OR future) is self-healing. ``retry_at_display``
    # is "HH:MM" when the retry is still in the future, "" when it is due but the
    # ≤5m sweeper has not yet fired (still scheduled recovery — do not flicker red).
    if retry_in_flight and status in ("failed", "pending_retry"):
        reason = f"will retry {retry_at_display}" if retry_at_display else "retrying shortly"
        return ("self_healing", True, reason)
    if status == "pending_retry":
        # Cadence-covered fallback: next natural fire reattempts the failed
        # scope, but no explicit ``next_retry_at`` backoff is in flight.
        return ("self_healing", True, "retry scheduled")
    if status == "failed" and manual_aged_exhausted:
        # #1689 — an aged, exhausted one-shot (bootstrap/backfill) failure is
        # history, not a steady-state alarm. Reads muted ``stale_manual`` and
        # folds into the collapsed Manual & backfill section. Reached only when
        # NO actionable wedge (block above) and NO retry-in-flight (branch
        # above) apply, and ``verdict_for_row`` role-gated it to bootstrap/
        # backfill — so a wedged or steady-state failure still reads attention.
        return ("stale_manual", False, "aged one-shot failure")
    if status == "failed":
        return ("attention", False, "last run failed")
    if status == "cancelled":
        # Task 5 (#1508): a cancel traceable to a deliberate operator stop
        # request (process_stop_requests join, resolved by the adapter) is
        # benign — reads Current (green) until the next natural fire. A cancel
        # NOT traceable to an operator request (system/crash) stays attention
        # "last run cancelled". Placed AFTER the actionable-stale block above,
        # so a benign cancel never masks a genuine wedge (ckpt-1 invariant).
        if cancel_was_operator_initiated:
            return ("current", False, "")
        return ("attention", False, "last run cancelled")
    if status == "pending_first_run":
        # C6 (#1508): zero lifetime rows AND now overdue past its first
        # expected fire (persisted first-seen + one cadence + grace, computed
        # by the adapter) — broken-from-day-one, not merely awaiting its first
        # natural slot. This outranks the watermark look-through below: a fresh
        # SOURCE watermark says the data is current, but THIS job has produced
        # nothing since it was first seen, so it must read attention.
        if never_started:
            return ("attention", False, "never started")
        # Look-through (#1511 / T5): a never-run steady-state poll whose
        # SEC source bootstrap already seeded — and which is still fresh
        # (``watermark_is_fresh``, computed by the adapter as covered-source
        # + MAX(filed_at) within cadence) — reads Current, not "first run
        # pending". The DATA is current; the job just has not reached its
        # first natural cadence slot. A covered-but-stale source returns
        # ``watermark_is_fresh=False`` here and stays "working" (and an
        # actionable stale reason, handled above, still outranks).
        if watermark_is_fresh:
            return ("current", False, "")
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


def verdict_for_row(row: ProcessRow, *, now: datetime) -> tuple[HealthVerdict, bool, str]:
    """Derive a row's verdict from a built ``ProcessRow``.

    The single choke point shared by ``/system/processes``
    (``api/processes.py::_convert_row``) and the legacy ``/system/jobs``
    overview (``api/system.py``, #1689) — so both surfaces render the SAME
    computed verdict instead of two drifting status models (the naive
    ``/system/jobs`` table previously rendered raw ``last_status`` →
    ``failure`` painted red regardless of retry/reap/role).

    The clock lives here, not in the pure ``compute_verdict``:
    ``retry_in_flight`` and the aged-failure check both compare
    ``next_retry_at`` / ``finished_at`` against ``now``. Callers pass one
    ``now`` so a row's inputs cannot straddle a clock tick.
    """
    # #1509 / T3 — a scheduled retry (``next_retry_at`` set, past OR future) is
    # recovery, not a red alarm. "HH:MM" label only while still in the future;
    # once due, ``compute_verdict`` renders "retrying shortly".
    retry_in_flight = row.next_retry_at is not None
    retry_at_display = (
        row.next_retry_at.strftime("%H:%M") if row.next_retry_at is not None and row.next_retry_at > now else ""
    )
    # #1689 — aged, exhausted one-shot failure → stale_manual. Role-gated to
    # bootstrap/backfill (a ``steady_state`` failure is a real alarm); requires
    # a permanent failure (no ``next_retry_at`` retry in flight) whose terminal
    # run finished longer ago than ``STALE_MANUAL_WINDOW``.
    manual_aged_exhausted = (
        row.role in ("bootstrap", "backfill")
        and row.status == "failed"
        and row.next_retry_at is None
        and row.last_run is not None
        and row.last_run.finished_at < now - STALE_MANUAL_WINDOW
    )
    return compute_verdict(
        status=row.status,
        stale_reasons=row.stale_reasons,
        watermark_is_fresh=row.source_watermark_fresh,
        retry_in_flight=retry_in_flight,
        retry_at_display=retry_at_display,
        liveness_kick_in_flight=row.liveness_kick_in_flight,
        never_started=row.never_started,
        cancel_was_operator_initiated=row.cancel_was_operator_initiated,
        manual_aged_exhausted=manual_aged_exhausted,
    )


__all__ = ["ACTIONABLE_STALE", "STALE_MANUAL_WINDOW", "compute_verdict", "verdict_for_row"]
