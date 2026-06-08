"""Exhaustive table-test for the single health verdict (#1512).

Spec: ``docs/specs/ui/2026-06-06-process-health-verdict.md`` §2.2.

Two guarantees:

1. **Total + contradiction-free.** Over the full ``ProcessStatus`` x
   every subset of ``StaleReason`` matrix, ``compute_verdict`` returns
   exactly one verdict from the literal set and never raises — so no
   input can produce two cells that disagree.
2. **Mapping correctness.** The precedence table is pinned cell-by-cell,
   including the Codex-ckpt-1 masking guards (an actionable stale reason
   is never hidden by ``running`` / ``pending_retry``).
"""

from __future__ import annotations

import itertools
import typing

import pytest

from app.services.processes import HealthVerdict, ProcessStatus, StaleReason
from app.services.processes.health_verdict import ACTIONABLE_STALE, compute_verdict

_ALL_STATUSES: tuple[ProcessStatus, ...] = typing.get_args(ProcessStatus)
_ALL_REASONS: tuple[StaleReason, ...] = typing.get_args(StaleReason)
_VALID_VERDICTS: frozenset[HealthVerdict] = frozenset(typing.get_args(HealthVerdict))


def _all_reason_subsets() -> list[tuple[StaleReason, ...]]:
    out: list[tuple[StaleReason, ...]] = []
    for k in range(len(_ALL_REASONS) + 1):
        for combo in itertools.combinations(_ALL_REASONS, k):
            out.append(combo)
    return out


def test_dead_stale_literal_removed() -> None:
    """The never-set ``stale`` status literal is gone (#1512)."""
    assert "stale" not in _ALL_STATUSES


def test_actionable_stale_is_every_reason() -> None:
    """v1: all four stale reasons are actionable (none auto-recovers)."""
    assert ACTIONABLE_STALE == frozenset(_ALL_REASONS)


@pytest.mark.parametrize("status", _ALL_STATUSES)
@pytest.mark.parametrize("reasons", _all_reason_subsets())
def test_total_and_single_valued(status: ProcessStatus, reasons: tuple[StaleReason, ...]) -> None:
    """Every (status, reason-subset) yields exactly one valid verdict."""
    verdict, self_healing, reason = compute_verdict(status=status, stale_reasons=reasons)
    assert verdict in _VALID_VERDICTS
    assert isinstance(self_healing, bool)
    assert isinstance(reason, str)
    # self_healing is the convenience boolean for the self_healing verdict.
    assert self_healing == (verdict == "self_healing")


@pytest.mark.parametrize("status", _ALL_STATUSES)
def test_disabled_always_attention(
    status: ProcessStatus,
) -> None:
    """Kill switch (disabled) outranks everything — incl. a stale reason."""
    if status != "disabled":
        pytest.skip("only disabled relevant here")
    for reasons in _all_reason_subsets():
        verdict, _, reason = compute_verdict(status="disabled", stale_reasons=reasons)
        assert verdict == "attention"
        assert reason == "kill switch active"


@pytest.mark.parametrize(
    "status",
    [s for s in _ALL_STATUSES if s != "disabled"],
)
@pytest.mark.parametrize("reason", _ALL_REASONS)
def test_actionable_stale_never_masked(status: ProcessStatus, reason: StaleReason) -> None:
    """Codex ckpt-1: any actionable stale reason (on a non-disabled row)
    forces ``attention`` — never hidden behind ``working`` /
    ``self_healing`` / ``current``.
    """
    verdict, self_healing, _ = compute_verdict(status=status, stale_reasons=(reason,))
    assert verdict == "attention"
    assert self_healing is False


def test_no_stale_status_mapping() -> None:
    """Pin the status-only column (no actionable stale reason)."""
    expected: dict[ProcessStatus, tuple[HealthVerdict, bool]] = {
        "disabled": ("attention", False),
        "running": ("working", False),
        "pending_retry": ("self_healing", True),
        "failed": ("attention", False),
        "cancelled": ("attention", False),
        "pending_first_run": ("working", False),
        "ok": ("current", False),
        "idle": ("current", False),
    }
    assert set(expected) == set(_ALL_STATUSES)
    for status, (verdict, self_healing) in expected.items():
        v, sh, _ = compute_verdict(status=status, stale_reasons=())
        assert (v, sh) == (verdict, self_healing), status


def test_running_with_stuck_headline() -> None:
    """A running row that is stuck reads 'running but no progress'."""
    v, _, reason = compute_verdict(status="running", stale_reasons=("mid_flight_stuck",))
    assert v == "attention"
    assert reason == "running but no progress"


def test_running_with_queue_stuck_not_working() -> None:
    """The exact masking gap Codex flagged: running + queue_stuck."""
    v, _, reason = compute_verdict(status="running", stale_reasons=("queue_stuck",))
    assert v == "attention"
    assert reason == "queue stuck"


def test_pending_retry_with_queue_stuck_not_self_healing() -> None:
    """Codex gap: pending_retry + queue_stuck must surface, not hide."""
    v, sh, reason = compute_verdict(status="pending_retry", stale_reasons=("queue_stuck",))
    assert v == "attention"
    assert sh is False
    assert reason == "queue stuck"


def test_failed_with_stale_prefers_failure_headline() -> None:
    """A failed row that is also overdue headlines 'last run failed'."""
    v, _, reason = compute_verdict(status="failed", stale_reasons=("schedule_missed",))
    assert v == "attention"
    assert reason == "last run failed"


def test_headline_uses_fixed_reason_order() -> None:
    """Multiple reasons → first in display order wins the headline."""
    _, _, reason = compute_verdict(status="ok", stale_reasons=("queue_stuck", "schedule_missed"))
    assert reason == "schedule missed"


def test_ok_overdue_is_attention_not_ok() -> None:
    """The headline contradiction #1489: ok + schedule_missed → attention."""
    v, _, reason = compute_verdict(status="ok", stale_reasons=("schedule_missed",))
    assert v == "attention"
    assert reason == "schedule missed"


def test_idle_overdue_is_attention() -> None:
    """idle + schedule_missed (catch-up trap surface) → attention."""
    v, _, _ = compute_verdict(status="idle", stale_reasons=("schedule_missed",))
    assert v == "attention"


# --- #1511 / T5 watermark look-through ----------------------------------


def test_watermark_default_preserves_first_run_pending() -> None:
    """Default ``watermark_is_fresh=False`` keeps the shipped behaviour:
    a never-run job reads blue 'first run pending'."""
    v, sh, reason = compute_verdict(status="pending_first_run", stale_reasons=())
    assert (v, sh, reason) == ("working", False, "first run pending")


def test_pending_first_run_fresh_watermark_reads_current() -> None:
    """Look-through: a never-run job whose bootstrap-covered source is still
    fresh reads green Current, not 'first run pending'."""
    v, sh, reason = compute_verdict(status="pending_first_run", stale_reasons=(), watermark_is_fresh=True)
    assert (v, sh, reason) == ("current", False, "")


def test_pending_first_run_stale_watermark_stays_working() -> None:
    """Covered-but-stale (watermark_is_fresh=False) stays 'first run pending'."""
    v, sh, reason = compute_verdict(status="pending_first_run", stale_reasons=(), watermark_is_fresh=False)
    assert (v, sh, reason) == ("working", False, "first run pending")


@pytest.mark.parametrize("reason", _ALL_REASONS)
def test_fresh_watermark_never_overrides_actionable_stale(reason: StaleReason) -> None:
    """The look-through must not mask an actionable stale reason: a fresh
    watermark on a pending_first_run row with a real stale reason still reads
    attention (stale precedence is above the status branch)."""
    v, sh, _ = compute_verdict(status="pending_first_run", stale_reasons=(reason,), watermark_is_fresh=True)
    assert v == "attention"
    assert sh is False


@pytest.mark.parametrize("status", [s for s in _ALL_STATUSES if s != "pending_first_run"])
def test_fresh_watermark_only_affects_pending_first_run(status: ProcessStatus) -> None:
    """Only ``pending_first_run`` consumes ``watermark_is_fresh`` — every other
    status maps identically with the flag set or unset."""
    base = compute_verdict(status=status, stale_reasons=())
    with_fresh = compute_verdict(status=status, stale_reasons=(), watermark_is_fresh=True)
    assert base == with_fresh


@pytest.mark.parametrize("status", _ALL_STATUSES)
@pytest.mark.parametrize("reasons", _all_reason_subsets())
def test_total_and_single_valued_with_fresh_watermark(status: ProcessStatus, reasons: tuple[StaleReason, ...]) -> None:
    """Totality holds with the look-through flag set, too."""
    verdict, self_healing, _ = compute_verdict(status=status, stale_reasons=reasons, watermark_is_fresh=True)
    assert verdict in _VALID_VERDICTS
    assert self_healing == (verdict == "self_healing")


# --- #1509 / T3 retry/backoff -------------------------------------------


def test_failed_with_future_retry_reads_will_retry() -> None:
    """A transiently-failed row with a future retry reads Self-healing
    'will retry HH:MM' instead of red attention."""
    v, sh, reason = compute_verdict(status="failed", stale_reasons=(), retry_in_flight=True, retry_at_display="14:30")
    assert (v, sh, reason) == ("self_healing", True, "will retry 14:30")


def test_failed_with_due_retry_reads_retrying_shortly() -> None:
    """Retry due (empty display) but sweeper not yet fired → still recovery,
    no red flicker; reads 'retrying shortly'."""
    v, sh, reason = compute_verdict(status="failed", stale_reasons=(), retry_in_flight=True, retry_at_display="")
    assert (v, sh, reason) == ("self_healing", True, "retrying shortly")


def test_pending_retry_with_explicit_retry_prefers_hhmm() -> None:
    """An explicit ``next_retry_at`` backoff label beats the cadence-covered
    'retry scheduled' fallback."""
    v, sh, reason = compute_verdict(
        status="pending_retry", stale_reasons=(), retry_in_flight=True, retry_at_display="09:05"
    )
    assert (v, sh, reason) == ("self_healing", True, "will retry 09:05")


def test_retry_suppresses_schedule_missed() -> None:
    """A pending retry IS the fix for a missed schedule — schedule_missed is
    suppressed so the row reads Self-healing, not attention."""
    v, sh, _ = compute_verdict(
        status="failed", stale_reasons=("schedule_missed",), retry_in_flight=True, retry_at_display="14:30"
    )
    assert (v, sh) == ("self_healing", True)


@pytest.mark.parametrize("wedge", ["queue_stuck", "mid_flight_stuck", "watermark_gap"])
def test_retry_never_masks_genuine_wedge(wedge: StaleReason) -> None:
    """Codex ckpt-1 invariant: a retry must NOT paint a genuinely-wedged row
    self-healing — queue_stuck / mid_flight_stuck / watermark_gap still win."""
    v, sh, _ = compute_verdict(status="failed", stale_reasons=(wedge,), retry_in_flight=True, retry_at_display="14:30")
    assert v == "attention"
    assert sh is False


@pytest.mark.parametrize("status", [s for s in _ALL_STATUSES if s not in ("failed", "pending_retry")])
def test_retry_only_affects_failed_and_pending_retry(status: ProcessStatus) -> None:
    """``retry_in_flight`` reclassifies only failed / pending_retry rows; every
    other status (no stale) maps identically with the flag set.

    (``schedule_missed`` is excluded from this comparison: the flag legitimately
    suppresses it, which is covered by ``test_retry_suppresses_schedule_missed``.)"""
    base = compute_verdict(status=status, stale_reasons=())
    with_retry = compute_verdict(status=status, stale_reasons=(), retry_in_flight=True, retry_at_display="14:30")
    assert base == with_retry


@pytest.mark.parametrize("status", _ALL_STATUSES)
@pytest.mark.parametrize("reasons", _all_reason_subsets())
def test_total_and_single_valued_with_retry(status: ProcessStatus, reasons: tuple[StaleReason, ...]) -> None:
    """Totality holds with the retry flag set, too."""
    verdict, self_healing, _ = compute_verdict(
        status=status, stale_reasons=reasons, retry_in_flight=True, retry_at_display="14:30"
    )
    assert verdict in _VALID_VERDICTS
    assert self_healing == (verdict == "self_healing")


# ----------------------------------------------------------------------
# #1510 / T4 — liveness_kick_in_flight (watchdog re-enqueue look-through)
# ----------------------------------------------------------------------


@pytest.mark.parametrize("status", ["ok", "idle", "pending_first_run"])
def test_liveness_kick_on_stalled_reads_self_healing(status: ProcessStatus) -> None:
    """A stalled job (overdue ``ok``/``idle``/``pending_first_run`` +
    schedule_missed) that the watchdog has re-enqueued reads Self-healing —
    crucially even though a kick does NOT flip the adapter status to running."""
    v, sh, reason = compute_verdict(status=status, stale_reasons=("schedule_missed",), liveness_kick_in_flight=True)
    assert v == "self_healing"
    assert sh is True
    assert reason == "re-enqueued, recovering"


def test_liveness_kick_suppresses_schedule_missed() -> None:
    """The kick IS the fix for a missed schedule — it suppresses that reason."""
    v, _, _ = compute_verdict(status="ok", stale_reasons=("schedule_missed",), liveness_kick_in_flight=True)
    assert v == "self_healing"


@pytest.mark.parametrize("wedge", ["queue_stuck", "mid_flight_stuck", "watermark_gap"])
def test_liveness_kick_never_masks_genuine_wedge(wedge: StaleReason) -> None:
    """Codex ckpt-1 invariant: a kick must NOT paint a genuinely-wedged row
    self-healing — a kick into a stuck queue does not un-stick it."""
    v, sh, _ = compute_verdict(status="running", stale_reasons=(wedge,), liveness_kick_in_flight=True)
    assert v == "attention"
    assert sh is False


def test_liveness_kick_on_recovered_row_reads_honest_status() -> None:
    """Codex ckpt-2: a kick request can linger pending/claimed after a natural
    fire already cleared the stall (no schedule_missed). The recovered row must
    read its honest status (current), NOT be repainted 're-enqueued, recovering'."""
    v, sh, reason = compute_verdict(status="ok", stale_reasons=(), liveness_kick_in_flight=True)
    assert v == "current"
    assert sh is False
    assert reason == ""


def test_disabled_outranks_liveness_kick() -> None:
    """Kill switch still wins over an in-flight kick."""
    v, sh, reason = compute_verdict(status="disabled", stale_reasons=("schedule_missed",), liveness_kick_in_flight=True)
    assert v == "attention"
    assert reason == "kill switch active"


@pytest.mark.parametrize("status", _ALL_STATUSES)
@pytest.mark.parametrize("reasons", _all_reason_subsets())
def test_total_and_single_valued_with_liveness_kick(status: ProcessStatus, reasons: tuple[StaleReason, ...]) -> None:
    """Totality holds with the liveness-kick flag set, too."""
    verdict, self_healing, _ = compute_verdict(status=status, stale_reasons=reasons, liveness_kick_in_flight=True)
    assert verdict in _VALID_VERDICTS
    assert self_healing == (verdict == "self_healing")


# ---------------------------------------------------------------------------
# C6 (#1508) — never-started bound on a persisted first-seen anchor.
# A scheduled job with zero lifetime rows that is now overdue past its first
# expected fire is broken-from-day-one (attention "never started"), not
# forever-green "first run pending".
# ---------------------------------------------------------------------------


def test_never_started_past_grace_is_attention() -> None:
    """Overdue past first expected fire with zero rows reads attention."""
    v, sh, reason = compute_verdict(status="pending_first_run", stale_reasons=(), never_started=True)
    assert v == "attention"
    assert sh is False
    assert reason == "never started"


def test_pending_first_run_within_grace_stays_working() -> None:
    """Within grace (``never_started=False``) keeps the shipped 'first run
    pending' behaviour — ``watermark_is_fresh=False`` so it does not fall into
    the look-through 'current' branch."""
    v, sh, reason = compute_verdict(
        status="pending_first_run", stale_reasons=(), never_started=False, watermark_is_fresh=False
    )
    assert v == "working"
    assert sh is False
    assert reason == "first run pending"


def test_never_started_outranks_fresh_watermark() -> None:
    """A genuinely broken-from-day-one job (never_started) reads attention even
    if its source watermark happens to look fresh — never_started is the
    stronger signal that this specific job has produced nothing."""
    v, _, reason = compute_verdict(
        status="pending_first_run", stale_reasons=(), never_started=True, watermark_is_fresh=True
    )
    assert v == "attention"
    assert reason == "never started"


@pytest.mark.parametrize("status", [s for s in _ALL_STATUSES if s != "pending_first_run"])
def test_never_started_only_affects_pending_first_run(status: ProcessStatus) -> None:
    """Only ``pending_first_run`` consumes ``never_started`` — every other
    status maps identically with the flag set or unset."""
    base = compute_verdict(status=status, stale_reasons=())
    with_flag = compute_verdict(status=status, stale_reasons=(), never_started=True)
    assert base == with_flag


@pytest.mark.parametrize("status", _ALL_STATUSES)
@pytest.mark.parametrize("reasons", _all_reason_subsets())
def test_total_and_single_valued_with_never_started(status: ProcessStatus, reasons: tuple[StaleReason, ...]) -> None:
    """Totality holds with the never-started flag set, too."""
    verdict, self_healing, _ = compute_verdict(status=status, stale_reasons=reasons, never_started=True)
    assert verdict in _VALID_VERDICTS
    assert self_healing == (verdict == "self_healing")


# --- Operator-cancel look-through (#1508 / Task 5) -----------------------


def test_operator_cancel_is_benign_green() -> None:
    """A deliberate operator cancel reads Current (green) until the next fire."""
    v, _, _ = compute_verdict(status="cancelled", stale_reasons=(), cancel_was_operator_initiated=True)
    assert v == "current"


def test_system_cancel_stays_attention() -> None:
    """A cancel NOT traceable to an operator request (system/crash) stays red."""
    v, _, reason = compute_verdict(status="cancelled", stale_reasons=(), cancel_was_operator_initiated=False)
    assert v == "attention" and reason == "last run cancelled"


def test_operator_cancel_never_masks_actionable_stale() -> None:
    """A benign operator cancel must NOT hide a genuine wedge (ckpt-1 invariant)."""
    v, _, _ = compute_verdict(status="cancelled", stale_reasons=("queue_stuck",), cancel_was_operator_initiated=True)
    assert v == "attention"


# --- Recovery-never-masks-a-wedge invariant (#1508 / Task 6) -------------
#
# Pins the #1509/#1510 guarantee: a recovery signal (retry-in-flight or
# liveness-kick-in-flight) suppresses ONLY ``schedule_missed`` — it must
# NEVER mask a genuine wedge (``watermark_gap`` / ``queue_stuck`` /
# ``mid_flight_stuck``). Those three stay attention even when paired with a
# recovery signal. Regression-proofs ``compute_verdict``'s precedence across
# the whole #1508 effort.


@pytest.mark.parametrize("wedge", ["watermark_gap", "queue_stuck", "mid_flight_stuck"])
@pytest.mark.parametrize("recover", ["retry_in_flight", "liveness_kick_in_flight"])
def test_recovery_signal_never_masks_a_wedge(wedge: str, recover: str) -> None:
    kw = {"status": "failed", "stale_reasons": ("schedule_missed", wedge), recover: True}
    if recover == "retry_in_flight":
        kw["retry_at_display"] = "21:20"
    verdict, _, _ = compute_verdict(**kw)  # type: ignore[arg-type]
    assert verdict == "attention", f"{recover} masked {wedge}"
