"""#2603 — re-arming a scheduled fire lost to a busy lane.

Pure tests only: the admission policy, the settled decision it must not
reverse, and the health verdict an armed row produces. The DB-backed half
(sweeping, the dispatch cap) lives in ``tests/services/test_job_retry.py``; the
arming site's own tests are in ``tests/test_lane_busy_retry.py``.
"""

from __future__ import annotations

import pytest

from app.jobs.runtime import lane_busy_rearm_delay_seconds, min_cadence_gap_seconds
from app.services.ops_monitor import RETRY_BASE_SECONDS
from app.services.processes.health_verdict import compute_verdict
from app.workers.scheduler import (
    JOB_CORE_ELIGIBILITY_REFRESH,
    JOB_CORE_REBALANCE_OBSERVATION,
    JOB_EXECUTE_APPROVED_ORDERS,
    SCHEDULED_JOBS,
    Cadence,
    ScheduledJob,
)

_BY_NAME = {job.name: job for job in SCHEDULED_JOBS}


# --------------------------------------------------------------------------
# min_cadence_gap_seconds — a LOWER BOUND, so it may understate, never overstate
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cadence", "expected"),
    [
        (Cadence.every_n_minutes(interval=1), 60),
        (Cadence.every_n_minutes(interval=5), 300),
        (Cadence.every_n_minutes(interval=6), 360),
        (Cadence.every_n_minutes(interval=30), 1_800),
        (Cadence.hourly(minute=20), 3_600),
        (Cadence.daily(hour=22, minute=45), 86_400),
        (Cadence.weekly(weekday=0, hour=1), 7 * 86_400),
        (Cadence.monthly(day=1, hour=1), 28 * 86_400),
        (Cadence.yearly(month=4, day=1, hour=1), 365 * 86_400),
    ],
)
def test_min_cadence_gap_seconds(cadence: Cadence, expected: int) -> None:
    assert min_cadence_gap_seconds(cadence) == expected


def test_min_cadence_gap_covers_every_registered_cadence_kind() -> None:
    """No registered job may hit an unmapped kind — a KeyError here is the bug.

    Pinned against the live registry rather than a hand-listed set, so a new
    ``CadenceKind`` cannot ship without this function learning about it.
    """
    for job in SCHEDULED_JOBS:
        assert min_cadence_gap_seconds(job.cadence) > 0


# --------------------------------------------------------------------------
# Admission
# --------------------------------------------------------------------------


def _job(name: str, cadence: Cadence, *, rearm: bool) -> ScheduledJob:
    return ScheduledJob(
        name=name,
        display_name=name,
        # ``source`` is a typed ``Lane`` literal, so a fixture cannot invent one.
        # Which lane is irrelevant here — admission reads the flag and the cadence.
        source="etoro_core_rebalance",
        description="fixture",
        cadence=cadence,
        rearm_on_lost_fire=rearm,
    )


def test_unknown_job_never_arms() -> None:
    """An unregistered name resolves to ``None`` and must not arm."""
    assert lane_busy_rearm_delay_seconds(None) is None


def test_opt_out_is_the_default_even_for_a_daily_job() -> None:
    """Cadence alone must never admit — this is the whole #2603 ckpt-1 finding."""
    daily_but_silent = _job("daily_no_optin", Cadence.daily(hour=3), rearm=False)
    assert lane_busy_rearm_delay_seconds(daily_but_silent) is None


@pytest.mark.parametrize(
    "cadence",
    [
        Cadence.hourly(minute=20),
        Cadence.daily(hour=22, minute=45),
        Cadence.weekly(weekday=0, hour=1),
        Cadence.monthly(day=1, hour=1),
        Cadence.yearly(month=4, day=1, hour=1),
        Cadence.every_n_minutes(interval=6),
    ],
)
def test_opted_in_slow_cadences_arm_at_the_retry_base(cadence: Cadence) -> None:
    assert lane_busy_rearm_delay_seconds(_job("slow", cadence, rearm=True)) == RETRY_BASE_SECONDS


@pytest.mark.parametrize("interval", [1, 2, 3, 4, 5])
def test_opted_in_fast_cadences_do_not_arm(interval: int) -> None:
    """The dominance guard: at or below the retry base the natural fire wins.

    ``interval=5`` is the boundary and must REJECT — its next fire lands at the
    same moment the first retry would, so arming could only add load.
    """
    cadence = Cadence.every_n_minutes(interval=interval)
    assert min_cadence_gap_seconds(cadence) <= RETRY_BASE_SECONDS
    assert lane_busy_rearm_delay_seconds(_job("fast", cadence, rearm=True)) is None


# --------------------------------------------------------------------------
# The settled decision this feature must not reverse
# --------------------------------------------------------------------------


def test_execute_approved_orders_is_never_rearmed() -> None:
    """⚠⚠ Order execution must never be re-fired as a surprise catch-up.

    ``execute_approved_orders`` is ``Cadence.daily(6, 30)`` with
    ``catch_up_on_boot=False`` and a registry comment stating that execution
    happens at its scheduled time and never later. A re-arm rule derived from
    cadence would have admitted it and re-fired ORDER SUBMISSION hours late.
    This test is the enforcement — the comment alone is not.
    """
    job = _BY_NAME[JOB_EXECUTE_APPROVED_ORDERS]
    assert job.rearm_on_lost_fire is False
    assert lane_busy_rearm_delay_seconds(job) is None


def test_exactly_the_two_core_producers_opt_in() -> None:
    """Pin the admitted set so a third job cannot join it unnoticed.

    Widening this set is a per-job judgement about idempotence and
    fire-time-indifference (see ``ScheduledJob.rearm_on_lost_fire``). Changing
    this assertion is the moment to make that argument in writing.
    """
    opted_in = {job.name for job in SCHEDULED_JOBS if job.rearm_on_lost_fire}
    assert opted_in == {JOB_CORE_REBALANCE_OBSERVATION, JOB_CORE_ELIGIBILITY_REFRESH}


def test_both_admitted_jobs_actually_arm() -> None:
    """The flag is inert unless the cadence guard also passes — check both."""
    for name in (JOB_CORE_REBALANCE_OBSERVATION, JOB_CORE_ELIGIBILITY_REFRESH):
        assert lane_busy_rearm_delay_seconds(_BY_NAME[name]) == RETRY_BASE_SECONDS


# --------------------------------------------------------------------------
# Health verdict — an armed row must not read Current
# --------------------------------------------------------------------------


def test_armed_skip_with_a_missed_schedule_reads_self_healing() -> None:
    """The false green this change would otherwise have introduced.

    A lane-busy skip leaves the adapter status ``idle``, so before #2603 the
    ``retry_in_flight`` filter stripped ``schedule_missed`` and the row fell
    through to Current — an armed retry hiding the staleness it is recovering.
    """
    verdict, healthy, reason = compute_verdict(
        status="idle",
        stale_reasons=("schedule_missed",),
        retry_in_flight=True,
        retry_at_display="23:45",
    )
    assert verdict == "self_healing"
    assert healthy is True
    assert reason == "lost fire, retrying 23:45"


def test_armed_skip_reads_self_healing_without_a_future_label() -> None:
    """Due but not yet swept: still scheduled recovery, not a red."""
    verdict, _healthy, reason = compute_verdict(
        status="idle",
        stale_reasons=("schedule_missed",),
        retry_in_flight=True,
        retry_at_display="",
    )
    assert verdict == "self_healing"
    assert reason == "lost fire, retrying shortly"


def test_armed_row_whose_stall_already_cleared_reads_its_honest_status() -> None:
    """A lingering armed row must not be repainted once a natural fire recovered it.

    Mirrors the ``kick_is_recovering`` gate: no ``schedule_missed`` means there
    is nothing being recovered, so the row reads Current on its own merits.
    """
    verdict, _healthy, _reason = compute_verdict(
        status="idle",
        stale_reasons=(),
        retry_in_flight=True,
    )
    assert verdict == "current"


def test_existing_failed_retry_copy_is_unchanged() -> None:
    """#1509's branch must keep its own verdict and wording.

    The #2603 branch sits after it deliberately; this pins that ordering so a
    later edit cannot let the new copy swallow the failure case.
    """
    verdict, healthy, reason = compute_verdict(
        status="failed",
        stale_reasons=("schedule_missed",),
        retry_in_flight=True,
        retry_at_display="04:10",
    )
    assert (verdict, healthy, reason) == ("self_healing", True, "will retry 04:10")


def test_running_still_outranks_an_armed_row() -> None:
    """A job that is actually running reads ``working``, not ``self_healing``."""
    verdict, _healthy, _reason = compute_verdict(
        status="running",
        stale_reasons=("schedule_missed",),
        retry_in_flight=True,
    )
    assert verdict == "working"


def test_a_genuine_wedge_is_never_masked_by_an_armed_retry() -> None:
    """ckpt-1 invariant, preserved: queue_stuck still outranks recovery."""
    verdict, healthy, _reason = compute_verdict(
        status="idle",
        stale_reasons=("schedule_missed", "queue_stuck"),
        retry_in_flight=True,
    )
    assert verdict == "attention"
    assert healthy is False


def test_exhausted_rearm_returns_to_attention_not_current() -> None:
    """Once re-dispatch is exhausted the row must go back to being a problem.

    ⚠ Codex ckpt-2 read this as a false green: clearing ``next_retry_at`` on an
    exhausted row leaves it ``skipped`` → ``idle``, which it argued would render
    Current until the next cadence. It does not, and the reason is upstream: a
    ``lane_busy`` skip is in ``scheduled_adapter._NON_ANCHORING_SKIP_PREFIXES``,
    so it never anchors ``expected_fire_at``. The anchor stays at the last
    genuine run, ``schedule_missed`` keeps firing, and once ``retry_in_flight``
    goes false nothing strips it any more.

    This test is the enforcement of that, because the reasoning lives in two
    files and a future edit to either could quietly make the objection true.
    """
    verdict, healthy, _reason = compute_verdict(
        status="idle",
        stale_reasons=("schedule_missed",),
        retry_in_flight=False,
    )
    assert verdict == "attention"
    assert healthy is False
