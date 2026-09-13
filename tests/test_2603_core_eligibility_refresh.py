"""#2603 item 2 — the revalidation half's constants, wiring and job behaviour.

Pure tier: no Postgres. The selection SQL is exercised in
``test_2603_core_eligibility_refresh_db``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest

from app.services.strategy_core_eligibility import (
    CORE_ELIGIBILITY_MAX_AGE,
    CORE_ELIGIBILITY_REQUEST_INTERVAL_S,
)
from app.services.strategy_core_eligibility_refresh import (
    CORE_ELIGIBILITY_REFRESH_AGE,
    CORE_ELIGIBILITY_REFRESH_MAX_PER_RUN,
    REQUEST_INTERVAL_S,
)
from app.workers.scheduler import JOB_CORE_ELIGIBILITY_REFRESH, SCHEDULED_JOBS


def _registered_job() -> Any:
    for job in SCHEDULED_JOBS:
        if job.name == JOB_CORE_ELIGIBILITY_REFRESH:
            return job
    raise AssertionError(f"{JOB_CORE_ELIGIBILITY_REFRESH} is not in SCHEDULED_JOBS")


def _registered_tick() -> timedelta:
    """The interval between two fires of the REGISTERED cadence.

    Deliberately resolved from the ``ScheduledJob`` rather than written down: a
    test against a literal ``timedelta(hours=1)`` keeps passing if the job is
    later moved to ``daily``, which is exactly the change that would silently
    delete the staleness margin.
    """
    cadence = _registered_job().cadence
    if cadence.kind == "hourly":
        return timedelta(hours=1)
    if cadence.kind == "daily":
        return timedelta(days=1)
    if cadence.kind == "every_n_minutes":
        return timedelta(minutes=cadence.interval)
    raise AssertionError(f"unhandled cadence kind {cadence.kind!r}")


# --------------------------------------------------------------------------
# The constants, and the inequality that justifies them
# --------------------------------------------------------------------------


def test_the_refresh_trigger_is_derived_from_the_freshness_bound() -> None:
    """Not an independently chosen number.

    If someone re-bases ``CORE_ELIGIBILITY_MAX_AGE``, the trigger has to move
    with it or the margin below is silently redefined.
    """
    assert CORE_ELIGIBILITY_REFRESH_AGE == CORE_ELIGIBILITY_MAX_AGE / 2


def test_a_proof_cannot_expire_within_one_registered_tick_of_its_refresh() -> None:
    """The whole construction, pinned against the REGISTERED cadence.

    Worst case an instrument is refreshed one tick after it passes
    ``REFRESH_AGE``. If that sum reaches ``MAX_AGE`` the job cannot keep any
    proof usable and every downstream gate fails between ticks.
    """
    assert CORE_ELIGIBILITY_REFRESH_AGE + _registered_tick() < CORE_ELIGIBILITY_MAX_AGE


def test_the_margin_leaves_room_for_consecutive_missed_ticks() -> None:
    """Stated as a number so a cadence change has to restate it.

    ⚠ NOMINAL ticks, not wall-clock: lane contention, request spacing and HTTP
    retries all consume real time this inequality does not model.
    """
    tick = _registered_tick()
    spare = CORE_ELIGIBILITY_MAX_AGE - CORE_ELIGIBILITY_REFRESH_AGE - tick
    assert spare // tick >= 11


def test_the_per_run_cap_fits_inside_one_tick_at_the_documented_spacing() -> None:
    """The cap is a RUNTIME bound, so it has to be one.

    It is not the endpoint's 100-ids-per-request ceiling reused: that figure
    bounds one request and says nothing about a job making singleton ones.
    """
    worst_case = timedelta(seconds=CORE_ELIGIBILITY_REFRESH_MAX_PER_RUN * REQUEST_INTERVAL_S)
    assert worst_case < _registered_tick()


def test_the_request_interval_has_one_definition() -> None:
    """The prover imports it rather than keeping a second copy.

    Two copies of a rate-limit constant is the shape where one gets tuned and
    the other does not.
    """
    import scripts.prove_2603_core_eligibility as prover

    assert prover.REQUEST_INTERVAL_S is CORE_ELIGIBILITY_REQUEST_INTERVAL_S
    assert REQUEST_INTERVAL_S is CORE_ELIGIBILITY_REQUEST_INTERVAL_S


# --------------------------------------------------------------------------
# Wiring — a ScheduledJob entry alone does not make a job run
# --------------------------------------------------------------------------


def test_the_job_is_registered_in_both_the_schedule_and_the_invoker_map() -> None:
    """``JobRuntime.start`` skips any SCHEDULED_JOBS entry absent from
    ``_INVOKERS`` with a bare ``continue`` — so a job declared and not wired is
    a job that never fires and never says so."""
    from app.jobs.runtime import _INVOKERS

    assert _registered_job() is not None
    assert JOB_CORE_ELIGIBILITY_REFRESH in _INVOKERS


def test_the_job_shares_the_etoro_lane() -> None:
    """The lane is what serialises this against the core observation job, so
    neither holds a broker session while the other is mid-batch."""
    assert _registered_job().source == "etoro"


# --------------------------------------------------------------------------
# Selection arithmetic, with the DB read stubbed
# --------------------------------------------------------------------------

_API = UUID("11111111-1111-1111-1111-111111111111")
_USER = UUID("22222222-2222-2222-2222-222222222222")
_OTHER = UUID("33333333-3333-3333-3333-333333333333")


@dataclass
class _FakeCursor:
    rows: list[tuple[Any, ...]]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class _FakeConn:
    """Returns a canned latest-proof-per-instrument result set."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.params: dict[str, Any] | None = None

    def execute(self, _sql: str, params: dict[str, Any]) -> _FakeCursor:
        self.params = params
        return _FakeCursor(self.rows)


def _row(
    instrument_id: int,
    *,
    age_hours: float,
    verdict: str = "underlying",
    superseded: bool = False,
    symbol: str | None = None,
) -> tuple[Any, ...]:
    return (
        instrument_id,
        symbol,
        instrument_id * 10,
        verdict,
        Decimal(str(age_hours * 3600)),
        superseded,
    )


def _select(rows: list[tuple[Any, ...]], **kwargs: Any) -> Any:
    from app.services.strategy_core_eligibility_refresh import select_proofs_to_revalidate

    return select_proofs_to_revalidate(
        _FakeConn(rows),  # type: ignore[arg-type]
        operator_id=uuid4(),
        provider="etoro",
        environment="demo",
        live_credential_ids=(_API, _USER),
        **kwargs,
    )


def test_a_fresh_proof_is_not_due() -> None:
    assert _select([_row(1, age_hours=1)]).due == ()


def test_a_proof_at_exactly_the_trigger_is_not_yet_due() -> None:
    """``<=`` is fresh, matching the direction ``require_core_eligibility``
    already settled for ``MAX_AGE``. One boundary convention, not two."""
    at_trigger = CORE_ELIGIBILITY_REFRESH_AGE.total_seconds() / 3600
    assert _select([_row(1, age_hours=at_trigger)]).due == ()


def test_a_proof_one_second_past_the_trigger_is_due() -> None:
    past = (CORE_ELIGIBILITY_REFRESH_AGE.total_seconds() + 1) / 3600
    due = _select([_row(1, age_hours=past)]).due
    assert [stale.instrument_id for stale in due] == [1]


def test_a_fresh_proof_under_superseded_credentials_is_due() -> None:
    """Arm 2. ``require_core_eligibility`` compares the credential pair, so such
    a proof is unusable however fresh it is — and an age-only rule would leave a
    rotated account unusable for half the freshness window."""
    scope = _select([_row(1, age_hours=0.5, superseded=True)])
    assert [stale.instrument_id for stale in scope.due] == [1]
    assert scope.due[0].credentials_superseded is True


def test_selection_is_oldest_first() -> None:
    """The starvation control: after downtime the most-expired go first, so the
    per-run cap cannot keep deferring the same tail."""
    scope = _select([_row(1, age_hours=13), _row(2, age_hours=40), _row(3, age_hours=20)])
    assert [stale.instrument_id for stale in scope.due] == [2, 3, 1]


def test_the_cap_defers_the_remainder_rather_than_raising_or_truncating_silently() -> None:
    """Raising would make a backlog a permanent outage that never makes
    progress; silent truncation would read as "everything was refreshed"."""
    scope = _select([_row(i, age_hours=13 + i) for i in range(1, 6)], limit=2)
    assert len(scope.due) == 2
    assert scope.deferred_count == 3
    assert [stale.instrument_id for stale in scope.due] == [5, 4]


def test_an_all_fresh_population_is_distinguishable_from_an_unproved_one() -> None:
    """The two produce different operator-facing skips, so the counts that
    decide between them have to survive selection."""
    assert _select([]).proved_instrument_count == 0
    assert _select([_row(1, age_hours=1), _row(2, age_hours=1)]).proved_instrument_count == 2


def test_the_prior_verdict_is_carried_so_a_transition_can_be_reported() -> None:
    """An ``underlying -> not_underlying`` flip moves ``quotes_refresh`` arm-5
    membership; finding that out must cost a glance at the job note."""
    scope = _select([_row(1, age_hours=30, verdict="not_underlying", symbol="X")])
    assert scope.due[0].prior_verdict == "not_underlying"
    assert scope.due[0].symbol == "X"


def test_the_live_pair_is_passed_to_the_query_rather_than_compared_in_python() -> None:
    """The supersession test is a SQL ``IS DISTINCT FROM`` so it cannot be
    defeated by a NULL on either side."""
    conn = _FakeConn([])
    from app.services.strategy_core_eligibility_refresh import select_proofs_to_revalidate

    select_proofs_to_revalidate(
        conn,  # type: ignore[arg-type]
        operator_id=uuid4(),
        provider="etoro",
        environment="demo",
        live_credential_ids=(_API, _OTHER),
    )
    assert conn.params is not None
    assert conn.params["api_key_credential_id"] == _API
    assert conn.params["user_key_credential_id"] == _OTHER


@pytest.mark.parametrize("limit", [0, -1])
def test_a_non_positive_limit_is_a_caller_error(limit: int) -> None:
    with pytest.raises(ValueError, match="limit must be at least 1"):
        _select([], limit=limit)


def test_a_negative_refresh_age_is_a_caller_error() -> None:
    with pytest.raises(ValueError, match="refresh_age must not be negative"):
        _select([], refresh_age=timedelta(seconds=-1))


# --------------------------------------------------------------------------
# The job reaches no mutating broker method
# --------------------------------------------------------------------------


def test_the_job_only_reaches_the_informational_eligibility_method() -> None:
    """``check_instrument_eligibility`` is deliberately OUTSIDE
    ``refuse_broker_mutation_if_unattended`` (#2645). This asserts the job does
    not drift onto a guarded method — the source scan is the drift detector, the
    runtime refusal is the control."""
    import inspect

    from app.workers import scheduler

    source = inspect.getsource(scheduler.core_eligibility_refresh)
    for mutating in (
        "submit_order",
        "close_position",
        "cancel_order",
        "edit_order",
        "put_trade_request",
    ):
        assert mutating not in source
    assert "check_instrument_eligibility" in source
