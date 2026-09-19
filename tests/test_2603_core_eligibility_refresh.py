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
    """The cap is a WORK bound, so it has to fit the tick it runs in.

    It is not the endpoint's 100-ids-per-request ceiling reused: that figure
    bounds one request and says nothing about a job making singleton ones.

    ⚠ It bounds the SLEEPS only. HTTP phases, retries, a 429's ``Retry-After``,
    DB work and advisory-lock waits are all outside this product, so a passing
    assertion is a design margin and not a guarantee that the run completes
    inside the tick -- the same distinction the ``REFRESH_AGE`` test above draws
    between nominal ticks and wall-clock. Calling it a "runtime bound" (as this
    docstring did before #2946 step 3) overstates what the arithmetic covers.
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


def test_the_job_owns_its_lane_and_shares_it_with_nothing() -> None:
    """This used to share ``etoro`` with the core observation job, so that
    "neither holds a broker session while the other is mid-batch".

    Both halves of that were revised (#2603): ``etoro`` is held 3.2-3.8h a day by
    ``daily_candle_refresh`` and cost this job 18 fires, and the serialisation
    was guarding an overlap that cannot contend — this job's endpoint is a
    DEDICATED 20/min eToro quota "not shared with any other endpoint", while the
    observation draws on ``E_account_read``. Separate lanes rather than one
    shared ``etoro_core``, because a 100-request batch here can still be running
    at the observation's :45 fire.
    """
    entry = _registered_job()
    assert entry.source == "etoro_core_eligibility"
    assert entry.source != "etoro"
    # Not merged with the observation's lane — that would re-create the exact
    # starvation this split exists to remove.
    assert entry.source != "etoro_core_rebalance"


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


def test_selection_is_oldest_first_among_in_scope_proofs() -> None:
    """The starvation control: after downtime the most-expired go first, so the
    per-run cap cannot keep deferring the same tail."""
    scope = _select([_row(1, age_hours=13), _row(2, age_hours=40), _row(3, age_hours=20)])
    assert [stale.instrument_id for stale in scope.due] == [2, 3, 1]


def test_a_rotation_entry_outranks_an_older_in_scope_one() -> None:
    """⚠ Age alone put arm-2 entries LAST, which was a defect (review WARNING on
    PR #2971): a superseded proof is selected precisely because its credentials
    changed, so it is typically FRESH and sorted to the back.

    A superseded proof is unusable RIGHT NOW — ``require_core_eligibility``
    compares the pair — where a stale in-scope one is only approaching that.
    """
    scope = _select([_row(1, age_hours=400), _row(2, age_hours=0.1, superseded=True)])
    assert [stale.instrument_id for stale in scope.due] == [2, 1]


def test_the_cap_cannot_defer_a_rotation_entry_behind_a_stale_backlog() -> None:
    """The regression the WARNING names, at the cap.

    Under the old age-only ordering the rotation entry would be item 6 of 6 and
    a ``limit=2`` run would defer it every tick — leaving a rotated account
    unusable for exactly as long as arm 2 exists to prevent.
    """
    rows = [_row(i, age_hours=100 + i) for i in range(1, 6)]
    rows.append(_row(99, age_hours=0.1, superseded=True))
    scope = _select(rows, limit=2)
    assert scope.due[0].instrument_id == 99
    assert scope.due[0].credentials_superseded is True
    assert scope.deferred_count == 4


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


# --------------------------------------------------------------------------
# Job control flow, with every external edge faked
# --------------------------------------------------------------------------


class _NullConn:
    """Enough connection for the paths that never touch SQL here.

    ``transaction()`` is real enough to be entered and exited: the job wraps the
    credential check and the INSERT in one so the ``FOR SHARE`` taken by
    ``live_credential_ids`` survives to the write. Both are faked out here, but
    the block still has to exist or the test would pass against a version that
    dropped it.
    """

    def __enter__(self) -> _NullConn:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def transaction(self) -> _NullConn:
        return self


@dataclass
class _Harness:
    skips: list[str]
    written: list[int]
    row_count: int | None = None
    note: str | None = None


def _install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    due: list[int],
    unlocked: Any,
    locked: Any,
    on_request: Any,
) -> _Harness:
    """Fake every edge of ``core_eligibility_refresh`` except its own logic."""
    import contextlib

    from app.services import strategy_core_eligibility as elig
    from app.services import strategy_core_eligibility_refresh as refresh
    from app.services import strategy_core_submission_gate as gate
    from app.workers import scheduler

    harness = _Harness(skips=[], written=[])

    monkeypatch.setattr(scheduler.settings, "etoro_env", "demo")
    monkeypatch.setattr(scheduler, "connect_job", lambda **_kw: _NullConn())
    monkeypatch.setattr(scheduler, "sole_operator_id", lambda _conn: uuid4())
    monkeypatch.setattr(scheduler, "_load_etoro_credentials", lambda _name: ("api", "user"))
    monkeypatch.setattr(
        scheduler,
        "_record_prereq_skip",
        lambda _name, detail: harness.skips.append(detail),
    )
    monkeypatch.setattr(elig, "live_credential_ids_unlocked", unlocked)
    monkeypatch.setattr(elig, "live_credential_ids", locked)

    @contextlib.contextmanager
    def _fake_lock(_conn: Any) -> Any:
        yield None

    monkeypatch.setattr(gate, "core_submission_lock", _fake_lock)

    def _record(_conn: Any, **kwargs: Any) -> int:
        harness.written.append(int(kwargs["instrument_id"]))
        return len(harness.written)

    monkeypatch.setattr(elig, "record_core_eligibility_proof", _record)
    monkeypatch.setattr(
        elig,
        "evaluate_core_eligibility",
        lambda response, **_kw: response,
    )

    class _FakeBroker:
        def __init__(self, **_kw: Any) -> None: ...

        def __enter__(self) -> _FakeBroker:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def check_instrument_eligibility(self, ids: list[int]) -> Any:
            return on_request(ids[0])

    monkeypatch.setattr(
        "app.providers.implementations.etoro_broker.EtoroBrokerProvider",
        _FakeBroker,
    )
    monkeypatch.setattr(
        refresh,
        "select_proofs_to_revalidate",
        lambda _conn, **_kw: refresh.RevalidationScope(
            due=tuple(
                refresh.StaleProof(
                    instrument_id=i,
                    symbol=f"S{i}",
                    prior_proof_id=i,
                    prior_verdict="underlying",
                    prior_age=timedelta(hours=20),
                    credentials_superseded=False,
                )
                for i in due
            ),
            proved_instrument_count=len(due),
            deferred_count=0,
        ),
    )

    import contextlib as _ctx

    @_ctx.contextmanager
    def _fake_tracked(_name: str) -> Any:
        class _T:
            row_count: int | None = None
            note: str | None = None

        tracker = _T()
        try:
            yield tracker
        finally:
            harness.row_count = tracker.row_count
            harness.note = tracker.note

    monkeypatch.setattr(scheduler, "_tracked_job", _fake_tracked)
    monkeypatch.setattr("time.sleep", lambda _s: None)
    return harness


_PAIR = (_API, _USER)


def _verdict(instrument_id: int) -> Any:
    class _A:
        verdict = "underlying"

    return _A()


def test_missing_credentials_skip_rather_than_failing_hourly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``live_credential_ids_unlocked`` RAISES on a missing or revoked pair — it
    is not a "return None" reader. Unguarded, an idle box with no demo
    credentials records an hourly FAILURE instead of an hourly skip, and the
    credentials-missing skip further down is unreachable."""
    from app.services.strategy_core_eligibility import CoreEligibilityError
    from app.workers import scheduler

    def _raise(*_a: Any, **_kw: Any) -> tuple[UUID, UUID]:
        raise CoreEligibilityError("no live etoro demo credential pair for this operator")

    harness = _install(monkeypatch, due=[1], unlocked=_raise, locked=_raise, on_request=_verdict)
    scheduler.core_eligibility_refresh()

    assert harness.written == []
    assert len(harness.skips) == 1
    assert "credentials missing" in harness.skips[0]


def test_a_rotation_mid_batch_aborts_the_run_instead_of_failing_one_instrument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The plaintext keys are loaded ONCE before the loop, so after a rotation
    every remaining request is still made with the superseded pair. Attributing
    those responses to the new credential ids would let old-account evidence
    pass ``require_core_eligibility`` for the new account.

    ⚠ The per-instrument ``except Exception`` must NOT swallow this.
    """
    from app.workers import scheduler

    rotated = (uuid4(), uuid4())
    harness = _install(
        monkeypatch,
        due=[1, 2, 3],
        unlocked=lambda *_a, **_kw: _PAIR,
        locked=lambda *_a, **_kw: rotated,
        on_request=_verdict,
    )
    with pytest.raises(RuntimeError, match="rotated during the eligibility refresh batch"):
        scheduler.core_eligibility_refresh()

    assert harness.written == []


def test_the_proof_is_attributed_to_the_credentials_that_made_the_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unchanged credentials: the batch records against the captured pair."""
    from app.workers import scheduler

    harness = _install(
        monkeypatch,
        due=[1, 2],
        unlocked=lambda *_a, **_kw: _PAIR,
        locked=lambda *_a, **_kw: _PAIR,
        on_request=_verdict,
    )
    scheduler.core_eligibility_refresh()

    assert harness.written == [1, 2]
    assert harness.row_count == 2
    assert harness.note is not None
    assert "written=2 failed=0" in harness.note


def test_one_instrument_failing_does_not_stop_the_others(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transport failure writes NOTHING for that instrument and is counted —
    storing it as an observation would turn "we could not ask" into "the broker
    said"."""
    from app.workers import scheduler

    def _flaky(instrument_id: int) -> Any:
        if instrument_id == 2:
            raise ConnectionError("boom")
        return _verdict(instrument_id)

    harness = _install(
        monkeypatch,
        due=[1, 2, 3],
        unlocked=lambda *_a, **_kw: _PAIR,
        locked=lambda *_a, **_kw: _PAIR,
        on_request=_flaky,
    )
    scheduler.core_eligibility_refresh()

    assert harness.written == [1, 3]
    assert harness.row_count == 2
    assert harness.note is not None
    assert "failed=1" in harness.note
    assert "failed_ids=2" in harness.note


def test_an_all_fail_run_reraises_the_cause_not_a_generic_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_tracked_job``'s classifier inspects the cause, so a generic wrapper
    turns an actionable auth or rate-limit failure into INTERNAL_ERROR. And a
    job that no-ops and reports success is invisible to every automated check
    this repo has."""
    from app.workers import scheduler

    def _always(instrument_id: int) -> Any:
        raise PermissionError("401")

    harness = _install(
        monkeypatch,
        due=[1, 2],
        unlocked=lambda *_a, **_kw: _PAIR,
        locked=lambda *_a, **_kw: _PAIR,
        on_request=_always,
    )
    with pytest.raises(PermissionError):
        scheduler.core_eligibility_refresh()

    assert harness.written == []
