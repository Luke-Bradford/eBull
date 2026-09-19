"""Regression: the two #2603 core-sleeve jobs own their lanes (#2603, 2026-09-19).

Both sat on the shared ``etoro`` source lane, where ``daily_candle_refresh`` —
an orchestrator LAYER whose ``adapters.py::_run_legacy`` holds the JobLock
across its whole body — holds ``job_source:etoro`` for **3.2-3.8 hours** a day
(measured on dev, 09-15 -> 09-18, corroborated by ``last_progress_at`` landing
3-4s before ``finished_at`` on the orphan-reaped runs, so the interval is a real
hold and not a reap artefact).

``core_rebalance_observation`` fires daily at 22:45, inside that window every
day. The ~11.5s patient acquire-backoff (#1710) cannot outlast a multi-hour
holder, so 4 of 4 fires recorded ``lane_busy`` and the job had **never completed
a run** — 0 ``success`` rows in 28 days. ``core_eligibility_refresh`` (hourly
@ :20) lost 18 fires the same way. The discriminator was 22/22 on the full
population: every ``lane_busy`` row coincided with a live sweep, and no other
outcome did.

Pure registry assertions (no DB) so the invariant gates every push. The
REAL-lock proof — that the split actually lets the movers acquire while a
sibling holds ``job_source:etoro``, with a control arm proving the holder
genuinely engages — lives in ``tests/test_etoro_core_lane_locks.py``, which is
a separate module on purpose: the ``db`` marker is applied per MODULE, so
putting the two in one file would evict these assertions from the fast tier.
"""

from __future__ import annotations

from app.jobs.runtime import (
    EXECUTION_LANE_GENERAL,
    _scheduler_executor_alias,
    execution_lane_for,
)
from app.jobs.sources import get_job_name_to_source, source_for
from app.workers.scheduler import (
    JOB_CORE_ELIGIBILITY_REFRESH,
    JOB_CORE_REBALANCE_OBSERVATION,
)

#: (job_name, its own lane). Names via the scheduler constants so a rename
#: cannot silently pass against a stale literal.
_CORE_LANES: tuple[tuple[str, str], ...] = (
    (JOB_CORE_REBALANCE_OBSERVATION, "etoro_core_rebalance"),
    (JOB_CORE_ELIGIBILITY_REFRESH, "etoro_core_eligibility"),
)

#: The lane that starves them, and the job that holds it. Pinned so a future
#: change that moves the HOLDER instead has to update this test in lockstep
#: rather than leaving a test that passes for the wrong reason.
_STARVED_LANE = "etoro"
_HOLDER = "daily_candle_refresh"


def test_each_core_job_resolves_to_its_own_lane() -> None:
    for job_name, expected in _CORE_LANES:
        assert source_for(job_name) == expected


def test_neither_core_job_is_on_the_lane_the_candle_sweep_holds() -> None:
    """The whole fix. ``_ALLOWED_SOURCES`` in ``test_job_registry`` cannot catch
    a re-collapse, because ``etoro`` legitimately remains an allowed lane — so
    the guard has to be this negative assertion.
    """
    assert source_for(_HOLDER) == _STARVED_LANE, (
        "the holder moved; this test's premise needs re-deriving rather than updating"
    )
    for job_name, _lane in _CORE_LANES:
        assert source_for(job_name) != _STARVED_LANE


def test_the_two_core_jobs_do_not_share_one_lane() -> None:
    """SEPARATE lanes, not one ``etoro_core``.

    The observation does not read eligibility proofs, so there is no ordering
    dependency a shared lane would preserve. A revalidation batch is capped at
    100 requests x 3.33s (~333s of sleeps before any HTTP; one observed failure
    ran 1022.8s), so a :20 fire can still be running at the observation's :45
    fire — a shared lane would re-create exactly this starvation between the
    pair. Same call as ``db_liveness`` / ``db_retry`` (#1526) over one
    ``db_infra``.
    """
    rebalance = source_for(JOB_CORE_REBALANCE_OBSERVATION)
    eligibility = source_for(JOB_CORE_ELIGIBILITY_REFRESH)
    assert rebalance != eligibility


def test_each_core_lane_is_owned_by_exactly_one_job() -> None:
    """Inverse check: nothing else may join either lane without failing here.

    A second holder would re-introduce the serialisation the split removes, and
    is the shape a well-meaning future "put the core jobs together" edit takes.
    """
    registry = get_job_name_to_source()
    for job_name, lane in _CORE_LANES:
        holders = {name for name, src in registry.items() if src == lane}
        assert holders == {job_name}, f"lane {lane!r} owned by {sorted(holders)!r}; expected just {job_name!r}"


def test_the_split_adds_no_execution_permit() -> None:
    """#1472 — the connection budget has zero headroom (usable 27, demand 27),
    so a change that added an execution lane would fail boot.

    A new *source* lane is not a new *execution* lane: ``execution_lane_for``
    branches on ``sec_rate`` / the paper job / the core-preflight producers and
    otherwise falls through to general, and ``build_scheduler_executors``
    enumerates ``EXECUTION_LANE_PERMITS``'s keys rather than ``Lane`` literals.
    Pinned because the failure mode would be a boot-time
    ``ConnectionBudgetExceeded``, not a test failure anywhere near this change.

    ⚠ The executor assertion changed with #3220 and the PROPERTY did not. It read
    ``== "default"`` while general shared that pool; general now has a pool of its
    own, so the same "no new execution lane" claim is written as "still on the
    general lane". A dispatch POOL costs threads; only a PERMIT costs connections.
    """
    for job_name, _lane in _CORE_LANES:
        assert execution_lane_for(job_name) == EXECUTION_LANE_GENERAL
        assert _scheduler_executor_alias(job_name) == EXECUTION_LANE_GENERAL
