"""Source-registry coverage for ``bootstrap_orchestrator`` (G14).

Surfaced as PR #1188 T9-POST: the admin retry endpoint publishes a
manual-queue request for ``JOB_BOOTSTRAP_ORCHESTRATOR`` via
``publish_manual_job_request``; the queue listener routes through
``_run_manual``; ``_run_manual`` constructs ``JobLock(job_name)``;
``JobLock.__init__`` calls ``app.jobs.sources.source_for(job_name)``.
Pre-fix the registry had no entry for ``bootstrap_orchestrator`` so
``source_for`` raised ``KeyError`` and the retry path required a
direct-Python invocation workaround that bypassed JobLock.

Pinning the contract here so a future PR that removes
``"bootstrap_orchestrator": "bootstrap"`` from
``MANUAL_TRIGGER_JOB_SOURCES`` fails CI rather than silently
regressing the manual-queue dispatch path.

Source choice — ``bootstrap`` (fresh lane, disjoint from every
per-stage lane):

- Bootstrap is a ``_PRELUDE_OPT_OUT_JOB`` (``app/jobs/runtime.py``):
  the runtime acquires the outer ``JobLock`` then invokes
  ``run_bootstrap_orchestrator`` directly. The orchestrator submits
  per-stage invokers to a ``ThreadPoolExecutor``
  (``app/services/bootstrap_orchestrator.py:1603``); per-stage
  invokers acquire inner ``JobLock(<stage_job>)`` from worker threads.
- Python's ``ContextVar`` is NOT auto-propagated across executor
  worker threads (regression-pinned by
  ``test_job_lock_reentrancy.py::test_threads_do_not_inherit_held_sources``),
  so the #1184 same-context re-entrancy bypass CANNOT fire inside
  the bootstrap executor's worker. Any source shared with an inner
  stage (``init`` with ``nightly_universe_sync``; ``db`` with several
  Phase E stages) would have the worker hit
  ``pg_try_advisory_lock`` on a lock the listener thread already
  holds → ``JobAlreadyRunning`` → stage fails.
- A fresh ``bootstrap`` lane is disjoint from every per-stage lane.
  Cross-thread inner acquisitions never contend with the outer lock
  by construction. The disjointness invariant is regression-pinned
  by ``test_bootstrap_lane_disjoint_from_all_stage_lanes`` below.

Pattern reference: ``tests/test_layer_123_wiring.py::TestSecRebuildRegistry``
for the sec_rebuild manual-trigger registry coverage shape.
"""

from __future__ import annotations

from collections.abc import Generator

import psycopg
import pytest

from app.jobs.locks import JobLock
from app.jobs.sources import MANUAL_TRIGGER_JOB_SOURCES, source_for
from app.services.bootstrap_orchestrator import JOB_BOOTSTRAP_ORCHESTRATOR
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request
from tests.fixtures.ebull_test_db import ebull_test_conn, test_database_url  # noqa: F401 — fixture re-export


class TestBootstrapOrchestratorRegistry:
    """``bootstrap_orchestrator`` resolves to the fresh ``bootstrap``
    source via ``MANUAL_TRIGGER_JOB_SOURCES``."""

    def test_bootstrap_orchestrator_in_manual_trigger_sources(self) -> None:
        assert JOB_BOOTSTRAP_ORCHESTRATOR in MANUAL_TRIGGER_JOB_SOURCES
        assert MANUAL_TRIGGER_JOB_SOURCES[JOB_BOOTSTRAP_ORCHESTRATOR] == "bootstrap"

    def test_source_for_resolves_bootstrap_orchestrator(self) -> None:
        """The KeyError regression — ``JobLock(...).__init__`` calls
        ``source_for`` directly. Pre-PR this raised at the queue
        listener's ``_run_manual`` entry point and stranded the
        admin retry path."""
        assert source_for(JOB_BOOTSTRAP_ORCHESTRATOR) == "bootstrap"

    def test_joblock_construct_does_not_raise(self) -> None:
        """The narrow regression gate: constructing a JobLock for
        ``bootstrap_orchestrator`` must NOT raise ``KeyError``.

        Construction alone is enough — ``JobLock.__init__`` calls
        ``source_for`` immediately. We do NOT enter the context
        manager here (no Postgres advisory lock acquired); the goal
        is to pin the registry lookup, not exercise the lock body."""
        lock = JobLock(test_database_url(), JOB_BOOTSTRAP_ORCHESTRATOR)
        assert lock._source == "bootstrap"  # noqa: SLF001 — invariant under test

    def test_bootstrap_lane_disjoint_from_all_stage_lanes(self) -> None:
        """Disjointness invariant — no per-stage job_name resolves to
        the ``bootstrap`` lane.

        This is the load-bearing safety property: if any inner-stage
        invoker resolves to ``bootstrap`` (via a future SCHEDULED_JOBS
        addition or a bootstrap stage that uses ``bootstrap`` as its
        StageSpec.lane), the worker thread's inner
        ``JobLock(<stage_job>)`` would attempt ``pg_try_advisory_lock``
        on the same key the listener thread already holds — cross-
        thread ContextVar bypass cannot fire — and the inner
        acquisition would fail with ``JobAlreadyRunning``.

        Two checks:

        1. Walk ``_BOOTSTRAP_STAGE_SPECS`` directly — every stage
           invoker MUST resolve to a non-``bootstrap`` lane via
           ``source_for``. Catches future stage additions even if
           the offending stage is also added to the registry under
           the bootstrap key (which would make the registry-occupants
           check pass false-positive).
        2. The registry occupants of ``bootstrap`` MUST be exactly
           ``{JOB_BOOTSTRAP_ORCHESTRATOR}``. Catches future
           SCHEDULED_JOBS / MANUAL_TRIGGER additions that introduce a
           new bootstrap-lane resident.

        Failing either is the canary that the source-choice argument
        in ``app/jobs/sources.py`` has been silently violated.
        """
        from app.jobs.sources import get_job_name_to_source
        from app.services.bootstrap_orchestrator import _BOOTSTRAP_STAGE_SPECS

        # Check 1 — every bootstrap-stage invoker resolves to a non-
        # bootstrap lane. Stage names that ARE the orchestrator itself
        # are excluded from this assertion (the orchestrator is the
        # outer holder, not an inner stage).
        bootstrap_lane_stage_violations = sorted(
            stage.job_name
            for stage in _BOOTSTRAP_STAGE_SPECS
            if stage.job_name != JOB_BOOTSTRAP_ORCHESTRATOR and source_for(stage.job_name) == "bootstrap"
        )
        assert not bootstrap_lane_stage_violations, (
            f"Bootstrap stage(s) resolve to the 'bootstrap' lane: "
            f"{bootstrap_lane_stage_violations!r}. Inner stage JobLock acquires "
            f"from worker threads; cross-thread ContextVar bypass is intentionally "
            f"NOT automatic (test_job_lock_reentrancy.py::"
            f"test_threads_do_not_inherit_held_sources). Move the offending "
            f"stage(s) to a stage-specific lane."
        )

        # Check 2 — registry occupants of 'bootstrap' are exactly the
        # orchestrator. Catches additions outside the stage table.
        registry = get_job_name_to_source()
        on_bootstrap_lane = sorted(name for name, lane in registry.items() if lane == "bootstrap")
        assert on_bootstrap_lane == [JOB_BOOTSTRAP_ORCHESTRATOR], (
            f"Expected only {JOB_BOOTSTRAP_ORCHESTRATOR!r} on the 'bootstrap' lane; "
            f"found {on_bootstrap_lane!r}. Any non-orchestrator job sharing the "
            f"'bootstrap' lane will collide with the outer listener-thread JobLock."
        )


@pytest.fixture()
def _cleanup_requests() -> Generator[list[int]]:
    created: list[int] = []
    yield created
    if created:
        with psycopg.connect(test_database_url(), autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM pending_job_requests WHERE request_id = ANY(%s)",
                    (created,),
                )


def test_publish_manual_job_request_bootstrap_orchestrator_no_keyerror(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811, ARG001 — ensures the worker test DB exists + is migrated
    monkeypatch: pytest.MonkeyPatch,
    _cleanup_requests: list[int],
) -> None:
    """End-to-end on the publish path: ``publish_manual_job_request``
    lands a ``pending_job_requests`` row for ``bootstrap_orchestrator``
    without raising.

    This is the path the admin retry endpoint takes
    (``app/api/bootstrap.py:498``). Pre-fix this published fine
    (publish writes a queue row), but the next step
    (``_run_manual``'s ``JobLock(job_name)`` construction) KeyError'd
    on the unresolvable source. The full listener loop is hard to
    exercise without a running jobs process; this test pins the
    publish leg + relies on the unit-level
    ``test_joblock_construct_does_not_raise`` above for the
    JobLock-construction leg.

    Together they cover the failure mode: row queued, JobLock
    constructs cleanly, listener can proceed past the KeyError that
    blocked the retry path."""
    monkeypatch.setattr("app.config.settings.database_url", test_database_url())

    request_id = publish_manual_job_request(JOB_BOOTSTRAP_ORCHESTRATOR)
    _cleanup_requests.append(request_id)

    with psycopg.connect(test_database_url(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT job_name, status FROM pending_job_requests WHERE request_id=%s",
                (request_id,),
            )
            row = cur.fetchone()
    assert row is not None
    assert row[0] == JOB_BOOTSTRAP_ORCHESTRATOR
    # Initial status is 'pending' — listener picks it up + transitions
    # to 'claimed' on dispatch. We assert no immediate rejection
    # (which would set status='rejected').
    assert row[1] == "pending"
