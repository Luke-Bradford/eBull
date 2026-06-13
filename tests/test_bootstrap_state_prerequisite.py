"""Tests for the ``_bootstrap_complete`` scheduler prerequisite (#996).

Spec: docs/superpowers/specs/2026-05-07-first-install-bootstrap.md.

Pins:

* ``_bootstrap_complete`` returns ``(False, ...)`` for every non-
  ``complete`` status so dependent jobs stay quiet on a fresh /
  half-populated install.
* Every gated SCHEDULED_JOBS entry from the spec carries the gate
  (or composes it via ``_all_of`` for jobs with a pre-existing
  prereq).
* The set of *non-gated* SCHEDULED_JOBS exactly matches the spec's
  "Do not wire the gate on" list — adding a new job without a
  conscious gating decision shows up in the test diff.
"""

from __future__ import annotations

import psycopg

from app.workers.scheduler import (
    JOB_CUSIP_EXTID_SWEEP,
    JOB_CUSIP_UNIVERSE_BACKFILL,
    JOB_DAILY_PORTFOLIO_SYNC,
    JOB_ETORO_LOOKUPS_REFRESH,
    JOB_EXCHANGES_METADATA_REFRESH,
    JOB_EXECUTE_APPROVED_ORDERS,
    JOB_FX_RATES_REFRESH,
    JOB_LIVENESS_WATCHDOG,
    JOB_MONITOR_POSITIONS,
    JOB_NCEN_CLASSIFIER,
    JOB_NIGHTLY_UNIVERSE_SYNC,
    JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC,
    JOB_ORPHAN_TEST_DB_REAP,
    JOB_OWNERSHIP_OBSERVATIONS_BACKFILL,
    JOB_RAW_DATA_RETENTION_SWEEP,
    JOB_RETRY_DEFERRED,
    JOB_RETRY_SWEEPER,
    JOB_SEC_13F_FILER_DIRECTORY_SYNC,
    JOB_SEC_DAILY_INDEX_RECONCILE,
    JOB_SEC_MANIFEST_WORKER,
    JOB_SEC_NPORT_FILER_DIRECTORY_SYNC,
    JOB_SEED_COST_MODELS,
    JOB_WEEKLY_REPORT,
    SCHEDULED_JOBS,
    _bootstrap_complete,
)


def _set_bootstrap_state(
    conn: psycopg.Connection[tuple],
    *,
    status: str,
) -> None:
    conn.execute("UPDATE bootstrap_state SET status = %s WHERE id = 1", (status,))
    conn.commit()


def test_bootstrap_complete_returns_false_on_pending(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _set_bootstrap_state(ebull_test_conn, status="pending")
    met, reason = _bootstrap_complete(ebull_test_conn)
    assert met is False
    assert "first-install bootstrap not complete" in reason


def test_bootstrap_complete_returns_false_on_running(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _set_bootstrap_state(ebull_test_conn, status="running")
    met, _ = _bootstrap_complete(ebull_test_conn)
    assert met is False


def test_bootstrap_complete_returns_false_on_partial_error(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _set_bootstrap_state(ebull_test_conn, status="partial_error")
    met, _ = _bootstrap_complete(ebull_test_conn)
    assert met is False


def test_bootstrap_complete_returns_true_on_complete(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _set_bootstrap_state(ebull_test_conn, status="complete")
    met, reason = _bootstrap_complete(ebull_test_conn)
    assert met is True
    assert reason == ""


# ---------------------------------------------------------------------------
# Gate-coverage invariants
# ---------------------------------------------------------------------------


# Spec §"Do not wire the gate on". Every entry here must be in
# SCHEDULED_JOBS without a ``_bootstrap_complete``-flavoured prereq.
NON_GATED_SCHEDULED: frozenset[str] = frozenset(
    {
        JOB_NIGHTLY_UNIVERSE_SYNC,
        JOB_DAILY_PORTFOLIO_SYNC,
        JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC,
        JOB_FX_RATES_REFRESH,
        JOB_ETORO_LOOKUPS_REFRESH,
        JOB_EXCHANGES_METADATA_REFRESH,
        JOB_RETRY_DEFERRED,
        JOB_MONITOR_POSITIONS,
        JOB_EXECUTE_APPROVED_ORDERS,
        # Bootstrap stage jobs that establish the gate's read state —
        # gating these would prevent the bootstrap from ever running.
        JOB_CUSIP_UNIVERSE_BACKFILL,
        JOB_SEC_13F_FILER_DIRECTORY_SYNC,
        JOB_SEC_NPORT_FILER_DIRECTORY_SYNC,
        JOB_OWNERSHIP_OBSERVATIONS_BACKFILL,
        # Maintenance jobs not bootstrap-dependent.
        JOB_CUSIP_EXTID_SWEEP,
        JOB_RAW_DATA_RETENTION_SWEEP,
        JOB_SEED_COST_MODELS,
        JOB_WEEKLY_REPORT,
        # Janitorial / infra jobs that MUST run regardless of bootstrap
        # state (default prerequisite=None by design):
        #   * orphan_test_db_reap — reaps leaked test DBs (#1444); gating it
        #     would re-enable the crash-loop it exists to prevent.
        #   * sec_manifest_worker — self-gates internally; safe to run during
        #     bootstrap (it drains the manifest the bootstrap seeds).
        #   * sec_daily_index_reconcile — idempotent maintenance, no-op on
        #     an empty DB (#1155).
        #   (sec_manifest_tombstone_stale lived here until #1614 retired it
        #   from SCHEDULED_JOBS to manual-trigger-only — no longer a
        #   scheduled job, so it cannot appear in this scheduled-job set.)
        JOB_ORPHAN_TEST_DB_REAP,
        JOB_SEC_MANIFEST_WORKER,
        JOB_SEC_DAILY_INDEX_RECONCILE,
        # #1504 — ncen_classifier_yearly carries NO per-job
        # _bootstrap_complete prereq by design: it is gated by the
        # UNIVERSAL bootstrap gate (#1181), which runs check_bootstrap_state_gate
        # on every scheduled fire for any non-exempt registered job
        # (app/jobs/runtime.py::_wrap_invoker, needs_gate = not is_exempt),
        # BEFORE the per-job prereq. The job is non-exempt
        # (exempt_from_universal_bootstrap_gate=False, scheduler.py:1091-1100)
        # so its Apr-1 scheduled fire rejects with bootstrap_not_complete
        # until bootstrap completes — adding a redundant per-job
        # _bootstrap_complete would double-gate. Belongs here (the
        # "deliberately no per-job prereq" set), not as a bug.
        JOB_NCEN_CLASSIFIER,
        # #1500 — jobs_liveness_watchdog carries no per-job prereq (it is
        # a monitor); gated by the universal bootstrap gate like any
        # non-exempt job (pauses cleanly during bootstrap). No bootstrap
        # dependency on its own.
        JOB_LIVENESS_WATCHDOG,
        # #1509 — jobs_retry_sweeper carries no per-job prereq (it is a
        # maintenance job re-firing failed jobs); gated by the universal
        # bootstrap gate like any non-exempt job (pauses cleanly during
        # bootstrap). No bootstrap dependency of its own.
        JOB_RETRY_SWEEPER,
    }
)


def _references_bootstrap_complete(prereq: object) -> bool:
    """True if the prerequisite callable references _bootstrap_complete.

    Either by being _bootstrap_complete itself or being an _all_of
    closure that wraps it. Inspects the closure cells for the
    ``_all_of`` case.
    """
    from app.workers.scheduler import _bootstrap_complete as bc

    if prereq is None:
        return False
    if prereq is bc:
        return True
    # _all_of returns a closure with a `prereqs` cell.
    closure = getattr(prereq, "__closure__", None)
    if closure is None:
        return False
    for cell in closure:
        try:
            value = cell.cell_contents
        except ValueError:
            continue
        if value is bc:
            return True
        if isinstance(value, tuple):
            if any(item is bc for item in value):
                return True
    return False


def test_every_scheduled_job_either_gated_or_explicitly_excluded() -> None:
    """Drift guard: every SCHEDULED_JOBS entry must be either:

      1. Gated by ``_bootstrap_complete`` (directly or via ``_all_of``).
      2. Listed in ``NON_GATED_SCHEDULED``.

    Adding a new SCHEDULED_JOBS entry without choosing one of the
    two paths fails this test, surfacing the decision in review.
    """
    by_name = {job.name: job for job in SCHEDULED_JOBS}
    gated: set[str] = set()
    ungated: set[str] = set()
    for name, job in by_name.items():
        if _references_bootstrap_complete(job.prerequisite):
            gated.add(name)
        else:
            ungated.add(name)

    unexpected_ungated = ungated - NON_GATED_SCHEDULED
    assert not unexpected_ungated, (
        f"Scheduled job(s) {sorted(unexpected_ungated)} are not gated by "
        f"_bootstrap_complete and not in NON_GATED_SCHEDULED. Decide one "
        f"of the two paths and update the test if intentional."
    )
    unexpected_gated = NON_GATED_SCHEDULED & gated
    assert not unexpected_gated, (
        f"Scheduled job(s) {sorted(unexpected_gated)} are listed as "
        f"non-gated in NON_GATED_SCHEDULED but actually carry the "
        f"_bootstrap_complete gate. Pick one."
    )
