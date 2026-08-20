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
    JOB_DAILY_NEWS_REFRESH,
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
    JOB_PG_SIZE_SAMPLE,
    JOB_QUOTES_REFRESH,
    JOB_RAW_DATA_RETENTION_SWEEP,
    JOB_RETRY_DEFERRED,
    JOB_RETRY_SWEEPER,
    JOB_SEC_13F_FILER_DIRECTORY_SYNC,
    JOB_SEC_DAILY_INDEX_RECONCILE,
    JOB_SEC_MANIFEST_WORKER,
    JOB_SEC_NPORT_FILER_DIRECTORY_SYNC,
    JOB_SEED_COST_MODELS,
    JOB_STRATEGY_HALT_FEED_REFRESH,
    JOB_STRATEGY_INTRADAY_HARVEST,
    JOB_THESIS_REFRESH,
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
        # #2212 (audited 2026-08-03) — three scheduled jobs that drifted in
        # without a per-job prereq. All three are
        # ``exempt_from_universal_bootstrap_gate=False``, so the UNIVERSAL gate
        # (#1181, app/jobs/runtime.py::_wrap_invoker) already rejects their
        # scheduled fires with ``bootstrap_not_complete`` until bootstrap
        # completes. Adding a per-job ``_bootstrap_complete`` would double-gate —
        # the same JOB_NCEN_CLASSIFIER call above.
        #   * daily_news_refresh — Yahoo-RSS ingest; no bootstrap-produced input.
        #   * pg_size_sample — DB-size telemetry sampler; meaningful (and
        #     arguably most useful) during bootstrap itself.
        #   * thesis_refresh — carries a prereq, but it gates LLM-provider
        #     reachability (``_llm_provider_resolvable``), not bootstrap state,
        #     so ``_references_bootstrap_complete`` correctly reports False.
        JOB_DAILY_NEWS_REFRESH,
        JOB_PG_SIZE_SAMPLE,
        JOB_THESIS_REFRESH,
        # #2449 audit — current quotes have no bootstrap-produced input and
        # the non-exempt job is already stopped by the universal gate. A
        # second per-job gate would duplicate that authority.
        JOB_QUOTES_REFRESH,
        # #2629 audit (2026-08-13) — two strategy collection jobs that drifted
        # in carrying a COLLECTION-WINDOW prerequisite
        # (``_strategy_intraday_collection_due`` / ``_strategy_halt_collection_due``)
        # rather than a bootstrap one, so ``_references_bootstrap_complete``
        # correctly reports False — the same shape as JOB_THESIS_REFRESH above,
        # whose prereq gates LLM reachability.
        #
        # Both are ``exempt_from_universal_bootstrap_gate=False`` — measured,
        # not assumed: of the 56 SCHEDULED_JOBS entries exactly two set it True
        # (``orchestrator_high_frequency_sync``, ``sec_daily_index_reconcile``)
        # and the exemption census below pins that. So the UNIVERSAL gate (#1181,
        # app/jobs/runtime.py::_wrap_invoker, ``needs_gate = not is_exempt``)
        # already rejects their scheduled fires with ``bootstrap_not_complete``
        # until bootstrap completes — and it runs BEFORE the per-job prereq, so
        # the operator-visible reason is the actionable one either way. Adding a
        # per-job ``_bootstrap_complete`` would double-gate, exactly as recorded
        # for JOB_NCEN_CLASSIFIER (#1504) and the #2212 trio.
        #   * strategy_intraday_harvest (#2477) — eToro research-window bars.
        #   * strategy_halt_feed_refresh (#2507) — Nasdaq halt RSS; a safety
        #     feed that can refuse but never create a trade.
        JOB_STRATEGY_INTRADAY_HARVEST,
        JOB_STRATEGY_HALT_FEED_REFRESH,
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


# The whole NON_GATED_SCHEDULED argument — "no per-job gate is needed because
# the universal gate already stops it" — is only true for a job the universal
# gate actually sees. ``exempt_from_universal_bootstrap_gate=True`` is the one
# flag that takes a job out of its reach, and nothing pinned that set, so a
# future exemption would quietly invalidate a dozen entries above without
# failing anything (#2629).
UNIVERSAL_GATE_EXEMPT: frozenset[str] = frozenset(
    {
        # Bypasses the gate so the 5-minute portfolio/FX sync keeps running
        # during bootstrap.
        JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC,
        # #1181 lane-B carve-out: idempotent, subject_resolver filters every
        # unknown CIK. See docs/superpowers/specs/2026-05-16-lane-b-discovery-firing.md §4.2.
        JOB_SEC_DAILY_INDEX_RECONCILE,
    }
)


def test_universal_gate_exemptions_are_only_the_declared_two() -> None:
    """Pins the set NON_GATED_SCHEDULED's rationale depends on.

    Entries in ``NON_GATED_SCHEDULED`` justify carrying no per-job
    ``_bootstrap_complete`` by pointing at the universal gate
    (``app/jobs/runtime.py::_wrap_invoker``, ``needs_gate = not is_exempt``).
    Flipping a job to ``exempt_from_universal_bootstrap_gate=True`` removes
    that backstop, so the exemption must be a deliberate, reviewed change —
    not a default a new job inherits.
    """
    exempt = {job.name for job in SCHEDULED_JOBS if job.exempt_from_universal_bootstrap_gate}
    assert exempt == set(UNIVERSAL_GATE_EXEMPT), (
        f"universal-bootstrap-gate exemptions changed: {sorted(exempt)}. Every "
        f"NON_GATED_SCHEDULED entry justifies itself by the universal gate still "
        f"applying, so an exemption invalidates that argument for the job it is "
        f"granted to. Re-check that job's gating and update both sets."
    )
