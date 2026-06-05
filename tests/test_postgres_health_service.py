"""Service-level integration tests for `collect_postgres_health`.

Codex 2 MED #3 regression: the endpoint-level isolation test patches
the snapshot, so removing `autocommit=True` from the service body
would not fail any test. These tests run the real service against the
test DB and prove that:

1. Removing autocommit causes a downstream metric query to fail with
   "current transaction is aborted" once one earlier query raises.
2. With autocommit (production path), a failing probe leaves later
   probes intact + populates `metric_errors`.

The strategy: monkey-patch one of the seven `_q_*` callables to
raise `psycopg.Error`. With autocommit on, the next call works. Drop
autocommit → next call fails with "InFailedSqlTransaction" inside
psycopg, the safety net we built explicitly for.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services import postgres_health
from app.services.postgres_health import collect_postgres_health
from tests.fixtures.ebull_test_db import test_database_url, test_db_available


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_autocommit_isolates_failed_probe_under_real_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing `_q_wal_dir` MUST NOT poison the other six probes.

    Codex 2 MED #3 regression. If `autocommit=True` is removed from
    `collect_postgres_health`'s `psycopg.connect(...)` call, this
    test fails because the next probe (`_q_wal_since_checkpoint`)
    hits "current transaction is aborted" instead of returning a
    real value.
    """

    def _explode(_conn: psycopg.Connection[tuple]) -> tuple[int, str]:
        raise psycopg.errors.InsufficientPrivilege("synthetic: pg_monitor role required")

    monkeypatch.setattr(postgres_health, "_q_wal_dir", _explode)

    snapshot = collect_postgres_health(database_url=test_database_url())

    # Failed probe contributes a null field + an error entry.
    assert snapshot.wal_dir_bytes is None
    assert snapshot.wal_breached_warn is None
    assert any(e.startswith("wal_dir:") for e in snapshot.metric_errors)

    # CRITICAL: probes dispatched AFTER `_q_wal_dir` in
    # `collect_postgres_health` MUST have succeeded — that's the real
    # autocommit-isolation guard. Bot review #1216 prevention: assert
    # on a probe N+k (k>=1), with N here = `_q_wal_dir`. Probes that
    # come BEFORE `_q_wal_dir` (`_q_db_size`, `_q_leaked_test_dbs`)
    # always succeed regardless of autocommit because the conn isn't
    # aborted yet — asserting on them would be a false guard.
    #
    # Dispatch order in collect_postgres_health:
    #   1. db_size            (before wal_dir — not a guard)
    #   2. leaked_test_dbs    (before wal_dir — not a guard)
    #   3. wal_dir            (PATCHED to raise)
    #   4. wal_since_checkpoint (after wal_dir — load-bearing guard)
    #   5. last_checkpoint     (after wal_dir — load-bearing guard)
    #   6. autovacuum_top10    (after wal_dir — load-bearing guard)
    #   7. default_partition_rows (after wal_dir — load-bearing guard)
    assert snapshot.wal_since_checkpoint_bytes is not None, (
        "post-wal_dir probe failed — autocommit=True missing from "
        "psycopg.connect(...) leaves the conn in ABORTED state after "
        "the patched probe raised."
    )
    assert snapshot.last_checkpoint_at is not None
    assert snapshot.autovacuum_top10 is not None
    assert snapshot.financial_facts_raw_default_rows is not None


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_safe_wrapper_catches_non_psycopg_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bot review #1216 regression: `_safe` MUST catch every exception
    type, not just `psycopg.Error`. Probe internals contain bare
    `assert row is not None` statements — an `AssertionError` from
    one of those would otherwise escape `_safe` + bypass the API's
    `psycopg.Error` handler + yield an unhandled 500 instead of a
    partial 200 with `metric_errors` populated.
    """

    def _bad_probe(_conn: psycopg.Connection[tuple]) -> int:
        raise AssertionError("synthetic: probe internal assertion failed")

    monkeypatch.setattr(postgres_health, "_q_default_partition_rows", _bad_probe)

    snapshot = collect_postgres_health(database_url=test_database_url())

    assert snapshot.financial_facts_raw_default_rows is None
    assert snapshot.financial_facts_raw_default_breached_warn is None
    assert any(e.startswith("default_partition_rows:") for e in snapshot.metric_errors), (
        f"expected default_partition_rows entry in metric_errors, got {snapshot.metric_errors}"
    )
    # Every other metric unaffected.
    assert snapshot.db_size_bytes is not None
    assert snapshot.last_checkpoint_at is not None


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_leaked_test_db_total_bytes_reflects_bloat() -> None:
    """#1444 — the bloat-visibility metric reports the on-disk size of
    leaked ``ebull_test_*`` DBs so an operator sees accumulating bloat
    BEFORE the next crash recovery has to fsync-walk it.

    The worker DB this suite runs against itself matches ``ebull_test_%``
    (it is excluded from the dev cluster but counted here), so with at
    least one leaked DB present the total is a positive int that is
    consistent with the reported count.
    """
    snapshot = collect_postgres_health(database_url=test_database_url())

    assert snapshot.leaked_test_db_count is not None
    assert snapshot.leaked_test_db_total_bytes is not None
    assert snapshot.leaked_test_db_total_bytes >= 0
    if snapshot.leaked_test_db_count > 0:
        assert snapshot.leaked_test_db_total_bytes > 0
    assert snapshot.leaked_test_db_total_pretty is not None


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_listener_connections_probe_counts_labelled_conns() -> None:
    """#1472 PR3 — the listener-cardinality probe counts LISTEN connections
    per ``application_name`` and zero-fills every known label."""
    from app.db.pg_settings import JOB_REQUEST_LISTENER_APPLICATION_NAME

    url = test_database_url()
    # No labelled conn open yet: every known label reports 0, no duplicate.
    baseline = collect_postgres_health(database_url=url)
    assert baseline.listener_connections is not None
    by_name = {lc.application_name: lc.count for lc in baseline.listener_connections}
    assert JOB_REQUEST_LISTENER_APPLICATION_NAME in by_name  # zero-filled
    assert baseline.listener_duplicate_detected is False

    # Hold one labelled conn open across the collect → that label counts >= 1.
    held = psycopg.connect(url, autocommit=True, application_name=JOB_REQUEST_LISTENER_APPLICATION_NAME)
    try:
        snap = collect_postgres_health(database_url=url)
        assert snap.listener_connections is not None
        counts = {lc.application_name: lc.count for lc in snap.listener_connections}
        assert counts[JOB_REQUEST_LISTENER_APPLICATION_NAME] >= 1
    finally:
        held.close()


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_listener_connections_probe_detects_duplicate() -> None:
    """Two LISTEN conns with the SAME label → count > 1 → duplicate flagged."""
    from app.db.pg_settings import JOB_REQUEST_LISTENER_APPLICATION_NAME

    url = test_database_url()
    a = psycopg.connect(url, autocommit=True, application_name=JOB_REQUEST_LISTENER_APPLICATION_NAME)
    b = psycopg.connect(url, autocommit=True, application_name=JOB_REQUEST_LISTENER_APPLICATION_NAME)
    try:
        snap = collect_postgres_health(database_url=url)
        assert snap.listener_connections is not None
        counts = {lc.application_name: lc.count for lc in snap.listener_connections}
        assert counts[JOB_REQUEST_LISTENER_APPLICATION_NAME] >= 2
        assert snap.listener_duplicate_detected is True
    finally:
        a.close()
        b.close()
