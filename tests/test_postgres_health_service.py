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

    # CRITICAL: subsequent probes — running on the SAME connection —
    # MUST have succeeded. If autocommit=True is missing, the conn is
    # in ABORTED state after the wal_dir probe and `db_size_bytes`
    # would also be None (with a misleading
    # `InFailedSqlTransaction` error). The non-None assertion below
    # is the guard.
    assert snapshot.db_size_bytes is not None, (
        "subsequent probe failed — autocommit=True missing from psycopg.connect(...)?"
    )
    assert snapshot.leaked_test_db_count is not None
    assert snapshot.last_checkpoint_at is not None
