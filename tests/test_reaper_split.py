"""Tests for reaper.reap_orphaned_syncs split behaviour (#645).

The reaper must distinguish never-started rows (`started_at IS NULL`)
from started-and-died rows (`started_at IS NOT NULL`). Only the latter
become `'failed'` with `'orchestrator_crash'`; the former become
`'cancelled'` with a `skip_reason` so the consecutive-failure streak
does not get inflated by uvicorn `--reload` noise during dev iteration.

Runs against `ebull_test` Postgres so the seed + assertion can be
exact. The dev-DB smoke test in `test_sync_orchestrator_api.py`
covers the no-rows-to-reap path.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest

from tests.fixtures.ebull_test_db import (
    test_database_url as _test_database_url,
)
from tests.fixtures.ebull_test_db import (
    test_db_available as _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test Postgres not reachable",
)


@pytest.fixture(autouse=True)
def _redirect_settings_to_test_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point `settings.database_url` at ebull_test for every test in
    this module. `reap_orphaned_syncs` opens its own connection via
    `settings.database_url`; without this redirect it would mutate
    the dev DB instead of the test seed."""
    from app.config import settings

    monkeypatch.setattr(settings, "database_url", _test_database_url())


_DELETE_RUNNING_LAYERS = (
    "DELETE FROM sync_layer_progress WHERE sync_run_id IN (SELECT sync_run_id FROM sync_runs WHERE status = 'running')"
)
_DELETE_RUNNING_RUNS = "DELETE FROM sync_runs WHERE status = 'running'"


def _drop_running_rows(c: psycopg.Connection[object]) -> None:
    with c.cursor() as cur:
        cur.execute(_DELETE_RUNNING_LAYERS)
        cur.execute(_DELETE_RUNNING_RUNS)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[object]]:
    """Yields a fresh test-DB connection. Each test deletes the
    pre-existing `status='running'` sync_runs rows it cares about
    BEFORE seeding so the partial unique index
    `idx_sync_runs_single_running` does not block — and reaps any
    seeded rows AFTER so the next test starts clean."""
    c: psycopg.Connection[object] = psycopg.connect(_test_database_url(), autocommit=True)
    _drop_running_rows(c)
    try:
        yield c
    finally:
        try:
            _drop_running_rows(c)
        finally:
            c.close()


def _seed_running_sync_with_layers(
    conn: psycopg.Connection[object],
    pending_never_started_layers: list[str],
    pending_or_running_after_start_layers: list[tuple[str, str]],
) -> int:
    """Insert a `status='running'` sync_runs row + the two kinds of
    progress rows the reaper has to distinguish.

    `pending_never_started_layers` -> rows with `status='pending'` and
    `started_at IS NULL`. These are what the reaper should `cancelled`.

    `pending_or_running_after_start_layers` -> list of (status, layer_name).
    `started_at` set to now() on insert. Reaper should `failed` these.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sync_runs
                (scope, trigger, started_at, status, layers_planned)
            VALUES ('full', 'manual', now(), 'running', %s)
            RETURNING sync_run_id
            """,
            (len(pending_never_started_layers) + len(pending_or_running_after_start_layers),),
        )
        row = cur.fetchone()
        assert row is not None
        sid = int(row[0])  # type: ignore[index]

        for layer in pending_never_started_layers:
            cur.execute(
                """
                INSERT INTO sync_layer_progress
                    (sync_run_id, layer_name, status, started_at)
                VALUES (%s, %s, 'pending', NULL)
                """,
                (sid, layer),
            )
        for status, layer in pending_or_running_after_start_layers:
            cur.execute(
                """
                INSERT INTO sync_layer_progress
                    (sync_run_id, layer_name, status, started_at)
                VALUES (%s, %s, %s, now())
                """,
                (sid, layer, status),
            )
    return sid


def _layer_status(conn: psycopg.Connection[object], sync_run_id: int, layer: str) -> tuple[str, str | None, str | None]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status, error_category, skip_reason
            FROM sync_layer_progress
            WHERE sync_run_id = %s AND layer_name = %s
            """,
            (sync_run_id, layer),
        )
        row = cur.fetchone()
        assert row is not None
        return (
            str(row[0]),  # type: ignore[index]
            None if row[1] is None else str(row[1]),  # type: ignore[index]
            None if row[2] is None else str(row[2]),  # type: ignore[index]
        )


class TestReaperCancelsNeverStartedRows:
    """`reap_orphaned_syncs(reap_all=True)` must cancel never-started
    rows, not fail them — that's the #645 fix that stops dev `--reload`
    cycles inflating the consecutive-failure streak."""

    def test_pending_no_started_at_becomes_cancelled(self, conn: psycopg.Connection[object]) -> None:
        from app.services.sync_orchestrator.reaper import reap_orphaned_syncs

        cancelled_layer = f"test-cancel-{uuid4()}"
        sid = _seed_running_sync_with_layers(
            conn,
            pending_never_started_layers=[cancelled_layer],
            pending_or_running_after_start_layers=[],
        )

        reap_orphaned_syncs(reap_all=True)

        status, error_cat, skip_reason = _layer_status(conn, sid, cancelled_layer)
        assert status == "cancelled"
        assert error_cat is None
        assert skip_reason == "worker died before adapter dispatched"

    def test_running_with_started_at_becomes_failed_orchestrator_crash(self, conn: psycopg.Connection[object]) -> None:
        from app.services.sync_orchestrator.reaper import reap_orphaned_syncs

        failed_layer = f"test-fail-{uuid4()}"
        sid = _seed_running_sync_with_layers(
            conn,
            pending_never_started_layers=[],
            pending_or_running_after_start_layers=[("running", failed_layer)],
        )

        reap_orphaned_syncs(reap_all=True)

        status, error_cat, skip_reason = _layer_status(conn, sid, failed_layer)
        assert status == "failed"
        assert error_cat == "orchestrator_crash"
        assert skip_reason is None

    def test_pending_with_started_at_becomes_failed_orchestrator_crash(self, conn: psycopg.Connection[object]) -> None:
        # Edge: row was bumped to status='pending' AND started_at was
        # populated (e.g. mid-transition between mark-running and
        # mark-pending — defensive check). Treat as failed since work
        # may have begun.
        from app.services.sync_orchestrator.reaper import reap_orphaned_syncs

        failed_layer = f"test-fail-pending-with-start-{uuid4()}"
        sid = _seed_running_sync_with_layers(
            conn,
            pending_never_started_layers=[],
            pending_or_running_after_start_layers=[("pending", failed_layer)],
        )

        reap_orphaned_syncs(reap_all=True)

        status, error_cat, skip_reason = _layer_status(conn, sid, failed_layer)
        assert status == "failed"
        assert error_cat == "orchestrator_crash"
        assert skip_reason is None

    def test_aggregate_counts_roll_cancelled_into_skipped(self, conn: psycopg.Connection[object]) -> None:
        # After reaper, sync_runs.layers_done + .layers_failed +
        # .layers_skipped must equal the total row count for the run.
        # `cancelled` rolls into the skipped bucket so that invariant
        # holds across the new status (#645 codex pre-push round 2).
        from app.services.sync_orchestrator.reaper import reap_orphaned_syncs

        cancelled_layer = f"test-agg-c-{uuid4()}"
        failed_layer = f"test-agg-f-{uuid4()}"
        sid = _seed_running_sync_with_layers(
            conn,
            pending_never_started_layers=[cancelled_layer],
            pending_or_running_after_start_layers=[("running", failed_layer)],
        )

        reap_orphaned_syncs(reap_all=True)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT layers_planned, layers_done, layers_failed, layers_skipped
                FROM sync_runs WHERE sync_run_id = %s
                """,
                (sid,),
            )
            row = cur.fetchone()
            assert row is not None
            planned, done, failed, skipped = row  # type: ignore[misc]

        assert planned == 2
        assert done == 0
        assert failed == 1
        assert skipped == 1  # cancelled row counted here
        assert done + failed + skipped == planned  # invariant

    def test_mixed_run_splits_correctly(self, conn: psycopg.Connection[object]) -> None:
        from app.services.sync_orchestrator.reaper import reap_orphaned_syncs

        cancelled = f"test-mixed-cancel-{uuid4()}"
        failed = f"test-mixed-fail-{uuid4()}"
        sid = _seed_running_sync_with_layers(
            conn,
            pending_never_started_layers=[cancelled],
            pending_or_running_after_start_layers=[("running", failed)],
        )

        reap_orphaned_syncs(reap_all=True)

        c_status, c_err, _c_reason = _layer_status(conn, sid, cancelled)
        f_status, f_err, _f_reason = _layer_status(conn, sid, failed)
        assert c_status == "cancelled"
        assert c_err is None
        assert f_status == "failed"
        assert f_err == "orchestrator_crash"
