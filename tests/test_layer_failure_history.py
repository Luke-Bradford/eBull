"""Tests for app.services.sync_orchestrator.layer_failure_history.

These run against the real ``ebull_test`` Postgres because the logic
is pure SQL with branching, and substring-matching test doubles would
miss bugs like sort order inversion or an off-by-one on the "stop at
first non-failed row" streak counter.

Every test seeds a fresh set of ``sync_layer_progress`` rows for a
layer name scoped to the test (``test-layer-<uuid4>``) so concurrent
runs and leftovers from other suites cannot cross-talk. The fixture
does not TRUNCATE ``sync_layer_progress`` itself — scoping by a
unique ``layer_name`` is enough to isolate.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest

from app.services.sync_orchestrator.layer_failure_history import (
    all_layer_histories,
    consecutive_failures,
    last_error_category,
)
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


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[object]]:
    c: psycopg.Connection[object] = psycopg.connect(_test_database_url())
    try:
        yield c
    finally:
        c.rollback()
        c.close()


def _seed_progress_rows(
    conn: psycopg.Connection[object],
    layer_name: str,
    statuses_with_offsets: list[tuple[str, int, str | None]],
) -> None:
    """Insert sync_layer_progress rows for ``layer_name``.

    `sync_layer_progress.PRIMARY KEY = (sync_run_id, layer_name)`, so
    each progress row gets its own freshly-created sync_run row.

    ``statuses_with_offsets`` is a list of (status, minutes_ago, error_category).
    """
    now = datetime.now(tz=UTC)
    with conn.cursor() as cur:
        for status, minutes_ago, error_category in statuses_with_offsets:
            started = now - timedelta(minutes=minutes_ago)
            finished = started + timedelta(seconds=5)
            cur.execute(
                """
                INSERT INTO sync_runs
                    (scope, trigger, started_at, finished_at, status, layers_planned)
                VALUES ('full', 'manual', %s, %s, 'complete', 1)
                RETURNING sync_run_id
                """,
                (started, finished),
            )
            row = cur.fetchone()
            assert row is not None
            sync_run_id = int(row[0])  # type: ignore[index]
            cur.execute(
                """
                INSERT INTO sync_layer_progress
                    (sync_run_id, layer_name, status, started_at,
                     finished_at, error_category)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (sync_run_id, layer_name, status, started, finished, error_category),
            )
    conn.commit()


class TestConsecutiveFailures:
    def test_zero_when_layer_has_no_history(self, conn: psycopg.Connection[object]) -> None:
        assert consecutive_failures(conn, f"test-layer-{uuid4()}") == 0

    def test_zero_when_latest_is_complete(self, conn: psycopg.Connection[object]) -> None:
        layer = f"test-layer-{uuid4()}"
        _seed_progress_rows(
            conn,
            layer,
            [
                ("failed", 30, "db_constraint"),
                ("complete", 10, None),  # most recent
            ],
        )
        assert consecutive_failures(conn, layer) == 0

    def test_counts_streak_at_head(self, conn: psycopg.Connection[object]) -> None:
        layer = f"test-layer-{uuid4()}"
        # Three failures most recently, then a success further back.
        _seed_progress_rows(
            conn,
            layer,
            [
                ("complete", 120, None),
                ("failed", 60, "db_constraint"),
                ("failed", 40, "db_constraint"),
                ("failed", 10, "db_constraint"),
            ],
        )
        assert consecutive_failures(conn, layer) == 3

    def test_streak_breaks_on_skipped(self, conn: psycopg.Connection[object]) -> None:
        layer = f"test-layer-{uuid4()}"
        _seed_progress_rows(
            conn,
            layer,
            [
                ("failed", 60, "db_constraint"),
                ("failed", 40, "db_constraint"),
                ("skipped", 20, None),  # breaks streak
                ("failed", 10, "db_constraint"),  # most recent
            ],
        )
        # Only the latest 'failed' counts; the earlier run hit a skipped.
        assert consecutive_failures(conn, layer) == 1

    def test_streak_breaks_on_pending(self, conn: psycopg.Connection[object]) -> None:
        layer = f"test-layer-{uuid4()}"
        _seed_progress_rows(
            conn,
            layer,
            [
                ("failed", 60, "db_constraint"),
                ("failed", 30, "db_constraint"),
                ("pending", 5, None),  # fresh attempt in flight
            ],
        )
        # Pending at the head is not "still failing".
        assert consecutive_failures(conn, layer) == 0

    def test_only_failures_in_history_counts_all(self, conn: psycopg.Connection[object]) -> None:
        layer = f"test-layer-{uuid4()}"
        _seed_progress_rows(
            conn,
            layer,
            [
                ("failed", 100, "db_constraint"),
                ("failed", 50, "db_constraint"),
                ("failed", 10, "db_constraint"),
            ],
        )
        assert consecutive_failures(conn, layer) == 3


class TestNullStartedAtOrdering:
    """Regression: sync_layer_progress.started_at is nullable — the
    ORDER BY must NOT let a null-started older row outrank a fresh
    failure and zero the streak."""

    def test_null_started_row_does_not_reset_streak(self, conn: psycopg.Connection[object]) -> None:
        # Three failures, plus a later null-started pending row
        # inserted with a NULL started_at (simulating a row frozen
        # mid-planning by the reaper).
        layer = f"test-layer-null-{uuid4()}"
        # Insert the failures first.
        _seed_progress_rows(
            conn,
            layer,
            [
                ("failed", 90, "db_constraint"),
                ("failed", 60, "db_constraint"),
                ("failed", 30, "db_constraint"),
            ],
        )
        # Now insert a pending row with started_at=NULL explicitly.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sync_runs
                    (scope, trigger, started_at, status, layers_planned)
                VALUES ('full', 'manual', NOW(), 'complete', 1)
                RETURNING sync_run_id
                """,
            )
            row = cur.fetchone()
            assert row is not None
            sid = int(row[0])  # type: ignore[index]
            cur.execute(
                """
                INSERT INTO sync_layer_progress
                    (sync_run_id, layer_name, status, started_at, finished_at,
                     error_category)
                VALUES (%s, %s, 'pending', NULL, NULL, NULL)
                """,
                (sid, layer),
            )
        conn.commit()
        # Without NULLS LAST the pending row would sort first on DESC
        # and break the streak → 0. With NULLS LAST the three
        # failures are the head, streak stays 3.
        assert consecutive_failures(conn, layer) == 3


class TestLastErrorCategory:
    def test_none_when_layer_has_no_history(self, conn: psycopg.Connection[object]) -> None:
        assert last_error_category(conn, f"test-layer-{uuid4()}") is None

    def test_none_when_no_row_has_category(self, conn: psycopg.Connection[object]) -> None:
        layer = f"test-layer-{uuid4()}"
        _seed_progress_rows(
            conn,
            layer,
            [
                ("complete", 60, None),
                ("complete", 30, None),
                ("skipped", 10, None),
            ],
        )
        assert last_error_category(conn, layer) is None

    def test_returns_most_recent_non_null(self, conn: psycopg.Connection[object]) -> None:
        layer = f"test-layer-{uuid4()}"
        _seed_progress_rows(
            conn,
            layer,
            [
                ("failed", 120, "network_timeout"),  # oldest
                ("failed", 60, "db_constraint"),  # newer
                ("skipped", 10, None),  # newest but null
            ],
        )
        # Most recent NON-NULL is the db_constraint row; the later
        # skipped-with-null does not clobber the "last known error".
        assert last_error_category(conn, layer) == "db_constraint"

    def test_returns_latest_even_when_layer_recovered(self, conn: psycopg.Connection[object]) -> None:
        layer = f"test-layer-{uuid4()}"
        _seed_progress_rows(
            conn,
            layer,
            [
                ("failed", 120, "network_timeout"),
                ("complete", 10, None),  # recovered
            ],
        )
        # Survives the recovery — the category from the failed run is
        # still "last known", which is what a triage panel wants.
        assert last_error_category(conn, layer) == "network_timeout"


class TestBatchedAllLayerHistories:
    """Batched equivalent called once by /sync/layers instead of
    30 round-trips. Must agree with the per-layer helpers."""

    def test_returns_streak_and_category_per_layer(self, conn: psycopg.Connection[object]) -> None:
        a = f"test-layer-a-{uuid4()}"
        b = f"test-layer-b-{uuid4()}"
        _seed_progress_rows(
            conn,
            a,
            [
                ("failed", 60, "db_constraint"),
                ("failed", 30, "db_constraint"),
                ("failed", 10, "db_constraint"),  # head is 3 failures
            ],
        )
        _seed_progress_rows(
            conn,
            b,
            [
                ("failed", 120, "network"),
                ("complete", 10, None),  # latest success → streak 0
            ],
        )
        streaks, categories = all_layer_histories(conn, [a, b])
        assert streaks[a] == 3
        assert streaks[b] == 0
        assert categories[a] == "db_constraint"
        assert categories[b] == "network"

    def test_empty_layer_list_returns_empty_dicts(self, conn: psycopg.Connection[object]) -> None:
        streaks, categories = all_layer_histories(conn, [])
        assert streaks == {}
        assert categories == {}

    def test_unknown_layer_name_not_in_result(self, conn: psycopg.Connection[object]) -> None:
        # Layer has no rows — neither dict should contain it.
        fresh = f"test-layer-never-{uuid4()}"
        streaks, categories = all_layer_histories(conn, [fresh])
        assert fresh not in streaks
        assert fresh not in categories

    def test_filter_excludes_other_layers(self, conn: psycopg.Connection[object]) -> None:
        # Two layers exist in the DB but only one is requested —
        # the filter must keep the other out.
        included = f"test-layer-inc-{uuid4()}"
        excluded = f"test-layer-exc-{uuid4()}"
        _seed_progress_rows(
            conn,
            included,
            [("failed", 20, "cat1"), ("failed", 10, "cat1")],
        )
        _seed_progress_rows(
            conn,
            excluded,
            [("failed", 20, "cat2"), ("failed", 10, "cat2")],
        )
        streaks, categories = all_layer_histories(conn, [included])
        assert streaks.get(included) == 2
        assert included in categories
        assert excluded not in streaks
        assert excluded not in categories
