"""Integration tests for the cascade_retry_queue outbox (#276 K.2).

Uses the real ``ebull_test`` Postgres because MagicMock cannot model
psycopg transaction semantics (aborted state, savepoint vs implicit
tx, ON CONFLICT UPSERT behaviour).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import psycopg
import pytest

from app.services.refresh_cascade import (
    ATTEMPT_CAP,
    LOCKED_BY_SIBLING,
    RERANK_MARKER,
    cascade_refresh,
    clear_retry_success,
    demote_to_rerank_needed,
    drain_retry_queue,
    enqueue_locked_by_sibling,
    enqueue_rerank_marker,
    enqueue_retry,
    instrument_lock,
)
from app.services.thesis import StaleInstrument
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (iid, symbol, symbol),
    )
    conn.commit()


def _queue_row(conn: psycopg.Connection[tuple], iid: int) -> tuple[int, str] | None:
    row = conn.execute(
        "SELECT attempt_count, last_error FROM cascade_retry_queue WHERE instrument_id = %s",
        (iid,),
    ).fetchone()
    return (int(row[0]), str(row[1])) if row else None


class TestEnqueueRetry:
    def test_first_enqueue_sets_attempt_count_one(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "AAPL")
        enqueue_retry(ebull_test_conn, 1, "RuntimeError")
        assert _queue_row(ebull_test_conn, 1) == (1, "RuntimeError")

    def test_second_enqueue_increments_attempt_count(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "AAPL")
        enqueue_retry(ebull_test_conn, 1, "RuntimeError")
        enqueue_retry(ebull_test_conn, 1, "ValueError")
        assert _queue_row(ebull_test_conn, 1) == (2, "ValueError")

    def test_enqueue_after_aborted_tx_recovers(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Simulate an aborted outer tx: start a bad statement, roll back,
        then enqueue in a fresh inner tx. The outbox write must durably
        commit even though the caller connection had been INERROR."""
        _seed_instrument(ebull_test_conn, 1, "AAPL")
        with pytest.raises(psycopg.Error):
            ebull_test_conn.execute("SELECT * FROM non_existent_table_abc")
        ebull_test_conn.rollback()
        enqueue_retry(ebull_test_conn, 1, "RuntimeError")
        assert _queue_row(ebull_test_conn, 1) == (1, "RuntimeError")


class TestClearRetrySuccess:
    def test_deletes_existing_row(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "AAPL")
        enqueue_retry(ebull_test_conn, 1, "RuntimeError")
        clear_retry_success(ebull_test_conn, 1)
        assert _queue_row(ebull_test_conn, 1) is None

    def test_idempotent_on_missing_row(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "AAPL")
        clear_retry_success(ebull_test_conn, 1)  # never enqueued
        assert _queue_row(ebull_test_conn, 1) is None


class TestDrainRetryQueue:
    def test_returns_oldest_first(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "A")
        _seed_instrument(ebull_test_conn, 2, "B")
        _seed_instrument(ebull_test_conn, 3, "C")
        enqueue_retry(ebull_test_conn, 2, "X")
        enqueue_retry(ebull_test_conn, 1, "X")
        enqueue_retry(ebull_test_conn, 3, "X")
        # Force enqueued_at to differ deterministically. Explicit
        # commit below so the drain SELECT reads the UPDATE from a
        # committed snapshot, not implicit-tx ambiguity.
        ebull_test_conn.execute(
            "UPDATE cascade_retry_queue SET enqueued_at = "
            "  CASE instrument_id "
            "    WHEN 1 THEN NOW() - INTERVAL '3 hours' "
            "    WHEN 2 THEN NOW() - INTERVAL '2 hours' "
            "    WHEN 3 THEN NOW() - INTERVAL '1 hour' "
            "  END"
        )
        ebull_test_conn.commit()
        assert drain_retry_queue(ebull_test_conn) == [1, 2, 3]

    def test_skips_at_cap_rows(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "A")
        _seed_instrument(ebull_test_conn, 2, "B")
        enqueue_retry(ebull_test_conn, 1, "X")
        enqueue_retry(ebull_test_conn, 2, "X")
        # Force id=2 to exactly ATTEMPT_CAP — must be skipped.
        ebull_test_conn.execute(
            "UPDATE cascade_retry_queue SET attempt_count = %s WHERE instrument_id = 2",
            (ATTEMPT_CAP,),
        )
        ebull_test_conn.commit()
        assert drain_retry_queue(ebull_test_conn) == [1]


class TestEnqueueRerankMarker:
    def test_fresh_insert_sets_attempt_zero_marker(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "A")
        enqueue_rerank_marker(ebull_test_conn, 1)
        assert _queue_row(ebull_test_conn, 1) == (0, RERANK_MARKER)

    def test_conflict_resets_at_cap_row_to_drainable(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A thesis success followed by rerank failure must reset a
        pre-existing at-cap row to RERANK_NEEDED / count=0 so the
        next cycle re-drains it. Codex v4 regression cover."""
        _seed_instrument(ebull_test_conn, 1, "A")
        enqueue_retry(ebull_test_conn, 1, "RuntimeError")
        ebull_test_conn.execute(
            "UPDATE cascade_retry_queue SET attempt_count = %s WHERE instrument_id = 1",
            (ATTEMPT_CAP,),
        )
        ebull_test_conn.commit()
        # At cap — not drainable.
        assert drain_retry_queue(ebull_test_conn) == []
        # Thesis succeeds, rerank fails → marker upsert.
        enqueue_rerank_marker(ebull_test_conn, 1)
        assert _queue_row(ebull_test_conn, 1) == (0, RERANK_MARKER)
        # Now drainable again.
        assert drain_retry_queue(ebull_test_conn) == [1]


class TestCascadeCompositionAtCapRerankFailure:
    """Full cascade_refresh path: pre-existing at-cap row + stale
    instrument succeeds via new-work path + rerank fails → the
    at-cap row is reset by enqueue_rerank_marker, so the next cycle
    re-drains the instrument. Codex K.2 pre-push LOW cover."""

    def test_at_cap_row_reset_by_cascade_on_rerank_failure(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 42, "AAPL")
        # Seed a pre-existing at-cap row — the cascade must not
        # leave this stuck as not-drainable.
        enqueue_retry(ebull_test_conn, 42, "RuntimeError")
        ebull_test_conn.execute(
            "UPDATE cascade_retry_queue SET attempt_count = %s WHERE instrument_id = 42",
            (ATTEMPT_CAP,),
        )
        ebull_test_conn.commit()
        assert drain_retry_queue(ebull_test_conn) == []

        stale_rows = [
            StaleInstrument(instrument_id=42, symbol="AAPL", reason="event_new_10q"),
        ]
        with (
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch("app.services.refresh_cascade.generate_thesis"),
            patch(
                "app.services.refresh_cascade.compute_rankings",
                side_effect=RuntimeError("scoring broke"),
            ),
        ):
            outcome = cascade_refresh(ebull_test_conn, MagicMock(), [42])

        assert outcome.thesis_refreshed == 1
        assert outcome.rankings_recomputed is False
        # At-cap row has been reset to RERANK_NEEDED / count=0.
        assert _queue_row(ebull_test_conn, 42) == (0, RERANK_MARKER)
        # Next cascade cycle will pick it up.
        assert drain_retry_queue(ebull_test_conn) == [42]


class TestInstrumentLockIntegration:
    """Real-DB lock behaviour — cross-connection contention,
    INERROR-recovery unlock path, release at session close."""

    def test_two_connections_mutex(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Conn A acquires the lock for iid=1. Conn B on a separate
        session gets False. After A releases, B can acquire."""
        from tests.fixtures.ebull_test_db import test_database_url

        with psycopg.connect(test_database_url()) as conn_b:
            with instrument_lock(ebull_test_conn, 1) as a_acquired:
                assert a_acquired is True
                with instrument_lock(conn_b, 1) as b_acquired:
                    assert b_acquired is False
            # After A's context exits, lock is released.
            with instrument_lock(conn_b, 1) as b_acquired_after:
                assert b_acquired_after is True

    def test_inerror_unlock_recovers_via_rollback(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Acquire lock, run invalid SQL inside the body to put the
        conn in INERROR, exit normally. The lock must still release
        — verified by a second conn acquiring immediately after."""
        from tests.fixtures.ebull_test_db import test_database_url

        with instrument_lock(ebull_test_conn, 2) as acquired:
            assert acquired is True
            with pytest.raises(psycopg.Error):
                ebull_test_conn.execute("SELECT * FROM non_existent_xyz")
        # Second connection should acquire immediately.
        with psycopg.connect(test_database_url()) as conn_b:
            with instrument_lock(conn_b, 2) as acquired_b:
                assert acquired_b is True


class TestEnqueueLockedBySiblingIntegration:
    def test_insert_on_empty_queue(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "A")
        enqueue_locked_by_sibling(ebull_test_conn, 1)
        assert _queue_row(ebull_test_conn, 1) == (0, LOCKED_BY_SIBLING)

    def test_preserves_existing_row(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """ON CONFLICT DO NOTHING — existing count + last_error stay."""
        _seed_instrument(ebull_test_conn, 1, "A")
        enqueue_retry(ebull_test_conn, 1, "RuntimeError")
        ebull_test_conn.execute(
            "UPDATE cascade_retry_queue SET attempt_count = %s WHERE instrument_id = 1",
            (ATTEMPT_CAP,),
        )
        ebull_test_conn.commit()
        # Sibling tries to enqueue LOCKED_BY_SIBLING — preserved.
        enqueue_locked_by_sibling(ebull_test_conn, 1)
        assert _queue_row(ebull_test_conn, 1) == (ATTEMPT_CAP, "RuntimeError")

    def test_does_not_trample_rerank_needed(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Codex K.3 pre-push check (2): LOCKED_BY_SIBLING on an
        existing RERANK_NEEDED row must preserve the marker via
        ON CONFLICT DO NOTHING."""
        _seed_instrument(ebull_test_conn, 1, "A")
        enqueue_rerank_marker(ebull_test_conn, 1)
        enqueue_locked_by_sibling(ebull_test_conn, 1)
        assert _queue_row(ebull_test_conn, 1) == (0, RERANK_MARKER)


class TestDemoteToRerankNeededIntegration:
    def test_thesis_failure_row_demoted(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "A")
        enqueue_retry(ebull_test_conn, 1, "RuntimeError")
        assert _queue_row(ebull_test_conn, 1) == (1, "RuntimeError")
        demote_to_rerank_needed(ebull_test_conn, 1)
        assert _queue_row(ebull_test_conn, 1) == (0, RERANK_MARKER)

    def test_locked_by_sibling_row_demoted(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "A")
        enqueue_locked_by_sibling(ebull_test_conn, 1)
        demote_to_rerank_needed(ebull_test_conn, 1)
        assert _queue_row(ebull_test_conn, 1) == (0, RERANK_MARKER)

    def test_rerank_needed_row_untouched(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Pre-existing RERANK_NEEDED row filtered out by the WHERE
        clause — demote_to_rerank_needed is a no-op."""
        _seed_instrument(ebull_test_conn, 1, "A")
        enqueue_rerank_marker(ebull_test_conn, 1)
        # Mutate the row to detect if demote touches it.
        ebull_test_conn.execute("UPDATE cascade_retry_queue SET last_attempted_at = NULL WHERE instrument_id = 1")
        ebull_test_conn.commit()
        demote_to_rerank_needed(ebull_test_conn, 1)
        # Row state unchanged — if demote had fired, last_attempted_at would have been set to NOW().
        row = ebull_test_conn.execute(
            "SELECT attempt_count, last_error, last_attempted_at FROM cascade_retry_queue WHERE instrument_id = 1"
        ).fetchone()
        assert row is not None
        assert row[0] == 0
        assert row[1] == RERANK_MARKER
        assert row[2] is None  # untouched


class TestDurabilityOfHelperCommits:
    """Regression for the K.3 commit-after-execute refactor — a
    later cascade-level rollback must NOT erase a prior enqueue
    write. Pre-K.3 the helper used ``with conn.transaction():``
    which creates a savepoint under the implicit outer tx; the
    savepoint's writes are lost when ``conn.rollback()`` fires."""

    def test_rollback_after_enqueue_preserves_row(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, 1, "A")
        enqueue_retry(ebull_test_conn, 1, "RuntimeError")
        # Simulate cascade's later rollback of the implicit tx.
        ebull_test_conn.rollback()
        assert _queue_row(ebull_test_conn, 1) == (1, "RuntimeError")
