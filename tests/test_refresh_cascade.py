"""Unit tests for refresh_cascade (#276 K.1 + K.2).

Mock-based so no real Claude API is called. Integration tests that
exercise the psycopg-aborted-transaction recovery path and real
DB behaviour live in tests/test_cascade_retry_queue_integration.py
and the scheduler-hook integration — MagicMock cannot model aborted
psycopg transactions end-to-end.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.services.refresh_cascade import (
    ATTEMPT_CAP,
    LOCKED_BY_SIBLING,
    RERANK_MARKER,
    CascadeOutcome,
    cascade_refresh,
    changed_instruments_from_outcome,
    demote_to_rerank_needed,
    drain_retry_queue,
    enqueue_locked_by_sibling,
    enqueue_retry,
    instrument_lock,
)
from app.services.sec_incremental import RefreshOutcome, RefreshPlan
from app.services.thesis import StaleInstrument

# ---------------------------------------------------------------------------
# changed_instruments_from_outcome
# ---------------------------------------------------------------------------


def _mock_conn_with_cik_lookup(cik_to_instrument: dict[str, int]) -> MagicMock:
    """Mock conn whose SELECT resolves CIKs to instrument_ids."""
    conn = MagicMock()

    def execute_side_effect(sql, params=None):  # type: ignore[no-untyped-def]
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        sql_str = sql if isinstance(sql, str) else str(sql)
        if "external_identifiers" in sql_str:
            # params is (cik_list,) — one tuple arg
            ciks = params[0] if isinstance(params, tuple) else (params or [])
            rows = []
            for cik in ciks:
                if cik in cik_to_instrument:
                    rows.append((cik_to_instrument[cik],))
            cursor.fetchall.return_value = sorted(rows)
        return cursor

    conn.execute.side_effect = execute_side_effect
    return conn


class TestChangedInstrumentsFromOutcome:
    def test_empty_plan_returns_empty_list(self) -> None:
        conn = MagicMock()
        plan = RefreshPlan()
        outcome = RefreshOutcome()
        assert changed_instruments_from_outcome(conn, plan, outcome) == []
        conn.execute.assert_not_called()

    def test_seeds_excluded(self) -> None:
        """Seeds don't cascade — prevents fresh-install Claude storm."""
        conn = _mock_conn_with_cik_lookup({"0000000001": 1})
        plan = RefreshPlan(seeds=["0000000001"])
        outcome = RefreshOutcome(seeded=1)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == []

    def test_refreshes_mapped_to_instrument_ids(self) -> None:
        conn = _mock_conn_with_cik_lookup({"0000000001": 101, "0000000002": 102})
        plan = RefreshPlan(
            refreshes=[("0000000001", "ACCN-1"), ("0000000002", "ACCN-2")],
        )
        outcome = RefreshOutcome(refreshed=2)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101, 102]

    def test_submissions_only_mapped_to_instrument_ids(self) -> None:
        conn = _mock_conn_with_cik_lookup({"0000000001": 101})
        plan = RefreshPlan(
            submissions_only_advances=[("0000000001", "ACCN-8K")],
        )
        outcome = RefreshOutcome(submissions_advanced=1)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101]

    def test_failed_ciks_excluded(self) -> None:
        """CIKs in outcome.failed drop out of the cascade set."""
        conn = _mock_conn_with_cik_lookup({"0000000001": 101, "0000000002": 102})
        plan = RefreshPlan(
            refreshes=[("0000000001", "A"), ("0000000002", "B")],
        )
        outcome = RefreshOutcome(
            refreshed=1,
            failed=[("0000000002", "RuntimeError")],
        )
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101]

    def test_seed_in_refresh_bucket_still_excluded(self) -> None:
        """Defensive: if a CIK appears in both seeds and refreshes
        (planner divergence or manual plan), the seed filter wins —
        no cascade for fresh-install CIKs."""
        conn = _mock_conn_with_cik_lookup({"0000000001": 101, "0000000002": 102})
        plan = RefreshPlan(
            seeds=["0000000001"],
            refreshes=[("0000000001", "A"), ("0000000002", "B")],
        )
        outcome = RefreshOutcome(seeded=1, refreshed=1)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [102]

    def test_unpadded_cik_normalized_to_zero_padded(self) -> None:
        """Defensive zfill(10): if a future caller hands us a
        raw-integer CIK string (e.g. '320193'), we still match the
        zero-padded identifier_value stored in external_identifiers
        ('0000320193'). Protects against silent zero-row misses."""
        conn = _mock_conn_with_cik_lookup({"0000320193": 101})
        plan = RefreshPlan(refreshes=[("320193", "ACCN-APPL")])
        outcome = RefreshOutcome(refreshed=1)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101]

    def test_mixed_padded_and_unpadded_cik_dedupe_after_padding(self) -> None:
        """Both unpadded and padded forms of the same CIK collapse
        to one mapping — de-dupe operates on the padded form."""
        conn = _mock_conn_with_cik_lookup({"0000320193": 101})
        plan = RefreshPlan(
            refreshes=[("320193", "A"), ("0000320193", "B")],
        )
        outcome = RefreshOutcome(refreshed=2)
        result = changed_instruments_from_outcome(conn, plan, outcome)
        assert result == [101]


# ---------------------------------------------------------------------------
# Retry outbox helpers (K.2) — pure SQL-emitting helpers
# ---------------------------------------------------------------------------


class TestInstrumentLock:
    def test_acquired_yields_true_and_unlocks_on_exit(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (True,)
        conn.execute.return_value = cursor
        with instrument_lock(conn, 42) as acquired:
            assert acquired is True
        # First call: pg_try_advisory_lock. Second: pg_advisory_unlock.
        assert conn.execute.call_count == 2
        first_sql = conn.execute.call_args_list[0].args[0]
        second_sql = conn.execute.call_args_list[1].args[0]
        assert "pg_try_advisory_lock" in first_sql
        assert "pg_advisory_unlock" in second_sql

    def test_not_acquired_yields_false_and_no_unlock(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (False,)
        conn.execute.return_value = cursor
        with instrument_lock(conn, 42) as acquired:
            assert acquired is False
        # Only the pg_try_advisory_lock call — no unlock.
        assert conn.execute.call_count == 1

    def test_unlocks_even_if_body_raises(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchone.return_value = (True,)
        conn.execute.return_value = cursor
        with pytest.raises(RuntimeError):
            with instrument_lock(conn, 42):
                raise RuntimeError("body failed")
        # Unlock still ran
        assert conn.execute.call_count == 2

    def test_unlock_retries_after_psycopg_error(self) -> None:
        conn = MagicMock()
        lock_cursor = MagicMock()
        lock_cursor.fetchone.return_value = (True,)
        # First unlock raises psycopg.Error; rollback succeeds; second unlock succeeds.
        import psycopg as _psycopg

        conn.execute.side_effect = [
            lock_cursor,  # pg_try_advisory_lock
            _psycopg.Error("connection INERROR"),  # first unlock
            MagicMock(),  # second unlock after rollback
        ]
        with instrument_lock(conn, 42) as acquired:
            assert acquired is True
        conn.rollback.assert_called_once()
        assert conn.execute.call_count == 3


class TestEnqueueLockedBySibling:
    def test_insert_on_conflict_do_nothing_and_commits(self) -> None:
        conn = MagicMock()
        enqueue_locked_by_sibling(conn, 42)
        conn.execute.assert_called_once()
        conn.commit.assert_called_once()
        sql = conn.execute.call_args.args[0]
        params = conn.execute.call_args.args[1]
        assert "INSERT INTO cascade_retry_queue" in sql
        assert "ON CONFLICT (instrument_id) DO NOTHING" in sql
        assert params == (42, LOCKED_BY_SIBLING)


class TestDemoteToRerankNeeded:
    def test_update_with_rerank_marker_filter_and_commits(self) -> None:
        conn = MagicMock()
        demote_to_rerank_needed(conn, 42)
        conn.execute.assert_called_once()
        conn.commit.assert_called_once()
        sql = conn.execute.call_args.args[0]
        params = conn.execute.call_args.args[1]
        assert "UPDATE cascade_retry_queue" in sql
        assert "last_error IS DISTINCT FROM %s" in sql
        assert "SET attempt_count = 0" in sql
        # (RERANK_MARKER for SET, instrument_id, RERANK_MARKER for WHERE filter)
        assert params == (RERANK_MARKER, 42, RERANK_MARKER)


class TestEnqueueRetry:
    def test_executes_upsert_and_commits(self) -> None:
        """K.3 refactor: helpers commit explicitly rather than using
        ``with conn.transaction():``. This makes the queue write
        durable regardless of implicit-tx state so a later
        cascade-level rollback cannot erase it."""
        conn = MagicMock()
        enqueue_retry(conn, 42, "RuntimeError")
        conn.execute.assert_called_once()
        conn.commit.assert_called_once()
        args, _ = conn.execute.call_args
        sql = args[0]
        params = args[1]
        assert "INSERT INTO cascade_retry_queue" in sql
        assert "ON CONFLICT (instrument_id) DO UPDATE" in sql
        assert "attempt_count = cascade_retry_queue.attempt_count + 1" in sql
        assert params == (42, "RuntimeError")


class TestDrainRetryQueue:
    def test_passes_cap_and_returns_ids(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = [(1,), (2,), (3,)]
        conn.execute.return_value = cursor
        result = drain_retry_queue(conn)
        assert result == [1, 2, 3]
        args, _ = conn.execute.call_args
        sql = args[0]
        params = args[1]
        assert "WHERE attempt_count < %s" in sql
        assert "ORDER BY enqueued_at ASC" in sql
        assert params == (ATTEMPT_CAP,)

    def test_custom_cap_respected(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn.execute.return_value = cursor
        drain_retry_queue(conn, cap=3)
        args, _ = conn.execute.call_args
        assert args[1] == (3,)

    def test_empty_queue_returns_empty_list(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn.execute.return_value = cursor
        assert drain_retry_queue(conn) == []


# ---------------------------------------------------------------------------
# cascade_refresh
# ---------------------------------------------------------------------------


class TestCascadeRefresh:
    def test_empty_ids_and_empty_queue_noop(self) -> None:
        conn = MagicMock()
        client = MagicMock()
        with patch(
            "app.services.refresh_cascade.drain_retry_queue",
            return_value=[],
        ):
            outcome = cascade_refresh(conn, client, [])
        assert outcome == CascadeOutcome(
            instruments_considered=0,
            thesis_refreshed=0,
            rankings_recomputed=False,
            retries_drained=0,
        )

    def test_no_stale_no_retries_skips_everything(self) -> None:
        """If find_stale returns nothing and queue is empty, no
        Claude calls and no rerank."""
        conn = MagicMock()
        client = MagicMock()
        with (
            patch(
                "app.services.refresh_cascade.drain_retry_queue",
                return_value=[],
            ),
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=[],
            ) as stale_mock,
            patch("app.services.refresh_cascade.generate_thesis") as gen_mock,
            patch("app.services.refresh_cascade.compute_rankings") as rank_mock,
        ):
            outcome = cascade_refresh(conn, client, [1, 2, 3])

        stale_mock.assert_called_once_with(conn, tier=None, instrument_ids=[1, 2, 3])
        gen_mock.assert_not_called()
        rank_mock.assert_not_called()
        assert outcome.thesis_refreshed == 0
        assert outcome.rankings_recomputed is False
        assert outcome.instruments_considered == 3

    def test_stale_instruments_trigger_thesis_and_single_rerank(self) -> None:
        """Stale instruments each get generate_thesis; rerank runs once
        at the end, not per-instrument. Success clears queue rows."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [
            StaleInstrument(instrument_id=1, symbol="A", reason="event_new_10q"),
            StaleInstrument(instrument_id=2, symbol="B", reason="event_new_8k"),
        ]
        with (
            patch("app.services.refresh_cascade.drain_retry_queue", return_value=[]),
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch("app.services.refresh_cascade.generate_thesis") as gen_mock,
            patch(
                "app.services.refresh_cascade.compute_rankings",
                return_value=MagicMock(scored=[MagicMock(), MagicMock()]),
            ) as rank_mock,
            patch("app.services.refresh_cascade.clear_retry_success") as clear_mock,
        ):
            outcome = cascade_refresh(conn, client, [1, 2])

        assert gen_mock.call_count == 2
        rank_mock.assert_called_once_with(conn)
        assert clear_mock.call_count == 2
        assert outcome.thesis_refreshed == 2
        assert outcome.rankings_recomputed is True
        assert outcome.failed == ()

    def test_per_instrument_failure_isolated_rerank_still_runs(self) -> None:
        """One instrument's thesis raising must not abort siblings,
        a successful sibling still triggers the rerank, and the
        failed instrument is enqueued to the retry outbox."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [
            StaleInstrument(instrument_id=1, symbol="BAD", reason="event_new_10k"),
            StaleInstrument(instrument_id=2, symbol="GOOD", reason="event_new_10q"),
        ]

        def gen_side_effect(iid, conn_, client_):  # type: ignore[no-untyped-def]
            if iid == 1:
                raise RuntimeError("boom")
            return MagicMock()

        with (
            patch("app.services.refresh_cascade.drain_retry_queue", return_value=[]),
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch(
                "app.services.refresh_cascade.generate_thesis",
                side_effect=gen_side_effect,
            ),
            patch(
                "app.services.refresh_cascade.compute_rankings",
                return_value=MagicMock(scored=[]),
            ) as rank_mock,
            patch("app.services.refresh_cascade.enqueue_retry") as enqueue_mock,
            patch("app.services.refresh_cascade.clear_retry_success") as clear_mock,
        ):
            outcome = cascade_refresh(conn, client, [1, 2])

        assert outcome.thesis_refreshed == 1
        assert ("RuntimeError") in {e[1] for e in outcome.failed}
        rank_mock.assert_called_once()
        assert outcome.rankings_recomputed is True
        enqueue_mock.assert_called_once_with(conn, 1, "RuntimeError")
        # GOOD (id=2) is the only processed_ok → cleared on rerank success
        clear_mock.assert_called_once_with(conn, 2)

    def test_all_thesis_fail_no_rerank_all_enqueued(self) -> None:
        """If zero theses refreshed, skip rerank. Each failed
        instrument lands in the outbox."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [StaleInstrument(instrument_id=1, symbol="BAD", reason="event_new_10q")]
        with (
            patch("app.services.refresh_cascade.drain_retry_queue", return_value=[]),
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch(
                "app.services.refresh_cascade.generate_thesis",
                side_effect=RuntimeError("boom"),
            ),
            patch("app.services.refresh_cascade.compute_rankings") as rank_mock,
            patch("app.services.refresh_cascade.enqueue_retry") as enqueue_mock,
        ):
            outcome = cascade_refresh(conn, client, [1])

        assert outcome.thesis_refreshed == 0
        rank_mock.assert_not_called()
        assert outcome.rankings_recomputed is False
        enqueue_mock.assert_called_once_with(conn, 1, "RuntimeError")

    def test_rerank_failure_marks_processed_ok_and_preserves_signal(self) -> None:
        """compute_rankings raising is captured as (-1, ExcType).
        processed_ok rows are NOT cleared; instead each one gets a
        RERANK_NEEDED marker so the next cycle can recover."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [StaleInstrument(instrument_id=1, symbol="A", reason="event_new_10q")]
        with (
            patch("app.services.refresh_cascade.drain_retry_queue", return_value=[]),
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch("app.services.refresh_cascade.generate_thesis"),
            patch(
                "app.services.refresh_cascade.compute_rankings",
                side_effect=RuntimeError("scoring broke"),
            ),
            patch("app.services.refresh_cascade.clear_retry_success") as clear_mock,
            patch("app.services.refresh_cascade.enqueue_rerank_marker") as marker_mock,
        ):
            outcome = cascade_refresh(conn, client, [1])

        assert outcome.thesis_refreshed == 1
        assert outcome.rankings_recomputed is False
        assert (-1, "RuntimeError") in outcome.failed
        # Clear NOT called — queue rows must survive as the durable signal.
        clear_mock.assert_not_called()
        # Marker written for the processed_ok id
        marker_mock.assert_called_once_with(conn, 1)
        # Rollback was invoked to recover from any psycopg-aborted state.
        conn.rollback.assert_called()

    def test_retry_queue_drained_bypasses_stale_gate(self) -> None:
        """Queued instrument_ids from the outbox get generate_thesis
        called even when find_stale_instruments returns them as
        non-stale. The outbox IS the signal."""
        conn = MagicMock()
        client = MagicMock()
        with (
            patch(
                "app.services.refresh_cascade.drain_retry_queue",
                return_value=[10, 20],
            ),
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=[],
            ),
            patch("app.services.refresh_cascade.generate_thesis") as gen_mock,
            patch(
                "app.services.refresh_cascade.compute_rankings",
                return_value=MagicMock(scored=[]),
            ) as rank_mock,
            patch("app.services.refresh_cascade.clear_retry_success") as clear_mock,
        ):
            outcome = cascade_refresh(conn, client, [])

        assert gen_mock.call_count == 2
        # Both retry ids processed via generate_thesis regardless of
        # find_stale's opinion.
        called_iids = {call.args[0] for call in gen_mock.call_args_list}
        assert called_iids == {10, 20}
        rank_mock.assert_called_once()
        assert outcome.retries_drained == 2
        assert outcome.thesis_refreshed == 2
        assert outcome.rankings_recomputed is True
        # Both cleared on rerank success
        assert clear_mock.call_count == 2

    def test_retry_failure_enqueues_again(self) -> None:
        """A queued instrument whose thesis fails on retry stays in
        the outbox with incremented attempt_count (via enqueue_retry)."""
        conn = MagicMock()
        client = MagicMock()
        with (
            patch(
                "app.services.refresh_cascade.drain_retry_queue",
                return_value=[7],
            ),
            patch("app.services.refresh_cascade.find_stale_instruments", return_value=[]),
            patch(
                "app.services.refresh_cascade.generate_thesis",
                side_effect=ValueError("still broken"),
            ),
            patch("app.services.refresh_cascade.compute_rankings") as rank_mock,
            patch("app.services.refresh_cascade.enqueue_retry") as enqueue_mock,
        ):
            outcome = cascade_refresh(conn, client, [])

        enqueue_mock.assert_called_once_with(conn, 7, "ValueError")
        assert (7, "ValueError") in outcome.failed
        rank_mock.assert_not_called()  # zero successes, no rerank

    def test_queued_instrument_not_processed_twice_when_also_stale(self) -> None:
        """If a CIK shows up in both the retry drain and the stale
        list, generate_thesis runs once — retry path wins."""
        conn = MagicMock()
        client = MagicMock()
        with (
            patch(
                "app.services.refresh_cascade.drain_retry_queue",
                return_value=[5],
            ),
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=[StaleInstrument(instrument_id=5, symbol="DUP", reason="event_new_10q")],
            ),
            patch("app.services.refresh_cascade.generate_thesis") as gen_mock,
            patch(
                "app.services.refresh_cascade.compute_rankings",
                return_value=MagicMock(scored=[]),
            ),
            patch("app.services.refresh_cascade.clear_retry_success"),
        ):
            outcome = cascade_refresh(conn, client, [5])

        assert gen_mock.call_count == 1
        assert outcome.thesis_refreshed == 1
        assert outcome.retries_drained == 1

    def test_rerank_marker_constant_exported(self) -> None:
        """Sanity: the marker string is importable and matches the
        spec wording used in admin surfaces (Chunk H)."""
        assert RERANK_MARKER == "RERANK_NEEDED"

    def test_locked_by_sibling_skips_generate_and_enqueues_marker(
        self,
    ) -> None:
        """K.3: when instrument_lock returns False, cascade must
        NOT call generate_thesis, MUST write a LOCKED_BY_SIBLING
        marker via enqueue_locked_by_sibling, and MUST increment
        locked_skipped without adding to failed."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [
            StaleInstrument(instrument_id=7, symbol="LOCKED", reason="event_new_10q"),
        ]

        from contextlib import contextmanager

        @contextmanager
        def fake_lock(conn_, iid):  # type: ignore[no-untyped-def]
            yield False  # never acquired

        with (
            patch("app.services.refresh_cascade.drain_retry_queue", return_value=[]),
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch("app.services.refresh_cascade.instrument_lock", fake_lock),
            patch("app.services.refresh_cascade.generate_thesis") as gen_mock,
            patch("app.services.refresh_cascade.enqueue_locked_by_sibling") as lbs_mock,
            patch("app.services.refresh_cascade.compute_rankings") as rank_mock,
        ):
            outcome = cascade_refresh(conn, client, [7])

        gen_mock.assert_not_called()
        lbs_mock.assert_called_once_with(conn, 7)
        rank_mock.assert_not_called()  # nothing to rerank
        assert outcome.locked_skipped == 1
        assert outcome.thesis_refreshed == 0
        assert outcome.failed == ()  # locked-skip is NOT a failure

    def test_locked_by_sibling_on_retry_path(self) -> None:
        """Retry path also respects the lock."""
        conn = MagicMock()
        client = MagicMock()

        from contextlib import contextmanager

        @contextmanager
        def fake_lock(conn_, iid):  # type: ignore[no-untyped-def]
            yield False

        with (
            patch(
                "app.services.refresh_cascade.drain_retry_queue",
                return_value=[99],
            ),
            patch("app.services.refresh_cascade.find_stale_instruments", return_value=[]),
            patch("app.services.refresh_cascade.instrument_lock", fake_lock),
            patch("app.services.refresh_cascade.generate_thesis") as gen_mock,
            patch("app.services.refresh_cascade.enqueue_locked_by_sibling") as lbs_mock,
        ):
            outcome = cascade_refresh(conn, client, [])

        gen_mock.assert_not_called()
        lbs_mock.assert_called_once_with(conn, 99)
        assert outcome.locked_skipped == 1
        assert outcome.retries_drained == 1
        assert outcome.failed == ()

    def test_non_psycopg_exception_from_enqueue_retry_does_not_abort_loop(
        self,
    ) -> None:
        """Fault isolation regression: a non-psycopg exception from
        enqueue_retry (programming error, CM internals, AttributeError)
        must be caught and logged — remaining stale instruments must
        still be processed and the rerank must still run."""
        conn = MagicMock()
        client = MagicMock()
        stale_rows = [
            StaleInstrument(instrument_id=1, symbol="BAD", reason="event_new_10q"),
            StaleInstrument(instrument_id=2, symbol="GOOD", reason="event_new_10q"),
        ]

        def gen_side_effect(iid, conn_, client_):  # type: ignore[no-untyped-def]
            if iid == 1:
                raise RuntimeError("thesis failed")
            return MagicMock()

        with (
            patch("app.services.refresh_cascade.drain_retry_queue", return_value=[]),
            patch(
                "app.services.refresh_cascade.find_stale_instruments",
                return_value=stale_rows,
            ),
            patch(
                "app.services.refresh_cascade.generate_thesis",
                side_effect=gen_side_effect,
            ),
            patch(
                "app.services.refresh_cascade.enqueue_retry",
                side_effect=AttributeError("helper internal bug"),
            ),
            patch(
                "app.services.refresh_cascade.compute_rankings",
                return_value=MagicMock(scored=[]),
            ) as rank_mock,
            patch("app.services.refresh_cascade.clear_retry_success"),
        ):
            outcome = cascade_refresh(conn, client, [1, 2])

        # Non-psycopg helper failure didn't break isolation:
        # GOOD (id=2) still processed + rerank still ran.
        assert outcome.thesis_refreshed == 1
        assert (1, "RuntimeError") in outcome.failed
        rank_mock.assert_called_once()
        assert outcome.rankings_recomputed is True
