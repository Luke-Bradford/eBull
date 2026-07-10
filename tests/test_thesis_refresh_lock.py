"""Unit tests for thesis_refresh (#1919 PR-B).

Mocks psycopg.connect + make_llm_client + generate_thesis +
instrument_lock + demote_to_rerank_needed to prove the scheduler's
per-instrument loop behaves per spec: acquired → generate + demote,
not acquired → skip + count; provider-unresolvable → PREREQ_SKIP
before _tracked_job. The scope/batch selection is pure and
table-tested separately (no DB).
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from app.services.llm_client import LLMProviderNotConfigured
from app.services.thesis import StaleInstrument
from app.workers import scheduler


def _stale(instrument_id: int, symbol: str) -> StaleInstrument:
    return StaleInstrument(instrument_id=instrument_id, symbol=symbol, reason="no_thesis")


@pytest.fixture
def mocked_env():  # type: ignore[no-untyped-def]
    """Common patchset: resolvable provider, one stale held candidate."""
    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"

    stale_item = _stale(101, "AAPL")

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "_record_prereq_skip") as prereq_skip,
        patch.object(scheduler, "_thesis_refresh_candidates") as candidates_mock,
        patch.object(scheduler, "find_stale_instruments") as find_stale,
        patch.object(scheduler, "make_llm_client") as make_client,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "connect_job") as connect_job_mock,
        patch.object(scheduler, "generate_thesis") as gen,
        patch.object(scheduler, "report_progress"),
    ):
        tracker = MagicMock()
        # Defensive init: scheduler's ``tracker.row_count = generated``
        # assignment runs inside the tracked-job body and will overwrite
        # this. Keeping it as an int (not None) makes the downstream
        # assertions unambiguous if the scheduler ever skipped the
        # assignment.
        tracker.row_count = 0
        tracked_cm.return_value.__enter__.return_value = tracker
        candidates_mock.return_value = [101]
        find_stale.return_value = [stale_item]
        client = MagicMock()
        client.provider_name = "openai_compatible"
        client.model = "qwen3:14b"
        make_client.return_value = client

        conn_mock = MagicMock()
        # The provider-resolvable guard opens a RAW psycopg conn BEFORE
        # _tracked_job (prevention-log rule); the loop conns come from
        # connect_job (app.jobs.job_connection) — patch both.
        psycopg_mod.connect.return_value.__enter__.return_value = conn_mock
        connect_job_mock.return_value.__enter__.return_value = conn_mock

        yield {
            "stale_item": stale_item,
            "conn": conn_mock,
            "generate_thesis": gen,
            "tracker": tracker,
            "tracked_cm": tracked_cm,
            "make_client": make_client,
            "prereq_skip": prereq_skip,
        }


def test_lock_acquired_generates_and_demotes(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """Acquired lock → generate_thesis runs with trigger='scheduled',
    demote_to_rerank_needed is called after success."""

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield True  # always acquire

    with (
        patch.object(scheduler, "instrument_lock", fake_lock),
        patch.object(scheduler, "demote_to_rerank_needed") as demote_mock,
    ):
        scheduler.thesis_refresh()

    mocked_env["generate_thesis"].assert_called_once()
    assert mocked_env["generate_thesis"].call_args.kwargs["trigger"] == "scheduled"
    demote_mock.assert_called_once_with(mocked_env["conn"], 101)
    # scheduler must NOT import clear_retry_success — only demote.
    assert not hasattr(scheduler, "clear_retry_success")
    # tracker.row_count was set to 1 (generated)
    assert mocked_env["tracker"].row_count == 1


def test_lock_not_acquired_skips_without_generate(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """Lock contention → generate_thesis NOT called, demote NOT
    called (no enqueue on skip per K.3 spec)."""

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield False  # sibling holds

    with (
        patch.object(scheduler, "instrument_lock", fake_lock),
        patch.object(scheduler, "demote_to_rerank_needed") as demote_mock,
    ):
        scheduler.thesis_refresh()

    mocked_env["generate_thesis"].assert_not_called()
    demote_mock.assert_not_called()
    # No successful generations.
    assert mocked_env["tracker"].row_count == 0


def test_provider_unresolvable_prereq_skips_before_tracked_job(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """LLMProviderNotConfigured → PREREQ_SKIP row, _tracked_job never
    entered, no LLM work attempted (prevention-log: guard OUTSIDE
    _tracked_job so exactly one job_runs row is written)."""
    mocked_env["make_client"].side_effect = LLMProviderNotConfigured(
        "llm_provider='anthropic' but ANTHROPIC_API_KEY is not set"
    )

    scheduler.thesis_refresh()

    mocked_env["prereq_skip"].assert_called_once()
    assert mocked_env["prereq_skip"].call_args.args[0] == scheduler.JOB_THESIS_REFRESH
    mocked_env["tracked_cm"].assert_not_called()
    mocked_env["generate_thesis"].assert_not_called()


# ---------------------------------------------------------------------------
# _select_thesis_batch — pure scope/batch selection (spec §6)
# ---------------------------------------------------------------------------


class TestSelectThesisBatch:
    def test_priority_order_and_bound(self) -> None:
        """Held-first candidate order is preserved; batch capped at the
        bound with the remainder reported as deferred."""
        candidates = [5, 3, 9, 1, 7, 2, 8]  # held first, then rank order
        stale = [_stale(iid, f"S{iid}") for iid in (1, 2, 3, 5, 7, 8, 9)]

        batch, deferred = scheduler._select_thesis_batch(candidates, stale)

        assert [item.instrument_id for item in batch] == [5, 3, 9, 1, 7]
        assert len(batch) == scheduler._THESIS_REFRESH_BATCH_LIMIT
        assert deferred == 2

    def test_fresh_candidates_drop_out(self) -> None:
        """Candidates absent from the stale set (fresh thesis or
        non-analysable) are not padded into the batch."""
        candidates = [5, 3, 9]
        stale = [_stale(9, "S9")]

        batch, deferred = scheduler._select_thesis_batch(candidates, stale)

        assert [item.instrument_id for item in batch] == [9]
        assert deferred == 0

    def test_empty_inputs(self) -> None:
        assert scheduler._select_thesis_batch([], []) == ([], 0)
