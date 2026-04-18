"""Unit tests for daily_thesis_refresh's K.3 advisory-lock wiring.

Mocks psycopg.connect + generate_thesis + instrument_lock +
demote_to_rerank_needed to prove the scheduler's per-instrument
loop behaves per spec: acquired → generate + demote, not acquired
→ skip + count.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from app.workers import scheduler


@pytest.fixture
def mocked_env():  # type: ignore[no-untyped-def]
    """Common patchset: skip settings check, one stale instrument."""
    stub_settings = MagicMock()
    stub_settings.anthropic_api_key = "test-key"
    stub_settings.database_url = "postgresql://test"

    stale_item = MagicMock()
    stale_item.instrument_id = 101
    stale_item.symbol = "AAPL"

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "_record_prereq_skip"),
        patch.object(scheduler, "find_stale_instruments") as find_stale,
        patch.object(scheduler, "anthropic") as anthropic_mod,
        patch.object(scheduler, "psycopg") as psycopg_mod,
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
        find_stale.side_effect = [[stale_item], []]  # T1 has one, T2 empty
        anthropic_mod.Anthropic.return_value = MagicMock()

        conn_mock = MagicMock()
        psycopg_mod.connect.return_value.__enter__.return_value = conn_mock

        yield {
            "stale_item": stale_item,
            "conn": conn_mock,
            "generate_thesis": gen,
            "tracker": tracker,
        }


def test_lock_acquired_generates_and_demotes(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """Acquired lock → generate_thesis runs, demote_to_rerank_needed
    is called after success, NOT clear_retry_success."""

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield True  # always acquire

    with (
        patch.object(scheduler, "instrument_lock", fake_lock),
        patch.object(scheduler, "demote_to_rerank_needed") as demote_mock,
    ):
        scheduler.daily_thesis_refresh()

    mocked_env["generate_thesis"].assert_called_once()
    demote_mock.assert_called_once_with(mocked_env["conn"], 101)
    # scheduler must NOT import clear_retry_success — only demote.
    assert not hasattr(scheduler, "clear_retry_success")
    # tracker.row_count was set to 1 (generated)
    assert mocked_env["tracker"].row_count == 1


def test_lock_not_acquired_skips_without_generate(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """Lock contention → generate_thesis NOT called, demote NOT
    called (daily doesn't enqueue on skip per K.3 spec)."""

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield False  # sibling holds

    with (
        patch.object(scheduler, "instrument_lock", fake_lock),
        patch.object(scheduler, "demote_to_rerank_needed") as demote_mock,
    ):
        scheduler.daily_thesis_refresh()

    mocked_env["generate_thesis"].assert_not_called()
    demote_mock.assert_not_called()
    # No successful generations.
    assert mocked_env["tracker"].row_count == 0
