"""Unit tests for thesis_refresh (#1919 PR-B; #2065 dropped the demote hook).

Mocks psycopg.connect + make_llm_clients + generate_thesis +
instrument_lock to prove the scheduler's per-instrument loop behaves
per spec: acquired → generate, not acquired → skip + count;
provider-unresolvable → PREREQ_SKIP before _tracked_job. The
scope/batch selection is pure and table-tested separately (no DB).
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
        patch.object(scheduler, "make_llm_clients") as make_client,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "connect_job") as connect_job_mock,
        patch.object(scheduler, "generate_thesis") as gen,
        patch.object(scheduler, "release_local_models") as release,
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
            "release": release,
            "candidates": candidates_mock,
        }


def test_lock_acquired_generates(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """Acquired lock → generate_thesis runs with trigger='scheduled'.
    No queue side-effects remain post-#2065 (rankings pick the thesis
    up at the next morning_candidate_review scoring run)."""

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield True  # always acquire

    with patch.object(scheduler, "instrument_lock", fake_lock):
        scheduler.thesis_refresh()

    mocked_env["generate_thesis"].assert_called_once()
    assert mocked_env["generate_thesis"].call_args.kwargs["trigger"] == "scheduled"
    # The K.2 outbox hooks must be gone from the scheduler (#2065).
    assert not hasattr(scheduler, "demote_to_rerank_needed")
    assert not hasattr(scheduler, "clear_retry_success")
    # tracker.row_count was set to 1 (generated)
    assert mocked_env["tracker"].row_count == 1


def test_lock_not_acquired_skips_without_generate(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """Lock contention → generate_thesis NOT called (no enqueue on
    skip per K.3 spec)."""

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield False  # sibling holds

    with patch.object(scheduler, "instrument_lock", fake_lock):
        scheduler.thesis_refresh()

    mocked_env["generate_thesis"].assert_not_called()
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
# Local-model release after the batch (#2187)
# ---------------------------------------------------------------------------


def test_batch_releases_local_model_when_done(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """#2187: the model stays warm across the batch, then is released —
    otherwise qwen3:14b sits 9.77 GB wired for the ~33 idle min/hour."""

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield True

    with patch.object(scheduler, "instrument_lock", fake_lock):
        scheduler.thesis_refresh()

    mocked_env["release"].assert_called_once_with(mocked_env["make_client"].return_value)


def test_batch_releases_local_model_when_a_generation_raises(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """A failed batch is exactly when the weights would otherwise stay
    resident until the next hourly fire — release runs in ``finally``."""
    mocked_env["generate_thesis"].side_effect = RuntimeError("decode blew up")

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield True

    with patch.object(scheduler, "instrument_lock", fake_lock):
        scheduler.thesis_refresh()

    assert mocked_env["tracker"].row_count == 0
    mocked_env["release"].assert_called_once()


def test_empty_batch_does_not_release(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """Nothing was loaded, so nothing is unloaded — on a shared box an
    unconditional release would evict another consumer's model."""
    mocked_env["candidates"].return_value = []

    scheduler.thesis_refresh()

    mocked_env["generate_thesis"].assert_not_called()
    mocked_env["release"].assert_not_called()


def test_all_locked_batch_does_not_release(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """Codex ckpt-2: a NON-empty batch that is entirely
    LOCKED_BY_SIBLING never loads a model here — and the sibling holding
    those locks is plausibly mid-generation with the same local model, so
    releasing would de-warm someone else's work."""

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield False  # sibling holds every one

    with patch.object(scheduler, "instrument_lock", fake_lock):
        scheduler.thesis_refresh()

    mocked_env["generate_thesis"].assert_not_called()
    mocked_env["release"].assert_not_called()


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
