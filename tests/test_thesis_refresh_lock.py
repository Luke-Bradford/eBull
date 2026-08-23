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
        # ⚠ The scheduler reads ``clients.writer.provider_name``, not
        # ``clients.provider_name`` — setting only the latter left the
        # attributes the job actually logs as auto-generated MagicMocks,
        # so nothing here constrained them until #2855 asserted on one.
        client.writer.provider_name = "openai_compatible"
        client.writer.model = "qwen3:14b"
        client.critic.model = "qwen3:14b"
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
# writer_loaded_s — the model load window (#2855)
# ---------------------------------------------------------------------------
#
# The duty cycle must be derivable from ``job_runs`` alone. ``row_count``
# already gives memos per run, but nothing recorded how long the 10 GB local
# writer was HELD — and run duration is not a substitute, because the
# candidate and staleness queries run before anything loads. Two sessions
# reasoned about this job's memory cost from stale docstring figures
# precisely because no measurement was stored.


def _loaded_s(note: str) -> int:
    return int(note.split("writer_loaded_s=")[1].split()[0])


def test_note_records_the_load_window_on_a_controlled_clock(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """The window is measured, not merely present.

    A stubbed generation is instant, so asserting ``>= 0`` would pass
    against an implementation that hard-coded zero. Pinning the clock is
    what proves the start is placed at the first ACQUIRED item and the
    end after ``release_local_models``.
    """

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield True

    with (
        patch.object(scheduler, "instrument_lock", fake_lock),
        patch.object(scheduler.time, "monotonic", side_effect=[1000.0, 1042.0]),
    ):
        scheduler.thesis_refresh()

    assert _loaded_s(mocked_env["tracker"].note) == 42
    # The provider travels with the number: writer_loaded_s is residency
    # only when the writer is local, and the note is read out of
    # job_runs.error_msg with no other context.
    assert "provider=openai_compatible" in mocked_env["tracker"].note


def test_load_window_starts_at_the_first_acquired_item_not_the_first_item(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """A LOCKED_BY_SIBLING prefix must not be billed as load time.

    Nothing is pulled into memory while the sibling holds the lock, so a
    clock started at the top of the batch would inflate the duty cycle by
    however long the contended items took to skip.
    """
    first, second = _stale(101, "AAPL"), _stale(202, "MSFT")
    mocked_env["candidates"].return_value = [101, 202]
    with patch.object(scheduler, "find_stale_instruments", return_value=[first, second]):
        acquired = iter([False, True])

        @contextmanager
        def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
            yield next(acquired)

        with (
            patch.object(scheduler, "instrument_lock", fake_lock),
            # Only two readings are consumed — if the implementation
            # started the clock on the locked item this would raise
            # StopIteration rather than silently pass.
            patch.object(scheduler.time, "monotonic", side_effect=[1000.0, 1007.0]),
        ):
            scheduler.thesis_refresh()

    assert _loaded_s(mocked_env["tracker"].note) == 7
    assert mocked_env["generate_thesis"].call_count == 1


def test_load_window_is_zero_when_nothing_loaded(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """An honest zero, not a missing value.

    A batch that is entirely LOCKED_BY_SIBLING never pulls weights into
    memory here, so this run's contribution to the duty cycle really is
    nil — and it must not be conflated with the run's wall-clock time,
    which is non-zero.
    """

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield False

    with patch.object(scheduler, "instrument_lock", fake_lock):
        scheduler.thesis_refresh()

    assert _loaded_s(mocked_env["tracker"].note) == 0


def test_empty_batch_still_records_a_zero_window(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """The no-stale-work early return is still a run.

    ``loaded_pct`` divides summed load time by wall-clock time, so a run
    that legitimately loaded nothing must be visible as a run. Omitting
    the field would make "no stale work" indistinguishable from a
    pre-#2855 row, which coalesces to zero for a different reason.
    """
    mocked_env["candidates"].return_value = []

    scheduler.thesis_refresh()

    assert _loaded_s(mocked_env["tracker"].note) == 0
    assert mocked_env["tracker"].row_count == 0


def test_load_window_survives_an_exception_out_of_the_batch(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """The run that dies mid-batch is the one whose load time matters most.

    ``generate_thesis`` raising is caught per-item, so force the failure
    past that guard — ``instrument_lock`` itself blowing up — and assert
    the window is still recorded. Computing it below the ``try`` instead
    of inside the ``finally`` would lose exactly this case.
    """

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield True

    calls = {"n": 0}

    def exploding_lock(conn, iid):  # type: ignore[no-untyped-def]
        calls["n"] += 1
        if calls["n"] > 1:
            raise RuntimeError("connection died mid-batch")
        return fake_lock(conn, iid)

    first, second = _stale(101, "AAPL"), _stale(202, "MSFT")
    mocked_env["candidates"].return_value = [101, 202]
    with patch.object(scheduler, "find_stale_instruments", return_value=[first, second]):
        with (
            patch.object(scheduler, "instrument_lock", exploding_lock),
            patch.object(scheduler.time, "monotonic", side_effect=[1000.0, 1013.0]),
        ):
            scheduler.thesis_refresh()

    # The per-item guard catches it and counts it as failed, so the run
    # completes — the assertion is that the window covers the work done
    # before the failure rather than collapsing to zero.
    assert _loaded_s(mocked_env["tracker"].note) == 13
    assert "failed=1" in mocked_env["tracker"].note


def test_load_window_survives_a_raising_release(mocked_env) -> None:  # type: ignore[no-untyped-def]
    """The measurement must not depend on another module's promise.

    ``release_local_models`` is documented as never raising, and today it
    does not. But this ticket exists because a docstring promise went
    stale silently, so the instrumentation is nested in its own
    ``finally`` rather than trusting that one — a release that started
    raising would otherwise send every failing run back to the
    indistinguishable-from-uninstrumented state (review bot, round 1).
    """

    @contextmanager
    def fake_lock(conn, iid):  # type: ignore[no-untyped-def]
        yield True

    with (
        patch.object(scheduler, "instrument_lock", fake_lock),
        patch.object(scheduler, "release_local_models", side_effect=RuntimeError("unload route gone")),
        patch.object(scheduler.time, "monotonic", side_effect=[1000.0, 1031.0]),
        pytest.raises(RuntimeError, match="unload route gone"),
    ):
        scheduler.thesis_refresh()

    assert _loaded_s(mocked_env["tracker"].note) == 31


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
