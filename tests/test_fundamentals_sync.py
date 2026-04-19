"""Tests for fundamentals_sync scheduler job.

Successor to the retired weekly_coverage_audit + weekly_coverage_review
jobs: phase 1 runs audit_all_instruments + per-eligible-instrument
backfill_filings, phase 2 runs review_coverage. See
docs/superpowers/specs/2026-04-19-research-tool-refocus.md §1.1.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.services.coverage import ReviewResult
from app.services.coverage_audit import AuditSummary
from app.services.filings_backfill import BackfillOutcome, BackfillResult
from app.workers import scheduler


def _stub_backfill_result(
    instrument_id: int,
    outcome: BackfillOutcome = BackfillOutcome.COMPLETE_OK,
) -> BackfillResult:
    return BackfillResult(
        instrument_id=instrument_id,
        outcome=outcome,
        pages_fetched=1,
        filings_upserted=0,
        eight_k_gap_filled=0,
        final_status="analysable",
    )


def _stub_review_result(promotions: int = 0, demotions: int = 0) -> ReviewResult:
    return ReviewResult(
        promotions=[MagicMock() for _ in range(promotions)],
        demotions=[MagicMock() for _ in range(demotions)],
        blocked=[],
        unchanged=0,
    )


def test_fundamentals_sync_runs_audit_backfill_then_review() -> None:
    """Full happy path: audit runs, eligible set backfilled, review runs,
    tracker.row_count reflects audit total_updated + backfill writes
    + review promotions+demotions."""
    summary = AuditSummary(
        analysable=42,
        insufficient=5,
        fpi=1,
        no_primary_sec_cik=3,
        total_updated=7,
        null_anomalies=0,
    )
    review = _stub_review_result(promotions=2, demotions=1)

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"

    tracker = MagicMock()
    tracker.row_count = None

    eligible_rows = [(101, "0000000101"), (102, "0000000102")]

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as provider_cls,
        patch.object(scheduler, "review_coverage", return_value=review) as review_mock,
        patch(
            "app.services.coverage_audit.audit_all_instruments",
            return_value=summary,
        ) as audit_mock,
        patch(
            "app.services.filings_backfill.backfill_filings",
            side_effect=lambda conn, provider, cik, iid: _stub_backfill_result(iid),
        ) as backfill_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        conn_mock = MagicMock()
        conn_mock.execute.return_value.fetchall.return_value = eligible_rows
        psycopg_mod.connect.return_value.__enter__.return_value = conn_mock
        provider_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.fundamentals_sync()

    audit_mock.assert_called_once_with(conn_mock)
    assert backfill_mock.call_count == 2
    review_mock.assert_called_once()
    # audit.total_updated (7) + 2 × COMPLETE_OK (2) + promotions+demotions (3)
    assert tracker.row_count == 12


def test_fundamentals_sync_per_instrument_error_is_isolated() -> None:
    """One backfill raising must not abort the whole batch or block review."""
    summary = AuditSummary(
        analysable=0,
        insufficient=3,
        fpi=0,
        no_primary_sec_cik=0,
        total_updated=3,
        null_anomalies=0,
    )
    review = _stub_review_result()

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"

    tracker = MagicMock()

    eligible_rows = [(201, "0000000201"), (202, "0000000202"), (203, "0000000203")]

    def flaky_backfill(conn: object, provider: object, cik: str, iid: int) -> BackfillResult:
        if iid == 202:
            raise RuntimeError("simulated provider crash")
        return _stub_backfill_result(iid)

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as provider_cls,
        patch.object(scheduler, "review_coverage", return_value=review) as review_mock,
        patch(
            "app.services.coverage_audit.audit_all_instruments",
            return_value=summary,
        ),
        patch(
            "app.services.filings_backfill.backfill_filings",
            side_effect=flaky_backfill,
        ) as backfill_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        conn_mock = MagicMock()
        conn_mock.execute.return_value.fetchall.return_value = eligible_rows
        psycopg_mod.connect.return_value.__enter__.return_value = conn_mock
        provider_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.fundamentals_sync()

    assert backfill_mock.call_count == 3
    conn_mock.rollback.assert_called()
    # Review still runs even after a per-instrument backfill error — we
    # completed phase 1 (not phase-fatal).
    review_mock.assert_called_once()


def test_fundamentals_sync_propagates_audit_failure_and_skips_review() -> None:
    """audit_all_instruments raising must propagate (so _tracked_job records
    failure) and review_coverage must NOT run — phase 2 is gated on phase 1
    completing."""
    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"

    tracker = MagicMock()

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "review_coverage") as review_mock,
        patch(
            "app.services.coverage_audit.audit_all_instruments",
            side_effect=RuntimeError("classifier broke"),
        ),
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        psycopg_mod.connect.return_value.__enter__.return_value = MagicMock()

        try:
            scheduler.fundamentals_sync()
        except RuntimeError as exc:
            assert "classifier broke" in str(exc)
        else:
            raise AssertionError("expected RuntimeError to propagate")

    review_mock.assert_not_called()
