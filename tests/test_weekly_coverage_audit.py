"""Tests for weekly_coverage_audit scheduler job (#268 Chunk E + F).

Full Chunk F body now: audit_all_instruments → query eligible
instruments → per-instrument backfill_filings → log outcome counts.
No post-backfill audit re-sweep (design v3 C1).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import app.services.coverage_audit  # noqa: F401
import app.services.filings_backfill  # noqa: F401
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


def test_weekly_coverage_audit_runs_audit_and_backfills_eligible() -> None:
    """Full path: audit runs, eligible set queried, each instrument
    is backfilled, tracker.row_count reflects audit total_updated."""
    summary = AuditSummary(
        analysable=42,
        insufficient=5,
        fpi=1,
        no_primary_sec_cik=3,
        total_updated=7,
        null_anomalies=0,
    )

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"

    tracker = MagicMock()
    tracker.row_count = None

    # Two eligible rows — both backfill to COMPLETE_OK.
    eligible_rows = [(101, "0000000101"), (102, "0000000102")]

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as provider_cls,
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

        scheduler.weekly_coverage_audit()

    audit_mock.assert_called_once_with(conn_mock)
    assert backfill_mock.call_count == 2
    # Row count = audit.total_updated + non-skipped backfill writes
    # (2 × COMPLETE_OK = 2). Per design v5.
    assert tracker.row_count == 9


def test_weekly_coverage_audit_per_instrument_error_is_isolated() -> None:
    """One backfill raising must not abort the whole batch."""
    summary = AuditSummary(
        analysable=0,
        insufficient=3,
        fpi=0,
        no_primary_sec_cik=0,
        total_updated=3,
        null_anomalies=0,
    )
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

        # Must not raise — the middle instrument errors but the
        # others still get processed.
        scheduler.weekly_coverage_audit()

    assert backfill_mock.call_count == 3
    # Rollback is called after the erroring instrument so the
    # shared connection isn't poisoned for the next iteration.
    conn_mock.rollback.assert_called()


def test_weekly_coverage_audit_propagates_audit_failure() -> None:
    """audit_all_instruments raising must propagate so _tracked_job
    records status=failure. No silent swallow."""
    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"

    tracker = MagicMock()

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch(
            "app.services.coverage_audit.audit_all_instruments",
            side_effect=RuntimeError("classifier broke"),
        ),
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        psycopg_mod.connect.return_value.__enter__.return_value = MagicMock()

        try:
            scheduler.weekly_coverage_audit()
        except RuntimeError as exc:
            assert "classifier broke" in str(exc)
        else:
            raise AssertionError("expected RuntimeError to propagate")
