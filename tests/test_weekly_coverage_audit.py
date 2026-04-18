"""Tests for the weekly_coverage_audit scheduler job (#268 Chunk F, minimal).

Audit-only variant shipped before Chunk E's backfill helper — the job
calls audit_all_instruments and logs the AuditSummary, no remediation.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# Import the module at module-level so ``patch("app.services.coverage_audit.X")``
# has a concrete target BEFORE the lazy ``from app.services.coverage_audit import
# audit_all_instruments`` inside ``weekly_coverage_audit`` resolves its binding.
import app.services.coverage_audit  # noqa: F401
from app.services.coverage_audit import AuditSummary
from app.workers import scheduler


def test_weekly_coverage_audit_runs_audit_and_sets_row_count() -> None:
    """Happy path: audit_all_instruments returns a summary; tracker.row_count
    is set to total_updated; no raise."""
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

    tracker = MagicMock()
    tracker.row_count = None

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch(
            "app.services.coverage_audit.audit_all_instruments",
            return_value=summary,
        ) as audit_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        conn_mock = MagicMock()
        psycopg_mod.connect.return_value.__enter__.return_value = conn_mock

        scheduler.weekly_coverage_audit()

    audit_mock.assert_called_once_with(conn_mock)
    assert tracker.row_count == 7


def test_weekly_coverage_audit_propagates_failure() -> None:
    """audit_all_instruments raising must propagate so _tracked_job
    records status=failure. No silent swallow."""
    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"

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
