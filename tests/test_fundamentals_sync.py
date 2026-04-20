"""Tests for fundamentals_sync scheduler job.

Successor to the retired weekly_coverage_audit + weekly_coverage_review
jobs: phase 1 runs audit_all_instruments + per-eligible-instrument
backfill_filings, phase 2 runs review_coverage. See
docs/superpowers/specs/2026-04-19-research-tool-refocus.md §1.1.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.services.coverage import AuditSummary, BackfillOutcome, BackfillResult, ReviewResult
from app.workers import scheduler


def _stub_two_connect_ctxes(psycopg_mod: MagicMock, phase1_conn: MagicMock, phase2_conn: MagicMock) -> None:
    """Wire psycopg.connect to return two distinct ``with``-context-manager
    connections in sequence (phase 1 then phase 2).

    fundamentals_sync calls ``psycopg.connect(...)`` twice — once per phase —
    and a shared MagicMock would alias both calls to the same connection,
    hiding any test that a phase uses the wrong one.
    """
    cm1 = MagicMock()
    cm1.__enter__.return_value = phase1_conn
    cm1.__exit__.return_value = None
    cm2 = MagicMock()
    cm2.__enter__.return_value = phase2_conn
    cm2.__exit__.return_value = None
    psycopg_mod.connect.side_effect = [cm1, cm2]


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

    phase1_conn = MagicMock()
    phase1_conn.execute.return_value.fetchall.return_value = eligible_rows
    phase2_conn = MagicMock()

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as provider_cls,
        patch.object(scheduler, "review_coverage", return_value=review) as review_mock,
        patch.object(scheduler, "daily_cik_refresh") as cik_mock,
        patch.object(scheduler, "daily_financial_facts") as facts_mock,
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ) as audit_mock,
        patch(
            "app.services.coverage.backfill_filings",
            side_effect=lambda conn, provider, cik, iid: _stub_backfill_result(iid),
        ) as backfill_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        _stub_two_connect_ctxes(psycopg_mod, phase1_conn, phase2_conn)
        provider_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.fundamentals_sync()

    # Each phase uses its own connection — no aliasing.
    audit_mock.assert_called_once_with(phase1_conn)
    review_mock.assert_called_once_with(phase2_conn)
    assert backfill_mock.call_count == 2
    # Phases 0 + 1 fired exactly once each before the audit/review phases.
    cik_mock.assert_called_once()
    facts_mock.assert_called_once()
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

    phase1_conn = MagicMock()
    phase1_conn.execute.return_value.fetchall.return_value = eligible_rows
    phase2_conn = MagicMock()

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
        patch.object(scheduler, "daily_cik_refresh") as cik_mock,
        patch.object(scheduler, "daily_financial_facts") as facts_mock,
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ),
        patch(
            "app.services.coverage.backfill_filings",
            side_effect=flaky_backfill,
        ) as backfill_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        _stub_two_connect_ctxes(psycopg_mod, phase1_conn, phase2_conn)
        provider_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.fundamentals_sync()

    cik_mock.assert_called_once()
    facts_mock.assert_called_once()
    assert backfill_mock.call_count == 3
    # Rollback is the phase-2 (audit) connection's — phase-3 (review) should
    # not have been touched by the failed backfill.
    phase1_conn.rollback.assert_called()
    # Review still runs even after a per-instrument backfill error — we
    # completed phase 1 (not phase-fatal).
    review_mock.assert_called_once_with(phase2_conn)


def test_fundamentals_sync_propagates_audit_failure_and_skips_review() -> None:
    """audit_all_instruments raising must propagate (so _tracked_job records
    failure) and review_coverage must NOT run — phase 2 is gated on phase 1
    completing."""
    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"

    tracker = MagicMock()

    phase1_conn = MagicMock()

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "review_coverage") as review_mock,
        patch.object(scheduler, "daily_cik_refresh") as cik_mock,
        patch.object(scheduler, "daily_financial_facts") as facts_mock,
        patch(
            "app.services.coverage.audit_all_instruments",
            side_effect=RuntimeError("classifier broke"),
        ),
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        # Only one connect() fires: audit raises before the eligible-rows
        # query and before phase 2.
        cm1 = MagicMock()
        cm1.__enter__.return_value = phase1_conn
        cm1.__exit__.return_value = None
        psycopg_mod.connect.side_effect = [cm1]

        try:
            scheduler.fundamentals_sync()
        except RuntimeError as exc:
            assert "classifier broke" in str(exc)
        else:
            raise AssertionError("expected RuntimeError to propagate")

    review_mock.assert_not_called()
    # Phases 0 + 1 still fire before the audit raises.
    cik_mock.assert_called_once()
    facts_mock.assert_called_once()
    assert psycopg_mod.connect.call_count == 1


def test_fundamentals_sync_phase2_failure_preserves_phase1_success() -> None:
    """review_coverage raising must still surface as a job failure so health
    surfaces see it, but phase 2 audit + backfill writes were already
    committed and tracker.row_count reflects them. The end-of-job raise
    marks the outer _tracked_job failed; phases 0/1/3 are all isolated so
    this is the only way phase-3 failures reach the health surface."""
    summary = AuditSummary(
        analysable=10,
        insufficient=2,
        fpi=0,
        no_primary_sec_cik=0,
        total_updated=4,
        null_anomalies=0,
    )

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"

    tracker = MagicMock()
    tracker.row_count = None

    eligible_rows = [(301, "0000000301")]

    phase1_conn = MagicMock()
    phase1_conn.execute.return_value.fetchall.return_value = eligible_rows
    phase2_conn = MagicMock()

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as provider_cls,
        patch.object(
            scheduler,
            "review_coverage",
            side_effect=RuntimeError("review crashed"),
        ) as review_mock,
        patch.object(scheduler, "daily_cik_refresh") as cik_mock,
        patch.object(scheduler, "daily_financial_facts") as facts_mock,
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ),
        patch(
            "app.services.coverage.backfill_filings",
            side_effect=lambda conn, provider, cik, iid: _stub_backfill_result(iid),
        ) as backfill_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        _stub_two_connect_ctxes(psycopg_mod, phase1_conn, phase2_conn)
        provider_cls.return_value.__enter__.return_value = MagicMock()

        # Raises at the end so health surfaces record the job as failed.
        # tracker.row_count is still set to the phase-2 contribution so
        # the audit trail shows what work DID happen.
        with pytest.raises(RuntimeError, match="phase 3"):
            scheduler.fundamentals_sync()

    review_mock.assert_called_once_with(phase2_conn)
    cik_mock.assert_called_once()
    facts_mock.assert_called_once()
    assert backfill_mock.call_count == 1
    # Only phase-2 contribution: audit total_updated (4) + 1 × COMPLETE_OK (1).
    # review_rows stays 0 because the review raised.
    assert tracker.row_count == 5


def test_fundamentals_sync_phase0_cik_failure_isolated() -> None:
    """Phase 0 (CIK refresh) raising must not prevent phases 1/2/3 from
    running, but the job must still surface as failed at the end so health
    surfaces see the outage (Codex/#351-review feedback). Downstream phases
    operate on the previously-persisted CIK map."""
    summary = AuditSummary(
        analysable=0,
        insufficient=0,
        fpi=0,
        no_primary_sec_cik=0,
        total_updated=0,
        null_anomalies=0,
    )
    review = _stub_review_result()

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"

    tracker = MagicMock()

    # Phase 0 + phase 1 don't open psycopg connections; the first connect()
    # is phase 2 (audit), the second is phase 3 (review). Name the locals
    # after their consumer to avoid confusing readers with "phase1_conn =
    # audit connection".
    audit_conn = MagicMock()
    audit_conn.execute.return_value.fetchall.return_value = []
    review_conn = MagicMock()

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as provider_cls,
        patch.object(scheduler, "review_coverage", return_value=review) as review_mock,
        patch.object(scheduler, "daily_cik_refresh", side_effect=RuntimeError("cik pull failed")) as cik_mock,
        patch.object(scheduler, "daily_financial_facts") as facts_mock,
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ) as audit_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        _stub_two_connect_ctxes(psycopg_mod, audit_conn, review_conn)
        provider_cls.return_value.__enter__.return_value = MagicMock()

        # Raises at the end — phase 0 failure is isolated but surfaced.
        with pytest.raises(RuntimeError, match="phase 0"):
            scheduler.fundamentals_sync()

    cik_mock.assert_called_once()
    facts_mock.assert_called_once()
    audit_mock.assert_called_once_with(audit_conn)
    review_mock.assert_called_once_with(review_conn)


def test_fundamentals_sync_phase1_xbrl_failure_isolated() -> None:
    """Phase 1 (XBRL + normalization) raising must not prevent phase 2/3
    from running; the job surfaces as failed at the end."""
    summary = AuditSummary(
        analysable=0,
        insufficient=0,
        fpi=0,
        no_primary_sec_cik=0,
        total_updated=0,
        null_anomalies=0,
    )
    review = _stub_review_result()

    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"

    tracker = MagicMock()

    # Phase 0 + phase 1 don't open psycopg connections; the first connect()
    # is phase 2 (audit), the second is phase 3 (review).
    audit_conn = MagicMock()
    audit_conn.execute.return_value.fetchall.return_value = []
    review_conn = MagicMock()

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as provider_cls,
        patch.object(scheduler, "review_coverage", return_value=review) as review_mock,
        patch.object(scheduler, "daily_cik_refresh") as cik_mock,
        patch.object(scheduler, "daily_financial_facts", side_effect=RuntimeError("xbrl outage")) as facts_mock,
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ) as audit_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        _stub_two_connect_ctxes(psycopg_mod, audit_conn, review_conn)
        provider_cls.return_value.__enter__.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="phase 1"):
            scheduler.fundamentals_sync()

    cik_mock.assert_called_once()
    facts_mock.assert_called_once()
    audit_mock.assert_called_once_with(audit_conn)
    review_mock.assert_called_once_with(review_conn)
