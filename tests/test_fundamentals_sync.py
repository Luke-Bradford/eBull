"""Tests for fundamentals_sync scheduler job.

Successor to the retired weekly_coverage_audit + weekly_coverage_review
jobs: phase 1 runs audit_all_instruments + per-eligible-instrument
backfill_filings, phase 2 runs review_coverage. See
docs/superpowers/specs/2026-04-19-research-tool-refocus.md §1.1.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import psycopg
import pytest

from app.services.coverage import AuditSummary, BackfillOutcome, BackfillResult, ReviewResult
from app.workers import scheduler


def _stub_two_connect_ctxes(psycopg_mod: MagicMock, phase1_conn: MagicMock, phase2_conn: MagicMock) -> None:
    """Wire psycopg.connect to return the ingest-gate connection plus two
    distinct phase connections in sequence (gate → phase 2 audit → phase
    3 review).

    fundamentals_sync opens ``psycopg.connect(...)`` once up-front for
    the layer_enabled[fundamentals_ingest] gate (#414) and then once per
    audit/review phase. A shared MagicMock would alias all three calls
    to the same connection, hiding any test that a phase uses the wrong
    one.
    """
    gate_cm = MagicMock()
    gate_cm.__enter__.return_value = MagicMock()
    gate_cm.__exit__.return_value = None
    cm1 = MagicMock()
    cm1.__enter__.return_value = phase1_conn
    cm1.__exit__.return_value = None
    cm2 = MagicMock()
    cm2.__enter__.return_value = phase2_conn
    cm2.__exit__.return_value = None
    psycopg_mod.connect.side_effect = [gate_cm, cm1, cm2]


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
    # Phase 1b (SEC snapshot dedupe under #414) is default-off. Must set
    # explicitly because MagicMock attribute access otherwise returns a
    # truthy MagicMock and fires phase 1b with an unstubbed connection.
    stub_settings.enable_sec_fundamentals_dedupe = False

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
    # Phase 1b (SEC snapshot dedupe under #414) is default-off. Must set
    # explicitly because MagicMock attribute access otherwise returns a
    # truthy MagicMock and fires phase 1b with an unstubbed connection.
    stub_settings.enable_sec_fundamentals_dedupe = False

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
    # Phase 1b (SEC snapshot dedupe under #414) is default-off. Must set
    # explicitly because MagicMock attribute access otherwise returns a
    # truthy MagicMock and fires phase 1b with an unstubbed connection.
    stub_settings.enable_sec_fundamentals_dedupe = False

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
        # Gate connect + phase-2 connect: audit raises before the
        # eligible-rows query and before phase 3.
        gate_cm = MagicMock()
        gate_cm.__enter__.return_value = MagicMock()
        gate_cm.__exit__.return_value = None
        cm1 = MagicMock()
        cm1.__enter__.return_value = phase1_conn
        cm1.__exit__.return_value = None
        psycopg_mod.connect.side_effect = [gate_cm, cm1]

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
    # Gate connect + phase-2 audit connect = 2; phase-3 never opens.
    assert psycopg_mod.connect.call_count == 2


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
    # Phase 1b (SEC snapshot dedupe under #414) is default-off. Must set
    # explicitly because MagicMock attribute access otherwise returns a
    # truthy MagicMock and fires phase 1b with an unstubbed connection.
    stub_settings.enable_sec_fundamentals_dedupe = False

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
    # Phase 1b (SEC snapshot dedupe under #414) is default-off. Must set
    # explicitly because MagicMock attribute access otherwise returns a
    # truthy MagicMock and fires phase 1b with an unstubbed connection.
    stub_settings.enable_sec_fundamentals_dedupe = False

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
    # Phase 1b (SEC snapshot dedupe under #414) is default-off. Must set
    # explicitly because MagicMock attribute access otherwise returns a
    # truthy MagicMock and fires phase 1b with an unstubbed connection.
    stub_settings.enable_sec_fundamentals_dedupe = False

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


def test_fundamentals_sync_phase1b_runs_when_dedupe_enabled() -> None:
    """Phase 1b (SEC fundamentals snapshot refresh under #414) fires
    between phase 1 (financial facts) and phase 2 (audit) when the
    operator flips ``enable_sec_fundamentals_dedupe=True``.

    The happy path pulls CIK-mapped tradable instruments, opens a
    SecFundamentalsProvider, and calls
    ``refresh_fundamentals(sec_fund, conn, symbols)``. Phase 1b is
    isolated from phase 2/3 — a snapshot failure must not block the
    audit.
    """
    from app.services.fundamentals import FundamentalsRefreshSummary

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
    stub_settings.enable_sec_fundamentals_dedupe = True

    tracker = MagicMock()

    # Four connect() contexts: phase 1b cik query, phase 1b snapshot
    # refresh, phase 2 audit, phase 3 review.
    cik_conn = MagicMock()
    cik_conn.execute.return_value.fetchall.return_value = [
        ("AAPL", "1", "0000320193"),
        ("MSFT", "2", "0000789019"),
    ]
    snapshot_conn = MagicMock()
    audit_conn = MagicMock()
    audit_conn.execute.return_value.fetchall.return_value = []
    review_conn = MagicMock()

    def _ctx(c: MagicMock) -> MagicMock:
        cm = MagicMock()
        cm.__enter__.return_value = c
        cm.__exit__.return_value = None
        return cm

    refresh_summary = FundamentalsRefreshSummary(symbols_attempted=2, snapshots_upserted=2, symbols_skipped=0)

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "SecFundamentalsProvider") as fund_cls,
        patch.object(scheduler, "refresh_fundamentals", return_value=refresh_summary) as refresh_mock,
        patch.object(scheduler, "review_coverage", return_value=review),
        patch.object(scheduler, "daily_cik_refresh"),
        patch.object(scheduler, "daily_financial_facts"),
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ),
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        psycopg_mod.connect.side_effect = [
            _ctx(MagicMock()),  # ingest gate
            _ctx(cik_conn),
            _ctx(snapshot_conn),
            _ctx(audit_conn),
            _ctx(review_conn),
        ]
        filings_cls.return_value.__enter__.return_value = MagicMock()
        fund_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.fundamentals_sync()

    refresh_mock.assert_called_once()
    args, _ = refresh_mock.call_args
    assert args[1] is snapshot_conn
    assert args[2] == [("AAPL", "1"), ("MSFT", "2")]


def test_fundamentals_sync_short_circuits_when_ingest_disabled() -> None:
    """Operator pause (#414 design goal F). When
    ``layer_enabled[fundamentals_ingest]=False`` the whole job short-
    circuits before ``_tracked_job`` opens — no CIK refresh, no XBRL
    fetch, no audit/review — and ``record_job_skip`` writes a
    ``status='skipped'`` ``job_runs`` row so the admin UI can
    distinguish the operator-initiated pause from a zero-row success.
    """
    stub_settings = MagicMock()
    stub_settings.database_url = "postgresql://test"
    stub_settings.sec_user_agent = "test-agent@example.com"
    stub_settings.enable_sec_fundamentals_dedupe = False

    gate_conn = MagicMock()

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "daily_cik_refresh") as cik_mock,
        patch.object(scheduler, "daily_financial_facts") as facts_mock,
        patch.object(scheduler, "review_coverage") as review_mock,
        patch.object(scheduler, "record_job_skip") as skip_mock,
        patch("app.services.layer_enabled.is_layer_enabled", return_value=False) as gate_mock,
    ):
        cm = MagicMock()
        cm.__enter__.return_value = gate_conn
        cm.__exit__.return_value = None
        psycopg_mod.connect.side_effect = [cm]

        scheduler.fundamentals_sync()

    # Gate queried once on the autocommit connection; tracked_job never
    # opened (pause is surfaced as 'skipped', not zero-row success).
    gate_mock.assert_called_once_with(gate_conn, "fundamentals_ingest")
    tracked_cm.assert_not_called()
    skip_mock.assert_called_once()
    skip_args = skip_mock.call_args
    assert skip_args.args[0] is gate_conn
    assert skip_args.args[1] == "fundamentals_sync"
    assert "paused by operator" in skip_args.args[2]
    cik_mock.assert_not_called()
    facts_mock.assert_not_called()
    review_mock.assert_not_called()
    # Connection opened with autocommit=True so record_job_skip's
    # explicit transaction block issues a real BEGIN/COMMIT.
    _, kwargs = psycopg_mod.connect.call_args
    assert kwargs.get("autocommit") is True


def test_fundamentals_sync_gate_read_failure_falls_open() -> None:
    """Fail-open posture on gate-read failures. If
    ``psycopg.connect`` or ``is_layer_enabled`` raises (DB unavailable,
    ``layer_enabled`` table missing on first boot) we MUST fall through
    to ``_tracked_job`` so the run still writes a job_runs row — either
    the body succeeds or a real failure lands. Silently vanishing
    would regress the runtime's prerequisite-check posture.
    """
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
    stub_settings.enable_sec_fundamentals_dedupe = False

    tracker = MagicMock()

    audit_conn = MagicMock()
    audit_conn.execute.return_value.fetchall.return_value = []
    review_conn = MagicMock()

    def _ctx(c: MagicMock) -> MagicMock:
        cm = MagicMock()
        cm.__enter__.return_value = c
        cm.__exit__.return_value = None
        return cm

    # First connect() (the gate) raises — every subsequent connect() returns
    # a usable context so the body runs normally.
    connect_calls: list[object] = []

    def _connect(*args: object, **kwargs: object) -> MagicMock:
        connect_calls.append((args, kwargs))
        if len(connect_calls) == 1:
            raise psycopg.OperationalError("gate db unreachable")
        return _ctx(audit_conn) if len(connect_calls) == 2 else _ctx(review_conn)

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "review_coverage", return_value=review) as review_mock,
        patch.object(scheduler, "record_job_skip") as skip_mock,
        patch.object(scheduler, "daily_cik_refresh") as cik_mock,
        patch.object(scheduler, "daily_financial_facts") as facts_mock,
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ) as audit_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        psycopg_mod.connect.side_effect = _connect
        filings_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.fundamentals_sync()

    # Gate raised -> body ran: record_job_skip NOT called, tracked_job
    # opened, audit + review both fired.
    skip_mock.assert_not_called()
    tracked_cm.assert_called_once()
    cik_mock.assert_called_once()
    facts_mock.assert_called_once()
    audit_mock.assert_called_once_with(audit_conn)
    review_mock.assert_called_once_with(review_conn)


def test_fundamentals_sync_phase1b_query_filters_primary_cik() -> None:
    """Phase 1b CIK query must restrict to ``ei.is_primary = TRUE`` so
    a symbol with a demoted historical SEC CIK row cannot appear twice
    and non-deterministically pick the wrong CIK via dict-overwrite.
    Matches the phase-2 audit query's filter, and is critical now that
    phase 1b is the sole SEC companyfacts writer when the dedupe flag
    is flipped on."""
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
    stub_settings.enable_sec_fundamentals_dedupe = True

    tracker = MagicMock()

    cik_cursor = MagicMock()
    cik_cursor.fetchall.return_value = []
    cik_conn = MagicMock()
    cik_conn.execute.return_value = cik_cursor
    audit_conn = MagicMock()
    audit_conn.execute.return_value.fetchall.return_value = []
    review_conn = MagicMock()

    def _ctx(c: MagicMock) -> MagicMock:
        cm = MagicMock()
        cm.__enter__.return_value = c
        cm.__exit__.return_value = None
        return cm

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "review_coverage", return_value=review),
        patch.object(scheduler, "daily_cik_refresh"),
        patch.object(scheduler, "daily_financial_facts"),
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ),
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        psycopg_mod.connect.side_effect = [
            _ctx(MagicMock()),  # ingest gate
            _ctx(cik_conn),
            _ctx(audit_conn),
            _ctx(review_conn),
        ]
        filings_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.fundamentals_sync()

    # Assert phase 1b SQL restricts to is_primary = TRUE.
    query = cik_conn.execute.call_args.args[0]
    normalised = " ".join(query.split()).lower()
    assert "ei.is_primary = true" in normalised
    assert "ei.identifier_type = 'cik'" in normalised


def test_fundamentals_sync_phase1b_row_count_contributes_to_tracker() -> None:
    """Row-count contract: once the dedupe flag flips, phase 1b
    snapshots are the bulk write path. ``tracker.row_count`` must
    include the snapshots_upserted count so a successful snapshot-only
    run (no audit/review changes) does not report as zero-row work.
    """
    from app.services.fundamentals import FundamentalsRefreshSummary

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
    stub_settings.enable_sec_fundamentals_dedupe = True

    tracker = MagicMock()
    tracker.row_count = None

    cik_conn = MagicMock()
    cik_conn.execute.return_value.fetchall.return_value = [
        ("AAPL", "1", "0000320193"),
    ]
    snapshot_conn = MagicMock()
    audit_conn = MagicMock()
    audit_conn.execute.return_value.fetchall.return_value = []
    review_conn = MagicMock()

    def _ctx(c: MagicMock) -> MagicMock:
        cm = MagicMock()
        cm.__enter__.return_value = c
        cm.__exit__.return_value = None
        return cm

    refresh_summary = FundamentalsRefreshSummary(symbols_attempted=1, snapshots_upserted=37, symbols_skipped=0)

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "SecFundamentalsProvider") as fund_cls,
        patch.object(scheduler, "refresh_fundamentals", return_value=refresh_summary),
        patch.object(scheduler, "review_coverage", return_value=review),
        patch.object(scheduler, "daily_cik_refresh"),
        patch.object(scheduler, "daily_financial_facts"),
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ),
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        psycopg_mod.connect.side_effect = [
            _ctx(MagicMock()),  # ingest gate
            _ctx(cik_conn),
            _ctx(snapshot_conn),
            _ctx(audit_conn),
            _ctx(review_conn),
        ]
        filings_cls.return_value.__enter__.return_value = MagicMock()
        fund_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.fundamentals_sync()

    # audit.total_updated (0) + review (0) + phase1b snapshots (37).
    assert tracker.row_count == 37


def test_fundamentals_sync_phase1b_failure_surfaces_at_end() -> None:
    """Phase 1b snapshot refresh is isolated from phase 2/3 — a
    transient failure must not block the audit or review, but must
    still surface as a job-level failure at the end so health dashboards
    see the outage. Mirrors the phase 0/1 isolation-with-surfacing
    contract."""
    from app.services.fundamentals import FundamentalsRefreshSummary  # noqa: F401

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
    stub_settings.enable_sec_fundamentals_dedupe = True

    tracker = MagicMock()

    cik_conn = MagicMock()
    cik_conn.execute.return_value.fetchall.return_value = [("AAPL", "1", "0000320193")]
    audit_conn = MagicMock()
    audit_conn.execute.return_value.fetchall.return_value = []
    review_conn = MagicMock()

    def _ctx(c: MagicMock) -> MagicMock:
        cm = MagicMock()
        cm.__enter__.return_value = c
        cm.__exit__.return_value = None
        return cm

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "SecFundamentalsProvider") as fund_cls,
        patch.object(
            scheduler,
            "refresh_fundamentals",
            side_effect=RuntimeError("sec companyfacts 502"),
        ),
        patch.object(scheduler, "review_coverage", return_value=review) as review_mock,
        patch.object(scheduler, "daily_cik_refresh"),
        patch.object(scheduler, "daily_financial_facts"),
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ) as audit_mock,
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        psycopg_mod.connect.side_effect = [
            _ctx(MagicMock()),  # ingest gate
            _ctx(cik_conn),
            _ctx(MagicMock()),  # phase 1b snapshot conn (refresh raises)
            _ctx(audit_conn),
            _ctx(review_conn),
        ]
        filings_cls.return_value.__enter__.return_value = MagicMock()
        fund_cls.return_value.__enter__.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="phase 1b"):
            scheduler.fundamentals_sync()

    # Phase 2/3 still ran despite the phase-1b failure.
    audit_mock.assert_called_once_with(audit_conn)
    review_mock.assert_called_once_with(review_conn)


def test_fundamentals_sync_phase1b_skipped_when_dedupe_disabled() -> None:
    """When ``enable_sec_fundamentals_dedupe=False`` (default), phase 1b
    must not run — ``daily_research_refresh`` still owns the SEC
    snapshot path until the operator flips the flag."""
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
    stub_settings.enable_sec_fundamentals_dedupe = False

    tracker = MagicMock()

    audit_conn = MagicMock()
    audit_conn.execute.return_value.fetchall.return_value = []
    review_conn = MagicMock()

    with (
        patch.object(scheduler, "settings", stub_settings),
        patch.object(scheduler, "_tracked_job") as tracked_cm,
        patch.object(scheduler, "psycopg") as psycopg_mod,
        patch.object(scheduler, "SecFilingsProvider") as filings_cls,
        patch.object(scheduler, "SecFundamentalsProvider") as fund_cls,
        patch.object(scheduler, "refresh_fundamentals") as refresh_mock,
        patch.object(scheduler, "review_coverage", return_value=review),
        patch.object(scheduler, "daily_cik_refresh"),
        patch.object(scheduler, "daily_financial_facts"),
        patch(
            "app.services.coverage.audit_all_instruments",
            return_value=summary,
        ),
    ):
        tracked_cm.return_value.__enter__.return_value = tracker
        _stub_two_connect_ctxes(psycopg_mod, audit_conn, review_conn)
        filings_cls.return_value.__enter__.return_value = MagicMock()

        scheduler.fundamentals_sync()

    refresh_mock.assert_not_called()
    fund_cls.assert_not_called()
