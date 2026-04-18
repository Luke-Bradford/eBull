"""Tests for sync orchestrator freshness predicates.

Focus: the critical invariants from spec §1.3 and the plan's
regression-prone edges. Not every predicate gets an exhaustive matrix —
the shared `_fresh_by_audit` helper is tested once per behavior branch
and each layer's content check is tested where one exists.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from app.services.sync_orchestrator.freshness import (
    _format_age,
    _fresh_by_audit,
    candles_is_fresh,
    cik_mapping_is_fresh,
    cost_models_is_fresh,
    financial_facts_is_fresh,
    financial_normalization_is_fresh,
    fundamentals_is_fresh,
    fx_rates_is_fresh,
    monthly_reports_is_fresh,
    news_is_fresh,
    portfolio_sync_is_fresh,
    recommendations_is_fresh,
    scoring_is_fresh,
    thesis_is_fresh,
    universe_is_fresh,
    weekly_reports_is_fresh,
)


def _mock_conn_with_row(row: tuple | None) -> MagicMock:
    """MagicMock conn that returns `row` from conn.execute(...).fetchone()."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = row
    conn.execute.return_value = cur
    return conn


def _mock_conn_with_rows(rows: list[tuple | None]) -> MagicMock:
    """MagicMock conn that returns each tuple in `rows` from successive
    conn.execute(...).fetchone() calls."""
    conn = MagicMock()
    cursors = []
    for row in rows:
        cur = MagicMock()
        cur.fetchone.return_value = row
        cursors.append(cur)
    conn.execute.side_effect = cursors
    return conn


class TestFreshByAudit:
    """Shared helper — critical invariants tested once."""

    def test_fresh_when_latest_is_success_within_window(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row((now - timedelta(hours=2), "success", None, timedelta(hours=2).total_seconds()))
        fresh, detail = _fresh_by_audit(conn, "some_job", timedelta(hours=24))
        assert fresh is True
        assert "some_job" in detail

    def test_fresh_when_latest_is_prereq_skip(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row(
            (now - timedelta(hours=2), "skipped", "prereq_missing: no key", timedelta(hours=2).total_seconds())
        )
        fresh, _ = _fresh_by_audit(conn, "some_job", timedelta(hours=24))
        assert fresh is True

    def test_stale_when_latest_is_unmarked_skip(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row(
            (now - timedelta(hours=2), "skipped", "legacy skip reason", timedelta(hours=2).total_seconds())
        )
        fresh, detail = _fresh_by_audit(conn, "some_job", timedelta(hours=24))
        assert fresh is False
        assert "skipped" in detail

    def test_newer_failure_invalidates_older_success(self) -> None:
        """Regression: must query latest row first. A newer failure at
        t-2h beats an older success at t-12h."""
        now = datetime.now(UTC)
        conn = _mock_conn_with_row((now - timedelta(hours=2), "failure", "boom", timedelta(hours=2).total_seconds()))
        fresh, detail = _fresh_by_audit(conn, "some_job", timedelta(hours=24))
        assert fresh is False
        assert "failure" in detail

    def test_stale_when_latest_success_is_outside_window(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row((now - timedelta(hours=48), "success", None, timedelta(hours=48).total_seconds()))
        fresh, detail = _fresh_by_audit(conn, "some_job", timedelta(hours=24))
        assert fresh is False
        assert "window" in detail

    def test_stale_when_no_runs_ever(self) -> None:
        conn = _mock_conn_with_row(None)
        fresh, detail = _fresh_by_audit(conn, "some_job", timedelta(hours=24))
        assert fresh is False
        assert "no job_runs row" in detail


class TestFormatAge:
    def test_minutes(self) -> None:
        assert _format_age(timedelta(minutes=30)) == "30m"

    def test_hours(self) -> None:
        assert _format_age(timedelta(hours=3, minutes=15)) == "3h 15m"

    def test_days(self) -> None:
        assert _format_age(timedelta(days=2, hours=5)) == "2d 5h"


class TestSimpleAuditOnlyPredicates:
    """Layers with no content check — cik_mapping, financial_facts,
    financial_normalization, news, portfolio_sync, fx_rates, cost_models,
    weekly_reports, monthly_reports, universe."""

    @pytest.mark.parametrize(
        "predicate,job_name,window",
        [
            (universe_is_fresh, "nightly_universe_sync", timedelta(days=7)),
            (cik_mapping_is_fresh, "daily_cik_refresh", timedelta(hours=24)),
            (financial_facts_is_fresh, "daily_financial_facts", timedelta(hours=24)),
            (
                financial_normalization_is_fresh,
                "daily_financial_facts",
                timedelta(hours=24),
            ),
            (news_is_fresh, "daily_news_refresh", timedelta(hours=4)),
            (portfolio_sync_is_fresh, "daily_portfolio_sync", timedelta(minutes=5)),
            (fx_rates_is_fresh, "fx_rates_refresh", timedelta(minutes=5)),
            (cost_models_is_fresh, "seed_cost_models", timedelta(hours=24)),
            (weekly_reports_is_fresh, "weekly_report", timedelta(days=7)),
            (monthly_reports_is_fresh, "monthly_report", timedelta(days=31)),
        ],
    )
    def test_fresh_when_recent_success(self, predicate, job_name, window) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row((now - window / 2, "success", None, (window / 2).total_seconds()))
        fresh, _ = predicate(conn)
        assert fresh is True


class TestCandlesIsFresh:
    def test_stale_when_audit_stale(self) -> None:
        conn = _mock_conn_with_row(None)  # no job_runs row
        fresh, _ = candles_is_fresh(conn)
        assert fresh is False

    def test_stale_when_audit_fresh_but_instruments_missing_candles(self, monkeypatch) -> None:
        now = datetime.now(UTC)
        # First query = _fresh_by_audit; second = content check returning 3 missing.
        conn = _mock_conn_with_rows(
            [(now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()), (3,)]
        )
        monkeypatch.setattr(
            "app.services.market_data._most_recent_trading_day",
            lambda _: date(2026, 4, 16),
        )
        fresh, detail = candles_is_fresh(conn)
        assert fresh is False
        assert "3 T1/T2 instruments missing" in detail

    def test_fresh_when_audit_and_content_ok(self, monkeypatch) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_rows(
            [(now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()), (0,)]
        )
        monkeypatch.setattr(
            "app.services.market_data._most_recent_trading_day",
            lambda _: date(2026, 4, 16),
        )
        fresh, _ = candles_is_fresh(conn)
        assert fresh is True


class TestFundamentalsIsFresh:
    def test_stale_when_audit_fresh_but_instrument_missing_quarter_snapshot(
        self,
    ) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_rows(
            [(now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()), (5,)]
        )
        fresh, detail = fundamentals_is_fresh(conn)
        assert fresh is False
        assert "5 tradable instruments lack fundamentals snapshot" in detail

    def test_fresh_when_all_have_quarter_snapshot(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_rows(
            [(now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()), (0,)]
        )
        fresh, _ = fundamentals_is_fresh(conn)
        assert fresh is True


class TestThesisIsFresh:
    def test_stale_when_find_stale_instruments_returns_some(self, monkeypatch) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row((now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()))
        monkeypatch.setattr(
            "app.services.thesis.find_stale_instruments",
            lambda _conn, tier: [1, 2, 3],
        )
        fresh, detail = thesis_is_fresh(conn)
        assert fresh is False
        assert "3 Tier 1 instruments" in detail

    def test_fresh_when_no_stale_instruments(self, monkeypatch) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row((now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()))
        monkeypatch.setattr(
            "app.services.thesis.find_stale_instruments",
            lambda _conn, tier: [],
        )
        fresh, _ = thesis_is_fresh(conn)
        assert fresh is True

    def test_fresh_via_cascade_when_daily_thesis_refresh_stale(self, monkeypatch) -> None:
        """K.4: if daily_thesis_refresh is stale/absent, a recent
        successful cascade_refresh ingestion run is sufficient
        audit evidence. Thesis is refreshed by either path."""
        now = datetime.now(UTC)
        # First fetchone: job_runs row is stale (48h old success).
        stale_audit = (
            now - timedelta(hours=48),
            "success",
            None,
            timedelta(hours=48).total_seconds(),
        )
        # Second fetchone: cascade_refresh run finished 2h ago, success.
        cascade_row = (now - timedelta(hours=2), "success")
        conn = _mock_conn_with_rows([stale_audit, cascade_row])
        monkeypatch.setattr(
            "app.services.thesis.find_stale_instruments",
            lambda _conn, tier: [],
        )
        fresh, detail = thesis_is_fresh(conn)
        assert fresh is True
        assert "cascade_refresh" in detail

    def test_stale_when_both_audit_and_cascade_stale(self, monkeypatch) -> None:
        """Both signals stale → thesis layer is stale."""
        now = datetime.now(UTC)
        stale_audit = (
            now - timedelta(hours=48),
            "success",
            None,
            timedelta(hours=48).total_seconds(),
        )
        # cascade_refresh last success 30h ago (outside 24h window).
        cascade_row = (now - timedelta(hours=30), "success")
        conn = _mock_conn_with_rows([stale_audit, cascade_row])
        fresh, detail = thesis_is_fresh(conn)
        assert fresh is False
        assert "cascade_refresh last success" in detail

    def test_stale_when_daily_stale_and_no_cascade_row(self, monkeypatch) -> None:
        """daily_thesis_refresh stale AND no cascade row at all → stale."""
        now = datetime.now(UTC)
        stale_audit = (
            now - timedelta(hours=48),
            "success",
            None,
            timedelta(hours=48).total_seconds(),
        )
        conn = _mock_conn_with_rows([stale_audit, None])
        fresh, _ = thesis_is_fresh(conn)
        assert fresh is False

    def test_stale_when_cascade_last_run_failed(self, monkeypatch) -> None:
        """daily stale AND cascade last run within-window but status=failed → stale."""
        now = datetime.now(UTC)
        stale_audit = (
            now - timedelta(hours=48),
            "success",
            None,
            timedelta(hours=48).total_seconds(),
        )
        cascade_row = (now - timedelta(hours=1), "failed")
        conn = _mock_conn_with_rows([stale_audit, cascade_row])
        fresh, _ = thesis_is_fresh(conn)
        assert fresh is False


class TestScoringIsFresh:
    def test_stale_when_latest_score_older_than_thesis(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_rows(
            [
                (now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()),
                (now - timedelta(hours=5), now - timedelta(hours=1), None),
            ]
        )
        fresh, detail = scoring_is_fresh(conn)
        assert fresh is False
        assert "thesis" in detail

    def test_stale_when_latest_score_older_than_candle(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_rows(
            [
                (now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()),
                (now - timedelta(hours=5), None, now - timedelta(hours=1)),
            ]
        )
        fresh, detail = scoring_is_fresh(conn)
        assert fresh is False
        assert "candle" in detail

    def test_fresh_when_score_newest(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_rows(
            [
                (now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()),
                (
                    now - timedelta(minutes=30),
                    now - timedelta(hours=3),
                    now - timedelta(hours=5),
                ),
            ]
        )
        fresh, _ = scoring_is_fresh(conn)
        assert fresh is True

    def test_stale_when_no_scores(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_rows(
            [(now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()), (None, None, None)]
        )
        fresh, detail = scoring_is_fresh(conn)
        assert fresh is False
        assert "no scores" in detail


class TestRecommendationsIsFresh:
    def test_fresh_by_newer_recommendation_vs_score(self) -> None:
        now = datetime.now(UTC)
        # First query returns (latest_rec, latest_score)
        conn = _mock_conn_with_row((now - timedelta(hours=1), now - timedelta(hours=3)))
        fresh, detail = recommendations_is_fresh(conn)
        assert fresh is True
        assert "newer" in detail

    def test_fresh_by_recent_morning_review(self) -> None:
        now = datetime.now(UTC)
        # First query: (None, None) -> no content-based freshness.
        # Second query: audit watermark returns recent success.
        conn = _mock_conn_with_rows(
            [
                (None, None),
                (now - timedelta(hours=2), "success", None, timedelta(hours=2).total_seconds()),
            ]
        )
        fresh, _ = recommendations_is_fresh(conn)
        assert fresh is True

    def test_stale_when_scores_newer_than_recs_and_morning_review_old(
        self,
    ) -> None:
        now = datetime.now(UTC)
        # Content check: scores newer than recs (rec older than score).
        # Fall back to audit watermark: older than 24h => stale.
        conn = _mock_conn_with_rows(
            [
                (now - timedelta(hours=5), now - timedelta(hours=1)),
                (now - timedelta(hours=48), "success", None, timedelta(hours=48).total_seconds()),
            ]
        )
        fresh, _ = recommendations_is_fresh(conn)
        assert fresh is False
