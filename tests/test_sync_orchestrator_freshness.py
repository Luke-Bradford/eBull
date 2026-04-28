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
    cost_models_is_fresh,
    fundamentals_is_fresh,
    fx_rates_is_fresh,
    monthly_reports_is_fresh,
    portfolio_sync_is_fresh,
    recommendations_is_fresh,
    scoring_is_fresh,
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
    """Layers with no content check — portfolio_sync, fx_rates,
    cost_models, weekly_reports, universe.

    ``monthly_reports`` is calendar-anchored (#335) and uses a wider
    row shape, so it has its own test class below.
    """

    @pytest.mark.parametrize(
        "predicate,job_name,window",
        [
            (universe_is_fresh, "nightly_universe_sync", timedelta(days=7)),
            (portfolio_sync_is_fresh, "daily_portfolio_sync", timedelta(minutes=5)),
            (fx_rates_is_fresh, "fx_rates_refresh", timedelta(hours=24)),
            (cost_models_is_fresh, "seed_cost_models", timedelta(hours=24)),
            (weekly_reports_is_fresh, "weekly_report", timedelta(days=7)),
        ],
    )
    def test_fresh_when_recent_success(self, predicate, job_name, window) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row((now - window / 2, "success", None, (window / 2).total_seconds()))
        fresh, _ = predicate(conn)
        assert fresh is True


class TestMonthlyReportsCalendarAnchored:
    """#335 — ``monthly_reports_is_fresh`` is calendar-anchored, not a
    flat 31-day window. The freshness boundary is the first instant of
    the current calendar month in UTC; anything older than that is
    stale, regardless of literal age in days. The month boundary is
    computed in Python (not SQL) to dodge timezone-coercion hazards
    when the DB session ``TimeZone`` is not UTC."""

    def test_fresh_when_latest_run_is_inside_current_calendar_month(self) -> None:
        now = datetime.now(UTC)
        # Pin started_at to the most recent first-of-month so the
        # assertion is stable regardless of which day the test runs.
        started_at = datetime(now.year, now.month, 1, 6, 0, tzinfo=UTC)
        age_seconds = (now - started_at).total_seconds()
        conn = _mock_conn_with_row((started_at, "success", None, age_seconds))
        fresh, detail = monthly_reports_is_fresh(conn)
        assert fresh is True, detail
        assert "this calendar month" in detail

    def test_stale_when_latest_run_is_strictly_before_current_month_start(self) -> None:
        """Regression for the flat-31d behavior: a run from the prior
        calendar month must always be considered stale, even when the
        literal age is below 31 days."""
        now = datetime.now(UTC)
        # 1 second before the start of the current calendar month UTC.
        month_start = datetime(now.year, now.month, 1, tzinfo=UTC)
        started_at = month_start - timedelta(seconds=1)
        age_seconds = (now - started_at).total_seconds()
        conn = _mock_conn_with_row((started_at, "success", None, age_seconds))
        fresh, detail = monthly_reports_is_fresh(conn)
        assert fresh is False
        assert "before the start of the current calendar month" in detail

    def test_stale_when_latest_status_is_failure(self) -> None:
        now = datetime.now(UTC)
        started_at = now - timedelta(hours=6)
        age_seconds = (now - started_at).total_seconds()
        conn = _mock_conn_with_row((started_at, "failure", "boom", age_seconds))
        fresh, detail = monthly_reports_is_fresh(conn)
        assert fresh is False
        assert "status=failure" in detail

    def test_stale_when_no_run_recorded(self) -> None:
        conn = _mock_conn_with_row(None)
        fresh, detail = monthly_reports_is_fresh(conn)
        assert fresh is False
        assert "no job_runs row" in detail

    def test_anchor_uses_finished_at_when_run_straddles_month_boundary(self) -> None:
        """A run that straddles the month boundary must count as a run
        in the *finishing* month — the SELECT anchors on
        ``COALESCE(finished_at, started_at)`` so the legacy predicate
        and the state machine (which also uses COALESCE in
        ``_latest_age_seconds_map``) agree on the month a run belongs
        to. Prevents /sync/layers reporting STALE while
        /sync/layers/v2 reports HEALTHY at a month edge.

        Anchored to the real current month so the test stays valid
        regardless of the wall-clock date the suite runs at.
        """
        now = datetime.now(UTC)
        month_start = datetime(now.year, now.month, 1, tzinfo=UTC)
        # Anchor (finished_at) just AFTER current month start — what
        # the production code computes when a run that started in the
        # prior month finished one minute into this month. The
        # predicate must treat this as in-month.
        finished_anchor = month_start + timedelta(minutes=1)
        age_seconds = (now - finished_anchor).total_seconds()
        conn = _mock_conn_with_row((finished_anchor, "success", None, age_seconds))
        fresh, detail = monthly_reports_is_fresh(conn)
        assert fresh is True, detail
        assert "this calendar month" in detail


class TestFxRatesIsFreshWindow:
    """Pin the ``fx_rates_is_fresh`` window at 24h (#502 PR C). The
    parametrized fresh-only assertion in the suite above passes any
    test param matching the production constant, so a regression that
    moves both back to 5 minutes would not be caught — these explicit
    boundary tests fail loud if the cadence shifts."""

    def test_fresh_at_twelve_hours_ago(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row((now - timedelta(hours=12), "success", None, timedelta(hours=12).total_seconds()))
        fresh, _ = fx_rates_is_fresh(conn)
        assert fresh is True

    def test_stale_at_twenty_five_hours_ago(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_row((now - timedelta(hours=25), "success", None, timedelta(hours=25).total_seconds()))
        fresh, _ = fx_rates_is_fresh(conn)
        assert fresh is False


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


class TestScoringIsFresh:
    def test_stale_when_latest_score_older_than_candle(self) -> None:
        now = datetime.now(UTC)
        # Row shape after Phase 1.2: (latest_score, latest_candle) — thesis
        # column retired when thesis went on-demand.
        conn = _mock_conn_with_rows(
            [
                (now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()),
                (now - timedelta(hours=5), now - timedelta(hours=1)),
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
                    now - timedelta(hours=5),
                ),
            ]
        )
        fresh, _ = scoring_is_fresh(conn)
        assert fresh is True

    def test_stale_when_no_scores(self) -> None:
        now = datetime.now(UTC)
        conn = _mock_conn_with_rows(
            [(now - timedelta(hours=1), "success", None, timedelta(hours=1).total_seconds()), (None, None)]
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
