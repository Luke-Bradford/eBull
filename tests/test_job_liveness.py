"""Tests for app.services.job_liveness (#1500 / GAP-D).

Structure:
  - TestCadencePeriod   — pure intrinsic-period derivation per cadence kind
  - TestFindStalledJobs — decision logic over mocked aggregate rows
  - TestFetchActiveRuns — age + ordering over mocked rows
  - TestLivenessIntegration — real job_runs SQL (FILTER / bool_or / DISTINCT ON)

The mocked-cursor tests pin the *decision* logic (lifetime / recent /
active-running combinations); the integration tests pin the SQL itself
against a per-worker DB. Entity-scoped assertions (unique job_name per
case) per the shared-DB rule — never global cardinality.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services.job_liveness import (
    cadence_period,
    fetch_active_runs,
    find_stalled_jobs,
    window_start_for,
)
from app.workers.scheduler import Cadence
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

_NOW = datetime(2026, 4, 6, 9, 0, 0, tzinfo=UTC)


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = rows[0] if rows else None
    cur.fetchall.return_value = rows
    return cur


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    conn = MagicMock()
    cursor_iter = iter(cursors)
    conn.cursor.side_effect = lambda **kwargs: next(cursor_iter)
    return conn


def _agg(
    *,
    recent: int,
    lifetime: int,
    last_fire_at: datetime | None,
    has_active_running: bool,
) -> dict[str, Any]:
    return {
        "recent": recent,
        "lifetime": lifetime,
        "last_fire_at": last_fire_at,
        "has_active_running": has_active_running,
    }


# ---------------------------------------------------------------------------
# TestCadencePeriod
# ---------------------------------------------------------------------------


class TestCadencePeriod:
    def test_every_n_minutes(self) -> None:
        assert cadence_period(Cadence.every_n_minutes(interval=5)) == timedelta(minutes=5)

    def test_hourly(self) -> None:
        assert cadence_period(Cadence.hourly(minute=30)) == timedelta(hours=1)

    def test_daily(self) -> None:
        assert cadence_period(Cadence.daily(hour=3, minute=15)) == timedelta(days=1)

    def test_weekly(self) -> None:
        assert cadence_period(Cadence.weekly(weekday=0, hour=6, minute=0)) == timedelta(days=7)

    def test_monthly_is_about_a_month(self) -> None:
        period = cadence_period(Cadence.monthly(day=1, hour=0))
        # Anchored at 2025-01-01 → Feb-01 next, so 31 days for this anchor.
        assert timedelta(days=28) <= period <= timedelta(days=31)

    def test_yearly_is_about_a_year(self) -> None:
        period = cadence_period(Cadence.yearly(month=4, day=1, hour=5))
        assert timedelta(days=365) <= period <= timedelta(days=366)


class TestWindowStartFor:
    """window_start_for must land on the K-th most recent REAL fire slot —
    not a representative period*K span (Codex ckpt-2)."""

    def test_daily_third_last_slot(self) -> None:
        # Daily 03:00 fires; now 04-06 09:00 → slots 04-06, 04-05, 04-04.
        # K=3 → window starts at 04-04 03:00.
        start = window_start_for(Cadence.daily(hour=3), _NOW, 3)
        assert start == datetime(2026, 4, 4, 3, 0, tzinfo=UTC)

    def test_monthly_uses_real_variable_slots_not_flat_period(self) -> None:
        # Monthly day=1 00:00; now 04-06 → slots 04-01, 03-01, 02-01.
        # K=3 → 02-01. A flat period*3 (representative 31d*3=93d) would
        # have started ~01-03 instead, wrongly widening the window.
        start = window_start_for(Cadence.monthly(day=1, hour=0), _NOW, 3)
        assert start == datetime(2026, 2, 1, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# TestFindStalledJobs (mocked aggregate rows)
# ---------------------------------------------------------------------------


class TestFindStalledJobs:
    _CADENCE = Cadence.daily(hour=3)  # period = 1 day, K=3 → window 3 days

    def test_flags_job_with_history_and_no_recent_fire(self) -> None:
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        _agg(
                            recent=0,
                            lifetime=12,
                            last_fire_at=_NOW - timedelta(days=10),
                            has_active_running=False,
                        )
                    ]
                )
            ]
        )
        result = find_stalled_jobs(conn, [("j", self._CADENCE)], _NOW)
        assert [s.job_name for s in result] == ["j"]
        # Window = span of the 3 most recent daily slots before _NOW
        # (03:00 fires, _NOW=09:00) → between 2 and 3 days, NOT a flat
        # period*3 (Codex ckpt-2: real-slot boundary, not representative).
        assert timedelta(days=2).total_seconds() <= result[0].window_seconds <= timedelta(days=3).total_seconds()
        assert result[0].last_fire_at == _NOW - timedelta(days=10)

    def test_not_stalled_when_recent_fire_exists(self) -> None:
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        _agg(
                            recent=2,
                            lifetime=14,
                            last_fire_at=_NOW - timedelta(hours=1),
                            has_active_running=False,
                        )
                    ]
                )
            ]
        )
        assert find_stalled_jobs(conn, [("j", self._CADENCE)], _NOW) == []

    def test_not_stalled_when_never_run(self) -> None:
        # lifetime == 0 → newly registered / never fired → not evaluated.
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        _agg(
                            recent=0,
                            lifetime=0,
                            last_fire_at=None,
                            has_active_running=False,
                        )
                    ]
                )
            ]
        )
        assert find_stalled_jobs(conn, [("j", self._CADENCE)], _NOW) == []

    def test_not_stalled_when_a_run_is_currently_active(self) -> None:
        # No fire in window, but a live running row → "stuck", not "silent".
        conn = _make_conn(
            [
                _make_cursor(
                    [
                        _agg(
                            recent=0,
                            lifetime=5,
                            last_fire_at=_NOW - timedelta(days=20),
                            has_active_running=True,
                        )
                    ]
                )
            ]
        )
        assert find_stalled_jobs(conn, [("j", self._CADENCE)], _NOW) == []

    def test_requires_timezone_aware_now(self) -> None:
        conn = _make_conn([])
        with pytest.raises(ValueError, match="timezone-aware"):
            find_stalled_jobs(conn, [], datetime(2026, 4, 6, 9, 0, 0))


# ---------------------------------------------------------------------------
# TestFetchActiveRuns (mocked rows)
# ---------------------------------------------------------------------------


class TestFetchActiveRuns:
    def test_computes_age_and_orders_oldest_first(self) -> None:
        rows = [
            {"job_name": "young", "started_at": _NOW - timedelta(minutes=5)},
            {"job_name": "old", "started_at": _NOW - timedelta(hours=3)},
        ]
        conn = _make_conn([_make_cursor(rows)])
        result = fetch_active_runs(conn, _NOW)
        assert [r.job_name for r in result] == ["old", "young"]
        assert result[0].age_seconds == timedelta(hours=3).total_seconds()

    def test_naive_started_at_treated_as_utc(self) -> None:
        naive = (_NOW - timedelta(minutes=10)).replace(tzinfo=None)
        conn = _make_conn([_make_cursor([{"job_name": "j", "started_at": naive}])])
        result = fetch_active_runs(conn, _NOW)
        assert result[0].age_seconds == pytest.approx(600.0, abs=1.0)


# ---------------------------------------------------------------------------
# TestLivenessIntegration — real SQL against a per-worker DB
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestLivenessIntegration:
    @staticmethod
    def _insert(
        conn: psycopg.Connection[tuple],
        job_name: str,
        *,
        started_at: datetime,
        status: str,
        finished: bool,
    ) -> None:
        conn.execute(
            """
            INSERT INTO job_runs (job_name, started_at, finished_at, status)
            VALUES (%(name)s, %(start)s, %(fin)s, %(status)s)
            """,
            {
                "name": job_name,
                "start": started_at,
                "fin": started_at + timedelta(minutes=1) if finished else None,
                "status": status,
            },
        )

    def test_old_only_fire_is_stalled_recent_is_not(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        now = datetime.now(UTC)
        cadence = Cadence.daily(hour=3)  # window = 3 days
        # Stalled: only a 100-day-old success.
        self._insert(
            ebull_test_conn,
            "liveness_stalled",
            started_at=now - timedelta(days=100),
            status="success",
            finished=True,
        )
        # Healthy: a fire 1 hour ago (a 'skipped' gate row still counts).
        self._insert(
            ebull_test_conn,
            "liveness_recent",
            started_at=now - timedelta(hours=1),
            status="skipped",
            finished=True,
        )
        ebull_test_conn.commit()

        stalled = find_stalled_jobs(
            ebull_test_conn,
            [("liveness_stalled", cadence), ("liveness_recent", cadence)],
            now,
        )
        names = {s.job_name for s in stalled}
        assert "liveness_stalled" in names
        assert "liveness_recent" not in names

    def test_active_running_row_excludes_from_stalled_and_surfaces(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        now = datetime.now(UTC)
        cadence = Cadence.daily(hour=3)
        # An old, still-running row: outside the window, but live.
        self._insert(
            ebull_test_conn,
            "liveness_active",
            started_at=now - timedelta(days=20),
            status="running",
            finished=False,
        )
        ebull_test_conn.commit()

        stalled = find_stalled_jobs(ebull_test_conn, [("liveness_active", cadence)], now)
        assert "liveness_active" not in {s.job_name for s in stalled}

        active = fetch_active_runs(ebull_test_conn, now)
        match = [a for a in active if a.job_name == "liveness_active"]
        assert len(match) == 1
        assert match[0].age_seconds == pytest.approx(timedelta(days=20).total_seconds(), rel=0.01)

    def test_never_run_job_not_flagged(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        now = datetime.now(UTC)
        cadence = Cadence.daily(hour=3)
        # No rows seeded for this name → lifetime 0 → not evaluated.
        stalled = find_stalled_jobs(ebull_test_conn, [("liveness_never_run_unique", cadence)], now)
        assert "liveness_never_run_unique" not in {s.job_name for s in stalled}

    def test_monthly_cadence_uses_real_window_not_flat_period(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Codex ckpt-2: a monthly job whose only fire is ~100 days ago must
        # be flagged — its 3 most recent monthly slots (~last 90d) are all
        # silent. A row 10 days ago (inside the most recent slot) must not.
        now = datetime.now(UTC)
        cadence = Cadence.monthly(day=1, hour=0)
        self._insert(
            ebull_test_conn,
            "liveness_monthly_stalled",
            started_at=now - timedelta(days=100),
            status="success",
            finished=True,
        )
        self._insert(
            ebull_test_conn,
            "liveness_monthly_recent",
            started_at=now - timedelta(days=10),
            status="success",
            finished=True,
        )
        ebull_test_conn.commit()

        stalled = find_stalled_jobs(
            ebull_test_conn,
            [
                ("liveness_monthly_stalled", cadence),
                ("liveness_monthly_recent", cadence),
            ],
            now,
        )
        names = {s.job_name for s in stalled}
        assert "liveness_monthly_stalled" in names
        assert "liveness_monthly_recent" not in names

    def test_future_dated_row_does_not_suppress_stall(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        # Codex ckpt-2: a future-dated row must not count as a recent fire.
        now = datetime.now(UTC)
        cadence = Cadence.daily(hour=3)
        self._insert(
            ebull_test_conn,
            "liveness_future",
            started_at=now - timedelta(days=100),  # real old fire (lifetime)
            status="success",
            finished=True,
        )
        self._insert(
            ebull_test_conn,
            "liveness_future",
            started_at=now + timedelta(days=1),  # bogus future row
            status="success",
            finished=True,
        )
        ebull_test_conn.commit()

        stalled = find_stalled_jobs(ebull_test_conn, [("liveness_future", cadence)], now)
        assert "liveness_future" in {s.job_name for s in stalled}
