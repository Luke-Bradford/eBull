"""Pure-logic tests for the #1564 db_size_growth_7d decision.

No DB: ``compute_db_size_growth_7d`` is a pure function over the live size, the
baseline sample, and ``today``. The SQL probe (``_q_db_size_growth_7d_baseline``)
is exercised by the DB-tier sampler test + the dev-verify step; the staleness
floor / cold-start / sign behaviour is pinned here.
"""

from __future__ import annotations

from datetime import date, timedelta

from app.services.postgres_health import (
    DB_SIZE_GROWTH_BASELINE_MAX_AGE_DAYS,
    compute_db_size_growth_7d,
)

_TODAY = date(2026, 6, 28)
_GB = 1024 * 1024 * 1024


def test_normal_delta() -> None:
    # Baseline 7 days ago at 45 GB, now 47 GB → +2 GB, baseline date echoed.
    growth, baseline_date = compute_db_size_growth_7d(47 * _GB, (45 * _GB, date(2026, 6, 21)), today=_TODAY)
    assert growth == 2 * _GB
    assert baseline_date == date(2026, 6, 21)


def test_cold_start_no_baseline() -> None:
    # <7d of history → no baseline row → both None.
    assert compute_db_size_growth_7d(47 * _GB, None, today=_TODAY) == (None, None)


def test_current_probe_failed() -> None:
    # db_size probe returned None (caught by _safe) → growth uncomputable.
    assert compute_db_size_growth_7d(None, (45 * _GB, date(2026, 6, 21)), today=_TODAY) == (
        None,
        None,
    )


def test_negative_delta_is_legitimate() -> None:
    # A retention sweep / VACUUM FULL shrank the DB — negative growth is real,
    # not clamped.
    growth, baseline_date = compute_db_size_growth_7d(44 * _GB, (47 * _GB, date(2026, 6, 21)), today=_TODAY)
    assert growth == -3 * _GB
    assert baseline_date == date(2026, 6, 21)


def test_stale_baseline_floored_to_none() -> None:
    # Sampler gap: newest baseline ≤7d-ago is 11 days old (> 10d floor) → don't
    # mislabel an 11-day delta as 7-day growth.
    stale = _TODAY.replace(day=17)  # 2026-06-17 = 11 days before 06-28
    assert compute_db_size_growth_7d(47 * _GB, (45 * _GB, stale), today=_TODAY) == (None, None)


def test_baseline_at_exact_floor_still_computes() -> None:
    # Boundary: a baseline exactly DB_SIZE_GROWTH_BASELINE_MAX_AGE_DAYS old is
    # still in-window (inclusive).
    boundary = _TODAY - timedelta(days=DB_SIZE_GROWTH_BASELINE_MAX_AGE_DAYS)
    growth, baseline_date = compute_db_size_growth_7d(47 * _GB, (45 * _GB, boundary), today=_TODAY)
    assert growth == 2 * _GB
    assert baseline_date == boundary
