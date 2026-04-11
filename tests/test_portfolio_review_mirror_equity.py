"""§8.6 Test 2 — AUM delta test for the run_portfolio_review path.

The mirror_aum_fixture's `scores` row is load-bearing: without
it, run_portfolio_review returns early at portfolio.py:791
before touching the AUM block, and this test is silently a
no-op (prevention discipline, spec §8.0 component 5).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from app.services.portfolio import _load_mirror_equity, run_portfolio_review
from tests.fixtures.copy_mirrors import _NOW, mirror_aum_fixture
from tests.test_operator_setup_race import (
    _assert_test_db,
    _test_database_url,
    _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping real-DB review mirror equity test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            cur.execute(
                "TRUNCATE copy_mirror_positions, copy_mirrors, "
                "copy_traders, quotes, scores, positions, "
                "cash_ledger, coverage, theses, trade_recommendations, "
                "instruments RESTART IDENTITY CASCADE"
            )
        c.commit()
        yield c
        c.rollback()


def _seed_review_preconditions(conn: psycopg.Connection[Any]) -> None:
    """Add the minimum rows beyond `mirror_aum_fixture` that
    `run_portfolio_review` reads from during its evaluation:
    coverage (Tier 1 for the ranked instrument) so the instrument
    survives to the AUM block.

    Schema reference — sql/001_init.sql `coverage` table columns:
    (instrument_id, coverage_tier, last_reviewed_at, review_frequency,
    analyst_status, notes). `coverage_tier` NOT `tier`; no `updated_at`.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO coverage (instrument_id, coverage_tier,
                                  review_frequency, last_reviewed_at)
            VALUES (990001, 1, 'weekly', %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET coverage_tier     = EXCLUDED.coverage_tier,
                  review_frequency  = EXCLUDED.review_frequency,
                  last_reviewed_at  = EXCLUDED.last_reviewed_at
            """,
            {"now": _NOW},
        )


def test_run_portfolio_review_total_aum_includes_mirror_equity(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.6 Test 2: result.total_aum carries the mirror contribution
    on top of positions + cash.
    """
    mirror_aum_fixture(conn)
    _seed_review_preconditions(conn)
    conn.commit()

    expected_mirror = _load_mirror_equity(conn)
    assert expected_mirror == pytest.approx(1550.0, abs=1e-6)

    result = run_portfolio_review(conn)
    # Base fixture has empty positions + cash, so expected total_aum
    # is exactly the mirror contribution.
    assert result.total_aum == pytest.approx(expected_mirror, abs=1e-6)


def test_run_portfolio_review_soft_close_baseline(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.6 Test 2 baseline: flip all mirrors to active=FALSE →
    result.total_aum returns to positions + cash (0.0 with the
    fixture's empty-positions invariant).
    """
    mirror_aum_fixture(conn)
    _seed_review_preconditions(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    conn.commit()

    result = run_portfolio_review(conn)
    assert result.total_aum == 0.0
