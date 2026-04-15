"""§8.6 Test 2 — AUM delta test for the run_portfolio_review path.

The mirror_aum_fixture's `scores` row is load-bearing: without
it, run_portfolio_review returns early at portfolio.py:801
before touching the AUM block, and this test is silently a
no-op (prevention discipline, spec §8.0 component 5).

Defence-in-depth: the non-early-return test captures
``caplog`` and asserts the **full-path** log line
(``"run_portfolio_review: positions=... ranked=..."`` emitted
at portfolio.py:828) is present and the **early-return** log
line (``"no ranked candidates and no open positions"`` emitted
at portfolio.py:802) is absent. This pins the code path
directly — a future fixture regression that accidentally
elided the scores row would fail the log assertion before
the arithmetic assertion, so the test cannot become
vacuously equivalent to the early-return test below.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from app.services.portfolio import _load_mirror_equity, run_portfolio_review
from tests.fixtures.copy_mirrors import mirror_aum_fixture
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


def test_run_portfolio_review_total_aum_includes_mirror_equity(
    conn: psycopg.Connection[Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """§8.6 Test 2: result.total_aum carries the mirror contribution
    on top of positions + cash.

    Asserts (via caplog) that the **full AUM-block path** runs —
    not the early-return path at portfolio.py:801. Without this
    log assertion the test could become vacuously equivalent to
    ``test_run_portfolio_review_early_return_reports_mirror_equity``
    if a future change elided the fixture's scores row.
    """
    mirror_aum_fixture(conn)
    conn.commit()

    expected_mirror = _load_mirror_equity(conn)
    assert expected_mirror == pytest.approx(1550.0, abs=1e-6)

    with caplog.at_level(logging.INFO, logger="app.services.portfolio"):
        result = run_portfolio_review(conn)

    # Base fixture has empty positions + cash, so expected total_aum
    # is exactly the mirror contribution.
    assert result.total_aum == pytest.approx(expected_mirror, abs=1e-6)

    # Prove the full AUM-block path ran — pin the distinct log line
    # emitted at portfolio.py:828 ("positions=... ranked=...") and
    # confirm the early-return log at portfolio.py:802 was NOT
    # emitted. The two log messages are disjoint by construction,
    # so this pins the execution path directly.
    full_path_log = next(
        (r for r in caplog.records if "ranked=1 model=v1.1-balanced" in r.getMessage()),
        None,
    )
    assert full_path_log is not None, (
        "run_portfolio_review took an unexpected code path — the full "
        "AUM-block log line was not emitted. The test may have "
        "regressed to the early-return path, making the total_aum "
        "assertion above vacuously equivalent to "
        "test_run_portfolio_review_early_return_reports_mirror_equity."
    )
    early_return_log = next(
        (r for r in caplog.records if "no ranked candidates and no open positions" in r.getMessage()),
        None,
    )
    assert early_return_log is None, (
        "run_portfolio_review took the early-return path — this test "
        "is meant to exercise the FULL AUM block, not the early-return. "
        "Check that mirror_aum_fixture is still seeding the scores row."
    )


def test_run_portfolio_review_soft_close_baseline(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.6 Test 2 baseline: flip all mirrors to active=FALSE →
    result.total_aum returns to positions + cash (0.0 with the
    fixture's empty-positions invariant).
    """
    mirror_aum_fixture(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    conn.commit()

    assert _load_mirror_equity(conn) == pytest.approx(0.0, abs=1e-6)

    result = run_portfolio_review(conn)
    assert result.total_aum == 0.0


def test_run_portfolio_review_early_return_reports_mirror_equity(
    conn: psycopg.Connection[Any],
) -> None:
    """§6.3 contract: if there are active mirrors but no scores and no
    positions, run_portfolio_review hits the early-return path and
    still reports ``total_aum = mirror_equity + cash``. Loading
    mirror equity is hoisted above the ``if not all_ids`` guard for
    exactly this reason — a review run that cannot do any trading
    work must still report an honest AUM figure to callers.
    """
    mirror_aum_fixture(conn)
    # Delete the scores row the fixture seeds so all_ids is empty
    # and run_portfolio_review takes the early-return branch.
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scores")
    conn.commit()

    expected_mirror = _load_mirror_equity(conn)
    assert expected_mirror == pytest.approx(1550.0, abs=1e-6)

    result = run_portfolio_review(conn)
    # No recommendations generated (empty all_ids), but total_aum
    # still includes mirror equity. Fixture has no cash, so the
    # expected value is mirror_equity alone.
    assert result.recommendations == []
    assert result.active_positions == 0
    assert result.total_aum == pytest.approx(expected_mirror, abs=1e-6)
