"""DB integration test for #3942: ``_fetch_valuation_ratios`` executes against the real view.

The summary's key_stats ratios are ``instrument_valuation`` columns. A column or alias typo
would be swallowed into ``{}`` by the handler's ``psycopg.Error`` fallback and read as
"no key stats", so this runs the query on a seeded legacy-arm row (``fundamentals_snapshot``
is a base table; ``financial_periods_ttm`` is a view).
"""

from __future__ import annotations

from decimal import Decimal

import psycopg
import pytest

from app.api.instruments import _fetch_valuation_ratios


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[tuple]) -> psycopg.Connection[tuple]:
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (31,'KST','Key Stats Co',TRUE)"
    )
    conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_flag) "
        "VALUES (31, now(), 99, 101, 100, FALSE)"
    )
    conn.execute(
        "INSERT INTO fundamentals_snapshot "
        "(instrument_id, as_of_date, revenue_ttm, gross_margin, operating_margin, "
        " fcf, cash, debt, net_debt, shares_outstanding, book_value, eps) "
        "VALUES (31, '2025-01-01', 1e11, 0.5, 0.3, 5e10, 1e10, 2e10, 1e10, 1e10, 30, 6)"
    )
    return conn


def test_reads_the_view_columns(_seed: psycopg.Connection[tuple]) -> None:
    got = _fetch_valuation_ratios(_seed, 31)
    assert set(got) == {"pe_ratio", "pb_ratio", "roe", "roa", "debt_to_equity"}
    row = _seed.execute(
        "SELECT pe_ratio, pb_ratio, roe, roa, debt_equity_ratio FROM instrument_valuation WHERE instrument_id = 31"
    ).fetchone()
    assert row is not None
    assert (got["pe_ratio"], got["pb_ratio"], got["roe"], got["roa"], got["debt_to_equity"]) == row
    # Legacy arm: debt / (book_value × shares) = 2e10 / 3e11. No complete TTM, so no earnings ratios.
    assert got["debt_to_equity"] is not None
    assert abs(got["debt_to_equity"] - Decimal(2) / Decimal(30)) < Decimal("1e-12")
    assert got["pe_ratio"] is None and got["roe"] is None and got["roa"] is None


def test_no_view_row_is_empty(_seed: psycopg.Connection[tuple]) -> None:
    assert _fetch_valuation_ratios(_seed, 999) == {}
