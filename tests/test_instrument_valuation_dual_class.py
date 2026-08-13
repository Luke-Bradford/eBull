"""DB integration test for the #1664 dual-class suppression in the
``instrument_valuation`` view (sql/201).

For a curated dual-class issuer (primary SEC CIK present in
``instrument_class_shares_outstanding``) the view NULLs the shares-distorted
columns; a single-class control keeps them, and the clean ``debt_equity_ratio``
survives for the dual-class rows. Seeds via the legacy CTE
(``fundamentals_snapshot`` is a base table; ``financial_periods_ttm`` is a view).
"""

from __future__ import annotations

import psycopg
import psycopg.rows
import pytest


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[tuple]) -> psycopg.Connection[tuple]:
    conn = ebull_test_conn
    # 1,2 = dual-class siblings sharing one SEC CIK; 3 = single-class control.
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES "
        "(1,'DCA','Dual A',TRUE),(2,'DCB','Dual B',TRUE),(3,'SC','Single Class',TRUE)"
    )
    conn.execute(
        "INSERT INTO external_identifiers "
        "(instrument_id, provider, identifier_type, identifier_value, is_primary) VALUES "
        "(1,'sec','cik','0001652044',TRUE),"
        "(2,'sec','cik','0001652044',TRUE),"
        "(3,'sec','cik','0000320193',TRUE)"
    )
    # Curated per-class rows for the two siblings → marks CIK 0001652044 dual-class.
    conn.execute(
        "INSERT INTO instrument_class_shares_outstanding "
        "(instrument_id, period_end, shares, class_member, source_cik, source_adsh, "
        " source_form_type, source_fsds_qtr, source_filed_at, resolution_method, parser_version) VALUES "
        "(1,'2024-12-31',5835000000,'CommonClassA','0001652044','0001652044-25-000014','10-K','2025q1','2025-02-01','curated','fsds_class_shares_v1'),"
        "(2,'2024-12-31',5515000000,'CapitalClassC','0001652044','0001652044-25-000014','10-K','2025q1','2025-02-01','curated','fsds_class_shares_v1')"
    )
    for iid, last in ((1, 200), (2, 190), (3, 150)):
        conn.execute(
            "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_flag) "
            "VALUES (%s, now(), %s, %s, %s, FALSE)",
            (iid, last - 1, last + 1, last),
        )
    # Legacy-path fundamentals (no complete TTM → instrument_valuation.legacy CTE).
    for iid in (1, 2, 3):
        conn.execute(
            "INSERT INTO fundamentals_snapshot "
            "(instrument_id, as_of_date, revenue_ttm, gross_margin, operating_margin, "
            " fcf, cash, debt, net_debt, shares_outstanding, book_value, eps) "
            "VALUES (%s, '2025-01-01', 1e11, 0.5, 0.3, 5e10, 1e10, 2e10, 1e10, 1e10, 30, 6)",
            (iid,),
        )
    conn.commit()
    return conn


def _val(conn: psycopg.Connection[tuple], iid: int) -> dict[str, object]:
    cur = conn.cursor(row_factory=psycopg.rows.dict_row)
    cur.execute(
        "SELECT market_cap_live, fcf_yield, pb_ratio, p_fcf_ratio, debt_equity_ratio "
        "FROM instrument_valuation WHERE instrument_id = %s",
        (iid,),
    )
    row = cur.fetchone()
    assert row is not None
    return row


@pytest.mark.db
def test_dual_class_columns_suppressed(_seed: psycopg.Connection[tuple]) -> None:
    for iid in (1, 2):
        row = _val(_seed, iid)
        assert row["market_cap_live"] is None
        assert row["fcf_yield"] is None
        assert row["pb_ratio"] is None
        assert row["p_fcf_ratio"] is None
        # Clean column (debt / total-equity, no shares×price term) is kept.
        assert row["debt_equity_ratio"] is not None


@pytest.mark.db
def test_single_class_control_unchanged(_seed: psycopg.Connection[tuple]) -> None:
    row = _val(_seed, 3)
    assert row["market_cap_live"] is not None
    assert row["fcf_yield"] is not None
    assert row["pb_ratio"] is not None
    assert row["debt_equity_ratio"] is not None
