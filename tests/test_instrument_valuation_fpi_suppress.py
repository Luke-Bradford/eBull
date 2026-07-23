"""DB integration test for the #1939 FPI ADR/ADS suppression (sql/237).

For a ``coverage.filings_status = 'fpi'`` instrument the view NULLs every
price-bearing column — a WIDER list than the #1664 dual-class suppression
(pe_ratio and dividend_yield are per-share-basis consistent across share
classes but not across the ADS ratio). Price-free columns and the per-ADS
price itself survive. ``resolve_market_cap_basis`` fails closed FIRST with
``fpi_adr_unavailable``. Seeds via the legacy CTE (``fundamentals_snapshot``
is a base table; ``financial_periods_ttm`` is a view).
"""

from __future__ import annotations

import psycopg
import psycopg.rows
import pytest

from app.services.xbrl_derived_stats import resolve_market_cap_basis


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[tuple]) -> psycopg.Connection[tuple]:
    conn = ebull_test_conn
    # 21 = FPI ADR (coverage fpi, no name marker); 22 = domestic control
    # (analysable, clean name); 23 = ONC-class: DOMESTIC-form ADR filer —
    # coverage analysable but name carries the ADR marker.
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES "
        "(21,'FADR','Foreign Issuer Co',TRUE),(22,'DOM','Domestic Co',TRUE),"
        "(23,'DADR','Domestic Filer Ltd-ADR',TRUE)"
    )
    conn.execute(
        "INSERT INTO external_identifiers "
        "(instrument_id, provider, identifier_type, identifier_value, is_primary) VALUES "
        "(21,'sec','cik','0009991939',TRUE),(22,'sec','cik','0009991940',TRUE),"
        "(23,'sec','cik','0009991941',TRUE)"
    )
    conn.execute(
        "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) VALUES "
        "(21, 3, 'fpi'), (22, 3, 'analysable'), (23, 3, 'analysable')"
    )
    for iid, last in ((21, 100), (22, 100), (23, 100)):
        conn.execute(
            "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_flag) "
            "VALUES (%s, now(), %s, %s, %s, FALSE)",
            (iid, last - 1, last + 1, last),
        )
    for iid in (21, 22, 23):
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
        "SELECT current_price, market_cap_live, pe_ratio, pb_ratio, p_fcf_ratio, "
        "fcf_yield, debt_equity_ratio, gross_margin "
        "FROM instrument_valuation WHERE instrument_id = %s",
        (iid,),
    )
    row = cur.fetchone()
    assert row is not None
    return row


@pytest.mark.db
def test_fpi_price_bearing_columns_suppressed(_seed: psycopg.Connection[tuple]) -> None:
    row = _val(_seed, 21)
    # Every ordinary-shares/per-share × per-ADS-price product is NULL —
    # including pe_ratio, which dual-class suppression keeps.
    assert row["market_cap_live"] is None
    assert row["pe_ratio"] is None
    assert row["pb_ratio"] is None
    assert row["p_fcf_ratio"] is None
    assert row["fcf_yield"] is None
    # Price-free figures and the (real) per-ADS price survive.
    assert row["current_price"] == 100
    assert row["debt_equity_ratio"] is not None
    assert row["gross_margin"] is not None


@pytest.mark.db
def test_domestic_control_unchanged(_seed: psycopg.Connection[tuple]) -> None:
    row = _val(_seed, 22)
    assert row["market_cap_live"] is not None
    assert row["pe_ratio"] is not None
    assert row["fcf_yield"] is not None


@pytest.mark.db
def test_name_marker_catches_domestic_form_adr(_seed: psycopg.Connection[tuple]) -> None:
    # ONC class: files domestic forms (coverage 'analysable') but the eToro
    # name carries the ADR marker — union catches it.
    row = _val(_seed, 23)
    assert row["market_cap_live"] is None
    assert row["pe_ratio"] is None
    assert row["current_price"] == 100


@pytest.mark.db
def test_resolver_fails_closed_fpi_first(_seed: psycopg.Connection[tuple]) -> None:
    assert resolve_market_cap_basis(_seed, instrument_id=21).basis == "fpi_adr_unavailable"
    assert resolve_market_cap_basis(_seed, instrument_id=22).basis == "not_multiclass"
    assert resolve_market_cap_basis(_seed, instrument_id=23).basis == "fpi_adr_unavailable"
