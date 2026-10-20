"""DB integration test for the #1939 FPI ADR/ADS suppression (sql/237).

For a ``coverage.filings_status = 'fpi'`` instrument the view NULLs every
price-bearing column — a WIDER list than the #1664 dual-class suppression
(pe_ratio, dividend_yield and pb_ratio are count-based since sql/506, sql/507
and sql/509, #3903, and NULL with the cap's fpi_adr reason). Price-free columns and the per-ADS
price itself survive. ``resolve_market_cap_basis`` fails closed FIRST with
``fpi_adr_unavailable``. Seeds via the legacy CTE (``fundamentals_snapshot``
is a base table; ``financial_periods_ttm`` is a view).
"""

from __future__ import annotations

from datetime import date
from typing import Any

import psycopg
import psycopg.rows
import pytest

from app.services.xbrl_derived_stats import resolve_market_cap_basis
from tests.fixtures.pit_market_cap import seed_pit_step


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[tuple]) -> psycopg.Connection[tuple]:
    conn = ebull_test_conn
    # 21 = FPI ADR (coverage fpi, no name marker); 22 = domestic control
    # (analysable, clean name); 23 = ONC-class: DOMESTIC-form ADR filer —
    # coverage analysable but name carries the ADR marker.
    # #2117: 24 = ratio-known ADR (fpi + ads_ratio row, ratio=10) → corrected,
    # not suppressed; 25 = multiclass issuer that ALSO has an ads_ratio row —
    # curated multiclass must DOMINATE (price NOT divided; Codex ckpt-1 #1).
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES "
        "(21,'FADR','Foreign Issuer Co',TRUE),(22,'DOM','Domestic Co',TRUE),"
        "(23,'DADR','Domestic Filer Ltd-ADR',TRUE),(24,'RADR','Ratio ADR Co',TRUE),"
        "(25,'MCLS','Multiclass Issuer Co',TRUE)"
    )
    conn.execute(
        "INSERT INTO external_identifiers "
        "(instrument_id, provider, identifier_type, identifier_value, is_primary) VALUES "
        "(21,'sec','cik','0009991939',TRUE),(22,'sec','cik','0009991940',TRUE),"
        "(23,'sec','cik','0009991941',TRUE),(24,'sec','cik','0009991942',TRUE),"
        "(25,'sec','cik','0009991943',TRUE)"
    )
    conn.execute(
        "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) VALUES "
        "(21, 3, 'fpi'), (22, 3, 'analysable'), (23, 3, 'analysable'), "
        "(24, 3, 'fpi'), (25, 3, 'analysable')"
    )
    for iid, last in ((21, 100), (22, 100), (23, 100), (24, 100), (25, 100)):
        conn.execute(
            "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_flag) "
            "VALUES (%s, now(), %s, %s, %s, FALSE)",
            (iid, last - 1, last + 1, last),
        )
    for iid in (21, 22, 23, 24, 25):
        conn.execute(
            "INSERT INTO fundamentals_snapshot "
            "(instrument_id, as_of_date, revenue_ttm, gross_margin, operating_margin, "
            " fcf, cash, debt, net_debt, shares_outstanding, book_value, eps) "
            "VALUES (%s, '2025-01-01', 1e11, 0.5, 0.3, 5e10, 1e10, 2e10, 1e10, 1e10, 30, 6)",
            (iid,),
        )
    # #2117 curated ADS ratios: 24 → 10:1 (corrected), 25 → 5:1 (must be
    # overridden by multiclass dominance).
    conn.execute("INSERT INTO ads_ratio (instrument_id, ratio, source_form) VALUES (24, 10, 'F-6'), (25, 5, 'F-6')")
    # 25 is a curated multiclass issuer (one FSDS row under its CIK is enough
    # for resolve's EXISTS check and the view's dual_class CTE).
    conn.execute(
        "INSERT INTO instrument_class_shares_outstanding "
        "(instrument_id, period_end, shares, class_member, source_cik, source_adsh, "
        " source_form_type, source_fsds_qtr, source_filed_at, resolution_method, parser_version, ingested_at) "
        "VALUES (25, '2025-01-01', 1e10, 'CommonClassA', '0009991943', '0000000000-00-000000', "
        "'10-K', '2025Q1', now(), 'curated', 1, now())"
    )
    # #3850: caps come from the #3841 PIT row at the session. 22 is the domestic control; 24 has a published PIT row
    # that the view still refuses (ads_ratio).
    for iid in (22, 24):
        seed_pit_step(conn, iid, session=date(2026, 10, 16), close=100, shares=1e10)
    conn.commit()
    return conn


def _val(conn: psycopg.Connection[tuple], iid: int) -> dict[str, Any]:
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
    assert row["fcf_yield"] is not None
    # #3903: the legacy arm has no earnings total, so it publishes no P/E for any instrument.
    assert row["pe_ratio"] is None


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


@pytest.mark.db
def test_ratio_known_adr_refuses_cap_keeps_per_share(_seed: psycopg.Connection[tuple]) -> None:
    # #2117 divided the per-ADS price by the curated 10:1 ratio. #3850: the cap is refused (`ads_ratio`) because no
    # ratio history exists, so an ADS re-basing cannot be told from a split; the raw per-ADS price still displays.
    # #3903: P/E is count-based now, so it is refused with the cap. The resolver reads the view's NULL back.
    row = _val(_seed, 24)
    assert row["current_price"] == 100
    assert row["market_cap_live"] is None
    assert row["fcf_yield"] is None
    assert row["pe_ratio"] is None
    res = resolve_market_cap_basis(_seed, instrument_id=24)
    assert res.basis == "fpi_adr_ratio"
    assert res.value is None


@pytest.mark.db
def test_multiclass_dominates_ads_ratio(_seed: psycopg.Connection[tuple]) -> None:
    # #2117 (Codex ckpt-1 #1): iid 25 is a curated multiclass issuer that ALSO
    # has a 5:1 ads_ratio row. Multiclass MUST dominate — the price is NOT
    # divided. resolve returns a multiclass basis, never fpi_adr_ratio.
    res = resolve_market_cap_basis(_seed, instrument_id=25)
    assert res.basis in ("total_company", "multiclass_unavailable")
    assert res.basis != "fpi_adr_ratio"
    # market_cap is dual-class suppressed (#1664), proving the ratio path did not touch it. #3903 made P/E
    # count-based, so it is refused with the cap; no per-share column of this row is published to show the
    # undivided price any more, and the resolver assertion above carries the dominance check.
    row = _val(_seed, 25)
    assert row["pe_ratio"] is None
    assert row["market_cap_live"] is None
