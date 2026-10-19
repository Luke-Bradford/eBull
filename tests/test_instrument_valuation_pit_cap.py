"""#3850: ``instrument_valuation``'s count-based columns read the #3841 PIT row at the session (sql/505).

Frozen synthetic cases (spec "Acceptance" item 5). Every instrument is on the legacy arm (``fundamentals_snapshot``
seeds it; ``financial_periods_ttm`` is a view): shares 1e10 there, which the view no longer multiplies, against a PIT
count of 1e9, so a published cap proves which count was used. The full-population wiring, including the complete-TTM
arm's formulas, is ``scripts/measure_3850_acceptance.py`` section 1.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import psycopg.rows
import pytest

from tests.fixtures.pit_market_cap import seed_pit_step

SESSION = date(2026, 10, 16)
EARLIER = date(2026, 10, 9)
LATER = date(2026, 10, 19)
ROLLBACK = Path(__file__).resolve().parents[1] / "sql" / "rollback" / "505_instrument_valuation_pit_market_cap.sql"
ITEM4 = ("market_cap_live", "enterprise_value", "price_sales", "p_fcf_ratio", "fcf_yield", "ev_revenue", "ev_ebitda")


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[Any]) -> psycopg.Connection[Any]:
    conn = ebull_test_conn
    names = {
        101: "Factor One",
        102: "Factor Two",
        103: "Factor Half",
        104: "Refused",
        105: "Earlier Step",
        106: "Revised",
        107: "Ratio Co",
        108: "Dual Class",
        109: "Quoted Later",
        110: "Bar After Frontier",
        111: "Bar After Frontier Revised",
        112: "No Steps",
        113: "No Close Before Frontier",
    }
    for iid, name in names.items():
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (iid, f"S{iid}", name),
        )
        conn.execute(
            "INSERT INTO fundamentals_snapshot "
            "(instrument_id, as_of_date, revenue_ttm, gross_margin, operating_margin, "
            " fcf, cash, debt, net_debt, shares_outstanding, book_value, eps) "
            "VALUES (%s, '2025-01-01', 1e11, 0.5, 0.3, 5e10, 1e10, 2e10, 1e10, 1e10, 30, 6)",
            (iid,),
        )
    for iid, factor in ((101, 1.0), (102, 2.0), (103, 0.5), (107, 1.0), (108, 1.0), (109, 1.0)):
        seed_pit_step(conn, iid, session=SESSION, close=150, shares=1e9, split_factor=factor)
    seed_pit_step(conn, 104, session=SESSION, close=150, shares=1e9, refusal="no_left_anchor")
    # The build saw the latest bar, but its only step covers an earlier session.
    seed_pit_step(conn, 105, session=EARLIER, close=150, shares=1e9, bars_through=SESSION)
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close, volume) VALUES (105, %s, 150, 1)", (SESSION,)
    )
    # A revision row the build never counted: the step goes basis_stale.
    seed_pit_step(conn, 106, session=SESSION, close=150, shares=1e9)
    conn.execute(
        "INSERT INTO price_daily_revision (instrument_id, price_date, revised_at, cause) "
        "VALUES (106, %s, now(), 'incremental')",
        (SESSION,),
    )
    conn.execute("INSERT INTO ads_ratio (instrument_id, ratio, source_form) VALUES (107, 10, 'F-6')")
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (108, 'sec', 'cik', '0009993850', TRUE)"
    )
    conn.execute(
        "INSERT INTO instrument_class_shares_outstanding "
        "(instrument_id, period_end, shares, class_member, source_cik, source_adsh, "
        " source_form_type, source_fsds_qtr, source_filed_at, resolution_method, parser_version, ingested_at) "
        "VALUES (108, '2025-01-01', 1e9, 'CommonClassA', '0009993850', '0000000000-00-000000', "
        "'10-K', '2025Q1', now(), 'curated', 1, now())"
    )
    # Amendment 1: a bar the build never saw (after its frontier) is not the session; the step still covers it.
    for iid in (110, 111):
        seed_pit_step(conn, iid, session=SESSION, close=150, shares=1e9, valid_to=LATER + timedelta(days=1))
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, close, volume) VALUES (%s, %s, 160, 1)", (iid, LATER)
        )
    # A revision row moves PIT's instrument-wide count: the earlier session is refused too.
    conn.execute(
        "INSERT INTO price_daily_revision (instrument_id, price_date, revised_at, cause) "
        "VALUES (111, %s, now(), 'incremental')",
        (SESSION,),
    )
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close, volume) VALUES (112, %s, 150, 1)", (SESSION,)
    )
    # No positive close on or before the frontier: the latest close is the session, and PIT refuses it.
    seed_pit_step(conn, 113, session=SESSION, close=150, shares=1e9, bars_through=EARLIER)
    # A quote newer than the session at a different price: the cap stays on the session close.
    conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_flag) "
        "VALUES (109, now(), 159, 161, 160, FALSE)"
    )
    conn.commit()
    return conn


def _val(conn: psycopg.Connection[Any], iid: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute("SELECT * FROM instrument_valuation WHERE instrument_id = %s", (iid,)).fetchone()
    assert row is not None
    return row


@pytest.mark.db
@pytest.mark.parametrize(("iid", "cap"), [(101, Decimal("1.5e11")), (102, Decimal("3e11")), (103, Decimal("7.5e10"))])
def test_published_at_the_measured_factor(_seed: psycopg.Connection[Any], iid: int, cap: Decimal) -> None:
    row = _val(_seed, iid)
    assert row["market_cap_reason"] is None
    assert row["session"] == SESSION
    assert row["valuation_price"] == 150
    assert row["market_cap_live"] == cap
    assert float(row["fcf_yield"]) == pytest.approx(float(Decimal("5e10") / cap))
    assert float(row["p_fcf_ratio"]) == pytest.approx(float(cap / Decimal("5e10")))
    # Legacy arm: EV and the revenue multiples stay structurally NULL; per-share P/B is unchanged.
    assert row["enterprise_value"] is None and row["price_sales"] is None
    assert row["pb_ratio"] == 5
    assert row["market_cap_pit"]["split_factor"] == float(iid == 102 and 2 or iid == 103 and 0.5 or 1)
    assert row["market_cap_pit"]["price_date"] == SESSION.isoformat()


@pytest.mark.db
@pytest.mark.parametrize(
    ("iid", "reason"),
    [
        (104, "no_left_anchor"),
        (105, "no_pit_row"),
        (106, "basis_stale"),
        (107, "ads_ratio"),
        (108, "dual_class"),
        (111, "basis_stale"),
        (112, "no_pit_row"),
        (113, "basis_stale"),
    ],
)
def test_refused_with_reason(_seed: psycopg.Connection[Any], iid: int, reason: str) -> None:
    row = _val(_seed, iid)
    assert row["market_cap_reason"] == reason
    assert row["session"] == SESSION
    for col in ITEM4:
        assert row[col] is None, col
    # #3903: P/E is count-based (and the legacy arm has no earnings total), so it is NULL here.
    assert row["pe_ratio"] is None
    assert row["current_price"] == (160 if iid == 111 else 150)


@pytest.mark.db
def test_bar_after_the_frontier_is_not_the_session(_seed: psycopg.Connection[Any]) -> None:
    # Spec Amendment 1: the build saw bars through SESSION; the LATER bar would read basis_stale, so the cap is
    # published at the frontier's close and `session` says so.
    row = _val(_seed, 110)
    assert row["current_price"] == 160
    assert row["session"] == SESSION
    assert row["valuation_price"] == 150
    assert row["market_cap_reason"] is None
    assert row["market_cap_live"] == Decimal("1.5e11")


@pytest.mark.db
def test_quote_after_the_session_does_not_price_the_cap(_seed: psycopg.Connection[Any]) -> None:
    row = _val(_seed, 109)
    assert row["current_price"] == 160
    assert row["valuation_price"] == 150
    assert row["market_cap_live"] == Decimal("1.5e11")
    # Legacy P/B stays per-share on the selected price (#3903 left it there).
    assert float(row["pb_ratio"]) == pytest.approx(160 / 30)


@pytest.mark.db
def test_cap_is_null_iff_a_reason_is_set(_seed: psycopg.Connection[Any]) -> None:
    rows = _seed.execute(
        "SELECT instrument_id, market_cap_live IS NULL, market_cap_reason IS NOT NULL FROM instrument_valuation"
    ).fetchall()
    assert len(rows) == 13
    assert all(null == reason for _, null, reason in rows)


@pytest.mark.db
def test_rollback_restores_sql_241(_seed: psycopg.Connection[Any]) -> None:
    conn = _seed
    body = ROLLBACK.read_text()
    statements = re.sub(r"^(BEGIN|COMMIT);$", "", body, flags=re.M)
    conn.execute(statements)  # type: ignore[arg-type]  # a reviewed file, not user input
    columns = [
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'instrument_valuation' "
            "ORDER BY ordinal_position"
        ).fetchall()
    ]
    assert columns[-3:] == ["dividend_yield", "ebitda_ttm", "fcf_ttm"]
    assert "session" not in columns and "market_cap_reason" not in columns
    # sql/241's cap: the selected price × the fundamentals_snapshot count.
    assert conn.execute("SELECT market_cap_live FROM instrument_valuation WHERE instrument_id = 101").fetchone() == (
        Decimal("1.5e12"),
    )
    conn.rollback()
    assert _val(conn, 101)["market_cap_live"] == Decimal("1.5e11")
