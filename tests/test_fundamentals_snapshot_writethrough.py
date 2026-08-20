"""DB integration test for the #2008 snapshot write-through + strict TTM view.

One test per genuinely-new SQL mechanism:

- ``_write_snapshots_from_periods``: strict 4-quarter window (per-column
  completeness + 330d adjacency, sporadic capex), balance-sheet anchor
  fields, and the DELETE-then-INSERT rewash purging legacy rows.
- ``financial_periods_ttm`` (sql/220): strict flow CASEs + adjacency in
  ``is_complete_ttm`` — partial sums must not leak (the CAT/AMZN class).

Spec: docs/specs/fundamentals/2026-07-12-2008-ttm-reconciliation.md.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import psycopg.rows
import pytest

from app.services.fundamentals import _write_snapshots_from_periods

# Four adjacent calendar quarters (span 273d) + one older quarter so the
# non-adjacent instrument (id=3) can pull a cross-year 4-row window.
_Q_ENDS = [date(2025, 6, 30), date(2025, 9, 30), date(2025, 12, 31), date(2026, 3, 31)]
_Q_TYPES = ["Q2", "Q3", "Q4", "Q1"]


def _insert_quarter(
    conn: psycopg.Connection[tuple],
    iid: int,
    period_end: date,
    period_type: str,
    *,
    revenue: float | None,
    capex: float | None = 10.0,
) -> None:
    conn.execute(
        """
        INSERT INTO financial_periods (
            instrument_id, period_end_date, period_type, fiscal_year,
            fiscal_quarter, revenue, gross_profit, operating_income,
            operating_cf, capex, eps_diluted, cash, long_term_debt,
            short_term_debt, shares_outstanding, shareholders_equity,
            filed_date, source, source_ref, reported_currency,
            normalization_status
        ) VALUES (
            %(iid)s, %(end)s, %(ptype)s, %(fy)s,
            %(fq)s, %(revenue)s, 40, 25,
            30, %(capex)s, 0.5, 100, 200,
            50, 1000, 4000,
            %(end)s, 'sec_edgar', 'test', 'USD',
            'normalized'
        )
        """,
        {
            "iid": iid,
            "end": period_end,
            "ptype": period_type,
            "fy": period_end.year,
            "fq": int(period_type[1]),
            "revenue": revenue,
            "capex": capex,
        },
    )


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[tuple]) -> psycopg.Connection[tuple]:
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES "
        "(1,'FULL','Complete Co',TRUE),(2,'HOLE','Null Revenue Q Co',TRUE),"
        "(3,'GAP','Missing Quarter Co',TRUE)"
    )
    # iid=1: 4 adjacent quarters, all columns present (capex NULL in one
    # quarter — sporadic, must not NULL the fcf).
    for i, (end, ptype) in enumerate(zip(_Q_ENDS, _Q_TYPES, strict=True)):
        _insert_quarter(conn, 1, end, ptype, revenue=100.0, capex=None if i == 1 else 10.0)
    # iid=2: 4 adjacent quarters but one NULL revenue → strict revenue_ttm NULL.
    for i, (end, ptype) in enumerate(zip(_Q_ENDS, _Q_TYPES, strict=True)):
        _insert_quarter(conn, 2, end, ptype, revenue=None if i == 2 else 100.0)
    # iid=3: only 3 recent quarters; 4th row is a year older → 4-row
    # window spans ~365d (non-adjacent, #1839 class).
    _insert_quarter(conn, 3, date(2025, 3, 31), "Q1", revenue=100.0)
    for end, ptype in list(zip(_Q_ENDS, _Q_TYPES, strict=True))[1:]:
        _insert_quarter(conn, 3, end, ptype, revenue=100.0)
    conn.commit()
    return conn


def _latest_snapshot(conn: psycopg.Connection[tuple], iid: int) -> dict[str, object]:
    cur = conn.cursor(row_factory=psycopg.rows.dict_row)
    cur.execute(
        "SELECT * FROM fundamentals_snapshot WHERE instrument_id = %s ORDER BY as_of_date DESC LIMIT 1",
        (iid,),
    )
    row = cur.fetchone()
    assert row is not None
    return row


@pytest.mark.db
def test_writethrough_strict_window_and_anchor_fields(_seed: psycopg.Connection[tuple]) -> None:
    conn = _seed
    for iid in (1, 2, 3):
        _write_snapshots_from_periods(conn, instrument_id=iid)

    # Complete instrument: TTM sums + margins + anchor balance-sheet fields.
    snap = _latest_snapshot(conn, 1)
    assert snap["as_of_date"] == date(2026, 3, 31)
    assert snap["revenue_ttm"] == Decimal("400.0000")
    assert snap["gross_margin"] == Decimal("0.4000")  # 160/400
    assert snap["operating_margin"] == Decimal("0.2500")  # 100/400
    # fcf = ocf_ttm(120) − |capex_ttm(30)|: capex is sporadic — the NULL
    # quarter contributes 0, it does NOT null the whole figure.
    assert snap["fcf"] == Decimal("90.0000")
    assert snap["eps"] == Decimal("2.0000")
    assert snap["cash"] == Decimal("100.0000")
    assert snap["debt"] == Decimal("250.0000")  # long 200 + short 50
    assert snap["net_debt"] == Decimal("150.0000")
    assert snap["book_value"] == Decimal("4.0000")  # 4000/1000

    # One NULL-revenue member: strict → revenue_ttm + margins NULL, but the
    # balance-sheet anchor row still exists (presence preserved).
    snap = _latest_snapshot(conn, 2)
    assert snap["revenue_ttm"] is None
    assert snap["gross_margin"] is None
    assert snap["cash"] == Decimal("100.0000")

    # Non-adjacent window (365d span): every TTM field NULL.
    snap = _latest_snapshot(conn, 3)
    assert snap["revenue_ttm"] is None
    assert snap["fcf"] is None
    assert snap["eps"] is None


@pytest.mark.db
def test_writethrough_rewash_purges_legacy_rows(_seed: psycopg.Connection[tuple]) -> None:
    conn = _seed
    # Legacy provider-era row at a cash-anchored (non-period-end) date.
    conn.execute(
        "INSERT INTO fundamentals_snapshot (instrument_id, as_of_date, revenue_ttm) VALUES (1, '2022-01-30', 26914)"
    )
    _write_snapshots_from_periods(conn, instrument_id=1)
    dates = [
        r[0]
        for r in conn.execute(
            "SELECT as_of_date FROM fundamentals_snapshot WHERE instrument_id = 1 ORDER BY as_of_date"
        ).fetchall()
    ]
    assert date(2022, 1, 30) not in dates
    assert dates == _Q_ENDS


@pytest.mark.db
def test_rekey_duplicate_period_end_not_double_counted(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # Fiscal-year-rekey (#1914 class): two period_type rows share one
    # period_end_date. The trailing-4 window must collapse them to the
    # latest-filed row FIRST, else it double-counts that quarter and
    # under-spans the true 4 distinct quarters (bot WARNING). Here the
    # newest period_end (2026-03-31) carries a stale Q4 row (rev 999,
    # filed earlier) AND the live Q1 row (rev 100, filed later); the
    # window must use 100 once, yielding revenue_ttm = 100+100+100+100.
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (4,'REKEY','Rekey Co',TRUE)"
    )
    _insert_quarter(conn, 4, date(2025, 6, 30), "Q2", revenue=100.0)
    _insert_quarter(conn, 4, date(2025, 9, 30), "Q3", revenue=100.0)
    _insert_quarter(conn, 4, date(2025, 12, 31), "Q4", revenue=100.0)
    _insert_quarter(conn, 4, date(2026, 3, 31), "Q1", revenue=100.0)
    # Stale duplicate at the newest period_end, filed a year earlier.
    conn.execute(
        """
        INSERT INTO financial_periods (
            instrument_id, period_end_date, period_type, fiscal_year,
            fiscal_quarter, revenue, operating_cf, eps_diluted,
            filed_date, source, source_ref, reported_currency,
            normalization_status
        ) VALUES (
            4, '2026-03-31', 'Q4', 2026, 4, 999, 999, 9.9,
            '2025-01-01', 'sec_edgar', 'stale', 'USD', 'normalized'
        )
        """
    )
    conn.commit()

    _write_snapshots_from_periods(conn, instrument_id=4)
    snap = _latest_snapshot(conn, 4)
    assert snap["as_of_date"] == date(2026, 3, 31)
    assert snap["revenue_ttm"] == Decimal("400.0000")  # 4×100, NOT 999-polluted

    cur = conn.cursor(row_factory=psycopg.rows.dict_row)
    cur.execute("SELECT revenue_ttm, is_complete_ttm FROM financial_periods_ttm WHERE instrument_id = 4")
    row = cur.fetchone()
    assert row is not None
    assert row["is_complete_ttm"] is True
    assert row["revenue_ttm"] == Decimal("400.0000")


@pytest.mark.db
def test_ttm_view_strict_sums_and_adjacency(_seed: psycopg.Connection[tuple]) -> None:
    conn = _seed
    cur = conn.cursor(row_factory=psycopg.rows.dict_row)
    cur.execute(
        "SELECT instrument_id, revenue_ttm, operating_cf_ttm, capex_ttm, is_complete_ttm "
        "FROM financial_periods_ttm WHERE instrument_id IN (1,2,3) ORDER BY instrument_id"
    )
    rows = {r["instrument_id"]: r for r in cur.fetchall()}

    assert rows[1]["is_complete_ttm"] is True
    assert rows[1]["revenue_ttm"] == Decimal("400.0000")
    # capex is sporadic: 3 present members sum to 30 despite one NULL quarter.
    assert rows[1]["capex_ttm"] == Decimal("30.0000")

    # NULL-revenue member: the CAT/AMZN class — the view must NOT emit a
    # 3-quarter partial sum; strict CASE yields NULL while other complete
    # columns still sum.
    assert rows[2]["is_complete_ttm"] is True
    assert rows[2]["revenue_ttm"] is None
    assert rows[2]["operating_cf_ttm"] == Decimal("120.0000")

    # Non-adjacent 4-row window: flagged incomplete AND no flow sums leak.
    assert rows[3]["is_complete_ttm"] is False
    assert rows[3]["revenue_ttm"] is None
