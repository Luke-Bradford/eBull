"""Migration 417 (#2182 item 2): delete canonical FY rows no filing presented.

Runs the real migration file against seeded rows: one row per branch of the
predicate, so a drift between the SQL and its stated rule fails here.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration

_MIGRATION_PATH = Path(__file__).resolve().parents[1] / "sql" / "417_delete_unpresented_fy_ghost_rows.sql"


def _run_migration(conn: psycopg.Connection[tuple]) -> None:
    conn.commit()
    conn.autocommit = True
    try:
        with psycopg.ClientCursor(conn) as cur:
            cur.execute(_MIGRATION_PATH.read_text(encoding="utf-8"))  # type: ignore[call-overload]
    finally:
        conn.autocommit = False


def _instrument(conn: psycopg.Connection[tuple], iid: int) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, 'test', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, f"G{iid}"),
    )


def _fy(
    conn: psycopg.Connection[tuple],
    iid: int,
    period_end: date,
    source_ref: str,
    **cells: Decimal,
) -> None:
    cols = ", ".join(cells)
    placeholders = ", ".join(["%s"] * len(cells))
    conn.execute(
        f"""
        INSERT INTO financial_periods (
            instrument_id, period_end_date, period_type, fiscal_year, fiscal_quarter,
            source, source_ref, reported_currency{", " + cols if cells else ""}
        ) VALUES (%s, %s, 'FY', %s, NULL, 'sec_edgar', %s, 'USD'{", " + placeholders if cells else ""})
        """,  # type: ignore[arg-type]
        (iid, period_end, period_end.year, source_ref, *cells.values()),
    )


def _surviving(conn: psycopg.Connection[tuple], iid: int) -> list[date]:
    rows = conn.execute(
        "SELECT period_end_date FROM financial_periods WHERE instrument_id = %s ORDER BY 1",
        (iid,),
    ).fetchall()
    return [r[0] for r in rows]


def test_deletes_only_unpresented_pre_first_year_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    eq = Decimal("1000")
    # A: first XBRL 10-K (ACC-A, FY2009). Its 3-04 rollforward opening balance minted
    # FY2006 — primary end from the citing FY2009 row (no filing_events row).
    _instrument(conn, 941701)
    _fy(conn, 941701, date(2009, 9, 26), "ACC-A", revenue=Decimal("5"), total_assets=Decimal("9"))
    _fy(conn, 941701, date(2006, 9, 30), "ACC-A", shareholders_equity=eq)  # ghost → deleted
    _fy(conn, 941701, date(2008, 9, 27), "ACC-A", shareholders_equity=eq)  # in scope (364d) → kept
    _fy(conn, 941701, date(2007, 9, 29), "ACC-A", shares_basic=eq)  # a duration → kept

    # B: primary ends from filing_events.report_date; no other row cites the accessions.
    _instrument(conn, 941702)
    conn.execute(
        """
        INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, provider_filing_id, report_date)
        VALUES (941702, '2022-03-01', '10-K', 'sec', 'ACC-B', '2021-12-31'),
               (941702, '2017-08-01', '10-K', 'sec', 'ACC-C', '2017-06-30')
        """
    )
    _fy(conn, 941702, date(2018, 6, 30), " ACC-B", cash=eq)  # ghost → deleted (whitespace in ref)
    # Unknown accession, cited only by itself: primary end falls back to its own end → kept.
    _fy(conn, 941702, date(2015, 12, 31), "ACC-UNKNOWN", shareholders_equity=eq)
    # Mixed ref: one out-of-scope accession, one in scope (181d) → kept.
    _fy(conn, 941702, date(2016, 12, 31), "ACC-B,ACC-C", shareholders_equity=eq)

    # C: thin row in MID-history (an earlier real year exists) → kept.
    _instrument(conn, 941703)
    conn.execute(
        """
        INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, provider_filing_id, report_date)
        VALUES (941703, '2020-02-01', '10-K', 'sec', 'ACC-D', '2019-12-31')
        """
    )
    _fy(conn, 941703, date(2012, 12, 31), "ACC-OLD", net_income=Decimal("1"))
    _fy(conn, 941703, date(2016, 12, 31), "ACC-D", shareholders_equity=eq)

    _run_migration(conn)

    assert _surviving(conn, 941701) == [date(2007, 9, 29), date(2008, 9, 27), date(2009, 9, 26)]
    assert _surviving(conn, 941702) == [date(2015, 12, 31), date(2016, 12, 31)]
    assert _surviving(conn, 941703) == [date(2012, 12, 31), date(2016, 12, 31)]

    # Idempotent: a second run deletes nothing more.
    _run_migration(conn)
    assert len(_surviving(conn, 941701)) == 3
