"""#2182 part B — the canonical merge keeps durable balance-sheet cells when the
winning raw row's balance sheet is not presented by any retained filing.

The P−2 year of the oldest retained 10-K (third Rule 3-02 income statement, no
Rule 3-01(a) balance sheet) used to overwrite canonical ``total_assets`` etc. with
NULL on every re-normalize (AAPL FY2021).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services.fundamentals import _PRESERVED_WHEN_UNPRESENTED_COLUMNS, _canonical_merge_instrument
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration

_IID = 2182
_END = date(2021, 9, 25)


def _seed(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, 'PTWO', 'PTWO test', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (_IID,),
    )


def _raw(
    conn: psycopg.Connection[tuple],
    *,
    presented: bool,
    revenue: str,
    total_assets: str | None,
    equity: str | None,
    filed: date,
) -> None:
    conn.execute("DELETE FROM financial_periods_raw WHERE instrument_id = %s", (_IID,))
    conn.execute(
        """
        INSERT INTO financial_periods_raw (
            instrument_id, period_end_date, period_type, fiscal_year, fiscal_quarter,
            revenue, total_assets, shareholders_equity, source, source_ref,
            reported_currency, filed_date, balance_sheet_presented
        ) VALUES (%s, %s, 'FY', 2021, NULL, %s, %s, %s, 'sec_edgar', 'acc', 'USD', %s, %s)
        """,
        (
            _IID,
            _END,
            Decimal(revenue),
            total_assets and Decimal(total_assets),
            equity and Decimal(equity),
            filed,
            presented,
        ),
    )
    _canonical_merge_instrument(conn, _IID)


def _canonical(conn: psycopg.Connection[tuple]) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    row = conn.execute(
        "SELECT revenue, total_assets, shareholders_equity FROM financial_periods WHERE instrument_id = %s",
        (_IID,),
    ).fetchone()
    assert row is not None
    return row[0], row[1], row[2]


def test_unpresented_row_keeps_balance_sheet_and_updates_income(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    # The FY2022 10-K presented FY2021's balance sheet.
    _raw(conn, presented=True, revenue="100", total_assets="351", equity="63", filed=date(2022, 10, 28))
    # Retention evicts it; the FY2023 10-K's P−2 row carries income + a rollforward equity.
    _raw(conn, presented=False, revenue="101", total_assets=None, equity="64", filed=date(2023, 11, 3))
    assert _canonical(conn) == (Decimal("101"), Decimal("351"), Decimal("63"))


def test_unpresented_row_fills_a_null_cell(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    _raw(conn, presented=False, revenue="101", total_assets=None, equity="64", filed=date(2023, 11, 3))
    assert _canonical(conn) == (Decimal("101"), None, Decimal("64"))


def test_presented_row_overwrites_every_cell_including_null(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """No stickiness: a presented balance sheet is authoritative, NULLs included."""
    conn = ebull_test_conn
    _seed(conn)
    _raw(conn, presented=True, revenue="100", total_assets="351", equity="63", filed=date(2022, 10, 28))
    _raw(conn, presented=True, revenue="100", total_assets=None, equity="62", filed=date(2023, 11, 3))
    assert _canonical(conn) == (Decimal("100"), None, Decimal("62"))


def test_preserved_set_is_the_instant_balance_sheet_columns() -> None:
    # The merge SQL spells these columns out; keep the constant and the SQL in step.
    import inspect

    src = " ".join(inspect.getsource(_canonical_merge_instrument).split())
    in_sql = {
        c
        for c in _PRESERVED_WHEN_UNPRESENTED_COLUMNS
        if f"CASE WHEN b.balance_sheet_presented THEN b.{c} ELSE COALESCE(c.{c}, b.{c}) END" in src
    }
    assert in_sql == _PRESERVED_WHEN_UNPRESENTED_COLUMNS
    assert src.count("CASE WHEN b.balance_sheet_presented") == len(_PRESERVED_WHEN_UNPRESENTED_COLUMNS)
    assert "antidilutive_securities" not in _PRESERVED_WHEN_UNPRESENTED_COLUMNS
