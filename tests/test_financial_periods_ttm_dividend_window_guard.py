"""#3903 sql/508: ``financial_periods_ttm.dividends_paid_ttm_observed``'s window guard (spec Amendment 2).

Frozen synthetic cases. Each instrument has four normalized quarters that pass sql/220's shape guard (end dates
within 330 days) and a fifth, older quarter paying 7e8 that must never enter the sum. Each case runs with every
member paying 1e8 (a positive total of 4e8) and with every member paying 0. The conditions, in order:

1. every member's ``period_start_date`` is non-NULL and not after its ``period_end_date``;
2. no member starts on or before the previous member's end;
3. a positive total's last end is before the first member's start + ``INTERVAL '1 year'``. A zero total skips it.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, LiteralString

import psycopg
import psycopg.rows
import pytest

from tests.fixtures.pit_market_cap import seed_pit_step

SESSION = date(2026, 10, 16)
SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
SQL_507 = SQL_DIR / "507_instrument_valuation_dividend_yield_total_form.sql"
SQL_508 = SQL_DIR / "508_financial_periods_ttm_dividend_window_guard.sql"
ROLLBACK_507 = SQL_DIR / "rollback" / "507_instrument_valuation_dividend_yield_total_form.sql"
ROLLBACK_508 = SQL_DIR / "rollback" / "508_financial_periods_ttm_dividend_window_guard.sql"
# sql/512 appends a financial_periods_ttm column that sql/507's and sql/508's CREATE OR REPLACE cannot drop; each
# block applies its rollback first.
ROLLBACK_512 = SQL_DIR / "rollback" / "512_financial_periods_ttm_stock_items_combine.sql"

D = date
Window = list[tuple[date | None, date]]
_STANDARD: Window = [
    (D(2025, 4, 1), D(2025, 6, 30)),
    (D(2025, 7, 1), D(2025, 9, 30)),
    (D(2025, 10, 1), D(2025, 12, 31)),
    (D(2026, 1, 1), D(2026, 3, 31)),
]


def _with(window: Window, index: int, start: date | None, end: date | None = None) -> Window:
    out = list(window)
    out[index] = (start, end if end is not None else out[index][1])
    return out


# name -> (members as (start, end), publishes a positive total, publishes a zero total)
CASES: dict[str, tuple[Window, bool, bool]] = {
    "anniversary, the day before": (_STANDARD, True, True),
    "anniversary, on it": (_with(_STANDARD, 3, D(2026, 1, 1), D(2026, 4, 1)), False, True),
    "anniversary, after it": (_with(_STANDARD, 3, D(2026, 1, 1), D(2026, 4, 2)), False, True),
    "leap year, 366 days ending 29 February": (
        [
            (D(2023, 3, 1), D(2023, 5, 31)),
            (D(2023, 6, 1), D(2023, 8, 31)),
            (D(2023, 9, 1), D(2023, 11, 30)),
            (D(2023, 12, 1), D(2024, 2, 29)),
        ],
        True,
        True,
    ),
    "leap year, starting 29 February": (
        [
            (D(2024, 2, 29), D(2024, 5, 28)),
            (D(2024, 5, 29), D(2024, 8, 28)),
            (D(2024, 8, 29), D(2024, 11, 28)),
            (D(2024, 11, 29), D(2025, 2, 28)),
        ],
        False,
        True,
    ),
    "NULL start on the first member": (_with(_STANDARD, 0, None), False, False),
    "NULL start on another member": (_with(_STANDARD, 2, None), False, False),
    "inverted interval": (_with(_STANDARD, 1, D(2025, 10, 1)), False, False),
    "one-day overlap": (_with(_STANDARD, 2, D(2025, 9, 30)), False, False),
    "cumulative members": (
        [
            (D(2025, 1, 1), D(2025, 3, 31)),
            (D(2025, 1, 1), D(2025, 6, 30)),
            (D(2025, 1, 1), D(2025, 9, 30)),
            (D(2025, 1, 1), D(2025, 12, 31)),
        ],
        False,
        False,
    ),
    "a gap between members": (_with(_STANDARD, 2, D(2025, 10, 5)), True, True),
    "a 334-day window": (_with(_STANDARD, 0, D(2025, 5, 2)), True, True),
    "368 days": (_with(_STANDARD, 0, D(2025, 3, 29)), False, True),
    "inverted interval inside an over-long window": (
        _with(_with(_STANDARD, 0, D(2025, 3, 29)), 1, D(2025, 10, 1)),
        False,
        False,
    ),
}
FIRST_ID = 331
IDS: dict[tuple[str, bool], int] = {
    (name, positive): FIRST_ID + 2 * i + (0 if positive else 1)
    for i, name in enumerate(CASES)
    for positive in (True, False)
}


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[Any]) -> psycopg.Connection[Any]:
    conn = ebull_test_conn
    for (name, positive), iid in IDS.items():
        members = CASES[name][0]
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (iid, f"W{iid}", f"Dividend Window {iid}"),
        )
        first_end = members[0][1]
        older = (first_end - timedelta(days=181), first_end - timedelta(days=91))
        rows = [(older, Decimal("7e8")), *((m, Decimal("1e8") if positive else Decimal(0)) for m in members)]
        for i, ((start, end), paid) in enumerate(rows):
            conn.execute(
                "INSERT INTO financial_periods (instrument_id, period_start_date, period_end_date, period_type, "
                " fiscal_year, source, source_ref, reported_currency, revenue, net_income, dividends_paid, "
                " superseded_at, normalization_status) "
                "VALUES (%s, %s, %s, %s, %s, 'test', %s, 'USD', 1e10, 1e9, %s, NULL, 'normalized')",
                (iid, start, end, f"Q{i % 4 + 1}", end.year, f"w{iid}{i}", paid),
            )
        seed_pit_step(conn, iid, session=SESSION, close=50, shares=1e8)
    conn.commit()
    return conn


def _observed(conn: psycopg.Connection[Any], iid: int) -> Any:
    row = conn.execute(
        "SELECT is_complete_ttm, dividends_paid_ttm_observed FROM financial_periods_ttm WHERE instrument_id = %s",
        (iid,),
    ).fetchone()
    assert row is not None and row[0] is True  # sql/220's shape guard passes in every case
    return row[1]


@pytest.mark.db
@pytest.mark.parametrize("name", list(CASES))
@pytest.mark.parametrize("positive", [True, False], ids=["positive", "zero"])
def test_window_guard(_seed: psycopg.Connection[Any], name: str, positive: bool) -> None:
    _, publishes_positive, publishes_zero = CASES[name]
    publishes = publishes_positive if positive else publishes_zero
    expected = (Decimal("4e8") if positive else Decimal(0)) if publishes else None
    assert _observed(_seed, IDS[(name, positive)]) == expected


@pytest.mark.db
@pytest.mark.parametrize(
    ("name", "expected"), [("anniversary, the day before", Decimal("8")), ("anniversary, after it", None)]
)
def test_valuation_yield_follows_the_guard_under_a_published_cap(
    _seed: psycopg.Connection[Any], name: str, expected: Decimal | None
) -> None:
    with _seed.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute(
            "SELECT market_cap_reason, market_cap_live, dividend_yield FROM instrument_valuation "
            "WHERE instrument_id = %s",
            (IDS[(name, True)],),
        ).fetchone()
    assert row is not None
    assert row["market_cap_reason"] is None
    assert row["market_cap_live"] == Decimal("5e9")
    assert row["dividend_yield"] == expected


def _apply(conn: psycopg.Connection[Any], path: Path) -> None:
    body = re.sub(r"^(BEGIN|COMMIT);$", "", path.read_text(), flags=re.M)
    conn.execute(body)  # type: ignore[arg-type]  # a reviewed file, not user input


_ROWS: dict[str, LiteralString] = {
    "financial_periods_ttm": "SELECT v::text FROM financial_periods_ttm v WHERE instrument_id = ANY(%s) ORDER BY 1",
    "instrument_valuation": "SELECT v::text FROM instrument_valuation v WHERE instrument_id = ANY(%s) ORDER BY 1",
}


def _state(conn: psycopg.Connection[Any]) -> tuple[Any, ...]:
    """Each view's definition, ordered columns and types, and the cases' rows."""
    ids = list(IDS.values())
    out: list[Any] = []
    for view, rows_sql in _ROWS.items():
        out.append(conn.execute("SELECT pg_get_viewdef(%s::regclass)", (view,)).fetchone())
        out.append(
            conn.execute(
                "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
                "WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
                (view,),
            ).fetchall()
        )
        out.append(conn.execute(rows_sql, (ids,)).fetchall())
    return tuple(out)


@pytest.mark.db
def test_rollback_restores_sql_507(_seed: psycopg.Connection[Any]) -> None:
    _apply(_seed, ROLLBACK_512)
    _apply(_seed, SQL_507)
    prior = _state(_seed)
    _seed.rollback()
    # sql/507's valuation body explicitly: a later migration (sql/509) replaces it in the test database.
    _apply(_seed, ROLLBACK_512)
    _apply(_seed, SQL_507)
    _apply(_seed, SQL_508)
    forward = _state(_seed)
    _apply(_seed, ROLLBACK_508)
    restored = _state(_seed)
    _seed.rollback()
    assert forward != prior  # the guard changes some cases
    assert restored == prior


@pytest.mark.db
def test_whole_construction_rollback_is_508_then_507(_seed: psycopg.Connection[Any]) -> None:
    _apply(_seed, ROLLBACK_512)
    _apply(_seed, SQL_507)
    _apply(_seed, ROLLBACK_507)
    expected = _state(_seed)
    _seed.rollback()
    _apply(_seed, ROLLBACK_512)
    _apply(_seed, SQL_508)
    _apply(_seed, ROLLBACK_508)
    _apply(_seed, ROLLBACK_507)
    got = _state(_seed)
    _seed.rollback()
    assert got == expected
