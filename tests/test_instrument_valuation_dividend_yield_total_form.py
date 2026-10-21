"""#3903: ``instrument_valuation.dividend_yield`` is TTM cash dividends paid / market cap (sql/507).

Frozen synthetic cases (spec "Acceptance" item 6). Every complete-TTM instrument has four adjacent normalized quarters
in ``financial_periods`` (``financial_periods_ttm`` is a view over them), with valid, contiguous intervals inside one
calendar year (sql/508's conditions; their own cases are in
``tests/test_financial_periods_ttm_dividend_window_guard.py``), and one #3841 PIT step at the session
(close 50 × 1e8 shares = a 5e9 cap unless refused). The per-share DPS is seeded across a split (25.00, then 1.00 ×
3), so sql/506's yield is 28 / 50 × 100 = 56% while the total form is 4e8 / 5e9 × 100 = 8%. ``dividends_paid`` is
NUMERIC(20,4), which refuses ±Infinity on insert, so ``NaN`` is the one non-finite value a member can carry. The
full-population wiring is ``scripts/measure_3903_dividend_acceptance.py`` section 1.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import psycopg.rows
import pytest

from tests.fixtures.pit_market_cap import seed_pit_step

SESSION = date(2026, 10, 16)
Q_STARTS = (date(2025, 7, 1), date(2025, 10, 1), date(2026, 1, 1), date(2026, 4, 1))
Q_ENDS = (date(2025, 9, 30), date(2025, 12, 31), date(2026, 3, 31), date(2026, 6, 30))
Q_TYPES = ("Q3", "Q4", "Q1", "Q2")
DPS = (Decimal("25.00"), Decimal("1.00"), Decimal("1.00"), Decimal("1.00"))
SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
SQL_506 = SQL_DIR / "506_instrument_valuation_pe_total_form.sql"
SQL_507 = SQL_DIR / "507_instrument_valuation_dividend_yield_total_form.sql"
ROLLBACK_507 = SQL_DIR / "rollback" / "507_instrument_valuation_dividend_yield_total_form.sql"
# sql/512 appends a financial_periods_ttm column that sql/507's CREATE OR REPLACE cannot drop; its rollback goes first.
ROLLBACK_512 = SQL_DIR / "rollback" / "512_financial_periods_ttm_stock_items_combine.sql"

PUBLISHED, ZEROS, NULL_MEMBER, NEGATIVE, NAN, REFUSED, FPI, DUAL, ADS, LEGACY = range(311, 321)
PAID: dict[int, list[str | None]] = {
    PUBLISHED: ["1e8"] * 4,
    ZEROS: ["0"] * 4,
    NULL_MEMBER: ["1e8", None, "1e8", "1e8"],
    NEGATIVE: ["1e8", "-1e8", "1e8", "1e8"],
    NAN: ["1e8", "NaN", "1e8", "1e8"],
    REFUSED: ["1e8"] * 4,
    FPI: ["1e8"] * 4,
    DUAL: ["1e8"] * 4,
    ADS: ["1e8"] * 4,
}
ALL_IDS = [*PAID, LEGACY]


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[Any]) -> psycopg.Connection[Any]:
    conn = ebull_test_conn
    for iid in ALL_IDS:
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
            (iid, f"D{iid}", f"Dividend Total Form {iid}"),
        )
    for iid, quarters in PAID.items():
        for i, (start, end, qt, paid) in enumerate(zip(Q_STARTS, Q_ENDS, Q_TYPES, quarters, strict=True)):
            conn.execute(
                "INSERT INTO financial_periods (instrument_id, period_start_date, period_end_date, period_type, "
                " fiscal_year, source, source_ref, reported_currency, revenue, net_income, dividends_paid, "
                " dps_declared, superseded_at, normalization_status) "
                "VALUES (%s, %s, %s, %s, %s, 'test', %s, 'USD', 1e10, 1e9, %s::numeric, %s, NULL, 'normalized')",
                (iid, start, end, qt, end.year, f"d{iid}{i}", paid, DPS[i]),
            )
        refusal = "no_left_anchor" if iid == REFUSED else None
        seed_pit_step(conn, iid, session=SESSION, close=50, shares=1e8, refusal=refusal)
    # The legacy arm: a fundamentals_snapshot row and no complete TTM window.
    conn.execute(
        "INSERT INTO fundamentals_snapshot "
        "(instrument_id, as_of_date, revenue_ttm, gross_margin, operating_margin, "
        " fcf, cash, debt, net_debt, shares_outstanding, book_value, eps) "
        "VALUES (%s, '2025-01-01', 1e11, 0.5, 0.3, 5e10, 1e10, 2e10, 1e10, 1e10, 30, 6)",
        (LEGACY,),
    )
    seed_pit_step(conn, LEGACY, session=SESSION, close=50, shares=1e8)
    conn.execute("INSERT INTO coverage (instrument_id, coverage_tier, filings_status) VALUES (%s, 3, 'fpi')", (FPI,))
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (%s, 'sec', 'cik', '0009993913', TRUE)",
        (DUAL,),
    )
    conn.execute(
        "INSERT INTO instrument_class_shares_outstanding "
        "(instrument_id, period_end, shares, class_member, source_cik, source_adsh, "
        " source_form_type, source_fsds_qtr, source_filed_at, resolution_method, parser_version, ingested_at) "
        "VALUES (%s, '2026-06-30', 1e8, 'CommonClassA', '0009993913', '0000000000-00-000000', "
        "'10-Q', '2026Q2', now(), 'curated', 1, now())",
        (DUAL,),
    )
    conn.execute("INSERT INTO ads_ratio (instrument_id, ratio, source_form) VALUES (%s, 2, 'F-6')", (ADS,))
    conn.commit()
    return conn


def _val(conn: psycopg.Connection[Any], iid: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute("SELECT * FROM instrument_valuation WHERE instrument_id = %s", (iid,)).fetchone()
    assert row is not None
    return row


def _apply(conn: psycopg.Connection[Any], path: Path) -> None:
    body = re.sub(r"^(BEGIN|COMMIT);$", "", path.read_text(), flags=re.M)
    conn.execute(body)  # type: ignore[arg-type]  # a reviewed file, not user input


def _yield_506(conn: psycopg.Connection[Any], iid: int) -> Any:
    _apply(conn, SQL_506)
    value = _val(conn, iid)["dividend_yield"]
    conn.rollback()
    return value


@pytest.mark.db
def test_published_on_the_total_form_across_a_split(_seed: psycopg.Connection[Any]) -> None:
    row = _val(_seed, PUBLISHED)
    assert row["market_cap_reason"] is None
    assert row["market_cap_live"] == Decimal("5e9")
    assert row["dividend_yield"] == Decimal("8")
    # sql/506 divided a per-share sum that crosses the split by the price: 28 / 50 × 100.
    assert _yield_506(_seed, PUBLISHED) == Decimal("56")


@pytest.mark.db
def test_four_observed_zeros_publish_zero(_seed: psycopg.Connection[Any]) -> None:
    assert _val(_seed, ZEROS)["dividend_yield"] == 0


@pytest.mark.db
def test_a_null_member_refuses_and_leaves_the_sporadic_sum(_seed: psycopg.Connection[Any]) -> None:
    assert _val(_seed, NULL_MEMBER)["dividend_yield"] is None
    ttm = _seed.execute(
        "SELECT dividends_paid_ttm, dividends_paid_ttm_observed FROM financial_periods_ttm WHERE instrument_id = %s",
        (NULL_MEMBER,),
    ).fetchone()
    assert ttm == (Decimal("3e8"), None)


@pytest.mark.db
@pytest.mark.parametrize("iid", [NEGATIVE, NAN])
def test_negative_or_non_finite_member_refuses(_seed: psycopg.Connection[Any], iid: int) -> None:
    row = _val(_seed, iid)
    assert row["market_cap_reason"] is None
    assert row["market_cap_live"] == Decimal("5e9")
    assert row["dividend_yield"] is None


@pytest.mark.db
@pytest.mark.parametrize(
    ("iid", "reason", "yield_506_positive"),
    [
        (REFUSED, "no_left_anchor", True),
        (DUAL, "dual_class", True),
        (ADS, "ads_ratio", True),
        (FPI, "fpi_adr", False),
    ],
)
def test_refused_cap_refuses_the_yield_with_its_reason(
    _seed: psycopg.Connection[Any], iid: int, reason: str, yield_506_positive: bool
) -> None:
    row = _val(_seed, iid)
    assert row["market_cap_reason"] == reason
    assert row["dividend_yield"] is None
    # The control: sql/506 published a per-share yield for each, except FPI, which it suppressed.
    before = _yield_506(_seed, iid)
    assert (before is not None and before > 0) is yield_506_positive


@pytest.mark.db
def test_legacy_arm_publishes_no_yield_either_way(_seed: psycopg.Connection[Any]) -> None:
    assert _val(_seed, LEGACY)["is_complete_ttm"] is None
    assert _val(_seed, LEGACY)["dividend_yield"] is None
    assert _yield_506(_seed, LEGACY) is None


def _shape_and_rows(conn: psycopg.Connection[Any]) -> tuple[list[tuple[str, str]], list[str]]:
    columns = conn.execute(
        "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'instrument_valuation'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
    ).fetchall()
    rows = conn.execute(
        "SELECT v::text FROM instrument_valuation v WHERE instrument_id = ANY(%s) ORDER BY instrument_id",
        (ALL_IDS,),
    ).fetchall()
    return [(c[0], c[1]) for c in columns], [r[0] for r in rows]


@pytest.mark.db
def test_rollback_restores_sql_506(_seed: psycopg.Connection[Any]) -> None:
    forward = _shape_and_rows(_seed)
    _apply(_seed, SQL_506)
    control = _shape_and_rows(_seed)
    _seed.rollback()
    _apply(_seed, ROLLBACK_512)
    _apply(_seed, SQL_507)
    _apply(_seed, ROLLBACK_507)
    restored = _shape_and_rows(_seed)
    _seed.rollback()
    assert forward[0] == control[0]
    assert restored == control
    assert len(control[1]) == len(ALL_IDS)
