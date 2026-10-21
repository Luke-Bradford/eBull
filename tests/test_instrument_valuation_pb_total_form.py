"""#3903: ``instrument_valuation.pb_ratio`` is market cap / stored equity in both arms (sql/509; sql/512 selects it).

Frozen synthetic cases (spec "Acceptance" item 5). A legacy-arm instrument has a ``fundamentals_snapshot`` row and
one normalized quarter in ``financial_periods`` (no complete TTM window); the complete-arm instrument has four
adjacent quarters. Each has one #3841 PIT step at its session (close 50 × 1e8 shares = a 5e9 cap unless stated).
``shareholders_equity`` is NUMERIC(20,4), which refuses ±Infinity on insert, so ``NaN`` is the one non-finite value
it can carry. The control is sql/507's body, applied inside the test's transaction and rolled back. The
full-population wiring is ``scripts/measure_3903_pb_acceptance.py`` section 1.
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
Q_END = date(2026, 6, 30)
SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
SQL_507 = SQL_DIR / "507_instrument_valuation_dividend_yield_total_form.sql"
SQL_509 = SQL_DIR / "509_instrument_valuation_pb_total_form.sql"
ROLLBACK_509 = SQL_DIR / "rollback" / "509_instrument_valuation_pb_total_form.sql"
# sql/512 appends a financial_periods_ttm column that sql/507's CREATE OR REPLACE cannot drop; its rollback goes first.
ROLLBACK_512 = SQL_DIR / "rollback" / "512_financial_periods_ttm_stock_items_combine.sql"

(
    LEGACY,
    NULL_SHARES,
    ZERO_SHARES,
    COMPLETE,
    REFUSED,
    E_ZERO,
    E_NEGATIVE,
    E_NAN,
    LATEST_NULL,
    BOUND_IN,
    BOUND_OUT,
    WEEK_IN,
    WEEK_OUT,
    QUOTED,
    DUAL,
    FPI,
    EUR,
    NO_CURRENCY,
    NO_SNAPSHOT,
    SPLIT,
    LATER_FY,
    LATER_FY_COMPLETE,
) = range(401, 423)
ALL_IDS = list(range(401, 423))

# Legacy-arm instruments: (quarter end, stored equity, quarter-end shares, snapshot shares, snapshot book value,
# session). book_value is what the normaliser would have written (equity / shares, NULL when shares are not positive).
LEGACY_ROWS: dict[int, tuple[date, str | None, str | None, str | None, str | None, date]] = {
    LEGACY: (Q_END, "1e9", "1e7", "1e7", "100", SESSION),
    NULL_SHARES: (Q_END, "1e9", None, None, None, SESSION),
    ZERO_SHARES: (Q_END, "1e9", "0", "0", None, SESSION),
    REFUSED: (Q_END, "1e9", "1e7", "1e7", "100", SESSION),
    E_ZERO: (Q_END, "0", "1e7", "1e7", "0", SESSION),
    E_NEGATIVE: (Q_END, "-1e9", "1e7", "1e7", "-100", SESSION),
    E_NAN: (Q_END, "NaN", "1e7", "1e7", "100", SESSION),
    BOUND_IN: (date(2024, 2, 29), "1e9", "1e7", "1e7", "100", date(2025, 8, 31)),
    BOUND_OUT: (date(2024, 2, 29), "1e9", "1e7", "1e7", "100", date(2025, 9, 1)),
    WEEK_IN: (date(2024, 3, 30), "1e9", "1e7", "1e7", "100", date(2025, 9, 30)),
    WEEK_OUT: (date(2024, 3, 30), "1e9", "1e7", "1e7", "100", date(2025, 10, 1)),
    QUOTED: (Q_END, "1e9", "1e7", "1e7", "100", SESSION),
    DUAL: (Q_END, "1e9", "1e7", "1e7", "100", SESSION),
    FPI: (Q_END, "1e9", "1e7", "1e7", "100", SESSION),
    EUR: (Q_END, "1e9", "1e7", "1e7", "100", SESSION),
    NO_CURRENCY: (Q_END, "1e9", "1e7", "1e7", "100", SESSION),
    SPLIT: (Q_END, "1e8", "1e7", "1e7", "10", SESSION),
    LATER_FY: (Q_END, "1e9", "1e7", "1e7", "100", SESSION),
}


def _quarter(
    conn: psycopg.Connection[Any],
    iid: int,
    end: date,
    qtype: str,
    equity: str | None,
    shares: str | None,
    days: int = 90,
) -> None:
    conn.execute(
        "INSERT INTO financial_periods (instrument_id, period_start_date, period_end_date, period_type, fiscal_year, "
        " source, source_ref, reported_currency, revenue, net_income, shareholders_equity, shares_outstanding, "
        " superseded_at, normalization_status) "
        "VALUES (%s, %s, %s, %s, %s, 'test', %s, 'USD', 1e10, 1e9, %s::numeric, %s::numeric, NULL, 'normalized')",
        (iid, end - timedelta(days=days), end, qtype, end.year, f"b{iid}{end}{qtype}", equity, shares),
    )


def _snapshot(conn: psycopg.Connection[Any], iid: int, shares: str | None, book_value: str | None) -> None:
    conn.execute(
        "INSERT INTO fundamentals_snapshot "
        "(instrument_id, as_of_date, revenue_ttm, gross_margin, operating_margin, "
        " fcf, cash, debt, net_debt, shares_outstanding, book_value, eps) "
        "VALUES (%s, '2025-01-01', 1e11, 0.5, 0.3, 5e10, 1e10, 2e10, 1e10, %s::numeric, %s::numeric, 6)",
        (iid, shares, book_value),
    )


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[Any]) -> psycopg.Connection[Any]:
    conn = ebull_test_conn
    currency = {EUR: "EUR", NO_CURRENCY: None}
    for iid in ALL_IDS:
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, currency) "
            "VALUES (%s, %s, %s, TRUE, %s)",
            (iid, f"B{iid}", f"Book Total Form {iid}", currency.get(iid, "USD")),
        )
    for iid, (end, equity, q_shares, s_shares, book_value, session) in LEGACY_ROWS.items():
        _quarter(conn, iid, end, "Q2", equity, q_shares)
        _snapshot(conn, iid, s_shares, book_value)
        if iid == SPLIT:
            # 1e7 shares at the quarter end, 4e7 after a 4:1 split; eToro back-adjusts the close to 25.
            seed_pit_step(conn, iid, session=session, close=25, shares=4e7)
        else:
            refusal = "no_left_anchor" if iid == REFUSED else None
            seed_pit_step(conn, iid, session=session, close=50, shares=1e8, refusal=refusal)
    # The latest quarter's equity is NULL; an older quarter's is positive. No fallback.
    _quarter(conn, LATEST_NULL, date(2026, 3, 31), "Q1", "1e9", "1e7")
    _quarter(conn, LATEST_NULL, Q_END, "Q2", None, "1e7")
    _snapshot(conn, LATEST_NULL, "1e7", "100")
    seed_pit_step(conn, LATEST_NULL, session=SESSION, close=50, shares=1e8)
    # The complete arm: four adjacent quarters.
    for iid in (COMPLETE, LATER_FY_COMPLETE):
        for end, qtype in ((date(2025, 9, 30), "Q3"), (date(2025, 12, 31), "Q4"), (date(2026, 3, 31), "Q1")):
            _quarter(conn, iid, end, qtype, "1e9", "1e8")
        _quarter(conn, iid, Q_END, "Q2", "2e9", "1e8")
        seed_pit_step(conn, iid, session=SESSION, close=50, shares=1e8)
    # A stored FY period ending after the latest quarter: sql/509 refused (Amendment 1); sql/512 reads its equity.
    for iid in (LATER_FY, LATER_FY_COMPLETE):
        _quarter(conn, iid, date(2026, 9, 30), "FY", "3e9", "1e8", days=364)
    # Stored equity and a cap, no snapshot row and no complete window: no legacy row.
    _quarter(conn, NO_SNAPSHOT, Q_END, "Q2", "1e9", "1e7")
    seed_pit_step(conn, NO_SNAPSHOT, session=SESSION, close=50, shares=1e8)
    # A quote newer than the session at a different price: current_price moves, the cap does not.
    conn.execute(
        "INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_flag) "
        "VALUES (%s, now(), 59, 61, 60, FALSE)",
        (QUOTED,),
    )
    conn.execute(
        "INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary) "
        "VALUES (%s, 'sec', 'cik', '0009993918', TRUE)",
        (DUAL,),
    )
    conn.execute(
        "INSERT INTO instrument_class_shares_outstanding "
        "(instrument_id, period_end, shares, class_member, source_cik, source_adsh, "
        " source_form_type, source_fsds_qtr, source_filed_at, resolution_method, parser_version, ingested_at) "
        "VALUES (%s, '2026-06-30', 1e8, 'CommonClassA', '0009993918', '0000000000-00-000000', "
        "'10-Q', '2026Q2', now(), 'curated', 1, now())",
        (DUAL,),
    )
    conn.execute("INSERT INTO coverage (instrument_id, coverage_tier, filings_status) VALUES (%s, 3, 'fpi')", (FPI,))
    conn.commit()
    return conn


def _val(conn: psycopg.Connection[Any], iid: int) -> dict[str, Any] | None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        return cur.execute("SELECT * FROM instrument_valuation WHERE instrument_id = %s", (iid,)).fetchone()


def _row(conn: psycopg.Connection[Any], iid: int) -> dict[str, Any]:
    row = _val(conn, iid)
    assert row is not None
    return row


def _apply(conn: psycopg.Connection[Any], path: Path) -> None:
    body = re.sub(r"^(BEGIN|COMMIT);$", "", path.read_text(), flags=re.M)
    conn.execute(body)  # type: ignore[arg-type]  # a reviewed file, not user input


def _pb_507(conn: psycopg.Connection[Any], iid: int) -> Any:
    _apply(conn, ROLLBACK_512)
    _apply(conn, SQL_507)
    row = _val(conn, iid)
    conn.rollback()
    return None if row is None else row["pb_ratio"]


@pytest.mark.db
def test_legacy_arm_publishes_market_cap_over_equity(_seed: psycopg.Connection[Any]) -> None:
    row = _row(_seed, LEGACY)
    assert row["is_complete_ttm"] is None
    assert row["market_cap_live"] == Decimal("5e9")
    assert row["pb_ratio"] == 5
    # sql/507: price 50 / book value per share 100.
    assert _pb_507(_seed, LEGACY) == Decimal("0.5")


@pytest.mark.db
@pytest.mark.parametrize("iid", [NULL_SHARES, ZERO_SHARES])
def test_legacy_arm_publishes_without_a_positive_quarter_count(_seed: psycopg.Connection[Any], iid: int) -> None:
    assert _row(_seed, iid)["pb_ratio"] == 5
    # sql/507 needed book_value, which the normaliser leaves NULL when the quarter's count is not positive.
    assert _pb_507(_seed, iid) is None


@pytest.mark.db
def test_complete_arm_divides_by_the_latest_quarter(_seed: psycopg.Connection[Any]) -> None:
    row = _row(_seed, COMPLETE)
    assert row["is_complete_ttm"] is True
    assert row["pb_ratio"] == Decimal("2.5")
    assert _pb_507(_seed, COMPLETE) == Decimal("2.5")


@pytest.mark.db
@pytest.mark.parametrize(("iid", "reason"), [(REFUSED, "no_left_anchor"), (DUAL, "dual_class"), (FPI, "fpi_adr")])
def test_refused_cap_refuses_with_its_reason(_seed: psycopg.Connection[Any], iid: int, reason: str) -> None:
    row = _row(_seed, iid)
    assert row["market_cap_reason"] == reason
    assert row["pb_ratio"] is None


@pytest.mark.db
@pytest.mark.parametrize("iid", [E_ZERO, E_NEGATIVE, E_NAN, LATEST_NULL])
def test_unusable_latest_equity_refuses(_seed: psycopg.Connection[Any], iid: int) -> None:
    row = _row(_seed, iid)
    assert row["market_cap_live"] == Decimal("5e9")
    assert row["pb_ratio"] is None


@pytest.mark.db
@pytest.mark.parametrize(
    ("iid", "published"), [(BOUND_IN, True), (BOUND_OUT, False), (WEEK_IN, True), (WEEK_OUT, False)]
)
def test_equity_is_usable_through_the_month_end_18_months_on(
    _seed: psycopg.Connection[Any], iid: int, published: bool
) -> None:
    row = _row(_seed, iid)
    assert row["session"] == LEGACY_ROWS[iid][5]
    assert row["market_cap_live"] == Decimal("5e9")
    assert (row["pb_ratio"] == 5) is published
    assert (row["pb_ratio"] is None) is not published


@pytest.mark.db
@pytest.mark.parametrize(("iid", "complete"), [(LATER_FY, None), (LATER_FY_COMPLETE, True)])
def test_a_later_stored_fy_supplies_the_equity(_seed: psycopg.Connection[Any], iid: int, complete: bool | None) -> None:
    """sql/509 refused here (Amendment 1); sql/512 (#3919) reads the later FY's equity, 5e9 / 3e9."""
    row = _row(_seed, iid)
    assert row["is_complete_ttm"] is complete
    assert row["market_cap_live"] == Decimal("5e9")
    assert round(row["pb_ratio"], 12) == round(Decimal(5) / Decimal(3), 12)


@pytest.mark.db
def test_a_newer_quote_does_not_price_the_ratio(_seed: psycopg.Connection[Any]) -> None:
    row = _row(_seed, QUOTED)
    assert row["current_price"] == 60
    assert row["valuation_price"] == 50
    assert row["pb_ratio"] == 5
    assert _pb_507(_seed, QUOTED) == Decimal("0.6")


@pytest.mark.db
@pytest.mark.parametrize("iid", [EUR, NO_CURRENCY])
def test_a_non_usd_or_unknown_currency_refuses(_seed: psycopg.Connection[Any], iid: int) -> None:
    row = _row(_seed, iid)
    assert row["market_cap_reason"] is None
    assert row["market_cap_live"] == Decimal("5e9")
    assert row["pb_ratio"] is None


@pytest.mark.db
def test_no_snapshot_row_means_no_legacy_row(_seed: psycopg.Connection[Any]) -> None:
    assert _val(_seed, NO_SNAPSHOT) is None
    assert _pb_507(_seed, NO_SNAPSHOT) is None


@pytest.mark.db
def test_a_split_after_the_quarter_end_does_not_move_the_ratio(_seed: psycopg.Connection[Any]) -> None:
    row = _row(_seed, SPLIT)
    assert row["market_cap_live"] == Decimal("1e9")
    assert row["pb_ratio"] == 10
    # sql/507: the back-adjusted close 25 over the pre-split book value per share 10.
    assert _pb_507(_seed, SPLIT) == Decimal("2.5")


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
def test_rollback_restores_sql_507(_seed: psycopg.Connection[Any]) -> None:
    forward = _shape_and_rows(_seed)
    _apply(_seed, ROLLBACK_512)
    _apply(_seed, SQL_507)
    control = _shape_and_rows(_seed)
    _seed.rollback()
    _apply(_seed, ROLLBACK_512)
    _apply(_seed, SQL_509)
    _apply(_seed, ROLLBACK_509)
    restored = _shape_and_rows(_seed)
    _seed.rollback()
    assert forward[0] == control[0]
    assert restored == control
    assert len(control[1]) == len(ALL_IDS) - 1
