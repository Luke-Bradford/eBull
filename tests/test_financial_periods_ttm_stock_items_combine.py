"""#3919 sql/512: ``financial_periods_ttm``'s stock items under JKP's per-item annual/quarterly combine.

Spec: ``docs/specs/fundamentals/2026-10-21-3919-ttm-stock-items-jkp-combine.md`` ("Acceptance", tests 1-14). One
fixture instrument per case, expected values written out by hand. The view's annual bound reads ``CURRENT_DATE``,
which a test cannot set, so period ends are placed relative to the database's ``CURRENT_DATE`` (month granularity,
test 6), and the day-granularity boundary is tested by applying sql/512's own body with ``CURRENT_DATE`` replaced by a
literal clock. ``financial_periods.reported_currency`` is NOT NULL, so the NULL-currency variants of test 8 drop the
constraint inside their own rolled-back transaction: the guard is defence in depth, and the test pins it anyway.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import psycopg.rows
import psycopg.sql
import pytest

from tests.fixtures.pit_market_cap import seed_pit_step

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
SQL_508 = SQL_DIR / "508_financial_periods_ttm_dividend_window_guard.sql"
SQL_509 = SQL_DIR / "509_instrument_valuation_pb_total_form.sql"
SQL_512 = SQL_DIR / "512_financial_periods_ttm_stock_items_combine.sql"
ROLLBACK_512 = SQL_DIR / "rollback" / "512_financial_periods_ttm_stock_items_combine.sql"

(
    NEWER_FY,
    FILL,
    Q_NEWER,
    TIE_PRESENT,
    TIE_MISSING,
    NO_FY,
    BOUND_IN,
    BOUND_OUT,
    SUP_FY_NULL,
    SUP_FY_CURRENCY,
    SUP_Q_NULL,
    SUP_Q_NULL_NO_FY,
    CUR_MISMATCH,
    MIXED,
    EXPIRED_Q,
    EXPIRED_BOTH,
    PB_NEWER_FY,
    PB_FILL,
    PB_BOUND_IN,
    PB_NEWER_FY_COMPLETE,
    PB_FILL_COMPLETE,
    PB_BOUND_IN_COMPLETE,
    PB_CROSSING,
    TIE_FILED,
) = range(39191, 39215)
ALL_IDS = list(range(39191, 39215))
PB_LEGACY = (PB_NEWER_FY, PB_FILL, PB_BOUND_IN, PB_CROSSING)
PB_COMPLETE = (PB_NEWER_FY_COMPLETE, PB_FILL_COMPLETE, PB_BOUND_IN_COMPLETE)

ITEMS = (
    "total_assets",
    "total_liabilities",
    "shareholders_equity",
    "cash",
    "long_term_debt",
    "short_term_debt",
    "shares_outstanding",
    "inventory",
    "receivables",
    "payables",
    "goodwill",
    "ppe_net",
)


def _month_end(d: date) -> date:
    nxt = date(d.year + d.month // 12, d.month % 12 + 1, 1)
    return nxt - timedelta(days=1)


def _months_back(d: date, n: int) -> date:
    """The month-end ``n`` months before ``d``'s month."""
    idx = d.year * 12 + d.month - 1 - n
    return _month_end(date(idx // 12, idx % 12 + 1, 1))


class _Clock:
    """Period ends relative to the database's CURRENT_DATE, filled in by the fixture."""

    today: date
    qe: date  # the default latest quarter end, 4 months back
    fy_new: date  # an FY newer than ``qe``
    fy_old: date  # an FY older than ``qe``
    session: date


CLOCK = _Clock()


def _period(
    conn: psycopg.Connection[Any],
    iid: int,
    end: date,
    ptype: str,
    *,
    currency: str | None = "USD",
    filed: date | None = None,
    **items: str | None,
) -> None:
    cols = [psycopg.sql.Identifier(c) for c in items]
    conn.execute(
        psycopg.sql.SQL(
            "INSERT INTO financial_periods (instrument_id, period_start_date, period_end_date, period_type, "
            " fiscal_year, source, source_ref, reported_currency, filed_date, superseded_at, "
            " normalization_status{cols}) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, 'normalized'{vals})"
        ).format(
            cols=psycopg.sql.SQL("").join(psycopg.sql.SQL(", {}").format(c) for c in cols),
            vals=psycopg.sql.SQL(", %s::numeric" * len(cols)),
        ),
        (
            iid,
            end - timedelta(days=364 if ptype == "FY" else 90),
            end,
            ptype,
            end.year,
            f"test-{end}",  # one source per period end keeps the (source, fiscal_year, type) label unique
            f"c{iid}{end}{ptype}",
            currency,
            filed,
            *items.values(),
        ),
    )


def _four_quarters(conn: psycopg.Connection[Any], iid: int, latest: date, equity: str | None) -> None:
    for back in (273, 182, 91):
        _period(conn, iid, latest - timedelta(days=back), "Q1", shareholders_equity="1")
    _period(conn, iid, latest, "Q2", shareholders_equity=equity)


def _seed_cases(conn: psycopg.Connection[Any]) -> None:
    c = CLOCK
    q = c.qe
    # 1. newer FY.
    _period(conn, NEWER_FY, q, "Q2", shareholders_equity="100", total_assets="1000")
    _period(conn, NEWER_FY, c.fy_new, "FY", shareholders_equity="200", total_assets="2000")
    # 2. an older FY fills a NULL quarter item; the quarter's own non-NULL item is newer and stays.
    _period(conn, FILL, q, "Q2", shareholders_equity=None, goodwill=None, total_assets="1000")
    _period(conn, FILL, c.fy_old, "FY", shareholders_equity="50", goodwill="7", total_assets="500")
    # 3. a quarter value newer than a non-NULL FY.
    _period(conn, Q_NEWER, q, "Q2", shareholders_equity="100")
    _period(conn, Q_NEWER, c.fy_old, "FY", shareholders_equity="50")
    # 4. equal ends: the annual value wins unless it is missing.
    for iid, annual in ((TIE_PRESENT, "300"), (TIE_MISSING, None)):
        _period(conn, iid, q, "Q4", shareholders_equity="100")
        _period(conn, iid, q, "FY", shareholders_equity=annual)
    # 5. no FY.
    _period(conn, NO_FY, q, "Q2", shareholders_equity="100")
    # 6. an FY ending in the month 18 months back is usable through this month-end; one month earlier is not.
    for iid, back in ((BOUND_IN, 18), (BOUND_OUT, 19)):
        fy = _months_back(c.today, back)
        _period(conn, iid, fy - timedelta(days=10), "Q2", shareholders_equity="100")
        _period(conn, iid, fy, "FY", shareholders_equity="200")
    # 7. supersession. The quarter is 6 months back, an older FY 4 months back holds a distinctive value, and the
    # latest FY 1 month back is NULL for the item or in another currency.
    q6 = _months_back(c.today, 6)
    for iid, latest_eq, latest_cur in ((SUP_FY_NULL, None, "USD"), (SUP_FY_CURRENCY, "200", "EUR")):
        _period(conn, iid, q6, "Q2", shareholders_equity="100")
        _period(conn, iid, _months_back(c.today, 4), "FY", shareholders_equity="999")
        _period(conn, iid, c.fy_new, "FY", currency=latest_cur, shareholders_equity=latest_eq)
    # The latest quarter's item is NULL over an older quarter that holds it: the usable FY's value, else NULL.
    for iid in (SUP_Q_NULL, SUP_Q_NULL_NO_FY):
        _period(conn, iid, q - timedelta(days=91), "Q1", shareholders_equity="888")
        _period(conn, iid, q, "Q2", shareholders_equity=None)
    _period(conn, SUP_Q_NULL, _months_back(c.today, 10), "FY", shareholders_equity="50")
    # 8. a currency mismatch refuses an annual value that would otherwise win (the NULL variants: test 8b).
    _period(conn, CUR_MISMATCH, q, "Q2", shareholders_equity="100")
    _period(conn, CUR_MISMATCH, c.fy_new, "FY", currency="EUR", shareholders_equity="200")
    # 9. mixed provenance: the quarter is newer for equity, the FY fills goodwill, both lack cash.
    _period(conn, MIXED, q, "Q2", shareholders_equity="100", goodwill=None, cash=None)
    _period(conn, MIXED, c.fy_old, "FY", shareholders_equity="50", goodwill="7", cash=None)
    # 10. departure 4: an expired quarter supplies an item the usable FY lacks; an expired FY newer than an expired
    # quarter gives the quarter's value, today's.
    _period(conn, EXPIRED_Q, _months_back(c.today, 24), "Q2", shareholders_equity="100", goodwill="3")
    _period(conn, EXPIRED_Q, _months_back(c.today, 2), "FY", shareholders_equity="200", goodwill=None)
    _period(conn, EXPIRED_BOTH, _months_back(c.today, 30), "Q2", shareholders_equity="100")
    _period(conn, EXPIRED_BOTH, _months_back(c.today, 24), "FY", shareholders_equity="200")
    # 12. P/B. A 5e9 cap at the session; the legacy arm has a snapshot row and one quarter, the complete arm four.
    for iid in (PB_NEWER_FY, PB_FILL, PB_BOUND_IN, PB_CROSSING):
        conn.execute(
            "INSERT INTO fundamentals_snapshot (instrument_id, as_of_date, cash, debt, shares_outstanding, book_value) "
            "VALUES (%s, '2025-01-01', 1e9, 2e9, 1e8, 10)",
            (iid,),
        )
    bound_fy = _months_back(c.today, 18)
    for legacy, complete in ((PB_NEWER_FY, PB_NEWER_FY_COMPLETE), (PB_FILL, PB_FILL_COMPLETE)):
        newer = legacy == PB_NEWER_FY
        q_eq = "1e9" if newer else None
        _period(conn, legacy, q, "Q2", shareholders_equity=q_eq)
        _four_quarters(conn, complete, q, q_eq)
        for iid in (legacy, complete):
            _period(conn, iid, c.fy_new if newer else c.fy_old, "FY", shareholders_equity="2e9")
    _period(conn, PB_BOUND_IN, bound_fy - timedelta(days=10), "Q2", shareholders_equity="1e9")
    _four_quarters(conn, PB_BOUND_IN_COMPLETE, bound_fy - timedelta(days=10), "1e9")
    for iid in (PB_BOUND_IN, PB_BOUND_IN_COMPLETE):
        _period(conn, iid, bound_fy, "FY", shareholders_equity="2e9")
    # Departure 5's crossing: the FY's bound (last month-end) is past at CURRENT_DATE but not at the session, so the
    # view returns the quarter, whose bound (the same month-end) P/B reads at the session.
    cross_fy = _months_back(c.today, 19)
    _period(conn, PB_CROSSING, cross_fy - timedelta(days=10), "Q2", shareholders_equity="1e9")
    _period(conn, PB_CROSSING, cross_fy, "FY", shareholders_equity="2e9")
    for iid in (*PB_LEGACY[:3], *PB_COMPLETE):
        seed_pit_step(conn, iid, session=c.session, close=50, shares=1e8)
    seed_pit_step(conn, PB_CROSSING, session=_months_back(c.today, 1) - timedelta(days=5), close=50, shares=1e8)
    # 14. the latest quarter end duplicated on one filed_date with differing values; an older-filed duplicate.
    _period(conn, TIE_FILED, q, "Q2", filed=q + timedelta(days=40), shareholders_equity="1", cash="11")
    _period(conn, TIE_FILED, q, "Q3", filed=q + timedelta(days=40), shareholders_equity="2", cash="22")
    _period(conn, TIE_FILED, q, "Q1", filed=q + timedelta(days=30), shareholders_equity="3", cash="33")


@pytest.fixture
def _seed(ebull_test_conn: psycopg.Connection[Any]) -> psycopg.Connection[Any]:
    conn = ebull_test_conn
    row = conn.execute("SELECT CURRENT_DATE").fetchone()
    assert row is not None
    CLOCK.today = row[0]
    CLOCK.qe = _months_back(CLOCK.today, 4)
    CLOCK.fy_new = _months_back(CLOCK.today, 1)
    CLOCK.fy_old = _months_back(CLOCK.today, 7)
    CLOCK.session = CLOCK.today - timedelta(days=3)
    for iid in ALL_IDS:
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, currency) "
            "VALUES (%s, %s, %s, TRUE, 'USD')",
            (iid, f"S{iid}", f"Stock Combine {iid}"),
        )
    _seed_cases(conn)
    conn.commit()
    return conn


def _ttm(conn: psycopg.Connection[Any], iid: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute("SELECT * FROM financial_periods_ttm WHERE instrument_id = %s", (iid,)).fetchone()
    assert row is not None
    return row


def _eq(conn: psycopg.Connection[Any], iid: int) -> tuple[Decimal | None, date | None]:
    row = _ttm(conn, iid)
    return row["shareholders_equity"], row["shareholders_equity_end"]


def _apply(conn: psycopg.Connection[Any], path: Path, clock: date | None = None) -> None:
    body = re.sub(r"^(BEGIN|COMMIT);$", "", path.read_text(), flags=re.M)
    if clock is not None:
        assert body.count("CURRENT_DATE <=") == 1
        body = body.replace("CURRENT_DATE", f"DATE '{clock.isoformat()}'")
    conn.execute(body)  # type: ignore[arg-type]  # a reviewed file, not user input


@pytest.mark.db
def test_1_newer_fy_supplies_its_items(_seed: psycopg.Connection[Any]) -> None:
    row = _ttm(_seed, NEWER_FY)
    assert row["ttm_end"] == CLOCK.qe
    assert row["shareholders_equity"] == 200
    assert row["total_assets"] == 2000
    assert row["shareholders_equity_end"] == CLOCK.fy_new


@pytest.mark.db
def test_2_older_fy_fills_a_null_quarter_item(_seed: psycopg.Connection[Any]) -> None:
    row = _ttm(_seed, FILL)
    assert row["shareholders_equity"] == 50
    assert row["goodwill"] == 7
    assert row["total_assets"] == 1000  # the quarter's own value, newer
    assert row["shareholders_equity_end"] == CLOCK.fy_old


@pytest.mark.db
def test_3_a_newer_quarter_value_wins(_seed: psycopg.Connection[Any]) -> None:
    assert _eq(_seed, Q_NEWER) == (100, CLOCK.qe)


@pytest.mark.db
@pytest.mark.parametrize(("iid", "expected"), [(TIE_PRESENT, 300), (TIE_MISSING, 100)])
def test_4_equal_ends_keep_the_annual_value_unless_missing(
    _seed: psycopg.Connection[Any], iid: int, expected: int
) -> None:
    assert _eq(_seed, iid) == (expected, CLOCK.qe)


@pytest.mark.db
def test_5_no_fy_keeps_the_quarter(_seed: psycopg.Connection[Any]) -> None:
    assert _eq(_seed, NO_FY) == (100, CLOCK.qe)


@pytest.mark.db
@pytest.mark.parametrize(("iid", "back", "annual"), [(BOUND_IN, 18, True), (BOUND_OUT, 19, False)])
def test_6_the_annual_row_is_usable_through_its_bound_month_end(
    _seed: psycopg.Connection[Any], iid: int, back: int, annual: bool
) -> None:
    fy = _months_back(CLOCK.today, back)
    assert _eq(_seed, iid) == ((200, fy) if annual else (100, fy - timedelta(days=10)))


@pytest.mark.db
def test_6_day_granularity_bound_through_the_view_body(_seed: psycopg.Connection[Any]) -> None:
    """sql/512's own body under literal clocks: usable on the bound's month-end, unusable the day after."""
    fy = _months_back(CLOCK.today, 18)
    bound = _month_end(CLOCK.today)
    got = {}
    for clock in (bound, bound + timedelta(days=1)):
        _apply(_seed, SQL_512, clock=clock)
        got[clock] = _eq(_seed, BOUND_IN)
        _seed.rollback()
    assert got == {bound: (200, fy), bound + timedelta(days=1): (100, fy - timedelta(days=10))}


@pytest.mark.db
def test_the_deployed_view_reads_current_date(_seed: psycopg.Connection[Any]) -> None:
    row = _seed.execute("SELECT pg_get_viewdef('financial_periods_ttm'::regclass)").fetchone()
    assert row is not None
    # Tolerant of the deparser's case and spacing; test 6 pins the behaviour through the view itself.
    assert re.search(r"current_date\s*<=", row[0], re.IGNORECASE)


@pytest.mark.db
@pytest.mark.parametrize("iid", [SUP_FY_NULL, SUP_FY_CURRENCY])
def test_7_an_unusable_latest_fy_never_makes_an_older_fy_eligible(_seed: psycopg.Connection[Any], iid: int) -> None:
    assert _eq(_seed, iid) == (100, _months_back(CLOCK.today, 6))


@pytest.mark.db
@pytest.mark.parametrize(("iid", "expected"), [(SUP_Q_NULL, "fy"), (SUP_Q_NULL_NO_FY, None)])
def test_7_a_null_latest_quarter_item_never_reads_an_older_quarter(
    _seed: psycopg.Connection[Any], iid: int, expected: str | None
) -> None:
    assert _eq(_seed, iid) == ((50, _months_back(CLOCK.today, 10)) if expected else (None, None))


@pytest.mark.db
def test_8_a_currency_mismatch_refuses_the_annual_row(_seed: psycopg.Connection[Any]) -> None:
    assert _eq(_seed, CUR_MISMATCH) == (100, CLOCK.qe)


@pytest.mark.db
@pytest.mark.parametrize(("q_cur", "a_cur"), [("USD", None), (None, "USD"), (None, None)])
def test_8b_a_null_currency_on_either_side_refuses_the_annual_row(
    _seed: psycopg.Connection[Any], q_cur: str | None, a_cur: str | None
) -> None:
    _seed.execute("ALTER TABLE financial_periods ALTER COLUMN reported_currency DROP NOT NULL")
    _seed.execute(
        "UPDATE financial_periods SET reported_currency = CASE period_type WHEN 'FY' THEN %s::text ELSE %s::text END "
        "WHERE instrument_id = %s",
        (a_cur, q_cur, NEWER_FY),
    )
    got = _eq(_seed, NEWER_FY)
    _seed.rollback()
    assert got == (100, CLOCK.qe)
    assert _eq(_seed, NEWER_FY) == (200, CLOCK.fy_new)  # the same rows in one currency


@pytest.mark.db
def test_9_mixed_provenance_on_one_instrument(_seed: psycopg.Connection[Any]) -> None:
    row = _ttm(_seed, MIXED)
    assert (row["shareholders_equity"], row["goodwill"], row["cash"]) == (100, 7, None)
    assert row["shareholders_equity_end"] == CLOCK.qe


@pytest.mark.db
def test_10_departure_4_expired_quarters_are_read_as_today(_seed: psycopg.Connection[Any]) -> None:
    row = _ttm(_seed, EXPIRED_Q)
    assert (row["shareholders_equity"], row["goodwill"]) == (200, 3)
    assert row["shareholders_equity_end"] == _months_back(CLOCK.today, 2)
    assert _eq(_seed, EXPIRED_BOTH) == (100, _months_back(CLOCK.today, 30))


def _pb(conn: psycopg.Connection[Any], iid: int) -> tuple[Any, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute(
            "SELECT is_complete_ttm, pb_ratio FROM instrument_valuation WHERE instrument_id = %s", (iid,)
        ).fetchone()
    assert row is not None
    return row["is_complete_ttm"], row["pb_ratio"]


@pytest.mark.db
@pytest.mark.parametrize(
    ("iid", "arm", "pb", "pb_509"),
    [
        # 5e9 / 2e9: the newer FY's equity; sql/509 refused on the later stored FY.
        (PB_NEWER_FY, None, Decimal("2.5"), None),
        (PB_NEWER_FY_COMPLETE, True, Decimal("2.5"), None),
        # The FY fills the quarter's NULL equity; sql/509 had no equity.
        (PB_FILL, None, Decimal("2.5"), None),
        (PB_FILL_COMPLETE, True, Decimal("2.5"), None),
        # The FY in its bound's last month; sql/509 bounded the quarter it read, and refused on the later FY.
        (PB_BOUND_IN, None, Decimal("2.5"), None),
        (PB_BOUND_IN_COMPLETE, True, Decimal("2.5"), None),
        # Departure 5: the view returns the quarter (the FY expired at CURRENT_DATE); P/B bounds it at the session.
        (PB_CROSSING, None, Decimal("5"), None),
    ],
)
def test_12_pb_in_both_arms(
    _seed: psycopg.Connection[Any], iid: int, arm: bool | None, pb: Decimal, pb_509: Decimal | None
) -> None:
    assert _pb(_seed, iid) == (arm, pb)
    _apply(_seed, ROLLBACK_512)
    control = _pb(_seed, iid)
    _seed.rollback()
    assert control == (arm, pb_509)


@pytest.mark.db
def test_12_the_crossing_reads_the_quarter_at_the_session(_seed: psycopg.Connection[Any]) -> None:
    cross_fy = _months_back(CLOCK.today, 19)
    assert _eq(_seed, PB_CROSSING) == (Decimal("1e9"), cross_fy - timedelta(days=10))
    row = _seed.execute("SELECT session FROM instrument_valuation WHERE instrument_id = %s", (PB_CROSSING,)).fetchone()
    assert row is not None
    assert row[0] <= _months_back(CLOCK.today, 1) < CLOCK.today


_META_SQL = """
SELECT c.relname, pg_get_viewdef(c.oid), pg_get_userbyid(c.relowner), c.relacl::text,
       obj_description(c.oid, 'pg_class'),
       (SELECT array_agg(format('%s %s', attname, format_type(atttypid, atttypmod)) ORDER BY attnum)
        FROM pg_attribute WHERE attrelid = c.oid AND attnum > 0 AND NOT attisdropped)
FROM pg_class c
WHERE c.relname IN ('financial_periods_ttm', 'instrument_valuation') AND c.relnamespace = 'public'::regnamespace
ORDER BY c.relname
"""


def _meta(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    return conn.execute(_META_SQL).fetchall()


def _binds(conn: psycopg.Connection[Any]) -> bool:
    row = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_depend d JOIN pg_rewrite r ON r.oid = d.objid "
        "WHERE d.classid = 'pg_rewrite'::regclass AND d.refobjid = 'financial_periods_ttm'::regclass "
        "AND r.ev_class = 'instrument_valuation'::regclass)"
    ).fetchone()
    assert row is not None
    return row[0]


@pytest.mark.db
def test_13_rollback_restores_sql_508_and_509(_seed: psycopg.Connection[Any]) -> None:
    forward = _meta(_seed)
    assert forward[0][5][-1] == "shareholders_equity_end date"
    _apply(_seed, ROLLBACK_512)
    restored = _meta(_seed)
    assert _binds(_seed)
    old_values = (_eq_508(_seed, NEWER_FY), _eq_508(_seed, FILL))
    _seed.rollback()
    # sql/508's and sql/509's definitions as created on a schema without either view.
    _seed.execute("DROP VIEW instrument_valuation")
    _seed.execute("DROP VIEW financial_periods_ttm")
    _apply(_seed, SQL_508)
    _apply(_seed, SQL_509)
    fresh = _meta(_seed)
    _seed.rollback()
    assert restored == fresh
    assert all(m[2] == forward[0][2] and m[3] is None for m in restored)
    assert all(m[4] for m in restored)
    # The old values: the latest quarter's equity, NULL for the NULL quarter item.
    assert old_values == (100, None)


def _eq_508(conn: psycopg.Connection[Any], iid: int) -> Any:
    row = conn.execute("SELECT shareholders_equity FROM financial_periods_ttm WHERE instrument_id = %s", (iid,))
    got = row.fetchone()
    assert got is not None
    return got[0]


@pytest.mark.db
def test_13_rollback_refuses_another_dependent(_seed: psycopg.Connection[Any]) -> None:
    _seed.execute("CREATE VIEW t3919_dependent AS SELECT instrument_id FROM financial_periods_ttm")
    with pytest.raises(psycopg.errors.RaiseException, match="t3919_dependent -> financial_periods_ttm"):
        _apply(_seed, ROLLBACK_512)
    _seed.rollback()


@pytest.mark.db
def test_14_a_filed_date_tie_returns_one_whole_candidate(_seed: psycopg.Connection[Any]) -> None:
    row = _ttm(_seed, TIE_FILED)
    got = (row["shareholders_equity"], row["cash"], row["shareholders_equity_end"])
    assert got in ((1, 11, CLOCK.qe), (2, 22, CLOCK.qe))
    assert all(row[c] is None for c in ITEMS if c not in ("shareholders_equity", "cash"))
