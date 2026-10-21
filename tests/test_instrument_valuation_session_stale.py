"""#3923: ``instrument_valuation`` refuses a cap whose session is a month or more before the build date (sql/513).

Spec ``docs/specs/fundamentals/2026-10-19-3850-live-cap-share-basis.md`` Amendment 2, acceptance item 2. Every test
inserts its build row with an explicit ``build_date`` before seeding, because ``seed_pit_step`` otherwise dates the
shared build at the first seeded session. Instruments are on the legacy arm (``fundamentals_snapshot``), where
``p_fcf_ratio`` and ``fcf_yield`` are published with the cap, so their NULLs prove the refusal reached them.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import psycopg
import psycopg.rows
import pytest

from tests.fixtures.pit_market_cap import BUILD_ID, seed_pit_step

COUNT_BASED = (
    "market_cap_live",
    "enterprise_value",
    "pe_ratio",
    "pb_ratio",
    "price_sales",
    "p_fcf_ratio",
    "fcf_yield",
    "ev_revenue",
    "ev_ebitda",
    "dividend_yield",
)


def _build(conn: psycopg.Connection[Any], build_id: int, build_date: date) -> None:
    conn.execute(
        "INSERT INTO market_cap_pit_builds (build_id, started_at, finished_at, status, build_date, rule_version, "
        "submissions_sha256, summary) VALUES (%s, now(), now(), 'complete', %s, 'r', 's', '{}')",
        (build_id, build_date),
    )


def _instrument(conn: psycopg.Connection[Any], iid: int) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (iid, f"S{iid}", f"Stale {iid}"),
    )
    conn.execute(
        "INSERT INTO fundamentals_snapshot "
        "(instrument_id, as_of_date, revenue_ttm, gross_margin, operating_margin, "
        " fcf, cash, debt, net_debt, shares_outstanding, book_value, eps) "
        "VALUES (%s, '2025-01-01', 1e11, 0.5, 0.3, 5e10, 1e10, 2e10, 1e10, 1e10, 30, 6)",
        (iid,),
    )


def _val(conn: psycopg.Connection[Any], iid: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute("SELECT * FROM instrument_valuation WHERE instrument_id = %s", (iid,)).fetchone()
    assert row is not None
    return row


def _assert_published(row: dict[str, Any]) -> None:
    assert row["market_cap_reason"] is None
    assert row["market_cap_live"] is not None and row["p_fcf_ratio"] is not None


def _assert_refused(row: dict[str, Any], reason: str) -> None:
    assert row["market_cap_reason"] == reason
    for col in COUNT_BASED:
        assert row[col] is None, col


@pytest.mark.db
@pytest.mark.parametrize(
    ("build_date", "refused", "published"),
    [
        (date(2026, 10, 21), date(2026, 9, 21), date(2026, 9, 22)),
        # 2026-03-01 is 30 days before the build: a fixed 30-day bound would refuse it.
        (date(2026, 3, 31), date(2026, 2, 28), date(2026, 3, 1)),
        (date(2028, 2, 29), date(2028, 1, 29), date(2028, 1, 30)),
    ],
)
def test_calendar_month_bound(
    ebull_test_conn: psycopg.Connection[Any], build_date: date, refused: date, published: date
) -> None:
    conn = ebull_test_conn
    _build(conn, BUILD_ID, build_date)
    for iid, session in ((201, refused), (202, published)):
        _instrument(conn, iid)
        seed_pit_step(conn, iid, session=session, close=150, shares=1e9)
    conn.commit()
    stale = _val(conn, 201)
    _assert_refused(stale, "session_stale")
    assert stale["session"] == refused and stale["current_price"] == 150
    # PIT's own row is unchanged: the refusal is the view's.
    assert stale["market_cap_pit"]["reason"] is None
    _assert_published(_val(conn, 202))


@pytest.mark.db
def test_an_already_refused_row_keeps_its_reason(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _build(conn, BUILD_ID, date(2026, 10, 21))
    _instrument(conn, 203)
    seed_pit_step(conn, 203, session=date(2026, 6, 1), close=150, shares=1e9, refusal="no_left_anchor")
    conn.commit()
    _assert_refused(_val(conn, 203), "no_left_anchor")


@pytest.mark.db
def test_a_bar_after_the_build_does_not_lift_the_refusal_until_a_build_covers_it(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    conn = ebull_test_conn
    _build(conn, BUILD_ID, date(2026, 10, 21))
    _instrument(conn, 204)
    seed_pit_step(conn, 204, session=date(2026, 9, 1), close=150, shares=1e9, valid_to=date(2026, 10, 22))
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close, volume) VALUES (204, '2026-10-20', 160, 1)"
    )
    conn.commit()
    # Amendment 1 holds the session at the build's frontier, so the new bar does not price a cap yet.
    row = _val(conn, 204)
    _assert_refused(row, "session_stale")
    assert row["session"] == date(2026, 9, 1) and row["current_price"] == 160

    # The next build saw the new bar: the session moves inside the bound and the cap publishes.
    _build(conn, BUILD_ID + 1, date(2026, 10, 21))
    conn.execute(
        "UPDATE instrument_share_count_pit SET build_id = %s, bars_through = '2026-10-20' WHERE instrument_id = 204",
        (BUILD_ID + 1,),
    )
    conn.commit()
    row = _val(conn, 204)
    _assert_published(row)
    assert row["session"] == date(2026, 10, 20) and row["valuation_price"] == 160


@pytest.mark.db
def test_a_later_build_ages_a_session_with_no_new_bar(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _build(conn, BUILD_ID, date(2026, 10, 21))
    _instrument(conn, 205)
    seed_pit_step(conn, 205, session=date(2026, 10, 1), close=150, shares=1e9)
    conn.commit()
    _assert_published(_val(conn, 205))

    _build(conn, BUILD_ID + 1, date(2026, 11, 5))
    conn.execute("UPDATE instrument_share_count_pit SET build_id = %s WHERE instrument_id = 205", (BUILD_ID + 1,))
    conn.commit()
    _assert_refused(_val(conn, 205), "session_stale")


@pytest.mark.db
def test_a_step_always_has_a_dated_build(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """sql/513 skips no age check on a NULL build_date because none can exist (sql/499).

    ``instrument_share_count_pit.build_id`` is NOT NULL with a deferred foreign key to ``market_cap_pit_builds``, and
    ``build_date`` is NOT NULL there; a cap publishes only with a PIT row, which needs a step. This pins both.
    """
    conn = ebull_test_conn
    nullable = conn.execute(
        "SELECT table_name, column_name, is_nullable FROM information_schema.columns "
        "WHERE (table_name, column_name) IN "
        "(('instrument_share_count_pit', 'build_id'), ('market_cap_pit_builds', 'build_date')) ORDER BY 1"
    ).fetchall()
    assert nullable == [
        ("instrument_share_count_pit", "build_id", "NO"),
        ("market_cap_pit_builds", "build_date", "NO"),
    ]
    _instrument(conn, 206)
    conn.commit()
    _build(conn, BUILD_ID, date(2026, 10, 21))
    seed_pit_step(conn, 206, session=date(2026, 10, 20), close=150, shares=1e9)
    conn.execute("UPDATE instrument_share_count_pit SET build_id = 999999 WHERE instrument_id = 206")
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.commit()
