from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services import momentum_book as mb
from app.services.momentum_book_job import form_book, formation_month_for
from app.services.strategy_control_plane import configure_deployment, create_strategy_trade, decide_funding

SESSION = date(2026, 10, 15)  # a Thursday
BASE = 991000
N_NAMES = 35
HELD_LOSER = BASE + 0  # the weakest name: held, must be exited


def _month_ends() -> list[date]:
    # 2025-09 .. 2026-09 (months t-12..t), one close per month.
    out = []
    for k in range(13):
        y, m = divmod(2025 * 12 + 8 + k, 12)
        out.append(date(y, m + 1, 25))
    return out


def _seed(conn: psycopg.Connection[Any]) -> None:
    conn.execute(
        "INSERT INTO etoro_instrument_types (instrument_type_id, description) VALUES (5, 'Stocks') "
        "ON CONFLICT DO NOTHING"
    )
    conn.execute(
        "INSERT INTO exchanges (exchange_id, description, asset_class) VALUES ('MBTEST', 'test', 'us_equity') "
        "ON CONFLICT DO NOTHING"
    )
    for k in range(N_NAMES):
        iid = BASE + k
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, instrument_type_id, is_tradable) "
            "VALUES (%s, %s, %s, 'MBTEST', 5, TRUE)",
            (iid, f"MB{k}", f"Momentum test {k}"),
        )
        price = 50.0
        drift = 0.001 * k  # higher id, stronger momentum
        for j, day in enumerate(_month_ends()):
            price *= 1 + drift + (0.01 if j % 2 else -0.01)
            conn.execute(
                "INSERT INTO price_daily (instrument_id, price_date, close, volume) VALUES (%s, %s, %s, 1000000)",
                (iid, day, price),
            )
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, close, volume) VALUES (%s, %s, %s, 1000000)",
            (iid, date(2026, 10, 14), price),
        )
    # A held momentum position on the weakest name, through the real control-plane writers.
    dep = configure_deployment(
        conn,
        strategy_id=mb.STRATEGY_ID,
        strategy_version=mb.STRATEGY_VERSION,
        mode="paper",
        capital_limit=Decimal("10000"),
        enabled=True,
        changed_by="test",
        reason="test",
    )
    sig = conn.execute(
        """
        INSERT INTO strategy_signals (strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind,
            verdict, fill_bar_date, fill_price, universe, input_rule_set_versions)
        VALUES (%s, %s, %s, '2026-09-01', 'entry', 'fired', '2026-09-02', 50, 'survivor_only', '{"momentum_book": "x"}')
        RETURNING signal_id
        """,
        (mb.STRATEGY_ID, mb.STRATEGY_VERSION, HELD_LOSER),
    ).fetchone()
    assert sig is not None
    fd = decide_funding(
        conn,
        signal_id=int(sig[0]),
        verdict="allocated",
        reason_code="test",
        deployment_id=dep.deployment_id,
        amount=Decimal("300"),
    )
    create_strategy_trade(conn, fd)
    conn.commit()


@pytest.mark.integration
def test_form_book_writes_entries_and_exits_once(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _seed(conn)

    formed = form_book(conn, session=SESSION)
    assert formed is not None and formed.created
    assert formed.formation_month == formation_month_for(SESSION) == date(2026, 9, 1)
    expected = tuple(BASE + k for k in range(N_NAMES - 1, N_NAMES - 1 - mb.BOOK_SIZE, -1))
    assert formed.book == expected  # strongest momentum first
    assert HELD_LOSER not in formed.book
    assert len(formed.entry_signal_ids) == mb.BOOK_SIZE  # the held name is not in the book
    assert len(formed.exit_trade_ids) == 1

    rows = conn.execute(
        """
        SELECT s.signal_kind, s.verdict, s.signal_bar_date, s.fill_bar_date, e.rank
        FROM momentum_book_entries e JOIN strategy_signals s USING (signal_id)
        WHERE e.formation_id = %s ORDER BY e.rank
        """,
        (formed.formation_id,),
    ).fetchall()
    assert [r[4] for r in rows] == list(range(1, mb.BOOK_SIZE + 1))
    assert {(r[0], r[1], r[2], r[3]) for r in rows} == {("entry", "fired", date(2026, 10, 14), SESSION)}
    exit_row = conn.execute(
        "SELECT instrument_id FROM momentum_book_exits WHERE formation_id = %s", (formed.formation_id,)
    ).fetchone()
    assert exit_row == (HELD_LOSER,)

    again = form_book(conn, session=SESSION)
    assert again is not None and not again.created
    assert again.formation_id == formed.formation_id
    assert again.entry_signal_ids == formed.entry_signal_ids
    assert again.exit_trade_ids == formed.exit_trade_ids
    count = conn.execute("SELECT count(*) FROM momentum_book_formations").fetchone()
    assert count == (1,)


@pytest.mark.integration
def test_form_book_refuses_a_closed_day(ebull_test_conn: psycopg.Connection[Any]) -> None:
    assert form_book(ebull_test_conn, session=date(2026, 10, 17)) is None  # Saturday


@pytest.mark.integration
def test_formations_are_insert_only(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _seed(conn)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    with pytest.raises(psycopg.errors.RaiseException, match="insert only"):
        conn.execute("DELETE FROM momentum_book_formations WHERE formation_id = %s", (formed.formation_id,))
    conn.rollback()
