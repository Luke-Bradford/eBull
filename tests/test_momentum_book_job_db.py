from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services import momentum_book as mb
from app.services.momentum_book_job import form_book, formation_month_for
from app.services.momentum_book_scorecard import load_scorecard
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
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


def _seed(conn: psycopg.Connection[Any], *, held: bool = True) -> None:
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
        conn.execute(
            "INSERT INTO price_quarantine_coverage (instrument_id, rule_set_version, first_bar, last_bar, "
            "bars_evaluated, transitions_evaluated) VALUES (%s, %s, %s, %s, 14, 13)",
            (iid, QUARANTINE_RULE_SET_VERSION, _month_ends()[0], date(2026, 10, 14)),
        )
    if not held:
        conn.commit()
        return
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


@pytest.mark.integration
def test_a_quarantined_close_is_not_scored(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    weakest = BASE + 0
    # A fake +900% close on the weakest name's month t-1 bar, quarantined as an unusable return.
    spike_day = _month_ends()[-2]
    conn.execute(
        "UPDATE price_daily SET close = close * 10 WHERE instrument_id = %s AND price_date = %s", (weakest, spike_day)
    )
    conn.execute(
        "INSERT INTO price_bar_quarantine (instrument_id, price_date, rule_set_version, range_usable, return_usable, "
        "provisional, rules) VALUES (%s, %s, %s, TRUE, FALSE, FALSE, ARRAY['test'])",
        (weakest, spike_day, QUARANTINE_RULE_SET_VERSION),
    )
    conn.commit()
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    assert weakest not in formed.book


@pytest.mark.integration
def test_a_missing_fill_close_defers_the_whole_formation(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    top = BASE + N_NAMES - 1
    conn.execute("DELETE FROM price_daily WHERE instrument_id = %s AND price_date >= '2026-10-08'", (top,))
    conn.commit()
    assert form_book(conn, session=SESSION) is None
    assert conn.execute("SELECT count(*) FROM momentum_book_formations").fetchone() == (0,)


@pytest.mark.integration
def test_form_book_records_its_universe(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _seed(conn)
    formed = form_book(conn, session=SESSION)
    assert formed is not None
    row = conn.execute(
        "SELECT universe, source FROM momentum_book_universes WHERE formation_id = %s", (formed.formation_id,)
    ).fetchone()
    assert row == ([BASE + k for k in range(N_NAMES)], "formation")


@pytest.mark.integration
def test_a_formation_without_a_universe_gets_one_reconstructed(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    fid = conn.execute(
        "INSERT INTO momentum_book_formations (strategy_version, formation_month, target_session, candidates, book, "
        "held) VALUES (%s, '2026-09-01', %s, 35, '[]', '[]') RETURNING formation_id",
        (mb.STRATEGY_VERSION, SESSION),
    ).fetchone()
    assert fid is not None
    conn.commit()
    existing = form_book(conn, session=SESSION)
    assert existing is not None and not existing.created
    row = conn.execute(
        "SELECT jsonb_array_length(universe), source FROM momentum_book_universes WHERE formation_id = %s", (fid[0],)
    ).fetchone()
    assert row == (N_NAMES, "reconstructed")


@pytest.mark.integration
def test_load_scorecard_scores_the_formation_against_spy(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    _seed(conn, held=False)
    spy = BASE + 500
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, exchange, instrument_type_id, is_tradable) "
        "VALUES (%s, 'SPY', 'SPDR S&P 500', 'MBTEST', 5, TRUE)",
        (spy,),
    )
    later = date(2026, 10, 26)
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, close) VALUES (%s, '2026-10-14', 100), (%s, %s, 102)",
        (spy, spy, later),
    )
    for k in range(N_NAMES):  # every name +10% to the 26th
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, close) "
            "SELECT instrument_id, %s, close * 1.1 FROM price_daily "
            "WHERE instrument_id = %s AND price_date = '2026-10-14'",
            (later, BASE + k),
        )
    conn.execute(
        "INSERT INTO price_quarantine_coverage (instrument_id, rule_set_version, first_bar, last_bar, "
        "bars_evaluated, transitions_evaluated) VALUES (%s, %s, '2026-10-14', %s, 2, 1)",
        (spy, QUARANTINE_RULE_SET_VERSION, later),
    )
    conn.execute(
        "UPDATE price_quarantine_coverage SET last_bar = %s WHERE instrument_id BETWEEN %s AND %s",
        (later, BASE, BASE + N_NAMES - 1),
    )
    conn.commit()
    formed = form_book(conn, session=SESSION)
    assert formed is not None

    card = load_scorecard(conn)
    (period,) = card.periods
    assert (period.start, period.end, period.names_priced) == (date(2026, 10, 14), later, mb.BOOK_SIZE)
    assert period.book_return == pytest.approx(0.1)
    assert period.spy_return == pytest.approx(0.02)
    assert period.control_median == pytest.approx(0.1)
    assert card.book_cumulative == pytest.approx(0.1)

    # A return-unusable end bar is not read: the name falls back to its 14th close, a stale mark at the 26th.
    conn.execute(
        "INSERT INTO price_bar_quarantine (instrument_id, price_date, rule_set_version, range_usable, return_usable, "
        "provisional, rules) VALUES (%s, %s, %s, TRUE, FALSE, FALSE, ARRAY['B1'])",
        (formed.book[0], later, QUARANTINE_RULE_SET_VERSION),
    )
    conn.commit()
    (period,) = load_scorecard(conn).periods
    assert (period.names_priced, period.names_stale) == (mb.BOOK_SIZE - 1, 1)
