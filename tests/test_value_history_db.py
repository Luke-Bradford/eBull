"""DB-backed test for the value-history ledger recompute (#1594 PR-B).

One integration test per genuinely-new mechanism (test-tiering 2026-06-07):
the ``trade_events``-driven recompute + persisted-snapshot overlay in
``get_value_history`` — closed positions enter then leave history, a
persisted ``portfolio_eod_snapshots`` row overrides its day, and markers
come from the ledger. The pure formula/carry-forward/overlay logic is
table-tested in ``tests/test_portfolio_value_history.py``; this test wires
it to real SQL.

Display currency is forced to USD and all fixtures are USD-native so no
FX is needed (``convert`` short-circuits when native == display) — the FX
carry-forward path is covered purely.

``trade_events`` / ``cash_ledger`` / ``price_daily`` / the snapshot tables
are not in the worker truncate list, so this test cleans up its own rows.
"""

from __future__ import annotations

from datetime import date, timedelta

import psycopg

from app.api.portfolio import get_value_history
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

_IID_OPEN = 789911  # position still open at `today`
_IID_CLOSED = 789912  # position opened then fully closed mid-window
_POS_OPEN = 991101
_POS_CLOSED = 991102
_POS_NULLPRICE = 991103  # NULL open price → excluded from line AND markers
_CASH_NOTE = "VHDB_PRB_TEST"


def _db_today(conn: psycopg.Connection[tuple]) -> date:
    cur = conn.execute("SELECT CURRENT_DATE")
    row = cur.fetchone()
    assert row is not None
    return row[0]


def _seed(conn: psycopg.Connection[tuple], today: date) -> None:
    conn.execute("UPDATE runtime_config SET display_currency = 'USD' WHERE id = TRUE")
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%(o)s, 'VHOPEN', 'Open Co', '4', 'USD', TRUE),
               (%(c)s, 'VHCLOSED', 'Closed Co', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        {"o": _IID_OPEN, "c": _IID_CLOSED},
    )
    # Ledger: P1 open 10u@100 (still open); P2 open 6u@50 then fully closed.
    open_at = today - timedelta(days=4)
    close_at = today - timedelta(days=2)
    conn.execute(
        """
        INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind,
                                  side, units, price, executed_at, investment_usd,
                                  realized_pnl_usd, source, raw_payload)
        VALUES
          -- P1: 10u @100 but only 900 invested (2x-ish leverage) → equity basis,
          -- not notional, must be used (cost_per_unit = 900/10 = 90).
          (%(po)s, %(o)s, %(o)s, 'open',  'buy',  10, 100, %(open)s,  900, NULL, 'etoro_sync', '{}'::jsonb),
          (%(pc)s, %(c)s, %(c)s, 'open',  'buy',   6,  50, %(open)s,  300, NULL, 'etoro_sync', '{}'::jsonb),
          (%(pc)s, %(c)s, %(c)s, 'close', 'sell',  6,  50, %(close)s, 300,    0, 'etoro_history', '{}'::jsonb),
          -- P3: NULL open price → excluded from the line; its close must NOT
          -- emit an orphan SELL marker (Codex ckpt-2 P2 / M2 guard).
          (%(pn)s, %(c)s, %(c)s, 'open',  'buy',   5, NULL, %(open)s,  250, NULL, 'etoro_sync', '{}'::jsonb),
          (%(pn)s, %(c)s, %(c)s, 'close', 'sell',  5,  50, %(close)s, 250,    0, 'etoro_history', '{}'::jsonb)
        ON CONFLICT DO NOTHING
        """,
        {
            "po": _POS_OPEN,
            "pc": _POS_CLOSED,
            "pn": _POS_NULLPRICE,
            "o": _IID_OPEN,
            "c": _IID_CLOSED,
            "open": open_at,
            "close": close_at,
        },
    )
    # Daily closes: P1 100→110→120; P2 flat 50.
    for offset, close_o in ((4, 100), (3, 100), (2, 110), (1, 110), (0, 120)):
        conn.execute(
            """
            INSERT INTO price_daily (instrument_id, price_date, close)
            VALUES (%(o)s, %(d)s, %(co)s), (%(c)s, %(d)s, 50)
            ON CONFLICT (instrument_id, price_date) DO UPDATE SET close = EXCLUDED.close
            """,
            {"o": _IID_OPEN, "c": _IID_CLOSED, "d": today - timedelta(days=offset), "co": close_o},
        )
    # Cash: +500 USD three days ago (exercises cash + cash_tracking_since).
    conn.execute(
        """
        INSERT INTO cash_ledger (event_time, event_type, amount, currency, note)
        VALUES (%(t)s, 'broker_sync', 500, 'USD', %(note)s)
        """,
        {"t": today - timedelta(days=3), "note": _CASH_NOTE},
    )
    # Persisted snapshot on `today` — overlay must override the recompute.
    conn.execute(
        """
        INSERT INTO portfolio_eod_snapshots (snapshot_date, display_currency, total_value,
                                             positions_value, cash_value, positions_total,
                                             positions_priced)
        VALUES (%(d)s, 'USD', 99999, 99999, 0, 1, 1)
        ON CONFLICT (snapshot_date) DO UPDATE SET total_value = EXCLUDED.total_value
        """,
        {"d": today},
    )
    conn.commit()


def _cleanup(conn: psycopg.Connection[tuple], today: date) -> None:
    conn.rollback()
    conn.execute(
        "DELETE FROM trade_events WHERE position_id IN (%(po)s, %(pc)s, %(pn)s)",
        {"po": _POS_OPEN, "pc": _POS_CLOSED, "pn": _POS_NULLPRICE},
    )
    conn.execute("DELETE FROM price_daily WHERE instrument_id IN (%(o)s, %(c)s)", {"o": _IID_OPEN, "c": _IID_CLOSED})
    conn.execute("DELETE FROM cash_ledger WHERE note = %(note)s", {"note": _CASH_NOTE})
    conn.execute("DELETE FROM portfolio_eod_snapshots WHERE snapshot_date = %(d)s", {"d": today})
    conn.execute("DELETE FROM instruments WHERE instrument_id IN (%(o)s, %(c)s)", {"o": _IID_OPEN, "c": _IID_CLOSED})
    conn.execute("UPDATE runtime_config SET display_currency = 'GBP' WHERE id = TRUE")
    conn.commit()


def test_ledger_recompute_closed_position_overlay_and_markers(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    today = _db_today(conn)
    _seed(conn, today)
    try:
        resp = get_value_history(range="1m", conn=conn)
        by_date = {p.date: p.value for p in resp.points}

        # Day of open: P1 equity 900 (cost 90/u, NOT notional 1000) + P2 6×50
        # = 300, cash not yet deposited → 1200. Notional pricing would read 1300.
        assert by_date[today - timedelta(days=4)] == 1200.0
        # +1 day: cash 500 lands → 900 + 300 + 500 = 1700. P2 still open.
        assert by_date[today - timedelta(days=3)] == 1700.0
        # Close day: P2 leaves (units→0). P1 equity 900+10·(110-100)=1000 + 500 = 1500.
        assert by_date[today - timedelta(days=2)] == 1500.0
        # Post-close day: recompute, P2 absent. P1 1000 + 500 = 1500.
        assert by_date[today - timedelta(days=1)] == 1500.0
        # Today: persisted snapshot overrides the recompute (would be 1600).
        assert by_date[today] == 99999.0

        # Closed position contributed before its close and not after.
        assert by_date[today - timedelta(days=4)] > by_date[today - timedelta(days=2)] - 500

        # cash_tracking_since = first cash_ledger date; fx historical.
        assert resp.cash_tracking_since == today - timedelta(days=3)
        assert resp.fx_mode == "historical"

        # Markers come from the ledger: open=BUY, close=SELL.
        events = {(e.symbol, e.side, e.units, e.source) for e in resp.events}
        assert ("VHOPEN", "BUY", 10.0, "open") in events
        assert ("VHCLOSED", "BUY", 6.0, "open") in events
        assert ("VHCLOSED", "SELL", 6.0, "close") in events
        # The NULL-open-price position (5 units) is excluded from the line and
        # emits NO marker — no orphan SELL without its basis (Codex ckpt-2 P2).
        assert not any(e.units == 5.0 for e in resp.events)
    finally:
        _cleanup(conn, today)
