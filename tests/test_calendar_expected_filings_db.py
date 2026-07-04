"""db-tier gate test for calendar expected-filings surfacing (#1907).

Pins the exact SQL gate the feature depends on: only an *open*-position
instrument's *unfulfilled*, *still-open-window* filing surfaces. Auto-marked
``db`` (pulls ``ebull_test_conn``).
"""

from __future__ import annotations

from datetime import date, timedelta

import psycopg

from app.api.calendar import calendar_events

_TODAY = date.today()
_FUTURE = _TODAY + timedelta(days=30)
_PAST = _TODAY - timedelta(days=10)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_position(conn: psycopg.Connection[tuple], iid: int, units: float) -> None:
    conn.execute(
        "INSERT INTO positions (instrument_id, current_units, source) VALUES (%s, %s, 'ebull')",
        (iid, units),
    )


def _seed_filing(
    conn: psycopg.Connection[tuple],
    iid: int,
    *,
    window_end: date,
    fulfilled: bool,
    window_start: date | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO expected_filings
            (instrument_id, expected_filing_type, anchor_period_end,
             expected_window_start, expected_window_end, fulfilled_at)
        VALUES (%s, '10-Q', %s, %s, %s, %s)
        """,
        (
            iid,
            window_end - timedelta(days=55),
            window_start or (window_end - timedelta(days=25)),
            window_end,
            _TODAY if fulfilled else None,
        ),
    )


def test_only_open_position_unfulfilled_open_window_surfaces(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    # expected_filings is UNIQUE per instrument (one next-expected filing each),
    # so each exclusion case gets its own instrument.

    # iid 1 — OPEN position, unfulfilled, still-open window -> the ONLY surface.
    _seed_instrument(conn, 1, "OPENCO")
    _seed_position(conn, 1, 100.0)
    _seed_filing(conn, 1, window_end=_FUTURE, fulfilled=False)

    # iid 2 — OPEN position, FULFILLED filing -> excluded.
    _seed_instrument(conn, 2, "FILEDCO")
    _seed_position(conn, 2, 100.0)
    _seed_filing(conn, 2, window_end=_FUTURE, fulfilled=True)

    # iid 3 — OPEN position, PAST window (window_end < today) -> excluded.
    _seed_instrument(conn, 3, "STALECO")
    _seed_position(conn, 3, 100.0)
    _seed_filing(conn, 3, window_end=_PAST, fulfilled=False)

    # iid 4 — CLOSED position (current_units = 0), valid filing -> excluded.
    _seed_instrument(conn, 4, "CLOSEDCO")
    _seed_position(conn, 4, 0.0)
    _seed_filing(conn, 4, window_end=_FUTURE, fulfilled=False)

    # iid 5 — not held / not watched, valid filing -> out of scope, excluded.
    _seed_instrument(conn, 5, "OTHERCO")
    _seed_filing(conn, 5, window_end=_FUTURE, fulfilled=False)
    conn.commit()

    events = calendar_events(conn=conn, scope="portfolio", days=7)

    surfaced = [(f.symbol, f.filing_type) for f in events.expected_filings]
    assert surfaced == [("OPENCO", "10-Q")]
    # The closed name must not leak into any event list either.
    assert "CLOSEDCO" not in {d.symbol for d in events.ex_dividends}
