"""#2414 — `price_daily_revision`, the per-bar overwrite log.

The sibling file (``test_market_data_bar_revision_counter_db.py``) pins the COUNTS.
This one pins the IDENTITY: one row per overwritten bar, carrying the write branch,
written inside the same transaction as the bar write.

⚠ Why a real DB: the row's existence is a property of transaction atomicity across
two tables, and the duplicate-date case depends on ``ON CONFLICT``'s real behaviour
within one transaction. A mocked cursor asserts whatever it was told to.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from app.providers.market_data import OHLCVBar
from app.services.market_data import most_recent_trading_day, refresh_market_data
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable",
)

_IID = 920_415


def _weekdays_back(end: date, n: int) -> list[date]:
    out: list[date] = []
    d = end
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return list(reversed(out))


def _bar(price_date: date, close: str) -> OHLCVBar:
    c = Decimal(close)
    return OHLCVBar(price_date=price_date, open=c, high=c, low=c, close=c, volume=1000)


def _seed(conn: psycopg.Connection[tuple], days: list[date], close: str) -> None:
    for d in days:
        conn.execute(
            "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
            "VALUES (%s, %s, %s, %s, %s, %s, 1000) ON CONFLICT DO NOTHING",
            (_IID, d, Decimal(close), Decimal(close), Decimal(close), Decimal(close)),
        )


def _instrument(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (%s, 'REVREC', 'Revision Record Co', TRUE) ON CONFLICT (instrument_id) DO NOTHING",
        (_IID,),
    )


def _revisions(conn: psycopg.Connection[tuple]) -> list[tuple[date, str]]:
    return [
        (r[0], r[1])
        for r in conn.execute(
            "SELECT price_date, cause FROM price_daily_revision WHERE instrument_id = %s ORDER BY revision_id",
            (_IID,),
        ).fetchall()
    ]


def _run(
    conn: psycopg.Connection[tuple],
    bars: list[OHLCVBar],
    *,
    extra_responses: list[list[OHLCVBar]] | None = None,
) -> None:
    provider = MagicMock()
    provider.get_daily_candles.side_effect = [bars, *(extra_responses or [])]
    refresh_market_data(
        provider,
        conn,
        instruments=[(_IID, "REVREC")],
        lookback_days=1000,
        skip_quotes=True,
    )


def test_one_row_per_overwritten_bar_with_the_branch_that_wrote_it(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    _instrument(conn)
    days = _weekdays_back(most_recent_trading_day(date.today()), 4)
    _seed(conn, days[:-1], "100")

    # days[-3] identical -> no write at all; days[-2] revised; days[-1] inserted.
    _run(conn, [_bar(days[-3], "100"), _bar(days[-2], "111"), _bar(days[-1], "123")])

    # ⚠ The INSERT and the IDENTICAL bar contribute NOTHING. This table is the
    # overwrite log, not a write log — an insert extends the series, it does not
    # destroy a value a decision was made on.
    assert _revisions(conn) == [(days[-2], "incremental")]


def test_a_heal_is_recorded_as_adjustment_heal_not_incremental(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The ordering `revision_cause` pins, asserted on the rows rather than the counters.

    A heal is reachable ONLY from the incremental branch, so a writer that
    reported ``fetch_reason`` first would label every heal ``incremental`` and
    the by-cause split would silently attribute split re-basings to ordinary
    daily maintenance.
    """
    conn = ebull_test_conn
    _instrument(conn)
    days = _weekdays_back(most_recent_trading_day(date.today()), 12)
    _seed(conn, days[:-1], "300")

    # Overlap comes back at a third of the stored basis — a 3:1 split, ratio 3.0,
    # over the 1.2 threshold — so the loop re-fetches the full history and heals.
    incremental = [_bar(d, "100") for d in days[-3:]]
    full_history = [_bar(d, "100") for d in days]
    _run(conn, incremental, extra_responses=[full_history])

    rows = _revisions(conn)
    assert rows, "the heal re-fetch rewrote nothing — the fixture is not exercising the branch"
    assert {cause for _, cause in rows} == {"adjustment_heal"}
    # Every bar that was actually on the old basis is named, not just the overlap.
    assert {d for d, _ in rows} == set(days[:-1])


def test_a_failure_after_the_upsert_leaves_no_revision_row_and_no_bar_change(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The property the write placement rests on.

    ``price_daily`` keeps no prior value, so a revision row that outlived a
    rolled-back write would assert a change nothing is left to contradict. The
    running counters are merged AFTER commit for the mirror-image reason
    (#1293) — they cannot roll back, and a row can.
    """
    conn = ebull_test_conn
    _instrument(conn)
    days = _weekdays_back(most_recent_trading_day(date.today()), 4)
    _seed(conn, days[:-1], "100")

    with patch(
        "app.services.market_data._compute_and_store_features",
        side_effect=RuntimeError("feature compute blew up after the upsert"),
    ):
        _run(conn, [_bar(days[-2], "111"), _bar(days[-1], "123")])

    assert _revisions(conn) == []
    stored = dict(
        conn.execute("SELECT price_date, close FROM price_daily WHERE instrument_id = %s", (_IID,)).fetchall()
    )
    # Both halves rolled back together, which is what makes the empty log honest.
    assert stored[days[-2]] == Decimal("100")
    assert days[-1] not in stored


def test_the_same_date_twice_in_one_payload_is_two_overwrites(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Why the table is append-only with no natural key.

    ``_normalise_candles`` flattens every group's inner array with no date dedup,
    so one provider response can carry a date more than once. Each occurrence
    that changes the stored value IS a separate overwrite; a
    ``UNIQUE (instrument_id, price_date)`` would raise on a payload the writer
    has to tolerate.
    """
    conn = ebull_test_conn
    _instrument(conn)
    days = _weekdays_back(most_recent_trading_day(date.today()), 4)
    _seed(conn, days[:-1], "100")

    # ⚠ Closes stay inside the 1.2 adjustment ratio (`_ADJUSTMENT_RATIO_THRESHOLD`).
    # A first draft used 100 -> 122, which is 1.22, tripped the split-cliff guard and
    # exercised the heal branch instead of this one.
    _run(
        conn,
        [_bar(days[-2], "100"), _bar(days[-2], "101"), _bar(days[-2], "102"), _bar(days[-1], "103")],
    )

    # First occurrence is identical and writes nothing; the next two each
    # overwrite the value the previous one left.
    assert _revisions(conn) == [(days[-2], "incremental"), (days[-2], "incremental")]


def test_a_cause_outside_the_closed_vocabulary_is_rejected(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The CHECK is the detector, so a typo is loud rather than a silent new bucket."""
    conn = ebull_test_conn
    _instrument(conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                "INSERT INTO price_daily_revision (instrument_id, price_date, cause) VALUES (%s, %s, 'incremenal')",
                (_IID, date(2026, 1, 2)),
            )
