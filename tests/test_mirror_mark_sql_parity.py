"""#3086: the mirror mark rule exists twice — assert the two agree.

``app.api.copy_trading._compute_position_mtm`` resolves a mirror lot's mark in
Python (``resolve_quote_price`` → positive ``daily_close`` → ``open_rate``);
``app.services.portfolio._MIRROR_MARK_SQL`` resolves the *same* lot in SQL for
the aggregate figures.  They drifted once already — the SQL copy silently
dropped the bid/ask-mid tier, so `/portfolio`'s mirror VALUE disagreed with its
own drill-down.  A shared constant synchronises the two SQL call sites but does
nothing about the Python/SQL split, so this table-driven differential is the
thing that actually catches the next divergence.

Driven straight at the expression over a ``VALUES`` row, so no mirror fixture is
needed and every tier (including the ones no live row exercises) is reachable.
"""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.portfolio import _MIRROR_MARK_SQL
from app.services.valuation import resolve_quote_price
from tests.fixtures.ebull_test_db import (
    assert_test_db as _assert_test_db,
)
from tests.fixtures.ebull_test_db import (
    test_database_url as _test_database_url,
)
from tests.fixtures.ebull_test_db import (
    test_db_available as _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping mirror mark SQL parity test",
)

OPEN_RATE = Decimal("100")

# (id, last, bid, ask, close, expected)
#
# ``expected`` is stated literally rather than computed, so a change to the
# Python reference cannot silently redefine what the test asserts; the Python
# chain is then checked against the same literal below.
CASES: list[tuple[str, str | None, str | None, str | None, str | None, str]] = [
    # Tier 1 — a positive `last` wins, even against a mid that disagrees.
    ("last_wins_over_mid", "50", "80", "82", "70", "50"),
    ("last_wins_over_close", "50", None, None, "70", "50"),
    # Tier 2 — the tier #3086 restored. `quotes.last` is NULL, not 0
    # (sql/181_quotes_last_positive.sql CHECKs `last IS NULL OR last > 0`),
    # and the live book beats a prior-session close.
    ("mid_when_last_null", None, "80", "82", "70", "81"),
    ("mid_when_no_close", None, "80", "82", None, "81"),
    # Tier 2 is a *two-sided* book: one usable side is not a mark.
    ("bid_only_falls_to_close", None, "80", None, "70", "70"),
    ("ask_only_falls_to_close", None, None, "82", "70", "70"),
    ("zero_bid_falls_to_close", None, "0", "82", "70", "70"),
    ("zero_ask_falls_to_close", None, "80", "0", "70", "70"),
    ("negative_bid_falls_to_close", None, "-5", "82", "70", "70"),
    # Tier 3 — the close, and only when strictly positive.
    ("close_when_no_quote_row", None, None, None, "70", "70"),
    ("zero_close_falls_to_entry", None, None, None, "0", "100"),
    ("negative_close_falls_to_entry", None, None, None, "-3", "100"),
    # Tier 4 — nothing usable anywhere.
    ("entry_when_nothing", None, None, None, None, "100"),
    # NaN. PostgreSQL orders numeric NaN ABOVE every non-NaN value, so
    # `NaN > 0` is TRUE and a bare positivity guard would accept it as a
    # mark; Python's `nan > 0` is False. Both must reject it.
    ("nan_last_falls_through", "NaN", "80", "82", "70", "81"),
    ("nan_bid_falls_to_close", None, "NaN", "82", "70", "70"),
    ("nan_ask_falls_to_close", None, "80", "NaN", "70", "70"),
    ("nan_close_falls_to_entry", None, None, None, "NaN", "100"),
]


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        yield c
        c.rollback()


def _sql_mark(
    conn: psycopg.Connection[Any],
    last: str | None,
    bid: str | None,
    ask: str | None,
    close: str | None,
) -> Decimal:
    """Evaluate ``_MIRROR_MARK_SQL`` over one synthetic row.

    The fragment's aliases (``q`` / ``pd`` / ``cmp``) are supplied by three
    single-row ``VALUES`` clauses, so the expression under test is byte-for-byte
    the one both production call sites embed.
    """
    sql = f"""
        SELECT {_MIRROR_MARK_SQL} AS mark
        FROM (VALUES (%(last)s::numeric, %(bid)s::numeric, %(ask)s::numeric))
                 AS q(last, bid, ask),
             (VALUES (%(close)s::numeric)) AS pd(close),
             (VALUES (%(open_rate)s::numeric)) AS cmp(open_rate)
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {"last": last, "bid": bid, "ask": ask, "close": close, "open_rate": OPEN_RATE},
        )
        row = cur.fetchone()
    assert row is not None
    return Decimal(str(row[0]))


def _python_mark(
    last: str | None,
    bid: str | None,
    ask: str | None,
    close: str | None,
) -> Decimal:
    """The Python chain `_compute_position_mtm` runs, on the same inputs."""
    quote_price = resolve_quote_price(
        float(last) if last is not None else None,
        float(bid) if bid is not None else None,
        float(ask) if ask is not None else None,
    )
    if quote_price is not None:
        return Decimal(str(quote_price))
    close_f = float(close) if close is not None else None
    if close_f is not None and close_f > 0:
        return Decimal(str(close_f))
    return OPEN_RATE


@pytest.mark.parametrize(("case", "last", "bid", "ask", "close", "expected"), CASES)
def test_sql_mark_matches_declared_expectation(
    conn: psycopg.Connection[Any],
    case: str,
    last: str | None,
    bid: str | None,
    ask: str | None,
    close: str | None,
    expected: str,
) -> None:
    assert _sql_mark(conn, last, bid, ask, close) == Decimal(expected), case


@pytest.mark.parametrize(("case", "last", "bid", "ask", "close", "expected"), CASES)
def test_python_mark_matches_declared_expectation(
    case: str,
    last: str | None,
    bid: str | None,
    ask: str | None,
    close: str | None,
    expected: str,
) -> None:
    assert _python_mark(last, bid, ask, close) == Decimal(expected), case
