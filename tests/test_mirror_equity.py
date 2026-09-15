"""§8.4 AUM identity tests for _load_mirror_equity.

Real test DB (ebull_test) — same isolation pattern as
tests/test_portfolio_sync_mirrors.py.
"""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.portfolio import _load_mirror_equity, load_mirror_breakdowns
from tests.fixtures.copy_mirrors import (
    mirror_aum_fixture,
    mtm_delta_mirror_fixture,
    no_quote_mirror_fixture,
)
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
    reason="ebull_test DB unavailable — skipping real-DB mirror equity test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    """Yield a fresh ebull_test connection with every table this
    test suite touches truncated at the start of each test.
    """
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            cur.execute(
                "TRUNCATE copy_mirror_positions, copy_mirrors, "
                "copy_traders, quotes, price_daily, scores, positions, "
                "cash_ledger, instruments RESTART IDENTITY CASCADE"
            )
        c.commit()
        yield c
        c.rollback()


def test_empty_copy_mirrors_returns_zero_not_none(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4 + §6.4 contract: empty copy_mirrors → float 0.0,
    not None. Regression test for the COALESCE(SUM(...), 0)
    contract and the dead-code-None-guard prevention rule.
    """
    result = _load_mirror_equity(conn)
    assert result == 0.0
    assert isinstance(result, float)


def test_no_quote_cost_basis_identity(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: available + SUM(amount) identity when no quotes exist.
    Matches the empirically-reconciled mirror 15712187 shape:
    2800.33 + 50.00 + 17039.33 = 19889.66.
    """
    no_quote_mirror_fixture(conn)
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == pytest.approx(19889.66, abs=1e-6)


def test_mtm_delta_with_fx(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: MTM delta is converted to USD using the entry-time
    conversion rate. Long position, quote above entry.
    """
    expected = mtm_delta_mirror_fixture(conn)
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == pytest.approx(float(expected), abs=1e-6)


def test_short_delta_positive_when_price_falls(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: short position is profitable when price falls —
    sign(-1) * positive_units * negative_delta * conv_rate → +USD.
    """
    expected = mtm_delta_mirror_fixture(
        conn,
        is_buy=False,
        quote_last=Decimal("1000.0"),
    )
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == pytest.approx(float(expected), abs=1e-6)
    # Sanity: short with price below entry should beat available+amount
    # by a positive delta.
    assert result > (2800.33 + 101.08)


def test_closed_mirror_excluded(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: WHERE m.active filter excludes closed mirrors.
    mirror_aum_fixture seeds one active (equity=1550.00) and one
    closed (would be 300.00 if not filtered). Expect 1550.00.
    """
    mirror_aum_fixture(conn)
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == pytest.approx(1550.00, abs=1e-6)


def test_all_mirrors_closed_returns_zero(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: if every mirror is active=FALSE, the WHERE filter
    leaves an empty result set → COALESCE returns 0.0.
    """
    mirror_aum_fixture(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == 0.0


# ---------------------------------------------------------------------------
# #3086 — the bid/ask-mid tier, on both mirror SQL paths
# ---------------------------------------------------------------------------
#
# `quotes.last` is NULL (not 0) for an un-freshly-traded instrument —
# `sql/181_quotes_last_positive.sql` CHECKs `last IS NULL OR last > 0` and its
# constraint COMMENT states the read-side contract: *"Read-side derives a mark
# from bid/ask when NULL."*  Both cases below seed a `price_daily.close` that
# DIFFERS from the mid, so passing requires the mid specifically — a test where
# the two agree would pass against the pre-#3086 SQL as well.

_MID = Decimal("1400.0")  # (1399.0 + 1401.0) / 2
_STALE_CLOSE = Decimal("1300.0")


def test_mid_is_used_when_last_is_null_load_mirror_equity(
    conn: psycopg.Connection[Any],
) -> None:
    """#3086: live two-sided book beats the previous session's close."""
    expected = mtm_delta_mirror_fixture(
        conn,
        quote_last=None,
        quote_bid=Decimal("1399.0"),
        quote_ask=Decimal("1401.0"),
        daily_close=_STALE_CLOSE,
        expected_mark=_MID,
    )
    conn.commit()

    assert _load_mirror_equity(conn) == pytest.approx(float(expected), abs=1e-6)


def test_mid_is_used_when_last_is_null_load_mirror_breakdowns(
    conn: psycopg.Connection[Any],
) -> None:
    """#3086: the per-mirror path marks identically to the aggregate one.

    The two restate the rule in separate SQL statements, which is how they
    could have diverged; both now embed ``_MIRROR_MARK_SQL``.
    """
    expected = mtm_delta_mirror_fixture(
        conn,
        quote_last=None,
        quote_bid=Decimal("1399.0"),
        quote_ask=Decimal("1401.0"),
        daily_close=_STALE_CLOSE,
        expected_mark=_MID,
    )
    conn.commit()

    breakdowns = load_mirror_breakdowns(conn)
    assert len(breakdowns) == 1
    assert breakdowns[0].mirror_equity_usd == pytest.approx(float(expected), abs=1e-6)
    assert breakdowns[0].mirror_equity_usd == pytest.approx(_load_mirror_equity(conn), abs=1e-6)


def test_one_sided_book_falls_through_to_the_close(
    conn: psycopg.Connection[Any],
) -> None:
    """#3086, the other direction: a mid needs BOTH sides positive.

    Without this, a fix that used `bid` alone — or averaged a zero side —
    would pass the two tests above.

    The absent side is `0`, not NULL: `quotes.bid` / `quotes.ask` are both
    NOT NULL (`information_schema`, dev + test DB), so a one-sided book can
    only ever reach us as a zero side. That is why the guard is `> 0` and
    not `IS NOT NULL` — the latter would accept a 0 side and average it
    into a fabricated mark of 699.5 here.
    """
    expected = mtm_delta_mirror_fixture(
        conn,
        quote_last=None,
        quote_bid=Decimal("1399.0"),
        quote_ask=Decimal("0"),
        daily_close=_STALE_CLOSE,
        expected_mark=_STALE_CLOSE,
    )
    conn.commit()

    assert _load_mirror_equity(conn) == pytest.approx(float(expected), abs=1e-6)
