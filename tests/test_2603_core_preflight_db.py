"""#2603 item 3 step 3b-1 — the two SQL mechanisms, against a real database.

The precedence order is pure and lives in ``test_2603_core_preflight``.  What needs
a database is exactly two things, and neither is provable any other way:

1. **The observation returns ONE row even when the instrument does not exist.**
   The query is anchored on the parameter rather than on ``instruments``
   specifically so an emergency stop stays readable in the branch where the
   instrument is missing.  Anchor it the obvious way instead and this test is the
   only thing that notices.
2. **The ``quotes_refresh`` scope arm reaches the mandate's core instrument.**
   Without it ``core_quote_missing`` is PERMANENT for any mandate naming an
   instrument that is not held, not Tier 1/2 and not a benchmark — measured on dev,
   that is `IVV`, `VTI` and `SPY.RTH` among the obvious core candidates.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a test
source db-marks the WHOLE module at collection.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from app.services.strategy_core_preflight import (
    _PREFLIGHT_SQL,
    StrategyCorePreflightError,
    preflight_core_submission,
)

# ⚠ Distinct from every other core-arc DB module's ids, and checked rather than
# assumed: `test_2603_core_trade_arc_db.py` already uses 920605/920606 and
# `test_2603_core_{eligibility,rebalance_intent}_db.py` use 920604.  Worker DBs are
# private and `_reset_planner_tables` wipes between tests, so a shared id is not a
# live race — but the seeding helper is `ON CONFLICT DO UPDATE`, so a reused id
# would silently absorb a cleanup leak instead of failing on it.
#
# ⚠ MISSING_INSTRUMENT_ID must be one that NOTHING in the suite ever inserts: the
# tests that use it assert absence, so an id another module seeds would turn a real
# regression into a confusing failure in this file.
INSTRUMENT_ID = 920_611
MISSING_INSTRUMENT_ID = 920_612


def _seed_instrument(conn: psycopg.Connection[Any], *, tradable: bool = True) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, exchange)
        VALUES (%s, 'CORETEST', 'Core Test ETF', %s, '5')
        ON CONFLICT (instrument_id) DO UPDATE SET is_tradable = EXCLUDED.is_tradable
        """,
        (INSTRUMENT_ID, tradable),
    )


# --------------------------------------------------------------------------
# 1. The observation is anchored on the parameter, not on `instruments`
# --------------------------------------------------------------------------


def test_the_observation_returns_one_row_even_for_an_instrument_that_does_not_exist(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ The whole reason the FROM clause is a one-row anchor.

    With ``instruments`` as the anchor a missing instrument returns NO row, and the
    kill switch and execution block become unreadable in precisely the branch where
    an emergency stop still has to be reported.  The decision function would then
    have to re-read them in a SECOND statement, from a different snapshot.
    """
    row = ebull_test_conn.execute(_PREFLIGHT_SQL, {"core_instrument_id": MISSING_INSTRUMENT_ID}).fetchone()
    assert row is not None
    assert row[0] is False, "instrument_present must report absence, not vanish"
    ebull_test_conn.rollback()


def test_the_observation_reads_the_kill_switch_alongside_a_missing_instrument(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """An active kill switch must be visible even with no instrument row.

    This is the fail-open this anchor prevents: a caller that learned only
    "instrument missing" would fix the mandate and retry straight into an active
    emergency stop.
    """
    ebull_test_conn.execute(
        "INSERT INTO kill_switch (id, is_active, reason) VALUES (true, true, 'test') "
        "ON CONFLICT (id) DO UPDATE SET is_active = true"
    )
    row = ebull_test_conn.execute(_PREFLIGHT_SQL, {"core_instrument_id": MISSING_INSTRUMENT_ID}).fetchone()
    assert row is not None
    assert row[0] is False  # instrument_present
    assert row[4] is True  # kill_switch_active — still readable
    ebull_test_conn.rollback()


def test_the_observation_binds_the_quote_and_exchange_to_the_instrument(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The join columns are easy to get wrong and silent when wrong.

    ``exchanges`` is keyed by a TEXT id that ``instruments`` stores in a column
    called ``exchange``, not ``exchange_id``.  A wrong join name would not error —
    it would leave ``asset_class`` NULL for every instrument, which refuses
    everything for a reason no one could diagnose from the code.
    """
    _seed_instrument(ebull_test_conn)
    ebull_test_conn.execute(
        "INSERT INTO exchanges (exchange_id, asset_class) VALUES ('5', 'us_equity') "
        "ON CONFLICT (exchange_id) DO UPDATE SET asset_class = 'us_equity'"
    )
    ebull_test_conn.execute(
        """
        INSERT INTO quotes (instrument_id, quoted_at, bid, ask, spread_flag)
        VALUES (%s, now(), 100.00, 100.02, false)
        ON CONFLICT (instrument_id) DO UPDATE SET quoted_at = EXCLUDED.quoted_at
        """,
        (INSTRUMENT_ID,),
    )
    row = ebull_test_conn.execute(_PREFLIGHT_SQL, {"core_instrument_id": INSTRUMENT_ID}).fetchone()
    assert row is not None
    assert row[0] is True  # instrument_present
    assert row[3] == "us_equity"  # asset_class, via i.exchange -> e.exchange_id
    assert row[8] is not None  # quoted_at
    ebull_test_conn.rollback()


# --------------------------------------------------------------------------
# The lock contract
# --------------------------------------------------------------------------


def test_the_preflight_refuses_to_run_without_the_submission_lock(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Not holding the lock is a caller BUG, so it raises rather than refusing.

    A refusal is something a caller logs and carries on from — straight into the
    race the lock exists to prevent, with the kill switch read at a moment that has
    no defined relationship to the submission.
    """
    with pytest.raises(StrategyCorePreflightError, match="core_submission_lock"):
        preflight_core_submission(
            ebull_test_conn,
            core_instrument_id=INSTRUMENT_ID,
            action="buy_core",
            now=datetime.now(UTC),
        )
    ebull_test_conn.rollback()


# --------------------------------------------------------------------------
# 2. The quotes_refresh scope arm — the permanent-refusal fix
# --------------------------------------------------------------------------

#: Lifted VERBATIM from ``quotes_refresh``'s scope SELECT.  ⚠ A copy, and copies
#: drift — which is what ``test_the_scope_sql_here_matches_the_scheduler_source``
#: below exists to catch.  Extracting it to a module constant would be the better
#: fix, and belongs to whoever next touches that job rather than to a refusal gate.
_SCOPE_ARM = """
SELECT 1
FROM instruments i
WHERE i.instrument_id = %(instrument_id)s
  AND i.is_tradable = TRUE
  AND i.instrument_id = (
      SELECT CASE WHEN m.enabled THEN m.core_instrument_id END
      FROM strategy_core_mandate_events m
      ORDER BY m.revision DESC
      LIMIT 1
  )
"""

_MANDATE_INSERT = """
INSERT INTO strategy_core_mandate_events (
    revision, enabled, base_currency, core_instrument_id, core_target_pct,
    liquidity_reserve_pct, rebalance_band_pct, min_rebalance_amount,
    policy_version, mode, changed_by, reason
) VALUES (
    %(revision)s, %(enabled)s, 'USD', %(instrument_id)s, 60, 5, 5, 100,
    'core-mandate-v2', 'paper', 'test', 'scope test'
)
"""


def _in_scope(conn: psycopg.Connection[Any]) -> bool:
    return conn.execute(_SCOPE_ARM, {"instrument_id": INSTRUMENT_ID}).fetchone() is not None


def test_an_enabled_mandate_puts_its_core_instrument_in_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The bootstrap case: the FIRST core buy is by definition not yet held.

    Without this arm the instrument is quoted only if it happens to be Tier 1/2 or
    a benchmark, so a mandate naming `IVV` refuses ``core_quote_missing`` for ever
    and no amount of waiting fixes it.
    """
    _seed_instrument(ebull_test_conn)
    assert _in_scope(ebull_test_conn) is False, "no mandate — must not be in scope"

    ebull_test_conn.execute(_MANDATE_INSERT, {"revision": 1, "enabled": True, "instrument_id": INSTRUMENT_ID})
    assert _in_scope(ebull_test_conn) is True
    ebull_test_conn.rollback()


def test_a_disabled_LATEST_revision_drops_the_instrument_from_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ THE mandate is the latest revision, not the latest ENABLED one.

    ``load_core_mandate`` is ``ORDER BY revision DESC LIMIT 1`` with no ``WHERE``.
    A scope arm written as ``WHERE enabled ORDER BY revision DESC`` would keep
    quoting the instrument of a superseded enabled revision after the operator
    disabled the mandate — harmless in itself, but a second definition of "the
    mandate", which is how the two silently diverge.
    """
    _seed_instrument(ebull_test_conn)
    ebull_test_conn.execute(_MANDATE_INSERT, {"revision": 1, "enabled": True, "instrument_id": INSTRUMENT_ID})
    assert _in_scope(ebull_test_conn) is True

    ebull_test_conn.execute(_MANDATE_INSERT, {"revision": 2, "enabled": False, "instrument_id": INSTRUMENT_ID})
    assert _in_scope(ebull_test_conn) is False
    ebull_test_conn.rollback()


def test_a_non_tradable_core_instrument_stays_out_of_the_quote_scope(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Consistent with the other three arms, all of which require ``is_tradable``."""
    _seed_instrument(ebull_test_conn, tradable=False)
    ebull_test_conn.execute(_MANDATE_INSERT, {"revision": 1, "enabled": True, "instrument_id": INSTRUMENT_ID})
    assert _in_scope(ebull_test_conn) is False
    ebull_test_conn.rollback()


def test_the_scope_sql_here_matches_the_scheduler_source() -> None:
    """A copied predicate and its original are two predicates.

    Pins the copy above to the text actually executed by ``quotes_refresh``, so a
    change to the job's arm fails here instead of leaving these three tests green
    against SQL that no longer runs anywhere.
    """
    from pathlib import Path

    source = (Path(__file__).resolve().parents[1] / "app" / "workers" / "scheduler.py").read_text()
    assert "SELECT CASE WHEN m.enabled THEN m.core_instrument_id END" in source
    assert "FROM strategy_core_mandate_events m" in source
    assert "ORDER BY m.revision DESC" in source
