"""#3025 — a recommendation EXIT must not close a lot the strategy engine owns.

Like #3006's file, the fix IS a predicate, so a mocked cursor could only prove
the SQL text contains the words. Whether an ACTIVE ownership row actually
excludes a lot — and, just as important, whether a RELEASED one leaves it
selectable — is answerable only by running the query against rows.

The trade rows here are CORE-arm (mandate event → rebalance intent → trade),
deliberately: the core sleeve is the path whose first demo position makes this
defect live. The predicate itself is arm-agnostic.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg

from app.services.order_client import _engine_owned_long_lot_count, _load_exit_lot
from app.services.strategy_core_mandate import CORE_MANDATE_MODE, CORE_MANDATE_POLICY_VERSION

INSTRUMENT_ID = 990_025
_NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)
#: ⚠ ``core_rebalance_intent_state_not_after_evaluation`` compares ``state_as_of``
#: against ``evaluated_at``, which DEFAULTS to ``now()`` — so a fixture stamp that
#: is merely "today" can still be in the future at run time and violate the CHECK.
_PAST = datetime(2026, 1, 2, 9, 0, tzinfo=UTC)


def _seed_instrument(conn: psycopg.Connection[Any]) -> None:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'EXIT.OWN','Exit Lot Ownership Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )


def _seed_lot(
    conn: psycopg.Connection[Any],
    *,
    position_id: int,
    units: str,
    opened_days_ago: int,
) -> None:
    conn.execute(
        """
        INSERT INTO broker_positions
            (position_id, instrument_id, is_buy, units, amount,
             initial_amount_in_dollars, open_rate, open_conversion_rate,
             open_date_time, is_no_stop_loss, is_no_take_profit,
             leverage, is_tsl_enabled, total_fees, source, raw_payload, updated_at)
        VALUES
            (%(pid)s, %(iid)s, TRUE, %(units)s, 1000,
             1000, 100, 1,
             %(opened)s, TRUE, TRUE,
             1, FALSE, 0, 'broker_sync', '{}'::jsonb, %(now)s)
        """,
        {
            "pid": position_id,
            "iid": INSTRUMENT_ID,
            "units": Decimal(units),
            "opened": _NOW - timedelta(days=opened_days_ago),
            "now": _NOW,
        },
    )


def _core_trade(conn: psycopg.Connection[Any]) -> int:
    """A core-arm ``strategy_trades`` row: mandate event → intent → trade.

    ⚠ ``strategy_trades_exactly_one_authorisation`` is
    ``num_nonnulls(funding_decision_id, core_rebalance_intent_id) = 1``, so there
    is no cheaper "just a trade row" — a trade must cite an authorisation. The
    core arm is chosen over the alpha arm because the core sleeve is the path
    whose first demo position makes this defect live; the predicate under test
    is arm-agnostic either way.
    """
    event = conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision,enabled,base_currency,core_instrument_id,core_target_pct,
            liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
            policy_version,changed_by,reason,mode
        ) VALUES (1,TRUE,'USD',%s,60,20,5,25,%s,'test','test',%s)
        RETURNING core_mandate_event_id
        """,
        (INSTRUMENT_ID, CORE_MANDATE_POLICY_VERSION, CORE_MANDATE_MODE),
    ).fetchone()
    assert event is not None
    intent = conn.execute(
        """
        INSERT INTO strategy_core_rebalance_intents (
            core_mandate_event_id, allocator_policy_version, recorded_by,
            core_instrument_id, currency, core_market_value, cash_balance,
            state_as_of, action, amount, core_pct, target_pct,
            lower_pct, upper_pct, effective_floor, floor_source,
            reserve_breached, reserve_margin_pct
        ) VALUES (%s,%s,'test',%s,'USD',600,400,%s,'buy_core',50,60,60,55,65,25,'mandate',FALSE,10)
        RETURNING core_rebalance_intent_id
        """,
        (event[0], CORE_MANDATE_POLICY_VERSION, INSTRUMENT_ID, _PAST),
    ).fetchone()
    assert intent is not None
    trade = conn.execute(
        "INSERT INTO strategy_trades (core_rebalance_intent_id,instrument_id,status) "
        "VALUES (%s,%s,'open') RETURNING strategy_trade_id",
        (intent[0], INSTRUMENT_ID),
    ).fetchone()
    assert trade is not None
    return int(trade[0])


def _claim(
    conn: psycopg.Connection[Any],
    *,
    position_id: int,
    trade_id: int,
    status: str = "active",
) -> None:
    """Give ``position_id`` to the engine under an existing core trade.

    ⚠ ``trade_id`` is a parameter rather than minted here because one trade may
    legitimately own SEVERAL lots — ``sql/282``'s unique index is
    ``(strategy_trade_id, broker_position_id)``, not on the trade alone — and
    because ``strategy_core_mandate_events.revision`` is UNIQUE, so a per-claim
    trade would collide on the second call.

    The release columns are driven by ``strategy_position_ownership_release_shape``
    — a ``released`` row without both ``released_at`` and a non-empty
    ``release_reason`` violates the CHECK, so the fixture supplies them rather
    than letting the test discover the constraint.
    """
    trade = (trade_id,)
    if status == "active":
        conn.execute(
            "INSERT INTO strategy_position_ownership (strategy_trade_id, broker_position_id, status) "
            "VALUES (%s, %s, 'active')",
            (trade[0], position_id),
        )
    else:
        conn.execute(
            "INSERT INTO strategy_position_ownership "
            "(strategy_trade_id, broker_position_id, status, released_at, release_reason) "
            "VALUES (%s, %s, 'released', %s, 'operator_close')",
            (trade[0], position_id, _NOW),
        )


def test_an_engine_owned_lot_is_skipped_for_a_younger_unowned_one(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """FIFO still applies — among the lots this path is ALLOWED to close.

    The owned lot is the oldest, so before #3025 it won the ordering and would
    have been closed at the broker, stranding its ownership row active on a
    position the account no longer carries (the #2979 wedge, from the legacy
    side).
    """
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=3308442058, units="1000", opened_days_ago=3)
    _seed_lot(ebull_test_conn, position_id=3310085041, units="500", opened_days_ago=1)
    _claim(ebull_test_conn, position_id=3308442058, trade_id=_core_trade(ebull_test_conn))

    lot = _load_exit_lot(ebull_test_conn, INSTRUMENT_ID)
    assert lot is not None
    assert lot.position_id == 3310085041
    assert lot.units == Decimal("500.00000000")


def test_every_long_lot_engine_owned_refuses(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """No lot left to close, and the count tells the caller WHY.

    ``None`` alone is indistinguishable from "this instrument has no
    broker-closeable long lot at all" — two situations with different operator
    fixes, which is why the caller reads the count rather than emitting one
    generic message (#3003).
    """
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=3308442058, units="1000", opened_days_ago=3)
    _seed_lot(ebull_test_conn, position_id=3310085041, units="500", opened_days_ago=1)
    # One trade owning both lots — the multi-position shape ``sql/282`` allows.
    trade_id = _core_trade(ebull_test_conn)
    _claim(ebull_test_conn, position_id=3308442058, trade_id=trade_id)
    _claim(ebull_test_conn, position_id=3310085041, trade_id=trade_id)

    assert _load_exit_lot(ebull_test_conn, INSTRUMENT_ID) is None
    assert _engine_owned_long_lot_count(ebull_test_conn, INSTRUMENT_ID) == 2


def test_a_released_ownership_row_leaves_the_lot_selectable(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The over-narrowing guard: ``released`` means the engine gave the lot up.

    Filtering on the ROW's existence rather than on ``status='active'`` would
    permanently strand every lot the engine has ever touched.
    """
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=3308442058, units="1000", opened_days_ago=3)
    _claim(
        ebull_test_conn,
        position_id=3308442058,
        trade_id=_core_trade(ebull_test_conn),
        status="released",
    )

    lot = _load_exit_lot(ebull_test_conn, INSTRUMENT_ID)
    assert lot is not None
    assert lot.position_id == 3308442058
    assert _engine_owned_long_lot_count(ebull_test_conn, INSTRUMENT_ID) == 0


def test_no_ownership_rows_at_all_is_unchanged(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Today's population: 0 ownership rows, so the narrowing rejects nothing.

    Pins that the new clause is inert until the engine actually trades — the
    reject-side census the narrowing-gate rule asks for, as an assertion rather
    than a claim in the PR description.
    """
    _seed_instrument(ebull_test_conn)
    _seed_lot(ebull_test_conn, position_id=3308442058, units="1000", opened_days_ago=3)

    lot = _load_exit_lot(ebull_test_conn, INSTRUMENT_ID)
    assert lot is not None
    assert lot.position_id == 3308442058
    assert _engine_owned_long_lot_count(ebull_test_conn, INSTRUMENT_ID) == 0
