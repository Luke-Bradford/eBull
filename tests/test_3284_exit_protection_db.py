"""#3284 item 4b — the reader's join and its ``status='active'`` filter, against real rows.

One integration test per genuinely-new SQL mechanism (repo test-quality rule). The
mechanism is a LEFT JOIN plus an ownership-status predicate, and neither is provable
against a mocked cursor:

* whether an active ownership with NO streak row survives the join at all (an INNER join
  would silently drop exactly the position nothing has looked at), and
* whether a RELEASED ownership carrying a live streak is excluded — the frozen-alarm case
  that made ownership liveness the "currently" conjunct instead of a broker flag.

⚠ ``_db`` module, separate from ``test_3284_exit_protection.py``: the ``db`` marker is
applied per MODULE, so one DB test there would evict all sixteen verdict tests from the
fast tier.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg

from app.services.strategy_core_mandate import CORE_MANDATE_MODE, CORE_MANDATE_POLICY_VERSION
from app.services.strategy_exit_protection import assess_exit_protection, read_exit_protection_inputs

INSTRUMENT_ID = 990_285
_NOW = datetime(2026, 9, 22, 12, 0, tzinfo=UTC)
#: ``core_rebalance_intent_state_not_after_evaluation`` compares ``state_as_of`` against
#: ``evaluated_at``, which defaults to ``now()`` — so the fixture stamp must be past.
_PAST = datetime(2026, 1, 2, 9, 0, tzinfo=UTC)


def _ownership(
    conn: psycopg.Connection[Any],
    *,
    broker_position_id: int,
    status: str = "active",
) -> int:
    """One core-arm ownership row at ``status``, returning its ``ownership_id``.

    ⚠ ``strategy_trades_exactly_one_authorisation`` is
    ``num_nonnulls(funding_decision_id, core_rebalance_intent_id) = 1``, so there is no
    cheaper "just a trade row" — the mandate event → intent → trade chain is the minimum.
    Shape lifted from ``test_3284_repair_streak_db.py`` rather than re-derived.
    """
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'XPR.PROT','Exit Protection Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )
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
    # ⚠ `strategy_position_ownership_release_shape` requires `released_at` AND a non-empty
    # `release_reason` on a released row — a bare status flip violates the CHECK.
    own = conn.execute(
        """
        INSERT INTO strategy_position_ownership (
            strategy_trade_id, broker_position_id, status, released_at, release_reason
        ) VALUES (%s, %s, %s, %s, %s) RETURNING ownership_id
        """,
        (
            trade[0],
            broker_position_id,
            status,
            None if status == "active" else _NOW,
            None if status == "active" else "closed in test",
        ),
    ).fetchone()
    assert own is not None
    conn.commit()
    return int(own[0])


def _streak_row(conn: psycopg.Connection[Any], ownership_id: int, *, refusals: int, reason: str) -> None:
    conn.execute(
        """
        INSERT INTO strategy_position_repair_streaks (
            ownership_id, consecutive_refusals, first_refused_at, last_refusal_reason, last_checked_at
        ) VALUES (%s,%s,%s,%s,%s)
        """,
        (ownership_id, refusals, _NOW, reason, _NOW),
    )
    conn.commit()


def test_an_active_ownership_with_no_streak_row_survives_the_join(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The LEFT JOIN is load-bearing: never-visited is the state that must not vanish.

    An INNER join reads as "nothing to report" for a position the fixed-exit arm has
    never evaluated — which is the exact invisibility #3284 item 4a was built to end.
    """
    conn = ebull_test_conn
    ownership_id = _ownership(conn, broker_position_id=7_284_101)

    rows = [row for row in read_exit_protection_inputs(conn) if row.ownership_id == ownership_id]
    assert len(rows) == 1
    assert rows[0].consecutive_refusals is None
    assert rows[0].last_checked_at is None

    verdict = assess_exit_protection(rows)[0]
    assert verdict.status == "never_checked"
    assert verdict.is_alerting is False


def test_a_released_ownership_is_excluded_even_while_its_streak_stands(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⛔ The frozen-alarm case, and the reason ownership liveness is the conjunct.

    A streak is never cleared by a close — ``record_repair_visit`` is only reached from
    the fixed-exit arm, and a visit that finds the position closed returns before it. So
    a position that refused twice and was then released keeps ``consecutive_refusals=2``
    for ever. Without the ``status='active'`` predicate that row would alert
    indefinitely, on a position that no longer exists.
    """
    conn = ebull_test_conn
    released_id = _ownership(conn, broker_position_id=7_284_102, status="released")
    _streak_row(conn, released_id, refusals=2, reason="fixed_exit_quote_unsafe")

    assert conn.execute(
        "SELECT consecutive_refusals FROM strategy_position_repair_streaks WHERE ownership_id=%s",
        (released_id,),
    ).fetchone() == (2,)
    assert [row for row in read_exit_protection_inputs(conn) if row.ownership_id == released_id] == []


def test_an_active_ownership_carries_its_streak_through_to_the_verdict(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The join actually matches, and the reason code reaches the budget lookup.

    ⚠ ``broker_fixed_exit_edit_not_allowed`` deliberately: it is the N=1 exception, so a
    join that dropped ``last_refusal_reason`` would silently fall back to the default
    budget of 2 and the entry would read ``repairing`` instead of alerting.
    """
    conn = ebull_test_conn
    ownership_id = _ownership(conn, broker_position_id=7_284_103)
    _streak_row(conn, ownership_id, refusals=1, reason="broker_fixed_exit_edit_not_allowed")

    rows = [row for row in read_exit_protection_inputs(conn) if row.ownership_id == ownership_id]
    verdict = assess_exit_protection(rows)[0]
    assert verdict.status == "unrepairable"
    assert verdict.is_alerting is True
    assert (verdict.consecutive_refusals, verdict.refusal_budget) == (1, 1)
    assert verdict.last_refusal_reason == "broker_fixed_exit_edit_not_allowed"
    assert verdict.first_refused_at == _NOW
