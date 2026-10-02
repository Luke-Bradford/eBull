"""Ranking-pot-v1's broker-held protection levels (#2842 slice 5c-ii-c; spec §7.4, "The broker-held levels"; r3-108).

``record_held_levels`` appends the four protection fields of a pot position, as the position manager read them,
to ``ranking_pot_exec_level_observations`` — only when they differ from that pair's latest row, so an unchanged
cycle writes nothing. The manager calls it right after the exact position is read and before any exit or repair
decision, so a position's first row precedes every repair this system sends. A record, never a gate.

A leaf module (``psycopg`` only), for the reason ``ranking_pot_exit_rule`` is one: the shared position manager
imports it, and the manager is itself imported by ``ai_trial_policy``, which the pot's policy module imports.

Policy-hashed (``ranking_pot_policy.POLICY_MODULES``).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus

# Row-wise IS NOT DISTINCT FROM: NULL equals NULL, and NUMERIC compares by value.
_INSERT_IF_CHANGED_SQL = """
    INSERT INTO ranking_pot_exec_level_observations
        (strategy_trade_id, broker_position_id, observed_at,
         stop_loss_rate, take_profit_rate, is_no_stop_loss, is_no_take_profit)
    SELECT %(trade)s, %(position)s, %(observed_at)s, %(sl)s, %(tp)s, %(no_sl)s, %(no_tp)s
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT stop_loss_rate, take_profit_rate, is_no_stop_loss, is_no_take_profit
            FROM ranking_pot_exec_level_observations
            WHERE strategy_trade_id = %(trade)s AND broker_position_id = %(position)s
            ORDER BY observation_id DESC
            LIMIT 1
        ) latest
        WHERE (latest.stop_loss_rate, latest.take_profit_rate, latest.is_no_stop_loss, latest.is_no_take_profit)
              IS NOT DISTINCT FROM (%(sl)s::numeric, %(tp)s::numeric, %(no_sl)s::boolean, %(no_tp)s::boolean)
    )
"""


def record_held_levels(
    conn: psycopg.Connection[Any],
    *,
    strategy_trade_id: int,
    broker_position_id: int,
    stop_loss_rate: Decimal | None,
    take_profit_rate: Decimal | None,
    is_no_stop_loss: bool,
    is_no_take_profit: bool,
    observed_at: datetime,
) -> bool:
    """Append the held levels when they changed; True when a row was written. Runs inside the caller's transaction,
    and serialises the compare-and-insert per position itself with a transaction-scoped advisory lock: unserialised,
    an uncommitted change could be overtaken by a later observation of the old value, losing the transition."""
    if conn.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError("record_held_levels must run inside a transaction")
    conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended('ranking_pot_exec_level_observations:' || %s::text, 0))",
        (broker_position_id,),
    )
    cur = conn.execute(
        _INSERT_IF_CHANGED_SQL,
        {
            "trade": strategy_trade_id,
            "position": broker_position_id,
            "observed_at": observed_at,
            "sl": stop_loss_rate,
            "tp": take_profit_rate,
            "no_sl": is_no_stop_loss,
            "no_tp": is_no_take_profit,
        },
    )
    return cur.rowcount == 1


__all__ = ["record_held_levels"]
