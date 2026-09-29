"""#3471 slice 2c-iii-a — spec §15 O10: "a position without an SL or TP is repaired within one
5-minute cycle. If repair fails, the position is closed. If both fail, the trial goes to
`halted_operator` and an operator alert is raised: an unprotected position is a refusal surface."

The repair and the close are the position manager's (``strategy_position_manager``: the
fixed-exit repair arm, ``_trial_supersede_trigger`` and ``_trial_close``). This module is the
third step: the halt.

The alert is not a new channel. The leg is still unprotected, so the refused repair visits are
already counted by ``strategy_position_repair_streak`` and raised by ``strategy_exit_protection``
on ``/system/status``. The halt adds the state that stops new entries (``ai_trial_intent``
refuses `trial_not_active`) and an ERROR log line naming the trade.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

import psycopg

logger = logging.getLogger(__name__)


EngineHalt = Literal["halted_harm", "halted_loss", "halted_operator"]


def halt_active_trial(conn: psycopg.Connection[Any], *, declaration_id: int, to_state: EngineHalt, reason: str) -> bool:
    """Move the trial ``active -> to_state`` as the engine. Returns False when the trial is not
    active (a halt is never stacked on a halt, and a resume is a supervisor action)."""
    with conn.transaction():
        # The state-event trigger takes the same lock; taking it first makes the read below the
        # current state rather than a stale one, so a concurrent halt is not a raised error.
        conn.execute(
            "SELECT 1 FROM ai_trial_declarations WHERE declaration_id = %s FOR NO KEY UPDATE", (declaration_id,)
        )
        state = conn.execute(
            "SELECT to_state FROM ai_trial_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
            (declaration_id,),
        ).fetchone()
        if state is None or state[0] != "active":
            return False
        conn.execute(
            "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
            "VALUES (%s, 'active', %s, %s, 'engine')",
            (declaration_id, to_state, reason),
        )
    return True


def halt_trial_for_unprotected_leg(conn: psycopg.Connection[Any], *, strategy_trade_id: int, reason: str) -> bool:
    """Move the leg's trial ``active -> halted_operator``. Returns False when the trial is
    already not active.
    """
    with conn.transaction():
        row = conn.execute(
            "SELECT p.declaration_id FROM ai_trial_trade_links l JOIN ai_trial_pairs p ON p.pair_id = l.pair_id "
            "WHERE l.strategy_trade_id = %s",
            (strategy_trade_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"strategy trade {strategy_trade_id} is not a trial leg")
        declaration_id = int(row[0])
        if not halt_active_trial(
            conn,
            declaration_id=declaration_id,
            to_state="halted_operator",
            reason=f"o10_unprotected_leg:trade={strategy_trade_id}:{reason}",
        ):
            return False
    logger.error(
        "ai trial %s halted: leg trade %s is unprotected and its close was refused (%s)",
        declaration_id,
        strategy_trade_id,
        reason,
    )
    return True


__all__ = ["EngineHalt", "halt_active_trial", "halt_trial_for_unprotected_leg"]
