"""Ranking-pot-v1's exits: the due rule, the wind-down stamps and the ``completed`` writer (#2842 slice 5c-i).

Spec ``docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md`` §7.4, "The exits, wind-down stamps and
``completed`` writer". The close itself runs in the shared position manager
(``strategy_position_manager.manage_owned_position``, trigger ``rerank_exit``), which reads
``ranking_pot_exit_rule.pot_exit_due`` (re-exported here); the
step job (``ranking_pot_step.run_step_job``) calls ``stamp_wind_down`` and ``complete_if_flat``.

Policy-hashed (``ranking_pot_policy.POLICY_MODULES``).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Final
from zoneinfo import ZoneInfo

import psycopg

from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim

# A module import, read at call time: ``ranking_pot_step`` imports this module for its job body.
from app.services import ranking_pot_step as step
from app.services.ranking_pot_exit_rule import POT_EXIT_TIME_UTC, pot_exit_due

Conn = psycopg.Connection[Any]

_NEW_YORK: Final = ZoneInfo("America/New_York")


class ExitsError(RuntimeError):
    """The executed book's stored state contradicts an invariant the exits rely on."""


def _state(conn: Conn, declaration_id: int) -> str | None:
    row = conn.execute(
        "SELECT to_state FROM ranking_pot_state_events WHERE declaration_id = %s ORDER BY event_id DESC LIMIT 1",
        (declaration_id,),
    ).fetchone()
    return None if row is None else str(row[0])


def stamp_wind_down(conn: Conn, declaration_id: int) -> int:
    """In ``winding_down``, stamp every ``allocated`` lifecycle that has no stamp — whatever its trade's status,
    including no trade yet — from the declaration's first ``winding_down`` event, ``exit_session`` = W (the step's
    own ``wind_down_session``). First writer wins (``ON CONFLICT DO NOTHING``). Returns the stamps written."""
    with conn.transaction():
        if _state(conn, declaration_id) != "winding_down":
            return 0
        event = conn.execute(
            "SELECT event_id, at, wind_down_reason FROM ranking_pot_state_events "
            "WHERE declaration_id = %s AND to_state = 'winding_down' ORDER BY event_id LIMIT 1",
            (declaration_id,),
        ).fetchone()
        if event is None:
            raise ExitsError(f"ranking pot {declaration_id} is winding_down with no winding_down event")
        cur = conn.execute(
            """
            INSERT INTO ranking_pot_exec_exit_stamps
                (lifecycle_id, declaration_id, state_event_id, exit_session, reason)
            SELECT l.lifecycle_id, l.declaration_id, %(event)s, %(w)s, %(reason)s
              FROM ranking_pot_exec_lifecycles l
              JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id AND fd.verdict = 'allocated'
             WHERE l.declaration_id = %(d)s
               AND NOT EXISTS (SELECT 1 FROM ranking_pot_exec_exit_stamps s WHERE s.lifecycle_id = l.lifecycle_id)
             ORDER BY l.lifecycle_id
            ON CONFLICT (lifecycle_id) DO NOTHING
            """,
            {
                "event": int(event[0]),
                "w": step.wind_down_session(event[1]),
                "reason": f"wind_down:{event[2]}",
                "d": declaration_id,
            },
        )
        return cur.rowcount


#: One row per lifecycle: the raw facts the flatness check reads (never the classification label).
_FLAT_FACTS_SQL: Final = """
    SELECT l.lifecycle_id, a.target_session, fd.verdict, t.strategy_trade_id, t.status AS trade_status,
           EXISTS (SELECT 1 FROM strategy_position_ownership own
                    WHERE own.strategy_trade_id = t.strategy_trade_id AND own.status = 'active') AS owned
      FROM ranking_pot_exec_lifecycles l
      JOIN ranking_pot_rebalance_attempts a ON a.attempt_id = l.attempt_id
      LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id
      LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
     WHERE l.declaration_id = %s
     ORDER BY l.lifecycle_id
"""

#: A trade in either status holds no position the pot can act on (the reconciliation's terminal states).
FLAT_TRADE_STATUSES: Final = frozenset({"closed", "failed"})


def executed_not_flat(rows: list[tuple[Any, ...]], *, ny_today: date) -> str | None:
    """Why the executed book is not flat, or ``None`` (pure; r3-64). Rows are ``_FLAT_FACTS_SQL``'s."""
    for lifecycle_id, target, verdict, trade_id, trade_status, owned in rows:
        if verdict is None:
            if trade_id is not None:
                raise ExitsError(f"lifecycle {lifecycle_id}: a trade without a funding decision")
            if ny_today <= target:
                return f"lifecycle {lifecycle_id}: entry undecided until {target}"
            continue
        if verdict == "rejected":
            if trade_id is not None:
                raise ExitsError(f"lifecycle {lifecycle_id}: a rejected funding decision with a trade")
            continue
        if verdict != "allocated":
            raise ExitsError(f"lifecycle {lifecycle_id}: unknown funding verdict {verdict!r}")
        if trade_id is None:
            return f"lifecycle {lifecycle_id}: allocated with no trade"
        if trade_status not in FLAT_TRADE_STATUSES:
            return f"lifecycle {lifecycle_id}: trade {trade_id} is {trade_status}"
        if owned:
            return f"lifecycle {lifecycle_id}: trade {trade_id} still owns a position"
    return None


def _books_not_flat(conn: Conn, decl: rb.PotDeclaration) -> str | None:
    """``sql/448``'s completion fence, read before the insert so an unmet fence is a quiet "not yet"."""
    decided = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM ranking_pot_rebalance_attempts "
        "WHERE declaration_id = %s AND outcome = 'decided')",
        (decl.declaration_id,),
    ).fetchone()
    if decided is None or not decided[0]:
        return None
    row = conn.execute(
        """
        SELECT count(*), max(book),
               count(*) FILTER (WHERE jsonb_array_length(state -> 'positions') > 0
                                   OR jsonb_array_length(state -> 'pending') > 0),
               EXISTS (SELECT 1 FROM ranking_pot_steps s
                        WHERE s.declaration_id = %(d)s
                          AND s.wind_down_event_id = (SELECT min(e.event_id) FROM ranking_pot_state_events e
                                                       WHERE e.declaration_id = %(d)s AND e.to_state = 'winding_down'))
          FROM ranking_pot_book_checkpoints WHERE declaration_id = %(d)s
        """,
        {"d": decl.declaration_id},
    ).fetchone()
    assert row is not None
    books, top, holding, applied = int(row[0]), row[1], int(row[2]), bool(row[3])
    k = int(decl.doc["terms"]["k_controls"])
    # The count and the highest book: with the (declaration, book) key and ``book >= 0``, exactly books 0..K + 1.
    if books != sim.book_count(k) or top != sim.variant_book(k):
        return f"books_incomplete ({books} checkpoints)"
    if holding:
        return f"books_not_flat ({holding} books)"
    if not applied:
        return "wind_down_not_stepped"
    return None


def complete_if_flat(conn: Conn, decl: rb.PotDeclaration, *, now: datetime) -> str | None:
    """Write the engine ``completed`` event once the trial winds down and every book is flat (r3-63, r3-64). Stamps
    first. Returns a note, or ``None`` when the state is not ``winding_down``. The caller (the step job) computes due
    looks first."""
    stamp_wind_down(conn, decl.declaration_id)
    with conn.transaction():
        state = _state(conn, decl.declaration_id)
        if state != "winding_down":
            return None
        rows = [tuple(r) for r in conn.execute(_FLAT_FACTS_SQL, (decl.declaration_id,)).fetchall()]
        why = executed_not_flat(rows, ny_today=now.astimezone(_NEW_YORK).date()) or _books_not_flat(conn, decl)
        if why is not None:
            return f"not completed: {why}"
        conn.execute(
            "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
            "VALUES (%s, 'winding_down', 'completed', %s, 'engine')",
            (decl.declaration_id, f"§7.1: executed book flat ({len(rows)} lifecycles), books flat"),
        )
        return "completed"


__all__ = [
    "FLAT_TRADE_STATUSES",
    "POT_EXIT_TIME_UTC",
    "ExitsError",
    "complete_if_flat",
    "executed_not_flat",
    "pot_exit_due",
    "stamp_wind_down",
]
