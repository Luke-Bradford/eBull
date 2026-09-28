"""#3471 slice 2c-iii-b — the pair lifecycle writer for ``ai_trial_pair_events`` (spec §9 "Unit",
"Cohort", "Unresolved legs"; §11; O11).

Every event is DERIVED from state other writers already own — the leg's funding decision, its
strategy trade, the entry order, the entry order's stored position executions and the trade's
ownership releases — so the writer is idempotent and can run on any cadence. It appends only;
``sql/432``'s trigger enforces the per-leg order ``submitted → [uncertain →] filled →
[censored →] closed`` and a single pair-level ``broken``.

- **Leg events.** ``submitted`` once the trade has left ``planned`` (the broker call was made);
  ``uncertain`` while the submission outcome is unknown (``reconcile_required``, no broker order
  id, no fill); ``filled`` once the entry order has a stored execution; ``censored`` when the
  leg is still open 10 sessions after its own ``exit_deadline_session`` (§9's only censoring
  clock); ``closed`` when the trade is. An event is the writer's OBSERVATION: an uncertainty
  that resolved between two passes is never written. The times the readout values against
  (fill, close) come from the source tables, never from ``at``.
- **Broken (pair-level, once).** Written when both legs are determined and either is not a
  fill in the pair's target session. A leg is determined when it filled (``late_fill`` if its
  fill session is after the target), was refused (its funding ``reason_code``, or
  ``broker_rejected`` for a trade the broker refused), or is still unfilled 10 sessions after
  the target session (``unresolved``). A leg that fills after the pair broke keeps its events,
  so it is still managed to its exits and reported (O11).
- **O11.** ``pair_unit_state`` is the readout's inclusion rule: a pair with a leg whose latest
  event is ``uncertain`` is ``blocked`` (excluded until reconciliation resolves it).

Not scheduled here: the jobs slice (2c-iv) runs ``record_pair_lifecycle`` each position cycle.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Final, Literal

import psycopg
from psycopg.pq import TransactionStatus

from app.services.ai_trial_deadline import TRIAL_EXIT_TIME_UTC, exit_deadline_session, fill_session

LegEvent = Literal["submitted", "uncertain", "filled", "censored", "closed"]
PairUnitState = Literal["broken", "blocked", "unit", "open"]

LEGS: Final = ("arm", "control")
#: §9 "Closing a leg": censored 10 sessions after the leg's own deadline — the only censoring clock.
TRIAL_CENSOR_SESSIONS: Final = 10
#: §9 "Unresolved legs": still unresolved 10 sessions after the target session → ``unresolved``.
TRIAL_UNRESOLVED_SESSIONS: Final = 10

#: Trade statuses reached only after the broker submission was attempted. A trial trade is
#: ``failed`` only by a broker rejection, which comes after the call.
_SUBMITTED_STATUSES: Final = frozenset({"submitted", "open", "closing", "closed", "reconcile_required", "failed"})
_REASON: Final = re.compile(r"[a-z][a-z0-9_]*")


class TrialPairLifecycleError(RuntimeError):
    pass


@dataclass(frozen=True)
class LegFacts:
    """One leg's state, read from the tables that own it. All ``None`` before execution."""

    funding_verdict: str | None = None
    refusal_code: str | None = None
    trade_status: str | None = None
    broker_order_ref: str | None = None
    #: Earliest stored execution time of the entry order.
    filled_at: datetime | None = None
    exit_deadline: date | None = None
    #: Latest ownership release of the trade; read only when the trade is ``closed``.
    closed_at: datetime | None = None


def clock_instant(session: date, sessions: int) -> datetime:
    """15:00 UTC on the session ``sessions`` after ``session`` — the same intra-day instant the
    §8 deadline exit uses, so a clock never runs out before that session's exit could fire."""
    return datetime.combine(exit_deadline_session(session, sessions), TRIAL_EXIT_TIME_UTC, tzinfo=UTC)


def _censored(facts: LegFacts, now: datetime) -> bool:
    if facts.exit_deadline is None:
        return False
    # A closed leg is judged at its close time, so a pass that first sees it after the close
    # still records a leg that closed late as censored.
    reference = facts.closed_at if facts.trade_status == "closed" else now
    return reference is not None and reference >= clock_instant(facts.exit_deadline, TRIAL_CENSOR_SESSIONS)


def next_leg_events(facts: LegFacts, last: LegEvent | None, now: datetime) -> list[LegEvent]:
    """The events owed after ``last``, in trigger order."""
    filled = facts.filled_at is not None
    closed = facts.trade_status == "closed"
    uncertain = facts.trade_status == "reconcile_required" and facts.broker_order_ref is None and not filled
    owed: list[LegEvent] = []
    state = last
    while True:
        nxt: LegEvent | None
        if state is None:
            nxt = "submitted" if filled or facts.trade_status in _SUBMITTED_STATUSES else None
        elif state == "submitted":
            nxt = "filled" if filled else ("uncertain" if uncertain else None)
        elif state == "uncertain":
            nxt = "filled" if filled else None
        elif state == "filled":
            nxt = "censored" if _censored(facts, now) else ("closed" if closed else None)
        elif state == "censored":
            nxt = "closed" if closed else None
        else:
            nxt = None
        if nxt is None:
            return owed
        owed.append(nxt)
        state = nxt


def _reason_code(code: str | None) -> str:
    """A funding ``reason_code`` as a ``broken`` reason; ``sql/432`` admits ``[a-z][a-z0-9_]*``."""
    if code is not None and _REASON.fullmatch(code):
        return code
    cleaned = re.sub(r"[^a-z0-9_]+", "_", (code or "").lower()).strip("_")
    return cleaned if _REASON.fullmatch(cleaned) else "refused"


def leg_outcome(facts: LegFacts, target_session: date, now: datetime) -> str | None:
    """``ok`` for a fill in the target session, a ``broken`` reason code otherwise, or None while
    the leg is still undetermined."""
    if facts.filled_at is not None:
        session = fill_session(facts.filled_at)
        if session == target_session:
            return "ok"
        return "late_fill" if session > target_session else "early_fill"
    if facts.funding_verdict == "rejected":
        return _reason_code(facts.refusal_code)
    if facts.trade_status == "failed":
        return "broker_rejected"
    if now >= clock_instant(target_session, TRIAL_UNRESOLVED_SESSIONS):
        return "unresolved"
    return None


def broken_reasons(outcomes: Sequence[str | None]) -> list[str] | None:
    """The pair's ``broken`` reasons once every leg is determined and one is not ``ok``."""
    if any(outcome is None for outcome in outcomes):
        return None
    reasons = sorted({outcome for outcome in outcomes if outcome is not None and outcome != "ok"})
    return reasons or None


def pair_unit_state(events: Sequence[tuple[str | None, str]]) -> PairUnitState:
    """O11 / §9 inclusion rule over a pair's ``(leg, event)`` history in ``event_id`` order.

    ``broken`` pairs are census only; a leg whose latest event is ``uncertain`` blocks the pair;
    a ``unit`` has both legs filled and each closed or censored; anything else is ``open``.
    """
    if any(event == "broken" for _, event in events):
        return "broken"
    history: dict[str, list[str]] = {leg: [] for leg in LEGS}
    for leg, event in events:
        if leg is not None:
            history[leg].append(event)
    if any(seen and seen[-1] == "uncertain" for seen in history.values()):
        return "blocked"
    if all("filled" in seen and seen[-1] in ("censored", "closed") for seen in history.values()):
        return "unit"
    return "open"


_LEG_FACTS_SQL: Final = """
    SELECT l.leg, fd.verdict, fd.reason_code, t.status, o.broker_order_ref,
           (SELECT min(e.execution_time) FROM strategy_order_position_executions e
            WHERE e.order_id = o.order_id) AS filled_at,
           t.exit_deadline_session,
           (SELECT max(ow.released_at) FROM strategy_position_ownership ow
            WHERE ow.strategy_trade_id = t.strategy_trade_id) AS closed_at
    FROM ai_trial_leg_links l
    LEFT JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id
    LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
    LEFT JOIN strategy_trade_orders sto ON sto.strategy_trade_id = t.strategy_trade_id AND sto.purpose = 'entry'
    LEFT JOIN orders o ON o.order_id = sto.order_id
    WHERE l.pair_id = %s
"""

_EVENT_SQL: Final = "INSERT INTO ai_trial_pair_events (pair_id, leg, event) VALUES (%s, %s, %s)"


def _record_pair(conn: psycopg.Connection[Any], pair_id: int, now: datetime) -> int:
    target = conn.execute(
        """
        SELECT r.session_date
        FROM ai_trial_pairs p
        JOIN ai_trial_decisions d ON d.decision_id = p.arm_decision_id
        JOIN ai_trial_runs r ON r.run_id = d.run_id
        WHERE p.pair_id = %s
        FOR NO KEY UPDATE OF p
        """,
        (pair_id,),
    ).fetchone()
    if target is None:
        raise TrialPairLifecycleError(f"pair {pair_id} has no run")
    history = conn.execute(
        "SELECT leg, event FROM ai_trial_pair_events WHERE pair_id = %s ORDER BY event_id", (pair_id,)
    ).fetchall()
    facts = {leg: LegFacts() for leg in LEGS}
    for row in conn.execute(_LEG_FACTS_SQL, (pair_id,)).fetchall():
        facts[row[0]] = LegFacts(*row[1:])

    written = 0
    for leg in LEGS:
        last: LegEvent | None = None
        for event_leg, event in history:
            if event_leg == leg:
                last = event
        for event in next_leg_events(facts[leg], last, now):
            conn.execute(_EVENT_SQL, (pair_id, leg, event))
            written += 1
    if not any(event == "broken" for _, event in history):
        reasons = broken_reasons([leg_outcome(facts[leg], target[0], now) for leg in LEGS])
        if reasons is not None:
            conn.execute(
                "INSERT INTO ai_trial_pair_events (pair_id, event, reasons) VALUES (%s, 'broken', %s)",
                (pair_id, reasons),
            )
            written += 1
    return written


def record_pair_lifecycle(conn: psycopg.Connection[Any], *, now: datetime | None = None) -> int:
    """Append every owed event for every pair not yet finished; returns the events written.

    One transaction per pair, holding the pair row, so a concurrent pass waits and then sees
    the events already written. A pair is finished, and no longer read, once its ``broken``
    verdict is settled and every leg is terminal: ``closed``, refused at funding, or a broker
    rejection whose ``submitted`` is recorded. An ``unresolved`` leg is never terminal, since
    it may still fill. Both legs ``closed`` settles the verdict: both filled, so it was decided
    no later than the pass that closed them.
    """
    if conn.info.transaction_status != TransactionStatus.IDLE:
        raise TrialPairLifecycleError("the pair lifecycle writer requires an idle connection")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    pair_ids = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT p.pair_id FROM ai_trial_pairs p
            CROSS JOIN LATERAL (
                SELECT count(*) FILTER (WHERE closed OR refused) AS terminal_legs,
                       count(*) FILTER (WHERE closed) AS closed_legs
                FROM (
                    SELECT
                        EXISTS (SELECT 1 FROM ai_trial_pair_events e
                                WHERE e.pair_id = p.pair_id AND e.leg = legs.leg AND e.event = 'closed') AS closed,
                        EXISTS (
                            SELECT 1 FROM ai_trial_leg_links l
                            JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id
                            LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
                            WHERE l.pair_id = p.pair_id AND l.leg = legs.leg
                              AND (fd.verdict = 'rejected'
                                   OR (t.status = 'failed' AND EXISTS (
                                       SELECT 1 FROM ai_trial_pair_events e
                                       WHERE e.pair_id = p.pair_id AND e.leg = legs.leg AND e.event = 'submitted')))
                        ) AS refused
                    FROM (VALUES ('arm'), ('control')) AS legs (leg)
                ) leg_state
            ) pair_state
            WHERE NOT (
                pair_state.terminal_legs = 2
                AND (pair_state.closed_legs = 2
                     OR EXISTS (SELECT 1 FROM ai_trial_pair_events e
                                WHERE e.pair_id = p.pair_id AND e.event = 'broken'))
            )
            ORDER BY p.pair_id
            """
        ).fetchall()
    ]
    conn.commit()
    written = 0
    for pair_id in pair_ids:
        with conn.transaction():
            written += _record_pair(conn, pair_id, observed)
    return written


__all__ = [
    "LEGS",
    "TRIAL_CENSOR_SESSIONS",
    "TRIAL_UNRESOLVED_SESSIONS",
    "LegFacts",
    "TrialPairLifecycleError",
    "broken_reasons",
    "clock_instant",
    "leg_outcome",
    "next_leg_events",
    "pair_unit_state",
    "record_pair_lifecycle",
]
