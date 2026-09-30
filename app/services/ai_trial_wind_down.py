"""#3515 §0 — is AI-discretionary-v1 wound down?

fund-v1 may start only when v1 is FULLY wound down (spec §0 rule 2): every v1 declaration is in a
terminal state AND, on either v1 leg, nothing is still in flight — no claimed-but-undecided run,
no published leg awaiting its order, and no open or pending lifecycle. Exits of open v1 positions
still run v1 code, so a terminal declaration with an open trade is NOT wound down.

Two readers share this module:

* fund-v1's start gate refuses ``v1_not_wound_down:<detail>`` on every run until it holds;
* ``scripts/ai_trial_policy_guard.py`` compares the working tree's ``policy_hash()`` with the
  stored hash of every v1 declaration NOT yet wound down (§0 rule 1).

Fail-closed by construction: no v1 declaration at all, no state event, or a state this module
does not know are all "not wound down". Nothing here writes a row.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final

import psycopg

from app.services.ai_trial_run import TRIAL_ARM_STRATEGY_ID

REFUSAL: Final = "v1_not_wound_down"

#: ``sql/432``'s ``ai_trial_state_events_transition``: these two admit no further transition.
#: ``halted_mandate`` / ``halted_operator`` are resumable by the supervisor, so a v1 trial paused
#: in either is NOT wound down — starting fund-v1 on a pause would strand v1 (§0 rule 3).
TERMINAL_STATES: Final = frozenset({"halted_harm", "halted_loss"})
#: The ``to_state`` CHECK in ``sql/432``. A state outside it is refused as ``trial_unknown_state``.
KNOWN_STATES: Final = TERMINAL_STATES | {"active", "halted_mandate", "halted_operator"}


@dataclass(frozen=True)
class V1Declaration:
    declaration_id: int
    #: The latest ``ai_trial_state_events.to_state``; ``None`` = no event.
    state: str | None
    #: ``doc ->> 'policy_hash'`` as stored; ``None`` when the document carries none.
    policy_hash: str | None
    #: ``ai_trial_runs`` still ``claimed``: a run may yet publish pairs.
    claimed_runs: int
    #: Leg links with no funding decision: a published leg still awaiting its order.
    undispatched_legs: int
    #: Allocated funding decisions on either leg's deployment whose trade is missing or not
    #: ``closed`` / ``failed`` — pending, submitted, uncertain and open trades (the executor's
    #: slot-holding population, O8).
    open_trades: int
    #: ``strategy_position_ownership`` rows still ``active`` for a trade on either leg.
    active_ownerships: int

    def outstanding(self) -> tuple[str, ...]:
        """Every reason this declaration is not wound down, in a fixed order; empty when it is."""
        reasons: list[str] = []
        if self.state is None:
            reasons.append("trial_no_state")
        elif self.state not in KNOWN_STATES:
            reasons.append("trial_unknown_state")
        elif self.state not in TERMINAL_STATES:
            reasons.append(f"trial_{self.state}")
        for count, name in (
            (self.claimed_runs, "claimed_run"),
            (self.undispatched_legs, "undispatched_leg"),
            (self.open_trades, "open_trade"),
            (self.active_ownerships, "active_ownership"),
        ):
            if count:
                reasons.append(name)
        return tuple(reasons)

    @property
    def wound_down(self) -> bool:
        return not self.outstanding()


def wind_down_refusal(declarations: Sequence[V1Declaration]) -> str | None:
    """``v1_not_wound_down:<detail>`` for the first outstanding reason of the lowest declaration
    id, or ``None`` when every v1 declaration is wound down. No declaration at all refuses
    ``declaration_missing``: fund-v1 cannot prove v1 ended if it cannot find v1."""
    if not declarations:
        return f"{REFUSAL}:declaration_missing"
    for declaration in sorted(declarations, key=lambda d: d.declaration_id):
        reasons = declaration.outstanding()
        if reasons:
            return f"{REFUSAL}:{reasons[0]}"
    return None


_V1_DECLARATIONS_SQL: Final = """
    SELECT dcl.declaration_id,
           (SELECT se.to_state FROM ai_trial_state_events se
             WHERE se.declaration_id = dcl.declaration_id
             ORDER BY se.event_id DESC LIMIT 1) AS state,
           dcl.doc ->> 'policy_hash' AS policy_hash,
           (SELECT count(*) FROM ai_trial_runs r
             WHERE r.declaration_id = dcl.declaration_id AND r.status = 'claimed') AS claimed_runs,
           (SELECT count(*) FROM ai_trial_pairs p
              JOIN ai_trial_leg_links l ON l.pair_id = p.pair_id
             WHERE p.declaration_id = dcl.declaration_id
               AND NOT EXISTS (SELECT 1 FROM strategy_funding_decisions fd
                                WHERE fd.signal_id = l.signal_id)) AS undispatched_legs,
           (SELECT count(*) FROM strategy_deployments d
              JOIN strategy_funding_decisions fd
                ON fd.deployment_id = d.deployment_id AND fd.verdict = 'allocated'
              LEFT JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
             WHERE d.strategy_id IN (dcl.strategy_id, dcl.strategy_id || '-control')
               AND d.strategy_version = dcl.strategy_version
               AND (t.strategy_trade_id IS NULL OR t.status NOT IN ('closed', 'failed'))) AS open_trades,
           (SELECT count(*) FROM strategy_deployments d
              JOIN strategy_funding_decisions fd ON fd.deployment_id = d.deployment_id
              JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
              JOIN strategy_position_ownership ow
                ON ow.strategy_trade_id = t.strategy_trade_id AND ow.status = 'active'
             WHERE d.strategy_id IN (dcl.strategy_id, dcl.strategy_id || '-control')
               AND d.strategy_version = dcl.strategy_version) AS active_ownerships
    FROM ai_trial_declarations dcl
    WHERE dcl.strategy_id = %s
    ORDER BY dcl.declaration_id
"""


def read_v1_declarations(conn: psycopg.Connection[Any]) -> list[V1Declaration]:
    """Every v1 declaration (every version of the v1 arm id) with its wind-down counts, read in
    one statement so the counts share a snapshot."""
    rows = conn.execute(_V1_DECLARATIONS_SQL, (TRIAL_ARM_STRATEGY_ID,)).fetchall()
    return [
        V1Declaration(
            declaration_id=int(r[0]),
            state=r[1],
            policy_hash=r[2],
            claimed_runs=int(r[3]),
            undispatched_legs=int(r[4]),
            open_trades=int(r[5]),
            active_ownerships=int(r[6]),
        )
        for r in rows
    ]
