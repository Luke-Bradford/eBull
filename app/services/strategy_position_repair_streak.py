"""Consecutive fixed-exit repair refusals, per ownership (#3284 item 4a).

Item 4 of #3284 is *"if the stop cannot be set after N attempts, that is an alert-class
event, not a silent carry-on"*.  This module is the recording half: it turns one visit of
``manage_owned_position``'s fixed-exit arm into a durable per-ownership streak.  Item 4b
reads that streak and decides what alerts — no threshold lives here.

WHY THIS EXISTS AT ALL.  Both fixed-exit refusals return before ``_persist_edit_intent``,
so a refused repair writes nothing, and ``run_strategy_paper_cycle`` discards the result
and counts ``managed`` on the attempt rather than the outcome.  A position refusing repair
on every visit therefore looks exactly like a position that needed none: the cycle reports
``success`` and the position stays naked.  ``sql/408`` carries the measurement and the
reason the counter cannot live in ``strategy_position_operations``.

THE THREE OUTCOMES, and why a two-way split would be wrong.  The obvious shape is
"refused or not", but ``PositionManagerResult`` can also come back genuinely undecided,
and folding that into either side is a real error in one direction or the other:

* ``cleared`` — nothing is outstanding.  Either the arm observed both levels already in
  place, or the broker ACCEPTED an edit.  Resets the streak.
* ``refused`` — a repair was needed and did not happen; the position stays unprotected.
  Increments the streak.
* ``unknown`` — the outcome is not established (``reconcile_required``, or an operation
  already in flight).  Stamps ``last_checked_at`` and touches nothing else.

⚠ ``reconcile_required`` is deliberately NOT a refusal.  A broker edit whose outcome is
uncertain may well have landed, and it is already loud by other means: the path sets
``strategy_trades.status='reconcile_required'`` and the resume machinery picks it up.
Counting it would raise a second alarm for a condition that already has one, and — worse
— an alarm that says "the stop cannot be set" about an edit that may be set.

⚠ An ACCEPTED edit (``submitted``) counts as ``cleared`` even though acceptance is not
landing, and that is not a blind spot.  ``idx_strategy_position_one_unresolved_operation``
admits at most one unresolved operation per ownership, so an accept-but-never-land loop
cannot repeat: the operation stays unresolved, ``_resume_operation`` is what handles it,
and ``_edit_landed`` only reaches ``applied`` by comparing the broker's own rates.  If an
edit genuinely fails to take effect, the next visit sees the gap again and the streak
climbs from zero — one cycle slower to alert, never silent.

WHY THE STREAK IS A COUNT OF VISITS.  ``_OWNED_BATCH_SQL`` rotates the owned batch by
five-minute slot, so how often one ownership is visited is a function of sleeve size.  A
wall-clock threshold would mean "two failures" on a small sleeve and "forty" on a large
one, and no published formulation exists to pick one — so per "source-rule before design"
the quantity is fixed BY CONSTRUCTION as consecutive visits, which is scale-free.  The
reset is what makes a later failure a new episode rather than an accumulating total.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

import psycopg

# Mirrors ``PositionManagerResult.state``.  Kept as its own alias rather than imported
# because ``strategy_position_manager`` imports THIS module; the vocabularies are pinned
# together by a test instead of by an import cycle.
RepairVisitState = Literal["no_change", "submitted", "pending", "applied", "rejected", "reconcile_required"]
RepairVisitOutcome = Literal["cleared", "refused", "unknown"]

_OUTCOME_BY_STATE: dict[str, RepairVisitOutcome] = {
    # The arm found both levels in place, so no repair was outstanding.
    "no_change": "cleared",
    # The broker accepted the edit (202) or it has been confirmed against broker state.
    "submitted": "cleared",
    "applied": "cleared",
    # The arm declined: broker capability, an unsafe quote, or a prior refusal of this
    # exact edit still standing.
    "rejected": "refused",
    # Undecided — see the module docstring.
    "pending": "unknown",
    "reconcile_required": "unknown",
}


class RepairStreakError(Exception):
    """A repair-visit state outside the manager's own result vocabulary."""


def classify_repair_visit(state: str) -> RepairVisitOutcome:
    """Map one ``PositionManagerResult.state`` to its effect on the streak.

    Raises rather than defaulting.  A silently-defaulted new state would be counted as
    whichever side the default happened to pick, which on this surface means either a
    false alarm about an unprotected position or a real one suppressed.
    """
    outcome = _OUTCOME_BY_STATE.get(state)
    if outcome is None:
        raise RepairStreakError(f"unknown repair visit state: {state!r}")
    return outcome


# ⚠ `first_refused_at` is COALESCEd, never overwritten: it dates the EPISODE, so item 4b
# can report how long the position has been refusing rather than when it last refused.
_REFUSED_SQL = """
    INSERT INTO strategy_position_repair_streaks (
        ownership_id, consecutive_refusals, first_refused_at, last_refusal_reason, last_checked_at
    ) VALUES (%s, 1, %s, %s, %s)
    ON CONFLICT (ownership_id) DO UPDATE SET
        consecutive_refusals = strategy_position_repair_streaks.consecutive_refusals + 1,
        first_refused_at = COALESCE(strategy_position_repair_streaks.first_refused_at, EXCLUDED.first_refused_at),
        last_refusal_reason = EXCLUDED.last_refusal_reason,
        last_checked_at = EXCLUDED.last_checked_at,
        updated_at = now()
"""

# ⚠ DO UPDATE, not DO NOTHING.  The whole point of this branch is to clear a streak that
# already exists; DO NOTHING would leave a stale count that item 4b would then alert on
# for a position it has just watched get protected.
_CLEARED_SQL = """
    INSERT INTO strategy_position_repair_streaks (
        ownership_id, consecutive_refusals, first_refused_at, last_refusal_reason, last_checked_at
    ) VALUES (%s, 0, NULL, NULL, %s)
    ON CONFLICT (ownership_id) DO UPDATE SET
        consecutive_refusals = 0,
        first_refused_at = NULL,
        last_refusal_reason = NULL,
        last_checked_at = EXCLUDED.last_checked_at,
        updated_at = now()
"""

# An undecided visit is evidence that the arm RAN, and nothing else — so it stamps the
# timestamp and leaves any live streak exactly where it was.
_UNKNOWN_SQL = """
    INSERT INTO strategy_position_repair_streaks (
        ownership_id, consecutive_refusals, first_refused_at, last_refusal_reason, last_checked_at
    ) VALUES (%s, 0, NULL, NULL, %s)
    ON CONFLICT (ownership_id) DO UPDATE SET
        last_checked_at = EXCLUDED.last_checked_at,
        updated_at = now()
"""


def record_repair_visit(
    conn: psycopg.Connection[Any],
    *,
    ownership_id: int,
    state: str,
    reason_code: str,
    observed_at: datetime,
) -> RepairVisitOutcome:
    """Record one fixed-exit repair visit against ``ownership_id``. Returns the outcome.

    Caller-owned transaction: this opens its own ``conn.transaction()`` block, so it must
    be called on an idle connection — which is where the arm leaves it, because
    ``_submit_edit`` and ``_prior_same_edit`` both commit before returning.

    ⚠ Call this ONLY from the fixed-exit arm.  ``last_checked_at`` means "the arm
    evaluated this ownership"; stamping it from the close, age-out or missing-position
    paths would make a position that is being closed look like one whose stop is being
    checked.
    """
    outcome = classify_repair_visit(state)
    stamped = observed_at.astimezone(UTC)
    with conn.transaction():
        if outcome == "refused":
            if not reason_code:
                raise RepairStreakError("a refused repair visit must carry a reason code")
            conn.execute(_REFUSED_SQL, (ownership_id, stamped, reason_code, stamped))
        elif outcome == "cleared":
            conn.execute(_CLEARED_SQL, (ownership_id, stamped))
        else:
            conn.execute(_UNKNOWN_SQL, (ownership_id, stamped))
    return outcome
