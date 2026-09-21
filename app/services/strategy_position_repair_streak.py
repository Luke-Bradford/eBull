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

* ``cleared`` — the position is CONFIRMED carrying its levels.  Either the arm observed
  both in place (``no_change``), or a submitted edit was confirmed against the broker's
  own rates by ``_edit_landed`` (``applied``).  Resets the streak.
* ``refused`` — a repair was needed and did not happen; the position stays unprotected.
  Increments the streak.
* ``unknown`` — the outcome is not established.  Stamps ``last_checked_at`` and touches
  nothing else.

⚠ ``reconcile_required`` is deliberately NOT a refusal.  A broker edit whose outcome is
uncertain may well have landed, and it is already loud by other means: the path sets
``strategy_trades.status='reconcile_required'`` and the resume machinery picks it up.
Counting it would raise a second alarm for a condition that already has one, and — worse
— an alarm that says "the stop cannot be set" about an edit that may be set.

⚠⚠ ``submitted`` IS NOT ``cleared``, AND ``pending`` IS A REFUSAL.  The first draft had
both the other way round, on the reasoning that an accepted edit settles the episode
because ``idx_strategy_position_one_unresolved_operation`` prevents an accept-but-never-land
loop from repeating.  **Codex checkpoint 2 falsified it by reading the path.**
``_resume_operation`` does NOT terminalise a ``submitted`` edit whose levels never arrive:
``strategy_position_manager.py``'s last branch returns ``("pending", "broker_edit_pending")``
and leaves the row ``submitted`` forever.  That return happens at the TOP of
``manage_owned_position``, before either recording site — so under the first draft the
streak was reset by the submit and then never touched again, while the unresolved-operation
index blocked any fresh repair.  A permanently naked position with a zero streak and a
frozen ``last_checked_at``: the precise failure this ticket exists to catch, shipped with a
docstring asserting it was safe.

So only BROKER-CONFIRMED state clears a streak.  Acceptance (202) is an
acknowledgement, which is ``unknown``; a submitted edit observed NOT in effect is the
refusal, counted on every visit it stays that way.  Sequence, in order: the submit stamps
and preserves the streak, and the next visit either confirms (``applied`` → cleared) or
counts (``pending`` → refused).

⚠ ``pending`` is only ever a refusal because its recording site filters on
``operation_type='fixed_exit_repair'``.  ``_resume_operation`` also returns ``pending`` for
a close lookup (``close_lookup_unavailable``, ``broker_close_pending``), which says nothing
about a stop — see ``_record_resumed_repair_visit``.

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
    # `_edit_landed` compared the broker's OWN rates against the intent and they match.
    # The only state that is evidence of protection rather than of intent.
    "applied": "cleared",
    # The arm declined: broker capability, an unsafe quote, or a prior refusal of this
    # exact edit still standing.
    "rejected": "refused",
    # A submitted edit that `_edit_landed` says is NOT in effect. The position is
    # unprotected and the repair has not happened, which is the refusal this ticket is
    # about — see the module docstring for why the first draft had this as `unknown`.
    "pending": "refused",
    # Acknowledged (202) but not confirmed, and uncertain-outcome mutations. Neither
    # establishes protection nor establishes a refusal.
    "submitted": "unknown",
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
