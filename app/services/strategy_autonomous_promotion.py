"""The policy approver — the second caller of the stage machine (#2843).

The person-gate this replaces was never a check.  Measured before writing this:

    grep -rn 'advance_strategy(' app/ scripts/ | grep -v 'def advance_strategy'
      app/api/strategies.py:3542

``advance_strategy`` assembles every piece of evidence itself -- the 24-combination
hold-out matrix, the identity pins, the prospective assessment, the weakening check --
and takes from its caller only ``(strategy_id, action, advanced_by, reason, as_of)``.
Its one production caller is ``POST /strategies/{id}/advance``, behind
``Depends(require_session)``, stamping ``advanced_by=session.username``.  **The
person-gate was exactly that: the only path in is an authenticated HTTP request, and
the username it stamps is the approval.**

So the operator's flag (settled decision 2026-08-22, "Live-capital approval is a mandate
FLAG, not a person-gate") does not need to touch a single gate.  It needs a second
caller, and this is it.  Nothing here evaluates evidence; it decides only WHO is asking.

⚠ This module cannot choose its own denominator, and that is structural rather than
careful: ``advance_strategy`` accepts no ``to_stage``, no ``strategy_version`` and no
``result_ids``.  Those are the three inputs #2770 deliberately removed from the caller's
reach, and removing them is what makes a headless caller safe to add at all.

⚠ What bounds forward observation is NOT here.  ``select_prospective_assessment``
refuses ``prospective_assessment_predates_forward_observation``, so ``paper_enabled`` is
unreachable until an assessment is computed after forward observation began.  No
minimum stage-dwell constant is introduced here, deliberately: all six
``RECENT_EVIDENCE_WINDOWS`` end at or before ``INTRADER_CAPTURE_DATE`` (2024-09-27), so
the historical matrix is frozen and elapsed time cannot change it.  A dwell threshold
would be an invented constant guarding a quantity that does not move.

Spec: ``docs/proposals/ta/2026-08-22-autonomy-approval-mode.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

import psycopg

from app.services.strategy_control_plane import (
    PaperPool,
    Stage,
    StrategyControlError,
    current_stage,
    load_paper_pool,
    registered_strategy_purpose,
)
from app.services.strategy_operator_promotion import (
    OperatorAction,
    advance_strategy,
    allowed_operator_action,
)
from app.services.strategy_result_identity import current_result_versions

#: The policy that approves.  Bumped when the rule set below changes -- the population
#: of promotions it has stamped is irrelevant, exactly as ``CORE_MANDATE_POLICY_VERSION``
#: reasons about its own bump.
AUTONOMY_POLICY_VERSION: Final = "autonomy-v1"

#: Stamped into ``strategy_promotions.promoted_by`` in place of an operator username.
#: This constant has a PERSISTER on this branch (``_advance_one`` below) -- a version
#: stamp whose only occurrence is its own definition is decorative, per the
#: prevention-log entry on declared-but-unwired symbols.
AUTONOMOUS_APPROVER: Final = f"policy@{AUTONOMY_POLICY_VERSION}"

#: The actions this approver may take.  ``register_research_candidate`` is absent on
#: purpose: it carries no evidence, so a policy that acts on evidence has nothing to act
#: on.  Registration stays an operator action.
AUTONOMOUS_ACTIONS: Final[frozenset[OperatorAction]] = frozenset(
    {"validate_historical", "start_forward_observation", "enable_paper"}
)


@dataclass(frozen=True)
class AutonomousAdvance:
    strategy_id: str
    strategy_version: str
    from_stage: Stage | None
    stage: Stage
    promotion_id: int
    evidence_ref: str | None


@dataclass(frozen=True)
class AutonomousPromotionReport:
    """What one cycle did, and why it did not do the rest.

    ``skipped_reason`` is cycle-level: it is set exactly when the authority itself
    refused, in which case no strategy was examined at all.
    """

    approval_mode: str
    skipped_reason: str | None
    advanced: tuple[AutonomousAdvance, ...]
    refusals: tuple[tuple[str, str], ...]

    @property
    def refusal_codes(self) -> tuple[str, ...]:
        """Distinct codes, sorted -- what a job note should carry.

        A cycle that advanced nothing must say WHY rather than report a bare zero;
        the per-strategy detail is in ``refusals`` and is not worth a table.
        """
        return tuple(sorted({code for _, code in self.refusals}))


def cycle_precondition_refusal(pool: PaperPool) -> str | None:
    """Why this authority may not approve anything, or ``None``.

    Pure, so all three refusals are table-testable without a database.

    ``paper_pool_disabled`` is a decision and not an oversight.  The flag is an
    attribute of a capital authority; an operator who has disabled the pool has
    withdrawn the authority the flag qualifies, and a policy approver advancing
    strategies toward ``paper_enabled`` against a withdrawn authority is the wrong
    direction to fail in.
    """
    if pool.approval_mode != "autonomous":
        return "approval_mode_manual"
    if not pool.mandate.configured:
        # Unreachable through `configure_paper_pool`, which refuses the pair, and
        # through sql/365's CHECK.  Kept because `PaperPool` is publicly constructible
        # and this is the read side of a safety flag.
        return "mandate_unconfigured"
    if not pool.enabled:
        return "paper_pool_disabled"
    return None


def planned_action(stage: Stage | None) -> tuple[OperatorAction | None, str | None]:
    """``(action to take, skip code)`` for one strategy's current stage.

    Pure.  Exactly one of the two is ``None``.  Advisory only -- ``advance_strategy``
    re-reads the stage under its own lock and stays authoritative, so this never
    duplicates a gate, only classifies a skip for the report.
    """
    action = allowed_operator_action(stage)
    if action is None:
        return None, "stage_terminal"
    if action not in AUTONOMOUS_ACTIONS:
        return None, "action_not_evidence_backed"
    return action, None


def _reason(pool: PaperPool) -> str:
    """Deterministic and bounded.

    ``strategy_promotions.reason`` is ``TEXT`` with only a ``<> ''`` CHECK
    (``sql/281:37``), so there is no length cliff; the bound is for legibility.
    """
    return f"autonomous advance under approval_mode=autonomous (pool event {pool.event_id}, {AUTONOMY_POLICY_VERSION})"


class _Refused(Exception):
    """Carries one strategy's refusal out of its transaction block, rolling it back.

    ``advance_strategy`` raises inside ``conn.transaction()``.  Catching it there and
    returning would COMMIT the block; letting it escape as itself would be
    indistinguishable from a fault.  A private exception rolls the one strategy back
    and is caught by the cycle, which is the behaviour wanted in both halves.
    """

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def _advance_one(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    pool: PaperPool,
    as_of: datetime,
) -> tuple[AutonomousAdvance | None, str | None]:
    """One strategy, in its OWN transaction.

    ⚠ Per-strategy and not per-cycle.  A cycle-wide transaction would let an
    unrelated failure on the last strategy roll back every promotion before it, and
    would make the iteration order decide who advances.  Committing per strategy is
    also what makes the report describe what is durably true.
    """
    with conn.transaction():
        action, skip = planned_action(current_stage(conn, strategy_id, strategy_version))
        if action is None:
            assert skip is not None
            return None, skip
        if registered_strategy_purpose(strategy_id) != "capital_candidate":
            # Advisory duplicate of `advance_strategy`'s own check, kept only so the
            # report distinguishes "not a capital candidate" from an evidence refusal.
            return None, "strategy_not_capital_candidate"
        try:
            outcome = advance_strategy(
                conn,
                strategy_id=strategy_id,
                action=action,
                advanced_by=AUTONOMOUS_APPROVER,
                reason=_reason(pool),
                as_of=as_of,
            )
        except StrategyControlError as exc:
            # A refusal is this job's NORMAL output, not a fault -- the same reading
            # `strategy_signal_scan._commit_strategy` gives a per-strategy failure.
            raise _Refused(str(exc)) from exc
    return (
        AutonomousAdvance(
            strategy_id=outcome.strategy_id,
            strategy_version=outcome.strategy_version,
            from_stage=outcome.from_stage,
            stage=outcome.stage,
            promotion_id=outcome.promotion.promotion_id,
            evidence_ref=outcome.evidence_ref,
        ),
        None,
    )


def run_autonomous_promotion_cycle(conn: psycopg.Connection[Any], *, as_of: datetime) -> AutonomousPromotionReport:
    """Advance every eligible strategy by at most one step, on policy authority.

    **At most one step per strategy per cycle** -- hygiene, not a safety invariant, and
    worth being blunt about which.  It keeps one report line per strategy and stops a
    tick acting on a stage it has just created.  It does NOT bound elapsed time: manual
    dispatch, catch-up and a cadence change all defeat it.  The elapsed-time bound that
    does hold is the prospective-assessment rule named in the module docstring.

    Strategies are visited in sorted id order so the report is stable, not because the
    order carries meaning -- each one commits independently.
    """
    pool = load_paper_pool(conn)
    refusal = cycle_precondition_refusal(pool)
    if refusal is not None:
        return AutonomousPromotionReport(
            approval_mode=pool.approval_mode, skipped_reason=refusal, advanced=(), refusals=()
        )

    advanced: list[AutonomousAdvance] = []
    refusals: list[tuple[str, str]] = []
    for strategy_id, strategy_version in sorted(current_result_versions().items()):
        try:
            outcome, skip = _advance_one(
                conn,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                pool=pool,
                as_of=as_of,
            )
        except _Refused as exc:
            refusals.append((strategy_id, exc.detail))
            continue
        if outcome is not None:
            advanced.append(outcome)
        elif skip is not None:
            refusals.append((strategy_id, skip))
    return AutonomousPromotionReport(
        approval_mode=pool.approval_mode,
        skipped_reason=None,
        advanced=tuple(advanced),
        refusals=tuple(refusals),
    )


__all__ = [
    "AUTONOMOUS_ACTIONS",
    "AUTONOMOUS_APPROVER",
    "AUTONOMY_POLICY_VERSION",
    "AutonomousAdvance",
    "AutonomousPromotionReport",
    "cycle_precondition_refusal",
    "planned_action",
    "run_autonomous_promotion_cycle",
]
