"""The promotion transition's replay policy, declared in one place (#2625).

``check_promotable`` runs at RESULT PRODUCTION. ``promote_strategy`` — the
transition that actually moves a strategy to ``historical_validated`` — cannot
call it, so it re-derives what it can from persisted records. #2621 did that for
the universe, #2505 for the edge evidence, #2625 for the §3.4 ambiguity
comparison. The blocker the remaining inputs share is not five schemas: it is
that **nobody had written down which input replays against what**, and the
rules genuinely diverge.

⚠ THE POLICY IS KEYED ON ``PromotionCandidate``'s FIELDS, NOT ON REFUSAL CODES.
Codes are a many-to-many projection of the inputs — one input emits several
codes, several inputs emit one — so a policy or a guard keyed on codes can be
fully satisfied while an input goes unclassified. Fields are the structural key
a new gate input actually arrives as. (The first draft of #2625's guard was
code-keyed; Codex checkpoint 1 killed it for exactly this.)

The vocabulary is three rules and no more:

``frozen``
    Replayed from a record written at result time and never re-derived from
    today's world. The default, and the right answer wherever the input
    describes a measurement rather than a live condition.

``today``
    Deliberately re-evaluated against the current world. Legitimate ONLY in one
    of THREE declared shapes; anywhere else it is an undeclared freshness rule,
    which is how a passing historical result silently stops passing.

    1. the stored record DECLARES its own validity window
       (``promotion_evidence``: ``cost_observed_on`` / ``cost_valid_through``);
    2. the stored record is explicitly supersedable;
    3. the record is an append-only AUDIT LOG, the clause is a comparison
       between that log and the rows it audits, and the criterion the clause
       serves requires the comparison to be CURRENT (the two hold-out counts
       against ``strategy_holdout_accesses``).

    ⚠ Shape 3 was added by #2639 and is worded this narrowly on purpose. The
    draft form — "a ledger of our own conduct" — would admit any mutable
    operational counter, which is most of the database. All three clauses
    together are the restriction, and ``TestTodayIsRestricted`` still pins the
    exact member set, so a fourth today-check fails the test whatever it claims
    about itself.

``not_re_read``
    Neither persisted nor re-derived. ⚠⚠ THIS IS A GAP, NOT A SOLUTION: the
    transition does not enforce that clause at all, and is still trusting a
    write-time verdict that died with ``WrittenRow``. It is spelled out as its
    own rule rather than left off the table so that it cannot be mistaken for
    coverage.

    ⚠ NO INPUT CARRIES IT TODAY — #2639 closed the last three, and
    ``unenforced_candidate_fields()`` returns an empty set. The rule stays in the
    vocabulary because the next unclassifiable input needs an honest label to
    land on, and deleting it would leave the alternative of mislabelling that
    input ``frozen``, which reads as coverage.

⚠ ``replayed_at_transition`` is a SEPARATE axis from the rule, and the two must
not be conflated: "this input's temporal rule is frozen" and "the transition
actually checks it" are different claims, and every gap in this module is a
field where the first is true and the second is false. Measured 2026-08-13:
``grep`` for ``universe_basis|carry_unmodelled|fx_unmodelled|deflated|
trial_count|effective_sample_size|synthetic_control`` in
``strategy_control_plane.py`` returned NOTHING before #2625 — the row's own
stamps were persisted and never replayed.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal

from app.services.strategy_result import PromotionCandidate

#: See the module docstring. ``not_re_read`` names a gap, deliberately.
ReplayRule = Literal["frozen", "today", "not_re_read"]


@dataclass(frozen=True)
class ReplayPolicyEntry:
    """One gate input's temporal rule, and whether the transition applies it."""

    rule: ReplayRule
    #: Whether ``promote_strategy`` actually re-derives this input today.
    #: ⚠ ``False`` beside ``rule="frozen"`` is an UNENFORCED input, not a
    #: settled one — the state this module exists to make countable.
    replayed_at_transition: bool
    #: Where the replayed value comes from (or would come from).
    source: str
    reason: str


REPLAY_TEMPORAL_POLICY: Final[Mapping[str, ReplayPolicyEntry]] = {
    "result": ReplayPolicyEntry(
        rule="frozen",
        replayed_at_transition=True,
        source=(
            "strategy_results_store — the structural stamps read directly by promote_strategy, the rest "
            "through result_ledger.stored_result_promotion_refusals (#2639)"
        ),
        reason=(
            "The row's stamps are immutable once written, so replaying them needs no record beyond the row. "
            "⚠ ONE FIELD, EIGHT CLAUSES, so 'replayed' is enumerated rather than asserted: universe_basis / "
            "carry_unmodelled / fx_unmodelled through the shared structural_promotion_refusals (#2625, the "
            "same single copy #2599's preregistration freeze calls, so the transition and the freeze cannot "
            "drift); purpose through purpose_promotion_refusals; deflated_sharpe / trial_count / "
            "trial_register_superseded / effective_sample_size through deflation_promotion_refusals; the two "
            "§9 verdicts through synthetic_control_promotion_refusals (#2639). Every one of those functions "
            "is the copy check_promotable itself calls. ⚠ trial_register_superseded compares a frozen column "
            "against the CURRENT TRIAL_REGISTER constant, which does not make the field 'today' — see "
            "promotion_evidence, where it is the current DATE that does. A register supersession "
            "invalidating older results is deliberate."
        ),
    ),
    "evaluated_instrument_ids": ReplayPolicyEntry(
        rule="frozen",
        replayed_at_transition=True,
        source="strategy_result_universe (#2621, sql/334)",
        reason=(
            "Today's load_validated_universe filters on is_tradable, so a re-check would let a later "
            "delisting retroactively invalidate a passing result — while the survivorship-free corpus "
            "deliberately evaluates delisted names. Order-time enforcement against the CURRENT universe is "
            "the execution guard's job, not promotion's."
        ),
    ),
    "validated_universe_ids": ReplayPolicyEntry(
        rule="frozen",
        replayed_at_transition=True,
        source="strategy_result_universe (#2621, sql/334)",
        reason="The other half of the same frozen record; see evaluated_instrument_ids.",
    ),
    "ambiguity_material": ReplayPolicyEntry(
        rule="frozen",
        replayed_at_transition=True,
        source="strategy_result_ambiguity (#2625, sql/339)",
        reason=(
            "A property of measurements taken during the run. No later observation could re-derive it, so "
            "there is no coherent today-rule. The record stores the comparison's INPUTS and the verdict is "
            "re-derived, so an auditor can disagree with it."
        ),
    ),
    "promotion_evidence": ReplayPolicyEntry(
        rule="today",
        replayed_at_transition=True,
        source="strategy_promotion_evidence (#2505, sql/327), evaluated with as_of=date.today()",
        reason=(
            "⚠ TODAY IS LEGITIMATE HERE PRECISELY BECAUSE THE RECORD DECLARES ITS OWN WINDOW: "
            "cost_observed_on and cost_valid_through. Executable costs go stale in wall-clock time, and the "
            "record says when. Every other clause of evidence_refusals compares frozen fields against "
            "constants — so the field as a whole is classified today, because the replay's result depends "
            "on the current date."
        ),
    ),
    "holdout_evaluations": ReplayPolicyEntry(
        rule="today",
        replayed_at_transition=True,
        source="result_ledger.holdout_access_counts (#2639), read live at the transition",
        reason=(
            "⚠⚠ TODAY BECAUSE FROZEN DEFEATS CRITERION 5, not because today is convenient. Both counts are "
            "scoped to (strategy_id, strategy_version), so a pair frozen when result #1 was written records "
            "the hold-out looks that had happened BY THEN: a strategy that later evaluates its hold-out four "
            "more times without recording an access would replay result #1 as (1, 1) — consistent, "
            "promotable, and blind to precisely the repeated unlogged look the criterion exists to catch. "
            "The retroactivity #2621 rejected for the universe is the DESIRED behaviour here, because the "
            "later event is our own conduct on this strategy version and not a change in the outside world. "
            "Qualifies under today-shape 3: an append-only audit log, a clause comparing that log against "
            "the rows it audits, and a criterion that requires the comparison to be current. ⚠ Re-reading "
            "is SAFE — holdout_access_counts is pure COUNTs and records nothing; #2639's inventory said "
            "otherwise and was wrong. ⚠ The clause is strategy-version-wide, so one unrecorded evaluation "
            "blocks every result of that version — check_promotable's own behaviour, inherited not invented "
            "— and it heals as well as blocks, since a record written later moves the answer back."
        ),
    ),
    "recorded_accesses": ReplayPolicyEntry(
        rule="today",
        replayed_at_transition=True,
        source="result_ledger.holdout_access_counts (#2639), read live at the transition",
        reason=(
            "The other half of the same comparison; see holdout_evaluations. ⚠ Both sides move: an access "
            "inserted later changes the answer as surely as an evaluation does, which is what makes the "
            "clause a self-consistency check on the log rather than a freshness rule about the world."
        ),
    ),
    "quarantine_arms_compared": ReplayPolicyEntry(
        rule="frozen",
        replayed_at_transition=True,
        source="strategy_results_store — both arms' rows, via result_ledger.quarantine_arm_pair_present (#2639)",
        reason=(
            "⚠ FROZEN AND NOT TODAY: both arms are rows written at result time and the store has no delete "
            "path, so the derived answer is monotone — it moves only from 'one arm' to 'both arms' when a "
            "sibling with the identical identity-minus-arm is stored. Nothing about today's world enters it. "
            "⚠⚠ THE TRANSITION DOES NOT CALL quarantine_arms_compared, which RECORDS a 'read' access on a "
            "hold_out identity (300 of 324 stored rows) and would turn the audit trail into a count of our "
            "own automation — 'it must not ask the database a question it is the answer to', one layer out. "
            "The counting half is split into quarantine_arm_pair_present, which records nothing; the "
            "recording door stays the one criterion 5 governs, and promotion is not an evaluation. ⚠ The "
            "sibling is DERIVED from the identity hash, never named by a stored pointer: a pointer is chosen "
            "by the writer and can name a compatible row that is not the one the identity admits. #2639's "
            "first draft proposed that pointer table; Codex checkpoint 1 killed it."
        ),
    ),
}


def unclassified_candidate_fields() -> frozenset[str]:
    """Every ``PromotionCandidate`` field this policy has not classified.

    ⚠ THE COUPLING GUARD'S WHOLE POINT. Read via ``dataclasses.fields`` rather
    than a hand-written list, so adding a gate input without deciding its
    temporal rule fails a test instead of silently inheriting one.
    """
    return frozenset(field.name for field in dataclasses.fields(PromotionCandidate)) - set(REPLAY_TEMPORAL_POLICY)


def unenforced_candidate_fields() -> frozenset[str]:
    """Classified inputs the transition does not actually replay.

    Not a failure — it is the honest inventory of what ``promote_strategy``
    still takes on trust, and a test pins it so the set cannot grow unnoticed.

    ⚠ EMPTY SINCE #2639, and the function stays because the invariant it pins is
    the useful part: an input added to ``PromotionCandidate`` and classified
    without being wired lands here and fails a test, instead of arriving as a
    gate clause nobody applies.
    """
    return frozenset(name for name, entry in REPLAY_TEMPORAL_POLICY.items() if not entry.replayed_at_transition)


__all__ = [
    "REPLAY_TEMPORAL_POLICY",
    "ReplayPolicyEntry",
    "ReplayRule",
    "unclassified_candidate_fields",
    "unenforced_candidate_fields",
]
