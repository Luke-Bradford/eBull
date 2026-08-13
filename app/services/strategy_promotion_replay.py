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
    Deliberately re-evaluated against the current world — and legitimate ONLY
    where the stored record DECLARES a validity window or is explicitly
    supersedable. A today-check on anything else is an undeclared freshness
    rule, which is how a passing historical result silently stops passing.

``not_re_read``
    Neither persisted nor re-derived. Tracked by #2639. ⚠⚠ THIS IS A GAP, NOT A SOLUTION: the
    transition does not enforce that clause at all, and is still trusting a
    write-time verdict that died with ``WrittenRow``. It is spelled out as its
    own rule rather than left off the table so that it cannot be mistaken for
    coverage.

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
        source="strategy_results_store columns (universe_basis, carry_unmodelled, fx_unmodelled)",
        reason=(
            "The row's stamps are immutable once written, so replaying them needs no record beyond the row. "
            "#2625 wires the three STRUCTURAL stamps through the shared structural_promotion_refusals — the "
            "same single copy #2599's preregistration freeze calls, so the transition and the freeze cannot "
            "drift. ⚠ PARTIAL: the deflation, effective-sample-size and synthetic-control clauses are "
            "columns on the same row and are still NOT replayed; they need their model objects rebuilt, "
            "which is follow-up work, not a different temporal rule."
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
        rule="not_re_read",
        replayed_at_transition=False,
        source="none — strategy_holdout_accesses is not consulted by the transition",
        reason=(
            "⚠ A GAP. result_ledger.holdout_access_counts is two pure COUNT statements and records nothing, "
            "so re-reading is SAFE — the issue's inventory table said otherwise and was wrong (the function "
            "that records is quarantine_arms_compared, and only on a hold_out identity). What blocks it is "
            "not safety but an undecided temporal rule: both counts are scoped to "
            "(strategy_id, strategy_version) rather than to a result, so a later hold-out evaluation "
            "retroactively changes what a replay of an OLDER pinned result would see. Frozen-at-result-time "
            "and today's-count genuinely differ, and neither is obviously right. Left explicitly undecided "
            "rather than answered silently."
        ),
    ),
    "recorded_accesses": ReplayPolicyEntry(
        rule="not_re_read",
        replayed_at_transition=False,
        source="none — strategy_holdout_accesses is not consulted by the transition",
        reason="The other half of the same undecided count; see holdout_evaluations.",
    ),
    "quarantine_arms_compared": ReplayPolicyEntry(
        rule="not_re_read",
        replayed_at_transition=False,
        source="none — result_ledger.quarantine_arms_compared is not called by the transition",
        reason=(
            "⚠ A GAP, and the one input where re-reading has a real governance cost. On a hold_out identity "
            "that function RECORDS a 'read' access, because looking is the event criterion 5 governs — and "
            "300 of the 324 stored results are hold_out. A transition that called it would write one audit "
            "row per promotion attempt into the log it is auditing. ⚠ It would NOT change the verdict: the "
            "recorded kind is 'read' and _COUNT_EVALUATE_ACCESSES filters access_kind = 'evaluate'. The "
            "argument is that the trail becomes a count of our own automation, not that the arithmetic "
            "breaks. Closing this means persisting the comparison at result time, the #2621 move."
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
    """
    return frozenset(name for name, entry in REPLAY_TEMPORAL_POLICY.items() if not entry.replayed_at_transition)


__all__ = [
    "REPLAY_TEMPORAL_POLICY",
    "ReplayPolicyEntry",
    "ReplayRule",
    "unclassified_candidate_fields",
    "unenforced_candidate_fields",
]
