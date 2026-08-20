"""#2614 — C-4's declaration gate: the pure half.

Spec: ``docs/proposals/ta/2026-08-12-c4-declaration-gate-binding.md``.

#2599 gated the ledger, correctly, on the premise that opening an outcome always
goes through it. C-4 falsifies that premise: it computes its statistics from raw
price windows and emits a signed artifact, storing no result row. What is
asserted here is the declaration C-4 must freeze, and the arithmetic behind its
forward-shadow floor. The DB half — the refusal itself — is in
``tests/test_c4_declaration_gate_db.py``.
"""

from __future__ import annotations

import math

from app.services.prereg_contract import declaration_refusals
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION
from scripts.evaluate_2582_schedule13d_outcomes import STRATEGY_ID, STRATEGY_VERSION
from scripts.freeze_2582_schedule13d_declaration import (
    MIN_FORWARD_CALENDAR_WEEKS,
    MIN_FORWARD_DECISION_DATES,
    build_declaration,
)


def test_the_declaration_is_coherent_and_declares_falsification_only() -> None:
    declaration = build_declaration()
    assert declaration_refusals(declaration) == ()
    assert declaration.prereg_purpose == "falsification_only"
    assert (declaration.strategy_id, declaration.strategy_version) == (STRATEGY_ID, STRATEGY_VERSION)
    assert declaration.structural_refusal_policy_version == STRUCTURAL_REFUSAL_POLICY_VERSION


def test_the_declared_stamps_produce_both_structural_refusals() -> None:
    """C-4 is structurally unpromotable before its first outcome is read.

    ⚠ Which is exactly why ``falsification_only`` is the only coherent purpose:
    ``declaration_refusals`` reads the RECOMPUTED list, so declaring
    ``capital_candidate`` over these stamps fires
    ``ineligible_trial_not_declared_falsification``.
    """

    declaration = build_declaration()
    assert declaration.declared_universe_basis == "survivor_only"
    assert declaration.declared_carry_unmodelled is True
    assert declaration.declared_fx_unmodelled is True
    assert set(declaration.expected_structural_refusals) == {
        "universe_basis_not_survivorship_free",
        "carry_unmodelled",
        "fx_unmodelled",
    }
    assert set(declaration.recomputed_structural_refusals) == set(declaration.expected_structural_refusals)


def test_a_capital_candidate_purpose_over_these_stamps_is_refused() -> None:
    from dataclasses import replace

    ineligible = replace(build_declaration(), prereg_purpose="capital_candidate")
    assert "ineligible_trial_not_declared_falsification" in declaration_refusals(ineligible)


def test_the_forward_shadow_floor_is_derived_not_chosen() -> None:
    """Re-derives both floors from the contract's power calculation.

    ⚠ THE POINT IS THAT NEITHER NUMBER IS TYPED IN. 785 is the contract's frozen
    ``minimum_planning_effective_sample_size``; 963/331/547 are the measured
    full-population, outcome-free arrival counts. If either input moves and the
    constants do not, this fails rather than shipping a floor nothing evidences —
    the #2600 padded-floor defect.
    """

    minimum_effective_events = 785
    clean_events, distinct_dates, span_days = 963, 331, 547

    assert MIN_FORWARD_DECISION_DATES == math.ceil(minimum_effective_events * distinct_dates / clean_events) == 270
    assert MIN_FORWARD_CALENDAR_WEEKS == math.ceil(MIN_FORWARD_DECISION_DATES * span_days / (distinct_dates * 7)) == 64


def test_the_floor_carries_its_derivation_and_names_the_lower_bound() -> None:
    floor = build_declaration().forward_shadow
    assert floor.min_independent_decision_dates == MIN_FORWARD_DECISION_DATES > 0
    assert floor.min_calendar_weeks == MIN_FORWARD_CALENDAR_WEEKS > 0
    # ⚠ The direction of the bound is part of the artefact, not commentary: an
    # effective sample size is <= the raw count, so both floors understate.
    assert "LOWER bounds" in floor.derivation
    assert "785" in floor.derivation and "963" in floor.derivation


def test_the_digest_is_stable_across_two_builds() -> None:
    """A declaration whose digest moves between builds could never be re-verified."""

    assert build_declaration().sha256 == build_declaration().sha256
