"""#2616 — declarations and re-run gate for the two pre-cutoff sealed openers.

The pure half, mirroring ``tests/test_c4_declaration_gate.py``: the declarations
the freeze script builds, the arithmetic behind their constructed forward-shadow
floors, and the three-rule register check that makes a re-run charge a NEW
register entry. The DB refusals themselves are #2599/#2614 machinery already
exercised by ``tests/test_c4_declaration_gate_db.py``.
"""

from __future__ import annotations

import math
from dataclasses import replace

import pytest

from app.services.prereg_contract import declaration_refusals
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION
from app.services.trial_register import TRIAL_REGISTER, DeclaredTrial, TrialExactness, TrialRegister
from scripts.freeze_2616_precutoff_declarations import (
    INSIDER_MIN_FORWARD_CALENDAR_WEEKS,
    INSIDER_MIN_FORWARD_DECISION_DATES,
    PEAD_MIN_FORWARD_CALENDAR_WEEKS,
    PEAD_MIN_FORWARD_DECISION_DATES,
    PEAD_MIN_FORWARD_EVENTS,
    build_insider_declaration,
    build_pead_declaration,
)
from scripts.sealed_rerun_gate import RerunGateRefusal, require_rerun_trial_id
from scripts.verify_2476_pead_outcomes import SEALED_TRIAL as PEAD_SEALED_TRIAL
from scripts.verify_2480_insider_outcomes import SEALED_TRIAL as INSIDER_SEALED_TRIAL

_BUILDERS = (build_pead_declaration, build_insider_declaration)


def test_both_declarations_are_coherent_and_declare_falsification_only() -> None:
    for build in _BUILDERS:
        declaration = build()
        assert declaration_refusals(declaration) == (), declaration.strategy_id
        assert declaration.prereg_purpose == "falsification_only"
        assert declaration.structural_refusal_policy_version == STRUCTURAL_REFUSAL_POLICY_VERSION


def test_the_declared_stamps_produce_both_structural_refusals() -> None:
    """Both trials are structurally unpromotable before any outcome is read."""

    for build in _BUILDERS:
        declaration = build()
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
    for build in _BUILDERS:
        ineligible = replace(build(), prereg_purpose="capital_candidate")
        assert "ineligible_trial_not_declared_falsification" in declaration_refusals(ineligible)


def test_the_pead_floor_is_derived_not_chosen() -> None:
    """Re-derives the floor from the sealed result document's frozen figures.

    ⚠ THE POINT IS THAT NO NUMBER IS TYPED INTO THE SCRIPT'S OUTPUT. 2,427/508/
    +0.440%/[-2.866, +3.763]/1,649 days are the published sealed-run figures
    (2026-08-10-pead-result.md); 2.8016 = z_0.975 + z_0.8, C-4's constants. If
    an input moves and the constants do not, this fails rather than shipping a
    floor nothing evidences — the #2600 padded-floor defect.
    """

    standard_error = (3.763 + 2.866) / (2 * 1.96)
    events = math.ceil(2427 * (standard_error * 2.8016 / 0.440) ** 2)
    dates = math.ceil(events * 508 / 2427)
    assert PEAD_MIN_FORWARD_EVENTS == events == 281385
    assert PEAD_MIN_FORWARD_DECISION_DATES == dates == 58898
    assert PEAD_MIN_FORWARD_CALENDAR_WEEKS == math.ceil(dates * 1649 / (508 * 7)) == 27313


def test_the_insider_floor_is_derived_not_chosen() -> None:
    """Same re-derivation for the monthly-formation trial (49 months sealed)."""

    standard_error = (4.215 + 1.452) / (2 * 1.96)
    months = math.ceil(49 * (standard_error * 2.8016 / 1.192) ** 2)
    assert INSIDER_MIN_FORWARD_DECISION_DATES == months == 566
    assert INSIDER_MIN_FORWARD_CALENDAR_WEEKS == math.ceil(months * 365.25 / (12 * 7)) == 2462


def test_the_derivations_record_the_falsified_power_calc_premise() -> None:
    """#2616 claimed both preregistrations carry a power calculation; neither
    does, and the correction must live in the frozen artefact itself, where the
    next reader of the floor will actually look."""

    for build in _BUILDERS:
        derivation = build().forward_shadow.derivation
        assert "No power calculation exists in" in derivation
        assert "2.8016" in derivation
        assert "falsification_only" in derivation
        # ⚠ Mirrors sql/333's CHECK (char_length BETWEEN 1 AND 1000) in the
        # tier that runs on every push; the DB caught the first draft over it.
        assert 1 <= len(derivation) <= 1000, len(derivation)


def test_the_digests_are_stable_across_two_builds() -> None:
    for build in _BUILDERS:
        assert build().sha256 == build().sha256


def test_the_identities_bind_the_register_entries_the_looks_were_charged_to() -> None:
    """Pins the attribution #2614/#2616 got wrong: the verify_2480 look was
    charged as form4-code-p-opportunistic-purchase-v1 (the sealed portfolio
    run), NOT insider-purchase-forward-returns-first-look-2026-08-09 (which is
    verify_2437_insider_forward_returns.py's distinct construction)."""

    assert PEAD_SEALED_TRIAL.original_trial_id == "pead-historical-sue-net-income-v1"
    assert INSIDER_SEALED_TRIAL.original_trial_id == "form4-code-p-opportunistic-purchase-v1"
    for trial in (PEAD_SEALED_TRIAL, INSIDER_SEALED_TRIAL):
        assert trial.original_trial_id in TRIAL_REGISTER.trial_ids
        assert trial.original_trial_id.startswith(trial.rerun_trial_id_prefix)


def test_a_rerun_may_not_charge_the_spent_original_entry() -> None:
    for trial in (PEAD_SEALED_TRIAL, INSIDER_SEALED_TRIAL):
        with pytest.raises(RerunGateRefusal, match="does not pre-pay"):
            require_rerun_trial_id(trial, trial.original_trial_id)


def test_a_rerun_may_not_name_an_unrelated_register_entry() -> None:
    """s1-time-series-momentum IS in the register; a bare membership check would
    accept it and this look would charge nothing new."""

    for trial in (PEAD_SEALED_TRIAL, INSIDER_SEALED_TRIAL):
        with pytest.raises(RerunGateRefusal, match="prefix"):
            require_rerun_trial_id(trial, "s1-time-series-momentum")


def test_a_rerun_may_not_run_before_its_entry_is_declared() -> None:
    undeclared = PEAD_SEALED_TRIAL.original_trial_id + "-rerun-undeclared"
    with pytest.raises(RerunGateRefusal, match="absent from"):
        require_rerun_trial_id(PEAD_SEALED_TRIAL, undeclared)


def test_a_missing_rerun_id_is_refused() -> None:
    for absent in (None, ""):
        with pytest.raises(RerunGateRefusal, match="must name the register entry"):
            require_rerun_trial_id(PEAD_SEALED_TRIAL, absent)


def test_a_declared_family_rerun_entry_passes() -> None:
    """The one path through the gate: a NEW register entry carrying the family
    prefix. Exercised against a synthetic register because the whole point of
    the rule is that no such entry exists yet in the real one."""

    rerun_id = PEAD_SEALED_TRIAL.original_trial_id + "-rerun-2026-09"
    register = TrialRegister(
        version="trial-register-test",
        trials=(
            DeclaredTrial(
                trial_id=rerun_id,
                description="synthetic re-run entry for the gate's success path",
                evidence="this test",
                exactness=TrialExactness.EXACT,
            ),
        ),
    )
    assert require_rerun_trial_id(PEAD_SEALED_TRIAL, rerun_id, register) == rerun_id
