"""#3610 — the two-track rule and the pre-declaration power check, enforced at freeze.

DB-free: the gate fires before ``freeze_preregistration`` touches its connection, so a bare
``object()`` connection separates "refused" (``ValueError``) from "admitted" (``AttributeError``),
the same property ``tests/test_2829_declaration_trial_binding.py`` leans on.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import replace
from typing import Any, cast

import pytest

from app.services.deflated_sharpe import expected_max_sharpe
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration
from app.services.result_ledger import freeze_preregistration
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION
from app.services.trial_register import (
    DESIGN_ALPHA,
    MIN_DESIGN_POWER,
    PRE_POWER_RULE_CLAIMS,
    TRIAL_REGISTER,
    DeclaredTrial,
    EvidenceTrack,
    TrialDesign,
    TrialExactness,
    TrialRegister,
    power_check,
)

_N = statistics.NormalDist()
_PAIR = ("power-test-trial", "v1")


def _design(**overrides: Any) -> TrialDesign:
    fields: dict[str, Any] = {
        "track": EvidenceTrack.ADOPTION,
        "effect_ir": 0.5,
        "effect_basis": "test prior",
        "effective_years": 60.0,
        "dependence": "test: annual non-overlapping",
    }
    return TrialDesign(**{**fields, **overrides})


def _trial(design: TrialDesign | None, *, searches: int = 1) -> DeclaredTrial:
    return DeclaredTrial(
        trial_id="power-test-trial",
        description="d",
        evidence="e",
        exactness=TrialExactness.EXACT,
        searches=searches,
        declared_for=_PAIR,
        design=design,
    )


def _declaration(strategy_id: str, strategy_version: str) -> PreregDeclaration:
    # A non-manifest id, so the cost-stamp check above the gates does not fire first.
    return PreregDeclaration(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        contract_version="test-contract-v1",
        prereg_purpose="falsification_only",
        structural_refusal_policy_version=STRUCTURAL_REFUSAL_POLICY_VERSION,
        declared_universe_basis="survivor_only",
        declared_carry_unmodelled=False,
        declared_fx_unmodelled=False,
        expected_structural_refusals=("universe_basis_not_survivorship_free",),
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=40, min_calendar_weeks=12, derivation="tests/test_3610_power_check.py"
        ),
        declared_by="tests/test_3610_power_check.py",
    )


def _freeze_with(monkeypatch: pytest.MonkeyPatch, register: TrialRegister, pair: tuple[str, str] = _PAIR) -> None:
    monkeypatch.setattr("app.services.result_ledger.TRIAL_REGISTER", register)
    freeze_preregistration(cast("Any", object()), _declaration(*pair))


class TestTheFormula:
    def test_the_research_process_worked_example(self) -> None:
        """``quant/research-process.md``: IR 0.5 over 15 years against t ~ 3 has power ~14%."""
        assert _N.cdf(0.5 * math.sqrt(15) - 3.0) == pytest.approx(0.144, abs=0.001)

    def test_one_trial_is_a_plain_one_sided_test(self) -> None:
        check = power_check(_design(), trials=1)
        assert check.critical_t == pytest.approx(_N.inv_cdf(1 - DESIGN_ALPHA))

    def test_many_trials_add_the_expected_maximum(self) -> None:
        check = power_check(_design(), trials=505)
        expected_max = expected_max_sharpe(trial_sharpe_variance=1.0, independent_trials=505)
        assert check.critical_t == pytest.approx(expected_max + _N.inv_cdf(1 - DESIGN_ALPHA))

    def test_required_years_deliver_exactly_the_target_power(self) -> None:
        """The two outputs are one formula read both ways."""
        check = power_check(_design(), trials=40)
        at_required = power_check(_design(effective_years=check.required_years), trials=40)
        assert at_required.power == pytest.approx(MIN_DESIGN_POWER)
        assert at_required.feasible

    def test_feasible_is_required_years_within_the_supply(self) -> None:
        check = power_check(_design(effective_years=1.0), trials=1)
        assert not check.feasible
        assert check.power < MIN_DESIGN_POWER


class TestTheDesignInputs:
    @pytest.mark.parametrize(
        "overrides",
        [
            {"effect_basis": " "},
            {"dependence": ""},
            {"effect_ir": 0.0},
            {"effect_ir": float("nan")},
            {"effective_years": -1.0},
            {"effective_years": True},
            {"target_power": 0.5},
            {"target_power": 1.0},
            {"track": "adoption"},
        ],
    )
    def test_an_unusable_input_is_refused(self, overrides: dict[str, Any]) -> None:
        with pytest.raises(ValueError):
            _design(**overrides)

    def test_a_design_needs_a_declaration_to_be_checked_against(self) -> None:
        with pytest.raises(ValueError, match="needs declared_for"):
            replace(_trial(None), declared_for=None, design=_design())


class TestTheFreezeGate:
    def test_a_claim_without_a_design_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        register = TrialRegister(version="t", trials=(_trial(None),))
        with pytest.raises(ValueError, match="declares no TrialDesign"):
            _freeze_with(monkeypatch, register)

    def test_an_infeasible_design_is_refused_as_data_infeasible(self, monkeypatch: pytest.MonkeyPatch) -> None:
        register = TrialRegister(version="t", trials=(_trial(_design(effective_years=2.0)),))
        with pytest.raises(ValueError, match="data_infeasible"):
            _freeze_with(monkeypatch, register)

    def test_a_feasible_design_reaches_the_connection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The positive arm: without it the refusals above pass for a gate that refuses everything."""
        register = TrialRegister(version="t", trials=(_trial(_design()),))
        with pytest.raises(AttributeError):
            _freeze_with(monkeypatch, register)

    def test_discovery_is_checked_against_the_global_count(self) -> None:
        """Same design, same entry: Track A sees every search in the register, Track B its own."""
        filler = DeclaredTrial(
            trial_id="filler", description="d", evidence="e", exactness=TrialExactness.EXACT, searches=500
        )
        for track, expected in ((EvidenceTrack.DISCOVERY, 503), (EvidenceTrack.ADOPTION, 3)):
            design = _design(track=track, effect_ir=1.5)
            register = TrialRegister(version="t", trials=(filler, _trial(design, searches=3)))
            record = register.freeze_power_record(register.trials[1])
            assert record is not None
            assert record["trials"] == expected
            assert record["track"] == track.value

    def test_the_record_carries_the_inputs_and_the_register_version(self) -> None:
        register = TrialRegister(version="t-v9", trials=(_trial(_design()),))
        record = register.freeze_power_record(register.trials[0])
        assert record is not None
        assert record["register_version"] == "t-v9"
        assert {"critical_t", "required_years", "power", "effect_ir", "effective_years", "dependence"} <= set(record)


class TestTheShippedRegister:
    def test_every_pre_rule_claim_is_a_real_claim(self) -> None:
        """A typo in the closed list would grandfather nothing and refuse a frozen trial's re-freeze."""
        claimed = {trial.declared_for for trial in TRIAL_REGISTER.trials if trial.declared_for is not None}
        assert claimed >= PRE_POWER_RULE_CLAIMS

    @pytest.mark.parametrize("pair", [("ranking-pot-v2", "v1"), ("ai-discretionary-fund-v1", "v1")])
    def test_the_unfrozen_claims_are_not_grandfathered(self, pair: tuple[str, str]) -> None:
        trial = TRIAL_REGISTER.trial_for_declaration(*pair)
        assert trial is not None
        assert pair not in PRE_POWER_RULE_CLAIMS
        with pytest.raises(ValueError, match="declares no TrialDesign"):
            TRIAL_REGISTER.freeze_power_record(trial)

    def test_a_pre_rule_claim_records_nothing(self) -> None:
        trial = TRIAL_REGISTER.trial_for_declaration("ranking-pot-v1", "v1")
        assert trial is not None
        assert TRIAL_REGISTER.freeze_power_record(trial) is None
