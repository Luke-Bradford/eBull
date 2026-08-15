"""MT-1 identities and immutable pre-outcome declarations (#2437)."""

from __future__ import annotations

import json
import math

import pytest

from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, FX_UNMODELLED
from app.services.equity_curve import EQUITY_CURVE_ENGINE_VERSION
from app.services.market_calendar import RULE_SET_VERSION as MARKET_CALENDAR_RULE_VERSION
from app.services.prereg_contract import declaration_refusals
from app.services.strategies.s8_range_mean_reversion import s8_identity
from app.services.strategies.s10_relative_strength_leader import s10_identity
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_mt1_books import BOOK_RULE_VERSION
from app.services.strategy_mt1_identity import (
    MT1_STRATEGY_ID,
    S8_CONTROL_STRATEGY_ID,
    mt1_identity,
    s8_control_identity,
)
from app.services.strategy_mt1_trial import (
    NEGATIVE_CONTROL_TRIAL_ID,
    TRIAL_CONTRACT_VERSION,
    TRIAL_EVALUATOR_VERSION,
    TRIAL_ID,
)
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION
from app.services.strategy_volatility_overlay import RULE_SET_VERSION as OVERLAY_RULE_SET_VERSION
from app.services.trial_register import TRIAL_REGISTER
from scripts import _prereg_freeze_guard as guard
from scripts.freeze_2437_mt1_declarations import (
    MIN_FORWARD_CALENDAR_WEEKS,
    MIN_FORWARD_DECISION_DATES,
    build_declarations,
    build_mt1_declaration,
    build_s8_control_declaration,
    main,
)

UNIVERSE = "survivorship_free"


def test_trial_ids_are_the_distinct_strategy_ids_and_registered_exactly() -> None:
    assert MT1_STRATEGY_ID == TRIAL_ID
    assert S8_CONTROL_STRATEGY_ID == NEGATIVE_CONTROL_TRIAL_ID
    assert MT1_STRATEGY_ID != S8_CONTROL_STRATEGY_ID
    for trial_id in (MT1_STRATEGY_ID, S8_CONTROL_STRATEGY_ID):
        assert trial_id in TRIAL_REGISTER.trial_ids
        assert trial_id not in STRATEGY_MANIFEST


def test_identities_bind_every_direct_trial_dependency() -> None:
    mt1 = mt1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
    control = s8_control_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
    s10 = s10_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
    s8 = s8_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)

    assert mt1.version != control.version
    assert mt1.version not in {s10.version, s8.version}
    assert control.version not in {s10.version, s8.version}
    assert mt1.params["source_strategy_version"] == s10.version
    assert mt1.params["decision_clock_strategy_version"] == s10.version
    assert control.params["source_strategy_version"] == s8.version
    assert control.params["decision_clock_strategy_version"] == s10.version
    for identity in (mt1, control):
        assert identity.params["overlay_rule_set_version"] == OVERLAY_RULE_SET_VERSION
        assert identity.params["four_arm_book_rule_version"] == BOOK_RULE_VERSION
        assert identity.params["trial_contract_version"] == TRIAL_CONTRACT_VERSION
        assert identity.params["trial_evaluator_version"] == TRIAL_EVALUATOR_VERSION
    assert EQUITY_CURVE_ENGINE_VERSION in BOOK_RULE_VERSION
    assert MARKET_CALENDAR_RULE_VERSION in BOOK_RULE_VERSION


def test_identity_moves_with_universe_and_cost_and_refuses_an_empty_cost() -> None:
    base = mt1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID)
    assert base.version != mt1_identity(universe="survivor_only", cost_model_id=COST_MODEL_ID).version
    assert base.version != mt1_identity(universe=UNIVERSE, cost_model_id=COST_MODEL_ID + "-changed").version
    with pytest.raises(ValueError, match="non-empty"):
        mt1_identity(universe=UNIVERSE, cost_model_id="  ")


def test_declarations_are_coherent_and_preserve_the_candidate_control_boundary() -> None:
    mt1, control = build_declarations()
    assert mt1 == build_mt1_declaration()
    assert control == build_s8_control_declaration()
    assert mt1.prereg_purpose == "capital_candidate"
    assert control.prereg_purpose == "falsification_only"
    for declaration in (mt1, control):
        assert declaration_refusals(declaration) == ()
        assert declaration.contract_version == TRIAL_CONTRACT_VERSION
        assert declaration.structural_refusal_policy_version == STRUCTURAL_REFUSAL_POLICY_VERSION
        assert declaration.declared_universe_basis == UNIVERSE
        assert declaration.declared_carry_unmodelled is CARRY_UNMODELLED is False
        assert declaration.declared_fx_unmodelled is FX_UNMODELLED is False
        assert declaration.expected_structural_refusals == ()
        assert declaration.recomputed_structural_refusals == ()


def test_forward_shadow_floor_is_rederived_from_the_frozen_power_calculation() -> None:
    derived = math.ceil(((1.959963984540054 + 0.8416212335729143) / 0.5) ** 2)
    assert derived == 32
    assert MIN_FORWARD_DECISION_DATES == max(36, derived) == 36
    assert MIN_FORWARD_CALENDAR_WEEKS == math.ceil(36 * 365.25 / (12 * 7)) == 157
    for declaration in build_declarations():
        assert declaration.forward_shadow.min_independent_decision_dates == 36
        assert declaration.forward_shadow.min_calendar_weeks == 157
        assert "n=ceil" in declaration.forward_shadow.derivation
        assert "moving-block bootstrap" in declaration.forward_shadow.derivation
        assert len(declaration.forward_shadow.derivation) <= 1000


def test_dry_run_prints_both_complete_distinct_digest_payloads(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(guard, "refresh_main_ref", lambda: True)
    monkeypatch.setattr(guard, "policy_version_on_main", lambda: STRUCTURAL_REFUSAL_POLICY_VERSION)
    assert main(["--dry-run"]) == 0
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(rows) == 2
    assert {row["strategy_id"] for row in rows} == {MT1_STRATEGY_ID, S8_CONTROL_STRATEGY_ID}
    assert len({row["strategy_version"] for row in rows}) == 2
    assert len({row["declaration_sha256"] for row in rows}) == 2
    for row, declaration in zip(rows, build_declarations(), strict=True):
        assert row["outcome"] == "dry_run"
        assert set(declaration.digest_payload) <= set(row)
        assert row["declaration_sha256"] == declaration.sha256
