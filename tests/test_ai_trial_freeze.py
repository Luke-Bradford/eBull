"""#3471 slice 3d — the declaration freeze's pure half: the document, the #2599 terms, the
register entry and the structural precondition table (spec §9 "Freeze")."""

from __future__ import annotations

from typing import Any

from app.services.ai_trial_freeze import (
    EXPECTED_REGISTER_ENTRY,
    build_declaration,
    config_refusals,
    prereg_declaration,
    prereg_terms,
)
from app.services.ai_trial_intent import declaration_digest
from app.services.ai_trial_policy import AI_TRIAL_POLICY_HASH, policy_hash, policy_manifest
from app.services.prereg_contract import declaration_refusals
from app.services.strategy_result import structural_promotion_refusals
from app.services.trial_register import TRIAL_REGISTER


def _doc() -> dict[str, Any]:
    return build_declaration(
        manifest=policy_manifest(), code_git_sha="a" * 40, spec_sha256="b" * 64, python_version="3.12.0"
    )


def test_the_register_charges_the_trial_once_with_the_pinned_entry() -> None:
    entries = [trial for trial in TRIAL_REGISTER.trials if trial.declared_for == EXPECTED_REGISTER_ENTRY.declared_for]
    assert entries == [EXPECTED_REGISTER_ENTRY]
    assert TRIAL_REGISTER.trial_for_declaration("ai-discretionary-v1", "v1") == EXPECTED_REGISTER_ENTRY


def test_the_manifest_is_what_policy_hash_hashes() -> None:
    # One snapshot: the document's modules and constants re-hash to its policy_hash.
    manifest = policy_manifest()
    assert manifest.digest() == policy_hash() == AI_TRIAL_POLICY_HASH
    doc = _doc()
    assert doc["policy_hash"] == AI_TRIAL_POLICY_HASH
    assert doc["policy_modules"] == manifest.module_sha256
    assert doc["frozen_constants"] == manifest.constant_repr
    assert all(isinstance(value, str) for value in doc["frozen_constants"].values())


def test_the_document_is_canonical_and_binds_the_prereg_terms() -> None:
    doc = _doc()
    assert declaration_digest(doc) == declaration_digest(_doc())
    assert (doc["strategy_id"], doc["strategy_version"], doc["control_strategy_id"]) == (
        "ai-discretionary-v1",
        "v1",
        "ai-discretionary-v1-control",
    )
    assert doc["prereg"] == prereg_terms()
    # Provenance moves the digest: a different commit is a different declaration.
    other = build_declaration(
        manifest=policy_manifest(), code_git_sha="c" * 40, spec_sha256="b" * 64, python_version="3.12.0"
    )
    assert declaration_digest(other) != declaration_digest(doc)


def test_the_prereg_terms_are_fixed_by_construction() -> None:
    terms = prereg_terms()
    assert terms["prereg_purpose"] == "falsification_only"
    assert terms["declared_universe_basis"] == "survivor_only"
    assert (terms["declared_carry_unmodelled"], terms["declared_fx_unmodelled"]) == (False, False)
    # §9 "Too little data" (10 clusters) and the 60-session cohort's calendar floor.
    assert (terms["min_forward_decision_dates"], terms["min_forward_calendar_weeks"]) == (10, 12)
    assert terms["expected_structural_refusals"] == list(
        structural_promotion_refusals(universe_basis="survivor_only", carry_unmodelled=False, fx_unmodelled=False)
    )
    # The #2599 contract accepts it: a falsification_only declaration over survivor-only stamps is coherent.
    assert declaration_refusals(prereg_declaration(doc_sha256="d" * 64, declared_by="test")) == ()


def _leg(**overrides: Any) -> dict[str, Any]:
    return {
        "deployment_id": 1,
        "enabled": True,
        "capital_limit": "1000.000000",
        "currency": "USD",
        "policy": {},
        **overrides,
    }


def test_config_refusals_check_structure_only() -> None:
    arm, control = "ai-discretionary-v1", "ai-discretionary-v1-control"
    assert config_refusals({arm: _leg(), control: _leg(deployment_id=2)}) == []
    assert config_refusals({arm: _leg()}) == ["trial_deployment_missing"]
    assert config_refusals({arm: _leg(enabled=False), control: _leg()}) == ["trial_deployment_disabled"]
    assert config_refusals({arm: _leg(capital_limit="0"), control: _leg(capital_limit="0")}) == [
        "trial_deployment_unfunded"
    ]
    assert config_refusals({arm: _leg(currency="GBP"), control: _leg()}) == ["trial_deployment_not_usd"]
    assert config_refusals({arm: _leg(capital_limit="999.000000"), control: _leg()}) == ["trial_capital_parity"]
