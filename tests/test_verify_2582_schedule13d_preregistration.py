from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.verify_2582_schedule13d_preregistration import load_and_verify


def test_contract_is_fail_closed_and_outcome_free() -> None:
    contract, digest = load_and_verify()

    assert len(digest) == 64
    assert contract["event_identity"]["prior_history_order"].startswith("strictly_earlier_sec_manifest")
    assert contract["eligibility"]["unknown_security_type"] == "refuse"
    assert contract["context"]["market_series"]["return_basis"] == "price_only_close_to_close"
    assert contract["challengers"]["unknown_13g_rule"] == "never_pool_with_known_rule"
    assert contract["challengers"]["matching"]["tie_break"].startswith("sha256_")
    assert contract["acceptance"]["next_stage_if_failed"].startswith("retain_rejection")


def test_verifier_rejects_a_bracket_smuggled_into_the_historical_trial(tmp_path: Path) -> None:
    source = Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json")
    contract = json.loads(source.read_text())
    contract["position"]["stop_loss"] = "2_atr"
    mutated = tmp_path / "contract.json"
    mutated.write_text(json.dumps(contract))

    with pytest.raises(AssertionError):
        load_and_verify(mutated)


def test_verifier_rejects_an_otherwise_unchecked_contract_mutation(tmp_path: Path) -> None:
    source = Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json")
    contract = json.loads(source.read_text())
    contract["hypothesis"] = "changed after outcomes"
    mutated = tmp_path / "contract.json"
    mutated.write_text(json.dumps(contract))

    with pytest.raises(AssertionError, match="frozen contract digest moved"):
        load_and_verify(mutated)
