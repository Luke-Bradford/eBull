"""#2842 slice 4a — ranking-pot-v1 identity, policy hash and declaration document (spec §7.1, §8). Pure."""

from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from fractions import Fraction
from pathlib import Path

from app.services import ranking_pot_policy
from app.services.ai_trial_freeze import Provenance
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot_freeze import (
    CONTRACT_PREFIX,
    EXPECTED_REGISTER_ENTRY,
    S0,
    build_declaration,
    prereg_declaration,
    prereg_terms,
)
from app.services.ranking_pot_policy import (
    FAMILY_ALPHA,
    LOOK_MONTHS,
    POLICY_MODULES,
    RANKING_POT_POLICY_HASH,
    SCORER_IMPORTS,
    declaration_alpha,
    per_look_alpha,
    policy_manifest_now,
)
from app.services.strategy_control_plane import registered_strategy_purpose
from app.services.trial_register import TRIAL_REGISTER

_SERVICES = Path(ranking_pot_policy.__file__).resolve().parent
_PROVENANCE = Provenance(code_git_sha="a" * 40, spec_sha256="b" * 64, python_version="3.12.0", refusals=())


def test_pot_is_a_demo_trial_and_not_a_capital_candidate() -> None:
    # Every capital, live and promotion chokepoint keys on this (spec §7.1 enumeration; the promote and live-gate
    # halves are parametrised over DEMO_TRIAL_STRATEGY_IDS in test_3471_demo_trial_purpose.py).
    assert registered_strategy_purpose("ranking-pot-v1") == "demo_trial"


def test_scorer_imports_are_exactly_scoring_py_in_repo_imports() -> None:
    """§8 hashes the scorer and its in-repo imports; a new import must not slip out of the hash."""
    tree = ast.parse((_SERVICES / "scoring.py").read_text())
    imported = {
        node.module.removeprefix("app.services.") + ".py"
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("app.")
    }
    assert imported == set(SCORER_IMPORTS)


def test_policy_modules_exist_are_sorted_and_hash_matches_disk() -> None:
    assert list(POLICY_MODULES) == sorted(POLICY_MODULES)
    assert all((_SERVICES / name).is_file() for name in POLICY_MODULES)
    assert {"scoring.py", "ranking_pot.py", "ranking_pot_sim.py", "ranking_pot_policy.py"} <= set(POLICY_MODULES)
    assert policy_manifest_now().digest() == RANKING_POT_POLICY_HASH


def test_policy_hash_moves_when_a_hashed_module_changes(tmp_path: Path) -> None:
    for name in POLICY_MODULES:
        (tmp_path / name).write_bytes((_SERVICES / name).read_bytes())
    assert policy_manifest_now(tmp_path).digest() == RANKING_POT_POLICY_HASH
    (tmp_path / "ranking_pot.py").write_bytes((_SERVICES / "ranking_pot.py").read_bytes() + b"\n")
    assert policy_manifest_now(tmp_path).digest() != RANKING_POT_POLICY_HASH


def test_family_spending_is_geometric_and_v1_spends_0_0125_per_look() -> None:
    assert per_look_alpha(1) == Fraction(1, 80)  # spec §9.3 condition 1
    assert declaration_alpha(1) == Fraction(1, 40)
    assert sum(declaration_alpha(m) for m in range(1, 60)) < FAMILY_ALPHA
    assert per_look_alpha(2) * len(LOOK_MONTHS) == declaration_alpha(2)


def test_register_holds_the_expected_entry_verbatim() -> None:
    assert EXPECTED_REGISTER_ENTRY in TRIAL_REGISTER.trials


def _doc() -> dict[str, object]:
    module_sha, constants = policy_manifest_now().module_sha256, policy_manifest_now().constant_repr
    return build_declaration(
        s0=S0(scored_at=datetime(2026, 10, 1, 17, 7, 54, tzinfo=UTC), instrument_ids=(3, 7, 11)),
        family_seq=1,
        module_sha256=module_sha,
        constant_repr=constants,
        policy_hash=RANKING_POT_POLICY_HASH,
        provenance=_PROVENANCE,
    )


def test_document_round_trips_through_json_and_binds_its_terms() -> None:
    doc = _doc()
    assert canonical_sha256(json.loads(json.dumps(doc))) == canonical_sha256(doc)
    assert doc["strategy_id"] == "ranking-pot-v1" and doc["family"] == "ranking-pot" and doc["family_seq"] == 1
    assert doc["s0"] == {
        "scored_at": "2026-10-01T17:07:54+00:00",
        "count": 3,
        "sha256": canonical_sha256([3, 7, 11]),
        "instrument_ids": [3, 7, 11],
    }
    assert doc["terms"] == {
        "n": 25,
        "k_controls": 9_999,
        "look_months": [12, 24],
        "declaration_alpha": "1/40",
        "per_look_alpha": "1/80",
        "harm_alpha": "1/40",
    }
    assert doc["prereg"] == prereg_terms()


def test_prereg_declaration_is_coherent_and_names_the_document() -> None:
    # Constructing one runs the sql/333 mirrored validation.
    declaration = prereg_declaration(doc_sha256="c" * 64, declared_by="supervisor")
    assert declaration.contract_version == CONTRACT_PREFIX + "c" * 64
    assert declaration.prereg_purpose == "falsification_only"


def test_no_pot_module_writes_evidence() -> None:
    """§7.1: no writes to the results store, promotion tables or another strategy's evidence."""
    banned = ("strategy_results_store", "strategy_promotions", "strategy_stage", "INSERT INTO ai_trial")
    for name in ("ranking_pot.py", "ranking_pot_sim.py", "ranking_pot_policy.py", "ranking_pot_freeze.py"):
        text = (_SERVICES / name).read_text()
        assert not [b for b in banned if b in text], name
