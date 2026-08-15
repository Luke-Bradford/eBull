"""Policy-only repair for the pre-outcome MT-1 declarations (#2697/#2437)."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from app.services.prereg_contract import changed_supersession_terms, declaration_refusals, supersession_refusals
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION
from scripts.freeze_2437_mt1_declarations import build_declarations
from scripts.supersede_2437_mt1_declarations import (
    ATTESTATION,
    DECLARED_BY,
    PREDECESSOR_POLICY_VERSION,
    build_successor,
)

if TYPE_CHECKING:
    from app.services.prereg_contract import PreregDeclaration


def _predecessors() -> tuple[PreregDeclaration, ...]:
    return tuple(
        replace(
            declaration,
            structural_refusal_policy_version=PREDECESSOR_POLICY_VERSION,
            declared_by="scripts/freeze_2437_mt1_declarations.py (#2437)",
        )
        for declaration in build_declarations()
    )


def test_both_successors_change_only_supersession_mutable_fields() -> None:
    for predecessor in _predecessors():
        successor = build_successor(predecessor)
        assert successor.structural_refusal_policy_version == STRUCTURAL_REFUSAL_POLICY_VERSION
        assert successor.declared_by == DECLARED_BY
        assert changed_supersession_terms(predecessor, successor) == ()
        assert declaration_refusals(successor) == ()
        assert supersession_refusals(predecessor, successor) == ()


def test_the_attestation_names_the_measured_and_unmeasurable_claims() -> None:
    assert "zero holdout evaluations" in ATTESTATION
    assert "zero recorded accesses" in ATTESTATION
    assert "No price, return, performance, sample-composition or outcome" in ATTESTATION
    assert "changes no trial term" in ATTESTATION
