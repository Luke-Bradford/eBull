"""#2634 — supersession's rules, as pure logic.

Spec: ``docs/proposals/ta/2026-08-13-preregistration-supersession.md``.
Rules: ``app/services/prereg_contract.py``. The relational half — the chain
constraints, the exposure refusals, the attribution column — is
``tests/test_2634_prereg_supersession_db.py``, because those are properties of
rows and a mocked cursor cannot stand in for one.

⚠ The property that matters most here is the LAST test: the partition of
declaration fields into "may change" and "must not" has to stay exhaustive, or a
field added later becomes silently mutable through supersession.
"""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path

import pytest

from app.services.prereg_contract import (
    DECLARATION_REFUSALS,
    SUPERSESSION_MUTABLE_FIELDS,
    SUPERSESSION_REFUSALS,
    ForwardShadowFloor,
    PreregDeclaration,
    Supersession,
    changed_supersession_terms,
    supersession_refusals,
)
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION

_STALE_POLICY = "structural-refusal-policy-2026-08-12-v1"
_INELIGIBLE = ("universe_basis_not_survivorship_free", "carry_unmodelled", "fx_unmodelled")


def _declaration(**overrides: object) -> PreregDeclaration:
    """A coherent falsification declaration over today's actual stamps."""
    base: dict[str, object] = {
        "strategy_id": "S-1",
        "strategy_version": "strategy-registry-v1+abc123",
        "contract_version": "test-contract-v1",
        "prereg_purpose": "falsification_only",
        "structural_refusal_policy_version": STRUCTURAL_REFUSAL_POLICY_VERSION,
        "declared_universe_basis": "survivor_only",
        "declared_carry_unmodelled": True,
        "declared_fx_unmodelled": True,
        "expected_structural_refusals": _INELIGIBLE,
        "forward_shadow": ForwardShadowFloor(
            min_independent_decision_dates=40,
            min_calendar_weeks=12,
            derivation="planning power calculation, candidate contract §statistics",
        ),
        "declared_by": "tests/test_2634_prereg_supersession.py",
    }
    base.update(overrides)
    return PreregDeclaration(**base)  # type: ignore[arg-type]


def _stranded() -> PreregDeclaration:
    """The predecessor: identical terms, frozen under the superseded policy."""
    return _declaration(structural_refusal_policy_version=_STALE_POLICY)


def test_a_stranded_declaration_is_repaired_by_an_identical_one_under_the_current_policy() -> None:
    """The whole point of the ticket, in one assertion.

    The successor differs in exactly the field the bump stranded, and in the
    declarer's name — which is permitted precisely because the predecessor row
    survives and still records who declared first.
    """
    assert supersession_refusals(_stranded(), _declaration(declared_by="somebody else")) == ()


def test_a_supersession_that_repairs_nothing_is_refused() -> None:
    """⚠ Not a harmless no-op. Every extra revision is another row an auditor has
    to read to establish what the trial declared, so a chain link that changes
    nothing is a cost with no benefit."""
    assert "supersession_not_required" in supersession_refusals(_declaration(), _declaration())


def test_superseding_into_another_stale_policy_version_is_refused() -> None:
    assert "supersession_policy_not_current" in supersession_refusals(
        _stranded(), _declaration(structural_refusal_policy_version="structural-refusal-policy-2026-01-01-v0")
    )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("contract_version", "a-different-contract-v2"),
        ("prereg_purpose", "capital_candidate"),
        ("declared_universe_basis", "survivorship_free"),
        ("declared_carry_unmodelled", False),
        ("declared_fx_unmodelled", False),
    ],
)
def test_a_supersession_may_not_move_any_declared_term(field_name: str, value: object) -> None:
    """⚠⚠ THIS IS THE SAFETY ARGUMENT, not a tidiness rule.

    A re-declaration path is an obvious adaptivity vector: the author has by then
    seen sample counts, missingness and corpus composition. Terms-identity is
    what leaves nothing to adapt — and note ``declared_universe_basis`` and both
    cost stamps are in this list, so the favourable direction (declaring
    survivorship-free or costs modelled after the fact) is exactly what it stops.
    """
    refusals = supersession_refusals(_stranded(), _declaration(**{field_name: value}))
    assert "supersession_terms_changed" in refusals
    assert changed_supersession_terms(_stranded(), _declaration(**{field_name: value})) == (field_name,)


def test_a_supersession_may_not_move_either_forward_shadow_floor() -> None:
    """The floors travel inside a nested dataclass, so a field-by-field walk of
    ``PreregDeclaration`` sees ONE field. Frozen-dataclass equality is what makes
    that comparison reach the numbers rather than the object identity."""
    lower = _declaration(
        forward_shadow=ForwardShadowFloor(
            min_independent_decision_dates=1,
            min_calendar_weeks=1,
            derivation="planning power calculation, candidate contract §statistics",
        )
    )
    assert "supersession_terms_changed" in supersession_refusals(_stranded(), lower)
    assert changed_supersession_terms(_stranded(), lower) == ("forward_shadow",)


def test_an_incoherent_successor_is_refused_naming_its_own_defect() -> None:
    """⚠ Surfaced HERE rather than at the next look. A malformed successor
    accepted into a chain would refuse every subsequent access with a code about
    the declaration rather than about the supersession that introduced it."""
    refusals = supersession_refusals(_stranded(), _declaration(expected_structural_refusals=("carry_unmodelled",)))
    assert "expected_structural_refusals_mismatch" in refusals


def test_all_refusals_are_returned_not_only_the_first() -> None:
    """Same contract ``declaration_refusals`` has: an operator acting on one code
    at a time re-runs and finds the next one."""
    refusals = supersession_refusals(
        _declaration(),
        _declaration(structural_refusal_policy_version=_STALE_POLICY, prereg_purpose="capital_candidate"),
    )
    assert "supersession_not_required" in refusals
    assert "supersession_policy_not_current" in refusals
    assert "supersession_terms_changed" in refusals


@pytest.mark.parametrize("attestation", ["", "   ", "\n\t "])
def test_a_blank_attestation_is_refused(attestation: str) -> None:
    """⚠ ``strip``, not truthiness. Three spaces is non-empty and says nothing,
    and the attestation is the only part of the no-exposure claim that a count
    cannot supply."""
    with pytest.raises(ValueError, match="non-empty statement of no exposure"):
        Supersession(reason="structural_refusal_policy_superseded", attestation=attestation)


def test_an_unknown_supersession_reason_is_refused() -> None:
    """The vocabulary has one member on purpose: any other reason to re-declare
    is the adaptivity supersession exists to forbid."""
    with pytest.raises(ValueError, match="unknown supersession reason"):
        Supersession(reason="operator_changed_their_mind", attestation="no outcome has been seen")  # type: ignore[arg-type]


def test_the_mutable_field_partition_covers_every_declaration_field() -> None:
    """⚠⚠ THE ANTI-ROT TEST, and the reason it exists is a defect one level over.

    ``SUPERSESSION_MUTABLE_FIELDS`` names what a supersession may change;
    everything else is compared. A field added to ``PreregDeclaration`` and
    forgotten here lands in the compared half by default — which is the SAFE
    direction, so this test is not guarding a hole so much as making the choice
    explicit at the moment somebody adds a field. #2631's ``digest_payload`` had
    the same shape and the same test.
    """
    declared = {field.name for field in fields(PreregDeclaration)}
    assert SUPERSESSION_MUTABLE_FIELDS <= declared, (
        f"SUPERSESSION_MUTABLE_FIELDS names fields PreregDeclaration does not have: "
        f"{sorted(SUPERSESSION_MUTABLE_FIELDS - declared)}"
    )
    compared = declared - SUPERSESSION_MUTABLE_FIELDS
    assert compared == {
        "strategy_id",
        "strategy_version",
        "contract_version",
        "prereg_purpose",
        "declared_universe_basis",
        "declared_carry_unmodelled",
        "declared_fx_unmodelled",
        "forward_shadow",
    }, (
        "the set of terms a supersession may NOT change has moved. If a field was added to "
        "PreregDeclaration, decide deliberately which half it belongs in and update this literal — "
        "an unlisted field is compared, which is fail-closed but should still be a choice."
    )


def test_every_refusal_code_the_rules_can_emit_is_in_the_declared_vocabulary() -> None:
    """⚠ The codes are STRING LITERALS inside the functions, so a typo is
    invisible: a misspelled code still refuses, still reads plausibly in an
    exception, and matches nothing an operator or a caller filters on.

    This makes ``SUPERSESSION_REFUSALS`` load-bearing rather than decorative
    (review nitpick, PR #2635) — and it covers ``DECLARATION_REFUSALS`` too,
    which has been exported without a consumer since #2599.
    """
    emitted: set[str] = set()
    for predecessor, successor in [
        (_stranded(), _declaration()),
        (_declaration(), _declaration()),
        (_stranded(), _declaration(structural_refusal_policy_version=_STALE_POLICY)),
        (_stranded(), _declaration(prereg_purpose="capital_candidate")),
        (_stranded(), _declaration(expected_structural_refusals=())),
    ]:
        emitted.update(str(code) for code in supersession_refusals(predecessor, successor))

    vocabulary = SUPERSESSION_REFUSALS | DECLARATION_REFUSALS
    assert emitted <= vocabulary, f"codes outside the declared vocabulary: {sorted(emitted - vocabulary)}"
    # Every PURE supersession code is reachable from the matrix above; the rest
    # of the vocabulary is the DB-side half, asserted below.
    assert {"supersession_not_required", "supersession_policy_not_current", "supersession_terms_changed"} <= emitted


def test_the_database_side_refusal_codes_match_what_the_ledger_actually_appends() -> None:
    """The vocabulary is declared in ``prereg_contract`` and half of it is emitted
    by ``result_ledger``. Nothing otherwise ties the two together, so a code
    renamed on one side would leave the other naming a state that never fires.

    ⚠ A quoted-literal match, not a bare substring: these strings appear nowhere
    else in that module, and the #2631 lesson is that a bare name is satisfied by
    an import line.
    """
    source = (Path(__file__).resolve().parents[1] / "app" / "services" / "result_ledger.py").read_text()
    db_side = {
        "supersession_nothing_frozen",
        "supersession_trial_already_exposed",
        "supersession_trial_has_holdout_results",
        "supersession_predecessor_already_superseded",
    }
    assert db_side <= SUPERSESSION_REFUSALS
    missing = {code for code in db_side if f'"{code}"' not in source}
    assert not missing, f"declared in the vocabulary but never appended by result_ledger: {sorted(missing)}"
