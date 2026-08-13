"""#2634 — supersession against a real relation.

Spec: ``docs/proposals/ta/2026-08-13-preregistration-supersession.md``.
Storage: ``sql/337``. Writer: ``app/services/result_ledger.py``.

⚠ NONE OF THESE ARE MOCKABLE. Every property here is about what a set of rows
can be made to contain — whether a second root can exist, whether a cycle can be
inserted, whether an access row commits before a re-declaration counts it. A
mocked cursor asserts that the code called what it called; the point is what the
database refuses.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration, Supersession
from app.services.result_ledger import (
    HoldoutAccess,
    PreregDeclarationRefused,
    freeze_preregistration,
    load_preregistration,
    record_holdout_access,
    supersede_preregistration,
    verify_outcome_access_provenance,
)
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION

_STALE_POLICY = "structural-refusal-policy-2026-08-12-v1"
_STRATEGY_ID = "S-2634"
_STRATEGY_VERSION = "supersession-test-v1"
_ACTOR = "tests/test_2634_prereg_supersession_db.py"
_ATTESTATION = "no outcome of this trial has been opened by any route; re-declared only to repair the policy version"
_INELIGIBLE = ("universe_basis_not_survivorship_free", "carry_unmodelled", "fx_unmodelled")


def _declaration(**overrides: object) -> PreregDeclaration:
    base: dict[str, object] = {
        "strategy_id": _STRATEGY_ID,
        "strategy_version": _STRATEGY_VERSION,
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
        "declared_by": _ACTOR,
    }
    base.update(overrides)
    return PreregDeclaration(**base)  # type: ignore[arg-type]


def _supersession() -> Supersession:
    return Supersession(reason="structural_refusal_policy_superseded", attestation=_ATTESTATION)


def _freeze_stranded(conn: psycopg.Connection[tuple]) -> int:
    """Freeze a declaration under the SUPERSEDED policy version.

    ⚠ ``freeze_preregistration`` refuses an incoherent declaration, and a stale
    policy version IS incoherent — that is the defect. So the row is inserted
    through the statement rather than the helper: this is reconstructing the
    state a bump leaves behind, not a path any writer takes.
    """
    row = conn.execute(
        """
        INSERT INTO strategy_preregistration_declarations (
            strategy_id, strategy_version, contract_version, prereg_purpose,
            structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
            declared_fx_unmodelled, expected_structural_refusals, min_forward_decision_dates,
            min_forward_calendar_weeks, forward_shadow_derivation, declared_by, declaration_sha256
        ) VALUES (
            %(strategy_id)s, %(strategy_version)s, 'test-contract-v1', 'falsification_only',
            %(policy)s, 'survivor_only', true,
            true, %(refusals)s, 40,
            12, 'planning power calculation, candidate contract §statistics', %(actor)s, %(digest)s
        )
        RETURNING declaration_id
        """,
        {
            "strategy_id": _STRATEGY_ID,
            "strategy_version": _STRATEGY_VERSION,
            "policy": _STALE_POLICY,
            "refusals": list(_INELIGIBLE),
            "actor": _ACTOR,
            "digest": _declaration(structural_refusal_policy_version=_STALE_POLICY).sha256,
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


def _access(**overrides: object) -> HoldoutAccess:
    base: dict[str, object] = {
        "strategy_id": _STRATEGY_ID,
        "strategy_version": _STRATEGY_VERSION,
        "result_version": None,
        "access_kind": "read",
        "accessed_by": _ACTOR,
        "purpose": "exercise supersession",
    }
    base.update(overrides)
    return HoldoutAccess(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# The repair itself
# ---------------------------------------------------------------------------


def test_a_stranded_trial_can_look_at_its_outcomes_again_after_a_supersession(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2634 in one test: the wedge, then the repair.

    ⚠ The FIRST assertion is the defect. Without it this test would pass on a
    branch where supersession does nothing, because the second half would be the
    only thing exercised.
    """
    with ebull_test_conn.transaction():
        stranded_id = _freeze_stranded(ebull_test_conn)

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        record_holdout_access(ebull_test_conn, _access())
    assert refused.value.refusals == ("structural_refusal_policy_superseded",)

    with ebull_test_conn.transaction():
        successor_id = supersede_preregistration(ebull_test_conn, _declaration(), _supersession())

    with ebull_test_conn.transaction():
        access_id = record_holdout_access(ebull_test_conn, _access())

    assert access_id > 0
    frozen = load_preregistration(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION)
    assert frozen is not None
    assert frozen.declaration_id == successor_id
    assert frozen.chain_declaration_ids == (stranded_id, successor_id)
    assert frozen.supersedes_declaration_id == stranded_id
    assert frozen.supersession_attestation == _ATTESTATION


def test_the_access_row_names_the_revision_that_authorised_it(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2634 scope item 2 — attribution across the chain.

    ⚠ The predecessor id would be the wrong answer and is the one a second load
    could return under a concurrent supersession, which is why
    ``record_holdout_access`` writes the row it CHECKED rather than re-loading.
    """
    with ebull_test_conn.transaction():
        stranded_id = _freeze_stranded(ebull_test_conn)
    with ebull_test_conn.transaction():
        successor_id = supersede_preregistration(ebull_test_conn, _declaration(), _supersession())
    with ebull_test_conn.transaction():
        access_id = record_holdout_access(ebull_test_conn, _access())

    row = ebull_test_conn.execute(
        "SELECT declaration_id FROM strategy_holdout_accesses WHERE access_id = %s", (access_id,)
    ).fetchone()
    assert row is not None
    assert row[0] == successor_id
    assert row[0] != stranded_id


def test_a_trial_with_no_declaration_still_records_a_null_attribution(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ NO RETROACTIVE INVALIDATION, still. #2599's rule was that a trial which
    froze nothing behaves as it did before, and the 304 existing access rows
    depend on it. A NOT NULL column here would have broken every one of them."""
    with ebull_test_conn.transaction():
        access_id = record_holdout_access(ebull_test_conn, _access(strategy_version="never-declared-v1"))
    row = ebull_test_conn.execute(
        "SELECT declaration_id FROM strategy_holdout_accesses WHERE access_id = %s", (access_id,)
    ).fetchone()
    assert row is not None
    assert row[0] is None


# ---------------------------------------------------------------------------
# The exposure disqualifiers
# ---------------------------------------------------------------------------


def test_a_trial_that_has_been_looked_at_cannot_be_re_declared(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The rule that keeps supersession from being a re-roll of a completed trial.

    ⚠ The access here is recorded against a COHERENT declaration, then that
    declaration is stranded — which is the real sequence: look first, bump later.
    """
    with ebull_test_conn.transaction():
        freeze_preregistration(ebull_test_conn, _declaration())
    with ebull_test_conn.transaction():
        record_holdout_access(ebull_test_conn, _access())
    with ebull_test_conn.transaction():
        ebull_test_conn.execute(
            "ALTER TABLE strategy_preregistration_declarations DISABLE TRIGGER "
            "trg_strategy_preregistration_declaration_immutable"
        )
        ebull_test_conn.execute(
            "UPDATE strategy_preregistration_declarations SET structural_refusal_policy_version = %s "
            "WHERE strategy_id = %s",
            (_STALE_POLICY, _STRATEGY_ID),
        )
        ebull_test_conn.execute(
            "ALTER TABLE strategy_preregistration_declarations ENABLE TRIGGER "
            "trg_strategy_preregistration_declaration_immutable"
        )

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        supersede_preregistration(ebull_test_conn, _declaration(), _supersession())
    assert "supersession_trial_already_exposed" in refused.value.refusals


def test_supersession_is_refused_when_nothing_is_frozen(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        supersede_preregistration(ebull_test_conn, _declaration(), _supersession())
    assert refused.value.refusals == ("supersession_nothing_frozen",)


def test_a_changed_term_is_refused_and_the_message_names_the_field(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ The code vocabulary is closed, so the field names travel in the message.
    Without them "the terms changed" gives an operator nothing to act on."""
    with ebull_test_conn.transaction():
        _freeze_stranded(ebull_test_conn)
    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        supersede_preregistration(
            ebull_test_conn, _declaration(declared_universe_basis="survivorship_free"), _supersession()
        )
    assert "supersession_terms_changed" in refused.value.refusals
    assert "changed_terms=declared_universe_basis" in refused.value.refusals


# ---------------------------------------------------------------------------
# The chain constraints (sql/337)
# ---------------------------------------------------------------------------


def test_a_second_root_declaration_is_still_refused(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ sql/333's ``UNIQUE (strategy_id, strategy_version)`` was DROPPED here,
    so the property it carried has to be re-proved: one trial, one first
    declaration. It now comes from the partial unique index on roots."""
    with ebull_test_conn.transaction():
        freeze_preregistration(ebull_test_conn, _declaration())
    with pytest.raises(psycopg.errors.UniqueViolation) as violated, ebull_test_conn.transaction():
        freeze_preregistration(ebull_test_conn, _declaration(declared_by="somebody else"))
    assert violated.value.diag.constraint_name == "strategy_preregistration_declaration_one_root"


def test_two_rows_cannot_supersede_the_same_predecessor(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """No branching — otherwise a trial has two current declarations and a caller
    picks whichever one the outcome favours, which is the fabrication sql/333
    exists to prevent in a third costume."""
    with ebull_test_conn.transaction():
        stranded_id = _freeze_stranded(ebull_test_conn)
    with ebull_test_conn.transaction():
        supersede_preregistration(ebull_test_conn, _declaration(), _supersession())

    with pytest.raises(psycopg.errors.UniqueViolation) as violated, ebull_test_conn.transaction():
        ebull_test_conn.execute(
            """
            INSERT INTO strategy_preregistration_declarations (
                strategy_id, strategy_version, contract_version, prereg_purpose,
                structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
                declared_fx_unmodelled, expected_structural_refusals, min_forward_decision_dates,
                min_forward_calendar_weeks, forward_shadow_derivation, declared_by, declaration_sha256,
                supersedes_declaration_id, supersession_reason, supersession_attestation
            ) VALUES (
                %(strategy_id)s, %(strategy_version)s, 'test-contract-v1', 'falsification_only',
                %(policy)s, 'survivor_only', true,
                true, %(refusals)s, 40,
                12, 'planning power calculation, candidate contract §statistics', 'a-rival', %(digest)s,
                %(predecessor)s, 'structural_refusal_policy_superseded', %(attestation)s
            )
            """,
            {
                "strategy_id": _STRATEGY_ID,
                "strategy_version": _STRATEGY_VERSION,
                "policy": STRUCTURAL_REFUSAL_POLICY_VERSION,
                "refusals": list(_INELIGIBLE),
                "digest": _declaration(declared_by="a-rival").sha256,
                "predecessor": stranded_id,
                "attestation": _ATTESTATION,
            },
        )
    assert violated.value.diag.constraint_name == "strategy_preregistration_declaration_supersedes_once"


def test_a_declaration_cannot_supersede_a_later_one(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ THE CYCLE GUARD, and it is the constraint the first draft of the spec
    did not have (Codex checkpoint 1). Uniqueness forbids branching and a second
    root; neither forbids a closed loop, which would leave the trial with ZERO
    current declarations — a different permanent wedge from the one #2634 fixes.
    Every real edge points at a smaller ``declaration_id`` because the
    predecessor is inserted first, so barring the other direction bars cycles.

    ⚠ PROBED BY INSERT WITH AN EXPLICIT ID, not by UPDATE. The first version of
    this test tried to UPDATE a row into a self-reference and passed for the
    WRONG REASON — sql/333's immutability trigger raised first, so the CHECK
    under test never ran. The shortest cycle is a row that supersedes itself,
    and a non-deferred FK is satisfied by a self-reference, so this reaches the
    CHECK and nothing else.
    """
    with ebull_test_conn.transaction():
        _freeze_stranded(ebull_test_conn)
    next_id = ebull_test_conn.execute(
        "SELECT max(declaration_id) + 1 FROM strategy_preregistration_declarations"
    ).fetchone()
    assert next_id is not None

    with pytest.raises(psycopg.errors.CheckViolation) as violated, ebull_test_conn.transaction():
        ebull_test_conn.execute(
            """
            INSERT INTO strategy_preregistration_declarations (
                declaration_id, strategy_id, strategy_version, contract_version, prereg_purpose,
                structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
                declared_fx_unmodelled, expected_structural_refusals, min_forward_decision_dates,
                min_forward_calendar_weeks, forward_shadow_derivation, declared_by, declaration_sha256,
                supersedes_declaration_id, supersession_reason, supersession_attestation
            ) VALUES (
                %(id)s, %(strategy_id)s, %(strategy_version)s, 'test-contract-v1', 'falsification_only',
                %(policy)s, 'survivor_only', true,
                true, %(refusals)s, 40,
                12, 'planning power calculation, candidate contract §statistics', %(actor)s, %(digest)s,
                %(id)s, 'structural_refusal_policy_superseded', %(attestation)s
            )
            """,
            {
                "id": int(next_id[0]),
                "strategy_id": _STRATEGY_ID,
                "strategy_version": _STRATEGY_VERSION,
                "policy": STRUCTURAL_REFUSAL_POLICY_VERSION,
                "refusals": list(_INELIGIBLE),
                "actor": _ACTOR,
                "digest": _declaration().sha256,
                "attestation": _ATTESTATION,
            },
        )
    assert violated.value.diag.constraint_name == "strategy_preregistration_declaration_supersedes_earlier"


def test_a_declaration_cannot_supersede_another_trials(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The composite FK. Without the trial columns in the reference, a
    supersession could move a DIFFERENT trial's terms — under the identity
    preregistration exists to hold fixed."""
    with ebull_test_conn.transaction():
        other_id = freeze_preregistration(
            ebull_test_conn, _declaration(strategy_id="S-2634-other", strategy_version="other-v1")
        )
        _freeze_stranded(ebull_test_conn)

    with pytest.raises(psycopg.errors.ForeignKeyViolation) as violated, ebull_test_conn.transaction():
        ebull_test_conn.execute(
            """
            INSERT INTO strategy_preregistration_declarations (
                strategy_id, strategy_version, contract_version, prereg_purpose,
                structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
                declared_fx_unmodelled, expected_structural_refusals, min_forward_decision_dates,
                min_forward_calendar_weeks, forward_shadow_derivation, declared_by, declaration_sha256,
                supersedes_declaration_id, supersession_reason, supersession_attestation
            ) VALUES (
                %(strategy_id)s, %(strategy_version)s, 'test-contract-v1', 'falsification_only',
                %(policy)s, 'survivor_only', true,
                true, %(refusals)s, 40,
                12, 'planning power calculation, candidate contract §statistics', %(actor)s, %(digest)s,
                %(predecessor)s, 'structural_refusal_policy_superseded', %(attestation)s
            )
            """,
            {
                "strategy_id": _STRATEGY_ID,
                "strategy_version": _STRATEGY_VERSION,
                "policy": STRUCTURAL_REFUSAL_POLICY_VERSION,
                "refusals": list(_INELIGIBLE),
                "actor": _ACTOR,
                "digest": _declaration().sha256,
                "predecessor": other_id,
                "attestation": _ATTESTATION,
            },
        )
    assert violated.value.diag.constraint_name == "strategy_preregistration_declaration_supersedes_same_trial"


def test_a_supersession_cannot_be_written_without_its_attestation(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The three supersession columns move together. A row naming a predecessor
    with no attestation is a writer bypassing the only part of the no-exposure
    claim that a count cannot supply."""
    with ebull_test_conn.transaction():
        stranded_id = _freeze_stranded(ebull_test_conn)
    with pytest.raises(psycopg.errors.CheckViolation) as violated, ebull_test_conn.transaction():
        ebull_test_conn.execute(
            """
            INSERT INTO strategy_preregistration_declarations (
                strategy_id, strategy_version, contract_version, prereg_purpose,
                structural_refusal_policy_version, declared_universe_basis, declared_carry_unmodelled,
                declared_fx_unmodelled, expected_structural_refusals, min_forward_decision_dates,
                min_forward_calendar_weeks, forward_shadow_derivation, declared_by, declaration_sha256,
                supersedes_declaration_id
            ) VALUES (
                %(strategy_id)s, %(strategy_version)s, 'test-contract-v1', 'falsification_only',
                %(policy)s, 'survivor_only', true,
                true, %(refusals)s, 40,
                12, 'planning power calculation, candidate contract §statistics', %(actor)s, %(digest)s,
                %(predecessor)s
            )
            """,
            {
                "strategy_id": _STRATEGY_ID,
                "strategy_version": _STRATEGY_VERSION,
                "policy": STRUCTURAL_REFUSAL_POLICY_VERSION,
                "refusals": list(_INELIGIBLE),
                "actor": _ACTOR,
                "digest": _declaration().sha256,
                "predecessor": stranded_id,
            },
        )
    assert violated.value.diag.constraint_name == "strategy_preregistration_declaration_supersession_complete"


def test_provenance_accepts_the_revision_the_access_row_names(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2634 — a look authorised by an earlier revision stays verifiable.

    ⚠⚠ THIS IS THE DOC-AND-CODE DISAGREEMENT CODEX CHECKPOINT 2 CAUGHT. The
    docstring on ``verify_outcome_access_provenance`` promised that provenance
    means "was current when the access happened", while the identity guard still
    demanded the CURRENT id — so every signed artifact naming a predecessor
    would have been refused ``declaration_identity_mismatch``, which is the wedge
    #2634 exists to remove, re-created inside the fix for it.

    ⚠ The state is built by direct INSERT because the paved path cannot reach
    it: supersession refuses a trial with any access row, so an access against a
    predecessor only arises from a writer that bypassed the ledger — §5 limit 6
    of the spec. That is exactly the case the guard has to answer correctly.
    """
    with ebull_test_conn.transaction():
        stranded_id = _freeze_stranded(ebull_test_conn)
    with ebull_test_conn.transaction():
        successor_id = supersede_preregistration(ebull_test_conn, _declaration(), _supersession())

    with ebull_test_conn.transaction():
        row = ebull_test_conn.execute(
            """
            INSERT INTO strategy_holdout_accesses (
                strategy_id, strategy_version, result_version, access_kind, accessed_by, purpose, declaration_id
            ) VALUES (%s, %s, NULL, 'read', %s, 'a look the ledger did not mediate', %s)
            RETURNING access_id
            """,
            (_STRATEGY_ID, _STRATEGY_VERSION, _ACTOR, stranded_id),
        ).fetchone()
    assert row is not None

    frozen = verify_outcome_access_provenance(
        ebull_test_conn,
        strategy_id=_STRATEGY_ID,
        strategy_version=_STRATEGY_VERSION,
        declaration_id=stranded_id,
        access_id=int(row[0]),
    )
    assert frozen.declaration_id == successor_id
    assert stranded_id in frozen.chain_declaration_ids
