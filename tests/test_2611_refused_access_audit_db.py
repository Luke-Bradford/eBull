"""#2611 — the refusal audit against a real relation.

Spec: ``docs/proposals/ta/2026-08-13-refused-outcome-access-audit.md``.
Storage: ``sql/340``. Writer: ``app/services/result_ledger.py``.

⚠ NONE OF THESE ARE MOCKABLE, and the first one least of all. The entire
question the ticket poses is what SURVIVES a rollback, and a mocked cursor
cannot have an opinion about that — it would assert that the code called what it
called, which is true on the branch where the row is written in the caller's
transaction and lost. The other two are about what a set of rows makes a
DIFFERENT function decide.

The pure half — that a failed audit never masks the refusal, and the AST guard
keeping the refusal exit a chokepoint — is in
``tests/test_2611_refused_access_audit.py``.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services import result_ledger
from app.services.prereg_contract import ForwardShadowFloor, PreregDeclaration, Supersession
from app.services.result_ledger import (
    HoldoutAccess,
    PreregDeclarationRefused,
    holdout_access_counts,
    read_access_refusals,
    record_holdout_access,
    require_outcome_access,
    require_outcome_access_with_declaration,
    supersede_preregistration,
)
from app.services.strategy_result import STRUCTURAL_REFUSAL_POLICY_VERSION

_STALE_POLICY = "structural-refusal-policy-2026-08-12-v1"
_STRATEGY_ID = "S-2611"
_STRATEGY_VERSION = "refusal-audit-v1"
_ACTOR = "tests/test_2611_refused_access_audit_db.py"
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


def _freeze_stranded(conn: psycopg.Connection[tuple]) -> int:
    """Freeze a declaration under the SUPERSEDED policy version.

    ⚠ Through the statement rather than ``freeze_preregistration``, which refuses
    an incoherent declaration — a stale policy version IS incoherent, and that is
    the state a policy bump leaves behind rather than a path any writer takes.
    Same construction as ``tests/test_2634_prereg_supersession_db.py``.
    """
    return _insert_declaration(conn, _STALE_POLICY)


def _freeze_coherent(conn: psycopg.Connection[tuple]) -> int:
    """Freeze a declaration the access door will ACCEPT.

    ⚠ Also through the statement, and for a DIFFERENT reason from
    ``_freeze_stranded`` — one worth stating so the next reader does not "fix" it
    by calling ``freeze_preregistration``. That function additionally enforces
    #2829's register join: a declaration may only be frozen for a trial that
    ``app/services/trial_register.py`` already counts in criterion 6's M. This
    module's ``S-2611`` is a fixture identity and deliberately has no register
    entry — adding one to satisfy a test would inflate the M that every deflated
    Sharpe in the repo is computed against, which is a far worse trade than
    inserting the row directly here.
    """
    return _insert_declaration(conn, STRUCTURAL_REFUSAL_POLICY_VERSION)


def _insert_declaration(conn: psycopg.Connection[tuple], policy_version: str) -> int:
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
            "policy": policy_version,
            "refusals": list(_INELIGIBLE),
            "actor": _ACTOR,
            "digest": _declaration(structural_refusal_policy_version=policy_version).sha256,
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
        "purpose": "exercise the refusal audit",
    }
    base.update(overrides)
    return HoldoutAccess(**base)  # type: ignore[arg-type]


def test_an_undeclared_trials_refusal_survives_the_callers_rollback(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ THE WHOLE TICKET. The refusal is an exception, so the caller's
    transaction is rolled back in essentially every real case — which is why a
    row written inside it would be lost in exactly the situation it exists for.

    The rollback here is not incidental to the test; it IS the test. On a branch
    where the audit is written in the caller's transaction, everything above the
    rollback passes and the read below returns nothing.
    """
    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        require_outcome_access(ebull_test_conn, _access())
    assert refused.value.refusals == ("preregistration_not_frozen",)

    recorded = read_access_refusals(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION)
    assert len(recorded) == 1
    assert recorded[0].refusals == ("preregistration_not_frozen",)
    assert recorded[0].declaration_id is None
    assert recorded[0].accessed_by == _ACTOR
    assert recorded[0].access_kind == "read"

    # ⚠ And no access row was written. A refused look is not a look.
    assert holdout_access_counts(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION).recorded_accesses == 0


def test_an_incoherent_declarations_refusal_records_the_declaration_it_resolved_to(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The other access door: a trial that HAS frozen a declaration, incoherently.

    ⚠ The recorded ``declaration_id`` is the row the refused look actually
    resolved to — which is what lets an auditor ask "refused against WHICH
    revision" on a trial whose declaration chain has since been superseded.
    """
    with ebull_test_conn.transaction():
        stranded_id = _freeze_stranded(ebull_test_conn)

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        record_holdout_access(ebull_test_conn, _access())
    assert refused.value.refusals == ("structural_refusal_policy_superseded",)

    recorded = read_access_refusals(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION)
    assert len(recorded) == 1
    assert recorded[0].refusals == ("structural_refusal_policy_superseded",)
    assert recorded[0].declaration_id == stranded_id


def test_a_refusal_is_neither_a_criterion_5_access_nor_supersession_exposure(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠⚠ THE REASON SQL/340 IS ITS OWN TABLE, AS A BEHAVIOURAL ASSERTION.

    Putting the row in ``strategy_holdout_accesses`` would have been the obvious
    move and would have broken two unrelated things at once: criterion 5's
    ``recorded_accesses`` would count a look that never happened, and
    ``supersede_preregistration`` — which reads that relation as exposure —
    would refuse ``supersession_trial_already_exposed`` forever, stranding the
    trial from #2634's repair over an attempt that returned nothing.

    So this test asserts the repair still WORKS after a refusal, which is the
    consequence an operator would actually feel.
    """
    with ebull_test_conn.transaction():
        _freeze_stranded(ebull_test_conn)

    with pytest.raises(PreregDeclarationRefused), ebull_test_conn.transaction():
        record_holdout_access(ebull_test_conn, _access())

    assert len(read_access_refusals(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION)) == 1
    assert holdout_access_counts(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION).recorded_accesses == 0

    with ebull_test_conn.transaction():
        successor_id = supersede_preregistration(
            ebull_test_conn,
            _declaration(),
            Supersession(
                reason="structural_refusal_policy_superseded",
                attestation="no outcome of this trial has been opened by any route; the refused attempt returned none",
            ),
        )
    assert successor_id > 0


def test_the_governance_read_is_bounded_and_newest_first(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Three attempts are three rows — a caller that retries really did retry."""
    for index in range(3):
        with pytest.raises(PreregDeclarationRefused), ebull_test_conn.transaction():
            require_outcome_access(ebull_test_conn, _access(purpose=f"attempt {index}"))

    everything = read_access_refusals(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION)
    assert [row.purpose for row in everything] == ["attempt 2", "attempt 1", "attempt 0"]

    assert len(read_access_refusals(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION, limit=1)) == 1
    with pytest.raises(ValueError):
        read_access_refusals(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION, limit=0)


def test_the_relation_refuses_a_refusal_that_names_no_reason(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ The constraint, not the writer. An audit row whose ``refusals`` is empty
    or carries a blank code has no content — the codes are the whole artefact.
    ``array_position`` is what catches the NULL element: ``'' <> ALL(ARRAY[NULL])``
    is NULL, and a NULL CHECK passes.
    """
    for refusals in ([], [None], [""], ["ok", ""]):
        with pytest.raises(psycopg.errors.CheckViolation) as violation, ebull_test_conn.transaction():
            ebull_test_conn.execute(
                """
                INSERT INTO strategy_holdout_access_refusals (
                    strategy_id, strategy_version, access_kind, accessed_by, purpose, refusals
                ) VALUES (%s, %s, 'read', %s, 'constraint probe', %s)
                """,
                (_STRATEGY_ID, _STRATEGY_VERSION, _ACTOR, refusals),
            )
        assert violation.value.diag.constraint_name == "strategy_holdout_access_refusals_names_a_reason"


def test_the_strict_door_returns_the_declaration_its_access_row_names(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2617 — one load, and the caller's copy is the one that was enforced.

    ⚠⚠ THE PROPERTY IS AGREEMENT, NOT PRESENCE. C-4's gate puts a
    ``declaration_id`` in its signed ``OutcomeGate`` while the ledger writes one
    into ``strategy_holdout_accesses``. Before #2617 those were two separate
    loads, so under a concurrent supersession the artifact could name a revision
    the access row did not — an attribution disagreement that nothing would
    notice, because each side is internally consistent. Asserting they are equal
    is what makes the single load load-bearing rather than merely tidy.
    """
    with ebull_test_conn.transaction():
        declaration_id = _freeze_coherent(ebull_test_conn)

    with ebull_test_conn.transaction():
        access_id, frozen = require_outcome_access_with_declaration(ebull_test_conn, _access())

    assert frozen.declaration_id == declaration_id
    row = ebull_test_conn.execute(
        "SELECT declaration_id FROM strategy_holdout_accesses WHERE access_id = %s",
        (access_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == declaration_id

    # ⚠ And it is a real look: the refusal path wrote nothing.
    assert read_access_refusals(ebull_test_conn, _STRATEGY_ID, _STRATEGY_VERSION) == ()

    # ⚠ Counted straight off the relation, NOT through ``holdout_access_counts``.
    # That helper counts ``access_kind = 'evaluate'`` only, so on this `read` it
    # returns 0 whether or not a row exists — an assertion against it here would
    # have been structurally unable to fail.
    written = ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_holdout_accesses WHERE strategy_id = %s AND strategy_version = %s",
        (_STRATEGY_ID, _STRATEGY_VERSION),
    ).fetchone()
    assert written is not None
    assert written[0] == 1


def test_the_strict_door_loads_the_declaration_once(
    ebull_test_conn: psycopg.Connection[tuple],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2617's actual claim, made falsifiable.

    ⚠ THE TEST ABOVE CANNOT SEE THIS. It asserts that the gate's copy and the
    access row name the same revision — which they also do under three loads, in
    one transaction, with nothing racing. Agreement is the property that matters
    in production; the load count is the property the ticket claims, and the two
    need different tests. Counting is the only way to pin the second.

    ⚠ Patched on the MODULE, because ``_declaration_refusal_codes`` resolves
    ``load_preregistration`` as a global — a patch on the imported name in this
    file would count nothing and pass.
    """
    with ebull_test_conn.transaction():
        _freeze_coherent(ebull_test_conn)

    real = result_ledger.load_preregistration
    calls = 0

    def counting_load(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return real(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(result_ledger, "load_preregistration", counting_load)
    with ebull_test_conn.transaction():
        result_ledger.require_outcome_access_with_declaration(ebull_test_conn, _access())

    assert calls == 1, f"the strict door loaded the declaration {calls} times; #2617 collapsed it to one"
