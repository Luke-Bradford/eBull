"""#2614 — C-4's declaration gate against a real relation.

Spec: ``docs/proposals/ta/2026-08-12-c4-declaration-gate-binding.md``.

⚠ THESE PROPERTIES ARE NOT MOCKABLE. Every one of them is about two rows and the
server-side timestamps on them — whether an access committed, whether a
declaration existed before it, whether a digest still matches its bytes. A mocked
cursor asserts the code called what it called; the point here is what the
database contains.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.result_ledger import (
    HoldoutAccess,
    PreregDeclarationRefused,
    freeze_preregistration,
    record_holdout_access,
    require_outcome_access,
    verify_outcome_access_provenance,
)
from scripts.evaluate_2582_schedule13d_outcomes import STRATEGY_ID, STRATEGY_VERSION
from scripts.freeze_2582_schedule13d_declaration import build_declaration

# #2829 — freezes synthetic or pre-mapped identities while testing a different
# gate; see `assume_trial_registered` in tests/conftest.py.
pytestmark = pytest.mark.usefixtures("assume_trial_registered")

_ACTOR = "tests/test_c4_declaration_gate_db.py"
_PURPOSE = "exercise C-4's declaration gate"


def _access(**overrides: object) -> HoldoutAccess:
    base: dict[str, object] = {
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "result_version": None,
        "access_kind": "read",
        "accessed_by": _ACTOR,
        "purpose": _PURPOSE,
    }
    base.update(overrides)
    return HoldoutAccess(**base)  # type: ignore[arg-type]


def test_an_outcome_look_is_refused_while_no_declaration_is_frozen(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The refusal #2614 exists to add.

    ⚠ Before this ticket the only thing holding C-4 shut was its absence from the
    trial register — and charging its arms to the register (scope item 3) removes
    exactly that. This is the door that replaces it.
    """

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        require_outcome_access(ebull_test_conn, _access())
    assert refused.value.refusals == ("preregistration_not_frozen",)


def test_a_frozen_declaration_authorises_exactly_one_read_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ ``read`` with a NULL ``result_version``, not ``evaluate``.

    ``sql/264``'s ``strategy_holdout_accesses_evaluate_names_a_result`` requires
    an ``evaluate`` to name the result row it authorises, and C-4 never writes
    one — so ``evaluate`` would stand for a row that never arrives.
    """

    # ⚠⚠ TWO TRANSACTIONS, AND NOT AS A STYLE CHOICE. Postgres `now()` is
    # TRANSACTION-START time, so freezing and accessing in one transaction gives
    # `frozen_at == accessed_at` to the microsecond and the strict `<` refuses
    # with `declaration_not_frozen_before_access`. That is the check working:
    # a declaration frozen in the same transaction as the look did not precede
    # it. The real flow cannot hit it —
    # `scripts/freeze_2582_schedule13d_declaration.py` is a separate process run
    # earlier — and this test now mirrors that flow instead of contradicting it.
    with ebull_test_conn.transaction():
        declaration_id = freeze_preregistration(ebull_test_conn, build_declaration())

    with ebull_test_conn.transaction():
        access_id = require_outcome_access(ebull_test_conn, _access())

    with ebull_test_conn.transaction():
        frozen = verify_outcome_access_provenance(
            ebull_test_conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=declaration_id,
            access_id=access_id,
        )
        assert frozen.declaration_id == declaration_id
        assert frozen.digest_intact
        row = ebull_test_conn.execute(
            "SELECT count(*), count(result_version) FROM strategy_holdout_accesses "
            "WHERE strategy_id = %(sid)s AND access_kind = 'read'",
            {"sid": STRATEGY_ID},
        ).fetchone()
        assert row == (1, 0)


def test_provenance_refuses_an_access_id_that_was_never_recorded(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ THE BYPASS THIS CHECK EXISTS FOR.

    ``OutcomeGate`` is a plain frozen dataclass, so a caller holding a real
    ``declaration_id`` can construct one and open every price window without
    ``require_outcome_access`` ever running. Validating only the declaration
    would let that through, leaving no audit row at all. Because a rolled-back
    INSERT leaves no visible row, this same lookup is what proves the access
    committed.
    """

    with ebull_test_conn.transaction():
        declaration_id = freeze_preregistration(ebull_test_conn, build_declaration())

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        verify_outcome_access_provenance(
            ebull_test_conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=declaration_id,
            access_id=2_147_483_000,
        )
    assert "outcome_access_not_recorded" in refused.value.refusals


def test_provenance_refuses_an_access_belonging_to_another_trial(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A real committed access row, for a different strategy, must not authorise C-4."""

    with ebull_test_conn.transaction():
        declaration_id = freeze_preregistration(ebull_test_conn, build_declaration())
        other_access_id = record_holdout_access(
            ebull_test_conn, _access(strategy_id="S-OTHER", strategy_version="strategy-registry-v1+other0")
        )

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        verify_outcome_access_provenance(
            ebull_test_conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=declaration_id,
            access_id=other_access_id,
        )
    assert "outcome_access_not_recorded" in refused.value.refusals


def test_provenance_refuses_an_evaluate_kind_access(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    with ebull_test_conn.transaction():
        declaration_id = freeze_preregistration(ebull_test_conn, build_declaration())
        evaluate_id = record_holdout_access(
            ebull_test_conn, _access(access_kind="evaluate", result_version="strategy-result-v1+c40000")
        )

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        verify_outcome_access_provenance(
            ebull_test_conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=declaration_id,
            access_id=evaluate_id,
        )
    assert "outcome_access_kind_mismatch" in refused.value.refusals


def test_provenance_refuses_a_stale_access_once_a_later_look_is_logged(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ Codex checkpoint 2: a STALE ``access_id`` must stop authorising looks.

    A caller could otherwise copy an old legitimate ``access_id`` into a
    hand-built ``OutcomeGate`` and keep evaluating past later activity. A real
    run always names the newest ``read``, because it has just written it.

    ⚠⚠ WHAT THIS DOES NOT ASSERT, per Codex checkpoint 3: this is not single-use.
    Holding the NEWEST id still authorises repeat evaluations until some other
    read is recorded, and the check is relative to the caller's REPEATABLE READ
    snapshot. Both limits are stated on
    ``result_ledger.verify_outcome_access_provenance``; naming them here stops
    this test from reading as a stronger guarantee than it is.
    """

    with ebull_test_conn.transaction():
        declaration_id = freeze_preregistration(ebull_test_conn, build_declaration())

    with ebull_test_conn.transaction():
        first_access_id = require_outcome_access(ebull_test_conn, _access())

    with ebull_test_conn.transaction():
        later_access_id = require_outcome_access(ebull_test_conn, _access())
    assert later_access_id != first_access_id

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        verify_outcome_access_provenance(
            ebull_test_conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=declaration_id,
            access_id=first_access_id,
        )
    assert "outcome_access_superseded_by_a_later_look" in refused.value.refusals

    # ⚠ And the newest one still authorises — a check that refused everything
    # would pass the assertion above while breaking every legitimate run.
    with ebull_test_conn.transaction():
        verify_outcome_access_provenance(
            ebull_test_conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=declaration_id,
            access_id=later_access_id,
        )


def test_provenance_refuses_a_declaration_frozen_after_the_look(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ THE ONE PROPERTY #2599 ACTUALLY EXISTS TO ESTABLISH.

    A declaration written after the outcomes were opened is a description of what
    was found, not a prediction. Simulated by pushing the declaration's server-set
    ``frozen_at`` forward rather than back-dating the access, so the comparison
    under test is the one the gate makes.
    """

    # ⚠ Separate transactions: `now()` is transaction-start time, so a
    # same-transaction freeze and access tie and the strict `frozen_at <
    # accessed_at` refuses. See the note on
    # `test_a_frozen_declaration_authorises_exactly_one_read_row`.
    with ebull_test_conn.transaction():
        declaration_id = freeze_preregistration(ebull_test_conn, build_declaration())

    with ebull_test_conn.transaction():
        access_id = require_outcome_access(ebull_test_conn, _access())

    with ebull_test_conn.transaction():
        # ⚠ sql/333's immutability trigger bars UPDATE, so it is disabled for
        # this one statement and restored inside the same transaction. Separate
        # execute() calls: psycopg prepares every query, so `ALTER; UPDATE;
        # ALTER` in one string fails with "cannot insert multiple commands into
        # a prepared statement".
        ebull_test_conn.execute(
            "ALTER TABLE strategy_preregistration_declarations DISABLE TRIGGER "
            "trg_strategy_preregistration_declaration_immutable"
        )
        ebull_test_conn.execute(
            "UPDATE strategy_preregistration_declarations SET frozen_at = now() + interval '1 hour' "
            "WHERE declaration_id = %(declaration_id)s",
            {"declaration_id": declaration_id},
        )
        ebull_test_conn.execute(
            "ALTER TABLE strategy_preregistration_declarations ENABLE TRIGGER "
            "trg_strategy_preregistration_declaration_immutable"
        )

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        verify_outcome_access_provenance(
            ebull_test_conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=declaration_id,
            access_id=access_id,
        )
    assert "declaration_not_frozen_before_access" in refused.value.refusals


def test_provenance_refuses_a_declaration_edited_around_the_immutability_trigger(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """⚠ COHERENCE AND DIGEST ARE SEPARATE CHECKS, AND BOTH ARE NEEDED (#2599).

    Raising the frozen floor leaves the declaration perfectly coherent — every
    rule in ``declaration_refusals`` still passes — while it now carries a
    different number than the one it was frozen over. Only the digest notices.
    """

    # ⚠ Separate transactions: `now()` is transaction-start time, so a
    # same-transaction freeze and access tie and the strict `frozen_at <
    # accessed_at` refuses. See the note on
    # `test_a_frozen_declaration_authorises_exactly_one_read_row`.
    with ebull_test_conn.transaction():
        declaration_id = freeze_preregistration(ebull_test_conn, build_declaration())

    with ebull_test_conn.transaction():
        access_id = require_outcome_access(ebull_test_conn, _access())

    with ebull_test_conn.transaction():
        ebull_test_conn.execute(
            "ALTER TABLE strategy_preregistration_declarations DISABLE TRIGGER "
            "trg_strategy_preregistration_declaration_immutable"
        )
        ebull_test_conn.execute(
            "UPDATE strategy_preregistration_declarations SET min_forward_decision_dates = 1 "
            "WHERE declaration_id = %(declaration_id)s",
            {"declaration_id": declaration_id},
        )
        ebull_test_conn.execute(
            "ALTER TABLE strategy_preregistration_declarations ENABLE TRIGGER "
            "trg_strategy_preregistration_declaration_immutable"
        )

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        verify_outcome_access_provenance(
            ebull_test_conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=declaration_id,
            access_id=access_id,
        )
    assert "declaration_digest_mismatch" in refused.value.refusals


def test_provenance_refuses_a_declaration_id_for_a_different_trial(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A real declaration, a real access, and the wrong pairing between them."""

    from dataclasses import replace

    with ebull_test_conn.transaction():
        c4_declaration_id = freeze_preregistration(ebull_test_conn, build_declaration())
        other_declaration_id = freeze_preregistration(
            ebull_test_conn, replace(build_declaration(), strategy_id="S-OTHER")
        )
        access_id = require_outcome_access(ebull_test_conn, _access())
    assert other_declaration_id != c4_declaration_id

    with pytest.raises(PreregDeclarationRefused) as refused, ebull_test_conn.transaction():
        verify_outcome_access_provenance(
            ebull_test_conn,
            strategy_id=STRATEGY_ID,
            strategy_version=STRATEGY_VERSION,
            declaration_id=other_declaration_id,
            access_id=access_id,
        )
    assert "declaration_identity_mismatch" in refused.value.refusals
