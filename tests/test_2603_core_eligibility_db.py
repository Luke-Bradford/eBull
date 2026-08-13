"""#2603 item 2 — what ``sql/346`` enforces, and what the mandate gate refuses.

⚠ THE MIGRATION'S BACKSTOP, NOT THE PYTHON RULE. The verdict arithmetic is
covered by pure tests in ``test_2603_core_eligibility``; the CHECK tests here
INSERT **raw SQL that bypasses the evaluator**, because a constraint that only
ever sees pre-validated values is not demonstrably a backstop at all.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a
test source db-marks the WHOLE module at collection.
"""

from datetime import timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest

from app.services.strategy_core_eligibility import (
    CORE_ELIGIBILITY_MAX_AGE,
    CORE_ELIGIBILITY_POLICY_VERSION,
    CoreEligibilityAssessment,
    CoreEligibilityError,
    record_core_eligibility_proof,
    require_core_eligibility,
)
from app.services.strategy_core_mandate import CoreMandateError, configure_core_mandate

INSTRUMENT_ID = 920604
DIGEST = "a" * 64

_INSERT = """
INSERT INTO strategy_core_eligibility_proofs (
    instrument_id, operator_id, provider, environment,
    api_key_credential_id, user_key_credential_id,
    verdict, reason_code, requested_currency, response_currency,
    settlement_type, direction, leverage_values, qualifying_arm_count,
    allow_open_position, response_digest, policy_version, recorded_by
) VALUES (
    %(instrument_id)s, %(operator_id)s, 'etoro', 'demo',
    %(api_key_credential_id)s, %(user_key_credential_id)s,
    %(verdict)s, %(reason_code)s, 'USD', %(response_currency)s,
    %(settlement_type)s, %(direction)s, %(leverage_values)s, %(qualifying_arm_count)s,
    %(allow_open_position)s, %(response_digest)s, %(policy_version)s, 'test'
)
"""


def _seed_instrument(conn: psycopg.Connection[Any]) -> int:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'CORE.ELIG','Core Eligibility Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )
    return INSTRUMENT_ID


def _seed_account(conn: psycopg.Connection[Any], *, environment: str = "demo") -> tuple[UUID, UUID, UUID]:
    """One operator plus its live ``api_key``/``user_key`` pair.

    The ciphertext is a placeholder: nothing here decrypts, and these tests are
    about which credential ROW a proof is attributed to.
    """
    operator_id = uuid4()
    conn.execute(
        "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s,%s,'x')",
        (operator_id, f"op_{operator_id.hex[:8]}"),
    )
    ids: list[UUID] = []
    for label in ("api_key", "user_key"):
        row = conn.execute(
            """
            INSERT INTO broker_credentials
                (operator_id, provider, label, environment, ciphertext, last_four, key_version)
            VALUES (%s,'etoro',%s,%s,'\\x00'::bytea,'0000',1)
            RETURNING id
            """,
            (operator_id, label, environment),
        ).fetchone()
        assert row is not None
        ids.append(row[0])
    return operator_id, ids[0], ids[1]


def _pass_row(operator_id: UUID, api_key_id: UUID, user_key_id: UUID) -> dict[str, Any]:
    return {
        "instrument_id": INSTRUMENT_ID,
        "operator_id": operator_id,
        "api_key_credential_id": api_key_id,
        "user_key_credential_id": user_key_id,
        "verdict": "underlying",
        "reason_code": None,
        "response_currency": "usd",
        "settlement_type": "real",
        "direction": "long",
        "leverage_values": [1],
        "qualifying_arm_count": 1,
        "allow_open_position": True,
        "response_digest": DIGEST,
        "policy_version": CORE_ELIGIBILITY_POLICY_VERSION,
    }


def _account(conn: psycopg.Connection[Any]) -> tuple[UUID, UUID, UUID]:
    _seed_instrument(conn)
    return _seed_account(conn)


# --------------------------------------------------------------------------
# sql/346 as a backstop
# --------------------------------------------------------------------------


def test_the_reference_row_inserts(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The control. Without it a CHECK test could pass for the wrong reason."""
    ebull_test_conn.execute(_INSERT, _pass_row(*_account(ebull_test_conn)))
    ebull_test_conn.rollback()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("settlement_type", None),
        ("direction", None),
        ("allow_open_position", False),
        ("qualifying_arm_count", 2),
        ("leverage_values", [2, 5]),
        ("response_currency", "gbp"),
    ],
)
def test_an_underlying_row_cannot_be_incomplete(
    ebull_test_conn: psycopg.Connection[Any], field: str, value: Any
) -> None:
    """Storing the projection is not enough: each of these looks like a pass.

    ``response_currency='gbp'`` is in the list because the minimums are
    denominated in it — an ``underlying`` verdict quoted in a currency the
    request did not ask for is not a readable proof.
    """
    row = {**_pass_row(*_account(ebull_test_conn)), field: value}
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(_INSERT, row)
    ebull_test_conn.rollback()


def test_a_failing_row_cannot_carry_pass_shaped_evidence(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    row = {
        **_pass_row(*_account(ebull_test_conn)),
        "verdict": "not_underlying",
        "reason_code": "no_underlying_arm",
    }
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(_INSERT, row)
    ebull_test_conn.rollback()


@pytest.mark.parametrize(
    ("verdict", "reason_code"),
    [("underlying", "no_underlying_arm"), ("not_underlying", None)],
)
def test_the_reason_is_present_exactly_when_the_verdict_fails(
    ebull_test_conn: psycopg.Connection[Any], verdict: str, reason_code: str | None
) -> None:
    """Both directions: a pass cannot carry a reason, a failure cannot omit one."""
    row = {
        **_pass_row(*_account(ebull_test_conn)),
        "verdict": verdict,
        "reason_code": reason_code,
        "settlement_type": None if verdict != "underlying" else "real",
        "direction": None if verdict != "underlying" else "long",
        "leverage_values": None if verdict != "underlying" else [1],
    }
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(_INSERT, row)
    ebull_test_conn.rollback()


def test_an_undeclared_reason_code_is_refused(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The SQL vocabulary mirrors the Python frozenset; both must move together."""
    row = {
        **_pass_row(*_account(ebull_test_conn)),
        "verdict": "unresolved",
        "reason_code": "made_up_reason",
        "settlement_type": None,
        "direction": None,
        "leverage_values": None,
    }
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(_INSERT, row)
    ebull_test_conn.rollback()


def test_evidence_outlives_tidying(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """``ON DELETE RESTRICT`` on the instrument FK: a proof is not collateral."""
    row = ebull_test_conn.execute(
        """
        SELECT rc.delete_rule FROM information_schema.referential_constraints rc
        JOIN information_schema.table_constraints tc
          ON tc.constraint_name = rc.constraint_name
        WHERE tc.table_name = 'strategy_core_eligibility_proofs'
          AND rc.delete_rule = 'RESTRICT'
        LIMIT 1
        """
    ).fetchone()
    assert row is not None


# --------------------------------------------------------------------------
# The freshness / attribution rules
# --------------------------------------------------------------------------


def _require(conn: psycopg.Connection[Any], operator_id: UUID) -> Any:
    return require_core_eligibility(
        conn,
        instrument_id=INSTRUMENT_ID,
        operator_id=operator_id,
        provider="etoro",
        environment="demo",
    )


def test_a_fresh_passing_proof_satisfies_the_requirement(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _pass_row(operator_id, api_key_id, user_key_id))
    proof = _require(ebull_test_conn, operator_id)
    assert proof.verdict == "underlying"
    assert proof.policy_version == CORE_ELIGIBILITY_POLICY_VERSION
    ebull_test_conn.rollback()


def test_no_observation_at_all_is_its_own_refusal(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    operator_id, _, _ = _account(ebull_test_conn)
    with pytest.raises(CoreEligibilityError, match="no etoro demo eligibility proof"):
        _require(ebull_test_conn, operator_id)
    ebull_test_conn.rollback()


def test_only_the_newest_observation_counts(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """A later failure supersedes an earlier pass — evidence is not cumulative."""
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _pass_row(operator_id, api_key_id, user_key_id))
    ebull_test_conn.execute(
        _INSERT,
        {
            **_pass_row(operator_id, api_key_id, user_key_id),
            "verdict": "not_underlying",
            "reason_code": "no_underlying_arm",
            "settlement_type": None,
            "direction": None,
            "leverage_values": None,
        },
    )
    with pytest.raises(CoreEligibilityError, match="not_underlying"):
        _require(ebull_test_conn, operator_id)
    ebull_test_conn.rollback()


def test_a_proof_exactly_at_the_boundary_is_still_fresh(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ #2670's lesson: a boundary nobody writes down is one two readers disagree on.

    The comparison is ``age <= MAX_AGE``, so exactly ``MAX_AGE`` passes and one
    second past it does not. Both sides asserted, because only the pair pins it.
    """
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _pass_row(operator_id, api_key_id, user_key_id))
    ebull_test_conn.execute(
        "UPDATE strategy_core_eligibility_proofs SET observed_at = now() - %s",
        (CORE_ELIGIBILITY_MAX_AGE,),
    )
    assert _require(ebull_test_conn, operator_id).verdict == "underlying"

    ebull_test_conn.execute(
        "UPDATE strategy_core_eligibility_proofs SET observed_at = now() - %s",
        (CORE_ELIGIBILITY_MAX_AGE + timedelta(seconds=1),),
    )
    with pytest.raises(CoreEligibilityError, match="past the"):
        _require(ebull_test_conn, operator_id)
    ebull_test_conn.rollback()


def test_a_credential_swap_invalidates_every_prior_proof(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The account triple survives a swap; a DIFFERENT eToro account must not
    inherit the old one's proofs. Revoking and re-adding changes the row ids,
    which is what makes the invalidation structural rather than remembered."""
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _pass_row(operator_id, api_key_id, user_key_id))
    assert _require(ebull_test_conn, operator_id).verdict == "underlying"

    ebull_test_conn.execute("UPDATE broker_credentials SET revoked_at = now() WHERE operator_id = %s", (operator_id,))
    for label in ("api_key", "user_key"):
        ebull_test_conn.execute(
            """
            INSERT INTO broker_credentials
                (operator_id, provider, label, environment, ciphertext, last_four, key_version)
            VALUES (%s,'etoro',%s,'demo','\\x00'::bytea,'0000',1)
            """,
            (operator_id, label),
        )
    with pytest.raises(CoreEligibilityError, match="no longer live"):
        _require(ebull_test_conn, operator_id)
    ebull_test_conn.rollback()


def test_an_account_with_no_live_pair_cannot_hold_a_proof(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _pass_row(operator_id, api_key_id, user_key_id))
    ebull_test_conn.execute(
        "UPDATE broker_credentials SET revoked_at = now() WHERE operator_id = %s AND label='user_key'",
        (operator_id,),
    )
    with pytest.raises(CoreEligibilityError, match="no live etoro demo credential pair"):
        _require(ebull_test_conn, operator_id)
    ebull_test_conn.rollback()


def test_a_proof_does_not_cross_environments(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """A demo proof must never be readable as a real one."""
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _pass_row(operator_id, api_key_id, user_key_id))
    with pytest.raises(CoreEligibilityError, match="no etoro real eligibility proof"):
        require_core_eligibility(
            ebull_test_conn,
            instrument_id=INSTRUMENT_ID,
            operator_id=operator_id,
            provider="etoro",
            environment="real",
        )
    ebull_test_conn.rollback()


def test_the_recorder_takes_no_observation_time(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """``observed_at`` is the database's ``now()``.

    A caller that could declare its own observation time could extend a proof's
    validity at will, so the parameter does not exist.
    """
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    proof_id = record_core_eligibility_proof(
        ebull_test_conn,
        assessment=CoreEligibilityAssessment(
            verdict="underlying",
            reason_code=None,
            response_currency="usd",
            settlement_type="real",
            direction="long",
            leverage_values=(1,),
            qualifying_arm_count=1,
            allow_open_position=True,
            allow_close_position=True,
            allow_partial_close_position=True,
            min_position_amount=Decimal("10"),
            min_position_exposure=Decimal("10"),
            max_units_per_order=Decimal("134"),
            response_digest=DIGEST,
        ),
        instrument_id=INSTRUMENT_ID,
        operator_id=operator_id,
        provider="etoro",
        environment="demo",
        api_key_credential_id=api_key_id,
        user_key_credential_id=user_key_id,
        recorded_by="test",
    )
    row = ebull_test_conn.execute(
        "SELECT observed_at <= now(), observed_at > now() - interval '1 minute' "
        "FROM strategy_core_eligibility_proofs WHERE core_eligibility_proof_id = %s",
        (proof_id,),
    ).fetchone()
    assert row == (True, True)
    ebull_test_conn.rollback()


# --------------------------------------------------------------------------
# The mandate gate — the one consumer
# --------------------------------------------------------------------------


def _configure(conn: psycopg.Connection[Any], operator_id: UUID, *, enabled: bool = True) -> Any:
    return configure_core_mandate(
        conn,
        enabled=enabled,
        core_instrument_id=INSTRUMENT_ID,
        core_target_pct=Decimal("60"),
        liquidity_reserve_pct=Decimal("20"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal("25"),
        changed_by="test",
        reason="core/cash mandate",
        operator_id=operator_id,
        provider="etoro",
        environment="demo",
    )


def test_an_enabled_mandate_is_refused_without_a_proof(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Naming a CFD as the core instrument has to fail at the selection point."""
    operator_id, _, _ = _account(ebull_test_conn)
    with pytest.raises(CoreEligibilityError, match="underlying product, not a CFD"):
        _configure(ebull_test_conn, operator_id)
    ebull_test_conn.rollback()


def test_an_enabled_mandate_is_refused_on_a_failing_proof(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The plain-SPY case: the response was read fine and the answer is no."""
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(
        _INSERT,
        {
            **_pass_row(operator_id, api_key_id, user_key_id),
            "verdict": "not_underlying",
            "reason_code": "no_underlying_arm",
            "settlement_type": None,
            "direction": None,
            "leverage_values": None,
        },
    )
    with pytest.raises(CoreEligibilityError, match="no_underlying_arm"):
        _configure(ebull_test_conn, operator_id)
    ebull_test_conn.rollback()


def test_an_enabled_mandate_is_accepted_on_a_fresh_passing_proof(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _pass_row(operator_id, api_key_id, user_key_id))
    mandate = _configure(ebull_test_conn, operator_id)
    assert mandate.revision == 1 and mandate.enabled
    ebull_test_conn.rollback()


def test_a_disabled_mandate_is_not_gated(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """⚠ It MAY still name an instrument — ``..._enabled_has_instrument`` is
    one-directional — and is ungated because it authorises nothing."""
    operator_id, _, _ = _account(ebull_test_conn)
    mandate = _configure(ebull_test_conn, operator_id, enabled=False)
    assert not mandate.enabled
    assert mandate.core_instrument_id == INSTRUMENT_ID
    ebull_test_conn.rollback()


def test_re_enabling_a_disabled_mandate_passes_through_the_gate(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The hole a write-time gate would otherwise leave: name it while disabled,
    then flip the flag."""
    operator_id, _, _ = _account(ebull_test_conn)
    _configure(ebull_test_conn, operator_id, enabled=False)
    with pytest.raises(CoreEligibilityError):
        _configure(ebull_test_conn, operator_id, enabled=True)
    ebull_test_conn.rollback()


def test_the_gate_does_not_move_the_mandate_policy_version(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """A precondition on WRITES is not a change to the arithmetic the version stamps.

    #2670 settled that 0 rows never excuses leaving a version alone — but that
    applies to a change in what a stored row MEANS. Every ``CoreMandate`` remains
    valid here with the identical meaning; the rule set that did change carries
    its own version (``core-eligibility-v1``) on the proof row.
    """
    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _pass_row(operator_id, api_key_id, user_key_id))
    mandate = _configure(ebull_test_conn, operator_id)
    assert mandate.policy_version == "core-mandate-v2"
    ebull_test_conn.rollback()


def test_a_refused_mandate_writes_no_row(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """⚠ The post-condition is a SUBSEQUENT statement, not the raised error.

    A test that only asserts the exception passes against a version that raises
    AFTER inserting (#2674's lesson). The advisory lock is taken before the gate,
    so this also pins that the refusal happens before any append.
    """
    operator_id, _, _ = _account(ebull_test_conn)
    with pytest.raises(CoreEligibilityError):
        _configure(ebull_test_conn, operator_id)
    remaining = ebull_test_conn.execute("SELECT count(*) FROM strategy_core_mandate_events").fetchone()
    assert remaining == (0,)
    ebull_test_conn.rollback()


def test_the_mandate_error_type_is_not_swallowed(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """``CoreEligibilityError`` is not a ``CoreMandateError``: an eligibility
    refusal is about the ACCOUNT, not about the mandate's arithmetic, and a
    caller that catches only the latter must not silently treat it as valid."""
    operator_id, _, _ = _account(ebull_test_conn)
    with pytest.raises(CoreEligibilityError):
        _configure(ebull_test_conn, operator_id)
    assert not issubclass(CoreEligibilityError, CoreMandateError)
    ebull_test_conn.rollback()


def test_the_gate_blocks_a_concurrent_credential_swap(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ The mandate advisory lock serialises MANDATE writers, not credential swaps.

    Without ``FOR SHARE`` on the live pair, a revoke committing between the
    freshness read and the mandate INSERT leaves an enabled mandate authorised by
    the account that just went away. The post-condition is a SECOND connection's
    behaviour, not this one's return value: a test that only re-read the ids here
    would pass against the unlocked version.
    """
    from tests.fixtures.ebull_test_db import test_database_url

    operator_id, api_key_id, user_key_id = _account(ebull_test_conn)
    ebull_test_conn.execute(_INSERT, _pass_row(operator_id, api_key_id, user_key_id))
    ebull_test_conn.commit()

    # Hold the gate's read open, as `configure_core_mandate` does between the
    # check and its INSERT.
    assert _require(ebull_test_conn, operator_id).verdict == "underlying"

    with psycopg.connect(test_database_url()) as other:
        other.execute("SET LOCAL lock_timeout = '1s'")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            other.execute(
                "UPDATE broker_credentials SET revoked_at = now() WHERE operator_id = %s",
                (operator_id,),
            )
        other.rollback()

    ebull_test_conn.rollback()
    ebull_test_conn.execute("DELETE FROM strategy_core_eligibility_proofs")
    ebull_test_conn.execute("DELETE FROM broker_credentials WHERE operator_id = %s", (operator_id,))
    ebull_test_conn.execute("DELETE FROM operators WHERE operator_id = %s", (operator_id,))
    ebull_test_conn.commit()
