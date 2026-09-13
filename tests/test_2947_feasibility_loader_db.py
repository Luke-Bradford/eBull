"""#2947 — the loader's ROW SELECTION, asserted by returned proof id.

⚠ Why this file exists, in the words of the mistake it corrects: the spec's first
draft refused SQL fixtures as *"asserting my own SQL back to me"* and leaned on a
dev-verify run instead. That was rationalisation. A dev-verify over three stale
``underlying`` candidates cannot exercise tie handling, failure supersession or
scope isolation — **picking the wrong row would produce identical output**.

So every test here asserts the returned ``core_eligibility_proof_id``. That is
what makes the assertion non-circular: it names WHICH row came back, not merely
that the verdict looked plausible.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a test
source db-marks the WHOLE module at collection, and the projection/CLI tests in
``test_2947_feasibility_loader`` must stay in the fast tier.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest

from app.services.portfolio_feasibility import AccountScope
from app.services.portfolio_feasibility_loader import load_leg_eligibility
from app.services.strategy_core_eligibility import CORE_ELIGIBILITY_POLICY_VERSION

INSTRUMENT_ID = 920947
OTHER_INSTRUMENT_ID = 920948
DIGEST = "b" * 64

_INSERT = """
INSERT INTO strategy_core_eligibility_proofs (
    instrument_id, operator_id, provider, environment,
    api_key_credential_id, user_key_credential_id, observed_at,
    verdict, reason_code, requested_currency, response_currency,
    settlement_type, direction, leverage_values, qualifying_arm_count,
    allow_open_position, response_digest, policy_version, recorded_by
) VALUES (
    %(instrument_id)s, %(operator_id)s, 'etoro', %(environment)s,
    %(api_key_credential_id)s, %(user_key_credential_id)s, %(observed_at)s,
    %(verdict)s, %(reason_code)s, 'USD', 'usd',
    %(settlement_type)s, %(direction)s, %(leverage_values)s, %(qualifying_arm_count)s,
    %(allow_open_position)s, %(response_digest)s, %(policy_version)s, 'test'
)
RETURNING core_eligibility_proof_id
"""


def _seed_instruments(conn: psycopg.Connection[Any]) -> None:
    for instrument_id, symbol in ((INSTRUMENT_ID, "FEAS.ONE"), (OTHER_INSTRUMENT_ID, "FEAS.TWO")):
        conn.execute(
            "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
            "VALUES (%s,%s,'Feasibility Loader Test',TRUE) ON CONFLICT DO NOTHING",
            (instrument_id, symbol),
        )


def _seed_credentials(
    conn: psycopg.Connection[Any], operator_id: UUID, *, environment: str, revoked: bool = False
) -> tuple[UUID, UUID]:
    """One ``(api_key, user_key)`` pair.

    ⚠ ``revoked`` is not a convenience flag — it is the only way a SECOND pair can
    exist for one account. ``broker_credentials_unique_active`` (sql/019) is unique
    on ``(operator_id, provider, label, environment) WHERE revoked_at IS NULL``, so
    a live duplicate is refused by the database. That constraint is also what makes
    the out-of-scope scenario REALISTIC rather than contrived: the rows that shadow
    a valid proof come from a pair that was rotated out, not from a phantom second
    live account.
    """
    ids: list[UUID] = []
    for label in ("api_key", "user_key"):
        row = conn.execute(
            """
            INSERT INTO broker_credentials
                (operator_id, provider, label, environment, ciphertext, last_four, key_version, revoked_at)
            VALUES (%s,'etoro',%s,%s,'\\x00'::bytea,'0000',1, CASE WHEN %s THEN now() ELSE NULL END)
            RETURNING id
            """,
            (operator_id, label, environment, revoked),
        ).fetchone()
        assert row is not None
        ids.append(row[0])
    return ids[0], ids[1]


def _scope(conn: psycopg.Connection[Any], *, environment: str = "demo") -> AccountScope:
    """One operator with a live pair. Built directly rather than via
    ``resolve_account_scope`` so these tests isolate SELECTION from RESOLUTION."""
    _seed_instruments(conn)
    operator_id = uuid4()
    conn.execute(
        "INSERT INTO operators (operator_id, username, password_hash) VALUES (%s,%s,'x')",
        (operator_id, f"op_{operator_id.hex[:8]}"),
    )
    api_key_id, user_key_id = _seed_credentials(conn, operator_id, environment=environment)
    return AccountScope(
        provider="etoro",
        environment=environment,
        operator_id=operator_id,
        api_key_credential_id=api_key_id,
        user_key_credential_id=user_key_id,
    )


def _insert(
    conn: psycopg.Connection[Any],
    scope: AccountScope,
    *,
    observed_at: str,
    verdict: str = "underlying",
    reason_code: str | None = None,
    instrument_id: int = INSTRUMENT_ID,
    api_key_credential_id: UUID | None = None,
    user_key_credential_id: UUID | None = None,
    environment: str | None = None,
) -> int:
    """One proof row; returns its id. ``observed_at`` is explicit so ordering is
    controlled rather than inherited from transaction timing."""
    passing = verdict == "underlying"
    row = conn.execute(
        _INSERT,
        {
            "instrument_id": instrument_id,
            "operator_id": scope.operator_id,
            "environment": environment or scope.environment,
            "api_key_credential_id": api_key_credential_id or scope.api_key_credential_id,
            "user_key_credential_id": user_key_credential_id or scope.user_key_credential_id,
            "observed_at": observed_at,
            "verdict": verdict,
            "reason_code": reason_code,
            "settlement_type": "real" if passing else None,
            "direction": "long" if passing else None,
            "leverage_values": [1] if passing else None,
            "qualifying_arm_count": 1 if passing else 0,
            "allow_open_position": True if passing else None,
            "response_digest": DIGEST,
            "policy_version": CORE_ELIGIBILITY_POLICY_VERSION,
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


def _load(conn: psycopg.Connection[Any], scope: AccountScope, instrument_ids: list[int] | None = None):  # noqa: ANN202
    return load_leg_eligibility(conn, instrument_ids=instrument_ids or [INSTRUMENT_ID], scope=scope)


# ---------------------------------------------------------------------------
# Latest wins — never an older pass over a newer failure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("verdict", "reason_code"),
    [("not_underlying", "no_underlying_arm"), ("unresolved", "instrument_not_resolved")],
)
def test_a_newer_failure_supersedes_an_older_pass(
    ebull_test_conn: psycopg.Connection[Any], verdict: str, reason_code: str
) -> None:
    """The ticket's rule verbatim: *must not prefer an older pass over a newer
    failure*. Both failure verdicts, because they mean different things and both
    must win over a stale pass."""
    scope = _scope(ebull_test_conn)
    _insert(ebull_test_conn, scope, observed_at="2026-08-01T00:00:00Z")
    newer = _insert(
        ebull_test_conn, scope, observed_at="2026-08-02T00:00:00Z", verdict=verdict, reason_code=reason_code
    )

    loaded = _load(ebull_test_conn, scope)
    assert loaded.evidence[INSTRUMENT_ID].proof_id == newer
    assert loaded.by_instrument[INSTRUMENT_ID].verdict == verdict
    ebull_test_conn.rollback()


def test_a_tie_on_observed_at_is_broken_by_the_higher_proof_id(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """``observed_at`` defaults to ``now()`` = TRANSACTION-START time, so a
    single-transaction batch writer stamps every row identically and the timestamp
    alone cannot order them. Today's recorder commits per instrument so no tie
    exists in stored data — this asserts the insurance actually works."""
    scope = _scope(ebull_test_conn)
    first = _insert(ebull_test_conn, scope, observed_at="2026-08-01T00:00:00Z")
    second = _insert(
        ebull_test_conn,
        scope,
        observed_at="2026-08-01T00:00:00Z",
        verdict="not_underlying",
        reason_code="no_underlying_arm",
    )
    assert second > first

    assert _load(ebull_test_conn, scope).evidence[INSTRUMENT_ID].proof_id == second
    ebull_test_conn.rollback()


def test_timestamp_order_beats_proof_id_order_when_they_disagree(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ The case a naive ``ORDER BY proof_id DESC`` passes and MUST NOT.

    The later-inserted row carries the EARLIER observation, so the correct answer
    is the lower id. Without this test the tiebreak could silently be doing the
    primary ordering.
    """
    scope = _scope(ebull_test_conn)
    later_observation = _insert(ebull_test_conn, scope, observed_at="2026-08-09T00:00:00Z")
    earlier_observation = _insert(
        ebull_test_conn,
        scope,
        observed_at="2026-08-01T00:00:00Z",
        verdict="not_underlying",
        reason_code="no_underlying_arm",
    )
    assert earlier_observation > later_observation

    loaded = _load(ebull_test_conn, scope)
    assert loaded.evidence[INSTRUMENT_ID].proof_id == later_observation
    assert loaded.by_instrument[INSTRUMENT_ID].verdict == "underlying"
    ebull_test_conn.rollback()


# ---------------------------------------------------------------------------
# Scope isolation — "latest wins" is a rule WITHIN one account
# ---------------------------------------------------------------------------


def test_a_newer_out_of_scope_row_does_not_shadow_an_older_in_scope_one(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠⚠ The design this loader was rewritten for.

    A row under a REVOKED credential pair is not a newer failure for this account,
    it is an observation of a DIFFERENT account. Letting it win would be
    cross-account interference wearing freshness as a disguise.
    """
    scope = _scope(ebull_test_conn)
    in_scope = _insert(ebull_test_conn, scope, observed_at="2026-08-01T00:00:00Z")
    other_api, other_user = _seed_credentials(ebull_test_conn, scope.operator_id, environment="demo", revoked=True)
    _insert(
        ebull_test_conn,
        scope,
        observed_at="2026-08-09T00:00:00Z",
        verdict="not_underlying",
        reason_code="no_underlying_arm",
        api_key_credential_id=other_api,
        user_key_credential_id=other_user,
    )

    loaded = _load(ebull_test_conn, scope)
    assert loaded.evidence[INSTRUMENT_ID].proof_id == in_scope
    assert loaded.by_instrument[INSTRUMENT_ID].verdict == "underlying"
    # It had an in-scope proof, so no diagnosis is owed for it.
    assert loaded.out_of_scope == {}
    ebull_test_conn.rollback()


def test_an_out_of_scope_only_instrument_is_absent_but_diagnosed(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """ "Proved, but under an account that is no longer live" is a different
    operator action from "never proved" — investigate a credential swap versus run
    a census. The screen must not see it; the artifact must."""
    scope = _scope(ebull_test_conn)
    other_api, other_user = _seed_credentials(ebull_test_conn, scope.operator_id, environment="demo", revoked=True)
    _insert(
        ebull_test_conn,
        scope,
        observed_at="2026-08-09T00:00:00Z",
        api_key_credential_id=other_api,
        user_key_credential_id=other_user,
    )

    loaded = _load(ebull_test_conn, scope)
    assert INSTRUMENT_ID not in loaded.by_instrument
    assert loaded.out_of_scope[INSTRUMENT_ID].row_count == 1
    ebull_test_conn.rollback()


def test_a_real_environment_row_does_not_satisfy_a_demo_request(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    scope = _scope(ebull_test_conn, environment="demo")
    real_api, real_user = _seed_credentials(ebull_test_conn, scope.operator_id, environment="real")
    _insert(
        ebull_test_conn,
        scope,
        observed_at="2026-08-09T00:00:00Z",
        environment="real",
        api_key_credential_id=real_api,
        user_key_credential_id=real_user,
    )

    loaded = _load(ebull_test_conn, scope)
    assert INSTRUMENT_ID not in loaded.by_instrument
    # The diagnostic is environment-scoped too, so a `real` row is not offered as
    # an explanation for a missing `demo` proof.
    assert loaded.out_of_scope == {}
    ebull_test_conn.rollback()


def test_an_instrument_with_no_row_at_all_is_simply_absent(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Never a fabricated ``unresolved`` projection: that is a real recorded
    verdict meaning "the broker's response did not answer", not "we did not ask"."""
    scope = _scope(ebull_test_conn)
    _insert(ebull_test_conn, scope, observed_at="2026-08-01T00:00:00Z")

    loaded = _load(ebull_test_conn, scope, [INSTRUMENT_ID, OTHER_INSTRUMENT_ID])
    assert set(loaded.by_instrument) == {INSTRUMENT_ID}
    assert OTHER_INSTRUMENT_ID not in loaded.evidence
    assert OTHER_INSTRUMENT_ID not in loaded.out_of_scope
    ebull_test_conn.rollback()


def test_each_instrument_gets_its_own_latest_row(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """``DISTINCT ON`` partitions per instrument — a single global newest row
    would pass every test above and fail here."""
    scope = _scope(ebull_test_conn)
    _insert(ebull_test_conn, scope, observed_at="2026-08-01T00:00:00Z")
    one_latest = _insert(ebull_test_conn, scope, observed_at="2026-08-02T00:00:00Z")
    two_latest = _insert(ebull_test_conn, scope, observed_at="2026-08-03T00:00:00Z", instrument_id=OTHER_INSTRUMENT_ID)

    loaded = _load(ebull_test_conn, scope, [INSTRUMENT_ID, OTHER_INSTRUMENT_ID])
    assert loaded.evidence[INSTRUMENT_ID].proof_id == one_latest
    assert loaded.evidence[OTHER_INSTRUMENT_ID].proof_id == two_latest
    ebull_test_conn.rollback()


def test_the_projection_keys_on_the_rows_own_instrument_id(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠⚠ The screen does NOT verify that a mapping key matches the proof it holds,
    so a mis-keyed mapping returns ``feasible``. Keying off the row is what makes
    that unreachable from this path."""
    scope = _scope(ebull_test_conn)
    _insert(ebull_test_conn, scope, observed_at="2026-08-01T00:00:00Z")

    loaded = _load(ebull_test_conn, scope, [INSTRUMENT_ID, OTHER_INSTRUMENT_ID])
    for key, leg in loaded.by_instrument.items():
        assert key == leg.instrument_id
    for key, proof in loaded.evidence.items():
        assert key == proof.instrument_id
    ebull_test_conn.rollback()


def test_the_loaded_proof_carries_the_scope_it_was_selected_under(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The screen re-checks scope on top of the loader's filter. Defence in depth:
    the loader guarantees the match, and the screen would still catch a caller that
    assembled the mapping some other way."""
    scope = _scope(ebull_test_conn)
    _insert(ebull_test_conn, scope, observed_at="2026-08-01T00:00:00Z")

    assert _load(ebull_test_conn, scope).by_instrument[INSTRUMENT_ID].scope == scope
    ebull_test_conn.rollback()


def test_age_is_measured_from_the_stored_observation(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Sanity check on what the screen's staleness bound will see: the loader
    hands over ``observed_at`` verbatim, not a recomputed or defaulted time."""
    scope = _scope(ebull_test_conn)
    _insert(ebull_test_conn, scope, observed_at="2026-08-01T12:34:56Z")

    observed = _load(ebull_test_conn, scope).by_instrument[INSTRUMENT_ID].observed_at
    assert observed.tzinfo is not None
    assert abs(observed - observed.replace(hour=12, minute=34, second=56)) < timedelta(seconds=1)
    ebull_test_conn.rollback()
