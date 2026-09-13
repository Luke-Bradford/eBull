"""#2603 item 2 — the revalidation selector against real rows.

⚠ These assert which PROOF ID the selector treats as "latest", over deliberately
conflicting rows. That is what makes them non-circular: a naive
``ORDER BY core_eligibility_proof_id DESC`` and a naive ``ORDER BY observed_at
DESC`` each pass a subset of them, so the ordering is demonstrated rather than
restated. Modelled on ``tests/test_2603_core_eligibility_db.py``.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a
test source db-marks the WHOLE module at collection.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg

from app.services.strategy_core_eligibility import CORE_ELIGIBILITY_POLICY_VERSION
from app.services.strategy_core_eligibility_refresh import (
    CORE_ELIGIBILITY_REFRESH_AGE,
    select_proofs_to_revalidate,
)

DIGEST = "b" * 64

_INSERT = """
INSERT INTO strategy_core_eligibility_proofs (
    instrument_id, operator_id, provider, environment,
    api_key_credential_id, user_key_credential_id,
    verdict, reason_code, requested_currency, response_currency,
    settlement_type, direction, leverage_values, qualifying_arm_count,
    allow_open_position, response_digest, policy_version, recorded_by,
    observed_at
) VALUES (
    %(instrument_id)s, %(operator_id)s, 'etoro', %(environment)s,
    %(api_key_credential_id)s, %(user_key_credential_id)s,
    %(verdict)s, %(reason_code)s, 'USD', 'usd',
    %(settlement_type)s, %(direction)s, %(leverage_values)s, %(qualifying_arm_count)s,
    %(allow_open_position)s, %(response_digest)s, %(policy_version)s, 'test',
    now() - %(age)s::interval
)
RETURNING core_eligibility_proof_id
"""


#: ``instruments.instrument_id`` is the BROKER's id, not a sequence — it is
#: assigned, so every seed supplies one. Based at a value no eToro instrument
#: uses so these rows cannot collide with a real one.
_BASE_INSTRUMENT_ID = 920_700


def _seed_instrument(conn: psycopg.Connection[Any], symbol: str) -> int:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    instrument_id = _BASE_INSTRUMENT_ID + (ord(symbol[-1]) - ord("A"))
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,%s,'Refresh Selector Test',TRUE) ON CONFLICT DO NOTHING",
        (instrument_id, symbol),
    )
    return instrument_id


def _seed_account(conn: psycopg.Connection[Any]) -> tuple[UUID, UUID, UUID]:
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
            VALUES (%s,'etoro',%s,'demo','\\x00'::bytea,'0000',1)
            RETURNING id
            """,
            (operator_id, label),
        ).fetchone()
        assert row is not None
        ids.append(row[0])
    return operator_id, ids[0], ids[1]


def _proof(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    operator_id: UUID,
    api_key_id: UUID,
    user_key_id: UUID,
    age: timedelta,
    verdict: str = "underlying",
    environment: str = "demo",
) -> int:
    passing = verdict == "underlying"
    row = conn.execute(
        _INSERT,
        {
            "instrument_id": instrument_id,
            "operator_id": operator_id,
            "environment": environment,
            "api_key_credential_id": api_key_id,
            "user_key_credential_id": user_key_id,
            "verdict": verdict,
            "reason_code": None if passing else "no_underlying_arm",
            "settlement_type": "real" if passing else None,
            "direction": "long" if passing else None,
            "leverage_values": [1] if passing else None,
            "qualifying_arm_count": 1 if passing else 0,
            "allow_open_position": True if passing else None,
            "response_digest": DIGEST,
            "policy_version": CORE_ELIGIBILITY_POLICY_VERSION,
            "age": f"{int(age.total_seconds())} seconds",
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


def _select(
    conn: psycopg.Connection[Any],
    operator_id: UUID,
    pair: tuple[UUID, UUID],
    *,
    environment: str = "demo",
    limit: int = 100,
) -> Any:
    return select_proofs_to_revalidate(
        conn,
        operator_id=operator_id,
        provider="etoro",
        environment=environment,
        live_credential_ids=pair,
        limit=limit,
    )


_STALE = CORE_ELIGIBILITY_REFRESH_AGE + timedelta(hours=1)
_FRESH = timedelta(hours=1)


def test_the_reference_case_selects(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """The control. Without it every negative below could pass for the wrong
    reason (an empty table selects nothing too)."""
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn, "REF.A")
    proof_id = _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE,
    )
    scope = _select(ebull_test_conn, operator_id, (api_key_id, user_key_id))
    assert [(s.instrument_id, s.prior_proof_id) for s in scope.due] == [(instrument_id, proof_id)]
    ebull_test_conn.rollback()


def test_the_predicate_is_on_the_latest_row_not_on_any_row(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """A fresh proof behind an older stale one must NOT be due.

    This is the case a ``WHERE age > trigger`` without ``DISTINCT ON`` passes,
    and it would re-ask the broker about an instrument just observed.
    """
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn, "REF.B")
    _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE,
    )
    _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_FRESH,
    )
    assert _select(ebull_test_conn, operator_id, (api_key_id, user_key_id)).due == ()
    ebull_test_conn.rollback()


def test_a_tie_on_observed_at_is_broken_by_the_higher_proof_id(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """``observed_at`` DEFAULTs to ``now()``, which is transaction-START time, so
    two rows written in one transaction TIE. Three readers of "the latest proof"
    that break a tie differently are three definitions, and the disagreement
    only ever shows up on the row that matters."""
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn, "REF.C")
    first = _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE,
        verdict="not_underlying",
    )
    second = _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE,
    )
    assert second > first
    scope = _select(ebull_test_conn, operator_id, (api_key_id, user_key_id))
    assert [s.prior_proof_id for s in scope.due] == [second]
    assert scope.due[0].prior_verdict == "underlying"
    ebull_test_conn.rollback()


def test_timestamp_order_beats_id_order_when_they_disagree(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The case a naive ``ORDER BY core_eligibility_proof_id DESC`` passes and
    must not: the later-inserted row carries the OLDER observation."""
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn, "REF.D")
    newer_observation = _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE,
        verdict="not_underlying",
    )
    older_observation = _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE + timedelta(days=5),
    )
    assert older_observation > newer_observation
    scope = _select(ebull_test_conn, operator_id, (api_key_id, user_key_id))
    assert [s.prior_proof_id for s in scope.due] == [newer_observation]
    assert scope.due[0].prior_verdict == "not_underlying"
    ebull_test_conn.rollback()


def test_a_different_environment_is_not_selected(ebull_test_conn: psycopg.Connection[Any]) -> None:
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn, "REF.E")
    _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE,
        environment="real",
    )
    assert _select(ebull_test_conn, operator_id, (api_key_id, user_key_id)).due == ()
    ebull_test_conn.rollback()


def test_a_different_operator_is_not_selected(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """Cross-account interference, the rule #2947's loader had to reverse a
    first draft over."""
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    other_operator_id, _, _ = _seed_account(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn, "REF.F")
    _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE,
    )
    assert _select(ebull_test_conn, other_operator_id, (api_key_id, user_key_id)).due == ()
    ebull_test_conn.rollback()


def test_a_fresh_proof_under_superseded_credentials_is_selected(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Arm 2 against real rows. ``require_core_eligibility`` compares the pair,
    so an age-only rule would leave a rotated account unusable for half the
    freshness window with nothing scheduled to heal it."""
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn, "REF.G")
    _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_FRESH,
    )
    rotated = (uuid4(), uuid4())
    scope = _select(ebull_test_conn, operator_id, rotated)
    assert [s.instrument_id for s in scope.due] == [instrument_id]
    assert scope.due[0].credentials_superseded is True
    ebull_test_conn.rollback()


def test_an_instrument_with_no_proof_is_never_selected(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠⚠ THE NON-WIDENING INVARIANT, asserted directly.

    Proof membership drives ``QUOTES_REFRESH_SCOPE_SQL`` arm 5, whose comment
    records that only deliberate proving can widen it. A scheduled prover that
    could mint a first proof would break that bound for the whole universe.
    """
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    proved = _seed_instrument(ebull_test_conn, "REF.H")
    _seed_instrument(ebull_test_conn, "REF.I")
    _proof(
        ebull_test_conn,
        instrument_id=proved,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE,
    )
    scope = _select(ebull_test_conn, operator_id, (api_key_id, user_key_id))
    assert [s.instrument_id for s in scope.due] == [proved]
    assert scope.proved_instrument_count == 1
    ebull_test_conn.rollback()


def test_selection_is_oldest_first_and_the_cap_defers_the_tail(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Oldest-first is the starvation control: a cap that kept deferring the
    same instruments would let them expire while runs stayed green."""
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    ages = {"REF.J": _STALE, "REF.K": _STALE + timedelta(days=3), "REF.L": _STALE + timedelta(days=1)}
    ids = {}
    for symbol, age in ages.items():
        instrument_id = _seed_instrument(ebull_test_conn, symbol)
        ids[symbol] = instrument_id
        _proof(
            ebull_test_conn,
            instrument_id=instrument_id,
            operator_id=operator_id,
            api_key_id=api_key_id,
            user_key_id=user_key_id,
            age=age,
        )
    scope = _select(ebull_test_conn, operator_id, (api_key_id, user_key_id), limit=2)
    assert [s.instrument_id for s in scope.due] == [ids["REF.K"], ids["REF.L"]]
    assert scope.deferred_count == 1
    assert scope.proved_instrument_count == 3
    ebull_test_conn.rollback()


def test_the_symbol_is_reported_for_the_job_note(ebull_test_conn: psycopg.Connection[Any]) -> None:
    """⚠ There is deliberately NO orphan test here, and its absence is the
    finding: ``sql/346`` line 32-33 makes ``instrument_id`` a
    ``REFERENCES instruments(instrument_id) ON DELETE RESTRICT``, so a proof
    without an instrument row cannot exist and LEFT vs INNER join is
    behaviourally identical today. The join stays LEFT so a future FK change
    cannot turn the selector into something that silently drops a row, but
    claiming a test demonstrates that would be claiming a case the schema
    forbids."""
    operator_id, api_key_id, user_key_id = _seed_account(ebull_test_conn)
    instrument_id = _seed_instrument(ebull_test_conn, "REF.M")
    _proof(
        ebull_test_conn,
        instrument_id=instrument_id,
        operator_id=operator_id,
        api_key_id=api_key_id,
        user_key_id=user_key_id,
        age=_STALE,
    )
    scope = _select(ebull_test_conn, operator_id, (api_key_id, user_key_id))
    assert scope.due[0].symbol == "REF.M"
    ebull_test_conn.rollback()
