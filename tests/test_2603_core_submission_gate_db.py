"""#2603 item 3 step 3a — what the submission gate admits, and what it refuses.

Every refusal against real rows, because the gate's whole substance is one SQL
statement and a mocked cursor would prove nothing about it.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a test
source db-marks the WHOLE module at collection.

⚠⚠ The gate REFUSES; it never submits.  Nothing here reaches a broker, and no test
in this module creates an order.  The trade rows it seeds exist to be blockers.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import psycopg
import pytest

from app.services.strategy_control_plane import PAPER_ALLOCATOR_ADVISORY_LOCK
from app.services.strategy_core_eligibility import CORE_ELIGIBILITY_POLICY_VERSION
from app.services.strategy_core_mandate import CORE_MANDATE_MODE, CORE_MANDATE_POLICY_VERSION
from app.services.strategy_core_submission_gate import (
    CORE_SUBMISSION_ADVISORY_LOCK,
    StrategyCoreSubmissionError,
    admit_core_rebalance_intent,
    core_submission_lock,
)

_INSTRUMENT_ID = 920705
_PAST = datetime(2026, 1, 2, 9, 0, tzinfo=UTC)


# --------------------------------------------------------------------------
# Seeds.
# --------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[Any]) -> None:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'CORE.GATE','Core Gate Test',TRUE) ON CONFLICT DO NOTHING",
        (_INSTRUMENT_ID,),
    )


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


def _seed_proof(
    conn: psycopg.Connection[Any],
    account: tuple[UUID, UUID, UUID],
    *,
    allow_partial_close: bool | None = True,
) -> None:
    operator_id, api_key_id, user_key_id = account
    conn.execute(
        """
        INSERT INTO strategy_core_eligibility_proofs (
            instrument_id, operator_id, provider, environment,
            api_key_credential_id, user_key_credential_id,
            verdict, reason_code, requested_currency, response_currency,
            settlement_type, direction, leverage_values, qualifying_arm_count,
            allow_open_position, allow_close_position, allow_partial_close_position,
            response_digest, policy_version, recorded_by
        ) VALUES (
            %s, %s, 'etoro', 'demo', %s, %s,
            'underlying', NULL, 'USD', 'USD',
            'real', 'long', ARRAY[1], 1,
            TRUE, TRUE, %s, %s, %s, 'test'
        )
        """,
        (
            _INSTRUMENT_ID,
            operator_id,
            api_key_id,
            user_key_id,
            allow_partial_close,
            "0" * 64,
            CORE_ELIGIBILITY_POLICY_VERSION,
        ),
    )


def _seed_mandate(
    conn: psycopg.Connection[Any],
    *,
    revision: int = 1,
    enabled: bool = True,
    mode: str = CORE_MANDATE_MODE,
) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_core_mandate_events (
            revision,enabled,base_currency,core_instrument_id,core_target_pct,
            liquidity_reserve_pct,rebalance_band_pct,min_rebalance_amount,
            policy_version,changed_by,reason,mode
        ) VALUES (%s,%s,'USD',%s,60,20,5,25,%s,'test','test',%s)
        RETURNING core_mandate_event_id
        """,
        (revision, enabled, _INSTRUMENT_ID, CORE_MANDATE_POLICY_VERSION, mode),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_intent(conn: psycopg.Connection[Any], *, event_id: int, action: str = "buy_core") -> int:
    """One actionable intent by default; ``action`` is what the callers vary."""
    refused = action == "refused"
    row = conn.execute(
        """
        INSERT INTO strategy_core_rebalance_intents (
            core_mandate_event_id, allocator_policy_version, recorded_by,
            core_instrument_id, currency, core_market_value, cash_balance,
            state_as_of, action, reason_code, amount, core_pct, target_pct,
            lower_pct, upper_pct, effective_floor, floor_source,
            reserve_breached, reserve_margin_pct
        ) VALUES (
            %(event_id)s, %(policy)s, 'test', %(instrument_id)s, 'USD', 600, 400,
            %(state_as_of)s, %(action)s, %(reason_code)s, %(amount)s,
            %(core_pct)s, %(target_pct)s, %(lower_pct)s, %(upper_pct)s,
            %(floor)s, %(floor_source)s, %(breached)s, %(margin)s
        )
        RETURNING core_rebalance_intent_id
        """,
        {
            "event_id": event_id,
            "policy": CORE_MANDATE_POLICY_VERSION,
            "instrument_id": _INSTRUMENT_ID,
            "state_as_of": _PAST,
            "action": action,
            "reason_code": "core_sleeve_empty" if refused else None,
            "amount": Decimal("0") if action in ("hold", "refused") else Decimal("50"),
            "core_pct": None if refused else Decimal("60"),
            "target_pct": None if refused else Decimal("60"),
            "lower_pct": None if refused else Decimal("55"),
            "upper_pct": None if refused else Decimal("65"),
            "floor": None if refused else Decimal("25"),
            "floor_source": None if refused else "mandate",
            "breached": None if refused else False,
            "margin": None if refused else Decimal("10"),
        },
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_blocking_trade(
    conn: psycopg.Connection[Any],
    *,
    intent_id: int,
    status: str = "submitted",
    reconciliation_state: str | None = "unresolved",
    link_order: bool = True,
) -> int:
    """A core trade, optionally with a linked order in a reconciliation state.

    ``link_order=False`` seeds the no-linked-order arm — the belt for a row
    inserted directly rather than through the executor. ``reconciliation_state=
    None`` with ``link_order=True`` seeds the harder case: a linked order that
    has NO reconciliation row, which nothing in the schema forbids.
    """
    trade = conn.execute(
        "INSERT INTO strategy_trades (core_rebalance_intent_id,instrument_id,status) "
        "VALUES (%s,%s,%s) RETURNING strategy_trade_id",
        (intent_id, _INSTRUMENT_ID, status),
    ).fetchone()
    assert trade is not None
    trade_id = int(trade[0])
    if link_order:
        order = conn.execute(
            """
            INSERT INTO orders (instrument_id, action, order_type, requested_amount, status, execution_origin)
            VALUES (%s,'BUY','MARKET',50,'submitted','strategy') RETURNING order_id
            """,
            (_INSTRUMENT_ID,),
        ).fetchone()
        assert order is not None
        order_id = int(order[0])
        conn.execute(
            "INSERT INTO strategy_trade_orders (strategy_trade_id,order_id,purpose) VALUES (%s,%s,'entry')",
            (trade_id, order_id),
        )
        if reconciliation_state is not None:
            conn.execute(
                "INSERT INTO strategy_order_reconciliation_state (order_id,state,reconciled_at) VALUES (%s,%s,%s)",
                (
                    order_id,
                    reconciliation_state,
                    _PAST if reconciliation_state in ("resolved", "rejected") else None,
                ),
            )
    return trade_id


def _admit(conn: psycopg.Connection[Any], intent_id: int, account: tuple[UUID, UUID, UUID]) -> Any:
    with core_submission_lock(conn):
        return admit_core_rebalance_intent(
            conn,
            intent_id=intent_id,
            operator_id=account[0],
            provider="etoro",
            environment="demo",
        )


# --------------------------------------------------------------------------
# The happy path, so every refusal below is a difference of one thing.
# --------------------------------------------------------------------------


def test_a_current_actionable_intent_with_a_live_proof_is_admitted(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert verdict.admitted is True
    assert verdict.reason_code is None
    assert verdict.action == "buy_core"
    assert verdict.amount == Decimal("50.000000")
    assert verdict.eligibility_proof_id is not None


def test_a_sell_is_admitted_when_partial_close_is_proved(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account, allow_partial_close=True)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn), action="sell_core")
    assert _admit(ebull_test_conn, intent_id, account).admitted is True


# --------------------------------------------------------------------------
# The lock is a control, not a docstring.
# --------------------------------------------------------------------------


def test_admission_without_the_lock_raises_rather_than_refusing(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """An unserialised caller is a BUG, and a returned refusal invites logging it.

    The UNIQUE index refuses the second INSERT only after both callers have
    reached the broker, which is the leg that costs money — so the serialisation
    has to be proved, not assumed.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    # Fixture setup acquires transaction-scoped credential and mandate locks;
    # release them so this assertion really begins outside the critical section.
    ebull_test_conn.commit()
    with pytest.raises(StrategyCoreSubmissionError, match="core_submission_lock"):
        admit_core_rebalance_intent(
            ebull_test_conn,
            intent_id=intent_id,
            operator_id=account[0],
            provider="etoro",
            environment="demo",
        )


def test_holding_only_the_submission_lock_still_raises(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Both keys are required: the mandate lock is what closes the revision TOCTOU.

    Without it a mandate revision can be appended between the staleness check and
    the trade INSERT, leaving a trade citing a mandate the operator has replaced.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    for key in (PAPER_ALLOCATOR_ADVISORY_LOCK, CORE_SUBMISSION_ADVISORY_LOCK):
        ebull_test_conn.execute("SELECT pg_advisory_lock(%s, %s)", key)
    ebull_test_conn.commit()
    try:
        with pytest.raises(StrategyCoreSubmissionError, match="mandate advisory lock"):
            admit_core_rebalance_intent(
                ebull_test_conn,
                intent_id=intent_id,
                operator_id=account[0],
                provider="etoro",
                environment="demo",
            )
    finally:
        for key in (CORE_SUBMISSION_ADVISORY_LOCK, PAPER_ALLOCATOR_ADVISORY_LOCK):
            ebull_test_conn.execute("SELECT pg_advisory_unlock(%s, %s)", key)
        ebull_test_conn.commit()


# --------------------------------------------------------------------------
# The refusals, in precedence order.
# --------------------------------------------------------------------------


def test_a_missing_intent_is_refused(ebull_test_conn: psycopg.Connection[Any]) -> None:
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    verdict = _admit(ebull_test_conn, 9_999_999, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_intent_missing")


@pytest.mark.parametrize("action", ["hold", "refused"])
def test_a_hold_or_refusal_cannot_authorise_a_submission(ebull_test_conn: psycopg.Connection[Any], action: str) -> None:
    """``sql/348`` stores every verdict as evidence, so the FK alone is not authority."""
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn), action=action)
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_intent_not_actionable")


def test_a_later_evaluation_supersedes_an_earlier_verdict(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The freshness rule, and it is supersession rather than an age threshold.

    ⚠ A later ``hold`` supersedes an earlier ``buy_core``: the allocator is a pure
    function of mandate and observed state, so a later row is a later observation
    of the same sleeve. Acting on the earlier verdict means acting on a world we
    have since re-measured.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    event_id = _seed_mandate(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=event_id)
    assert _admit(ebull_test_conn, intent_id, account).admitted is True
    newer_id = _seed_intent(ebull_test_conn, event_id=event_id, action="hold")
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_intent_superseded")
    assert str(newer_id) in (verdict.detail or "")


def test_a_verdict_computed_under_a_replaced_mandate_is_refused(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn, revision=1))
    _seed_mandate(ebull_test_conn, revision=2)
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_mandate_revision_stale")


def test_a_mandate_disabled_after_the_evaluation_is_refused(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠ Reachable only because the revision check proved the mandate current.

    The allocator's own ``core_mandate_disabled`` belongs to a revision this
    intent predates, so a mandate disabled AFTER the evaluation passes every other
    check here. Reading the row and not testing its flag would be the defect this
    ticket keeps finding.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    event_id = _seed_mandate(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=event_id)
    ebull_test_conn.execute(
        "UPDATE strategy_core_mandate_events SET enabled=FALSE WHERE core_mandate_event_id=%s",
        (event_id,),
    )
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_mandate_disabled")


def test_an_intent_that_already_has_a_trade_is_refused_before_any_broker_leg(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The UNIQUE index bounds STORAGE; this bounds the leg that costs money.

    ``detail`` carries the existing trade's status, because an uncertain prior
    submission needs reconciliation rather than a retry.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    _seed_blocking_trade(
        ebull_test_conn, intent_id=intent_id, status="reconcile_required", reconciliation_state="resolved"
    )
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_intent_already_submitted")
    assert "reconcile_required" in (verdict.detail or "")


@pytest.mark.parametrize("state", ["unresolved", "pending", "not_found", "ambiguous", "error"])
def test_an_unreconciled_core_order_blocks_a_second_submission(
    ebull_test_conn: psycopg.Connection[Any], state: str
) -> None:
    """The in-flight rule, sourced from ``sql/285``'s own terminal set.

    The hazard is named in ``strategy_core_rebalance_intent``: the allocator is
    stateless and re-recommends a trade already in flight, because an unfilled buy
    is not yet in the broker's position snapshot.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    event_id = _seed_mandate(ebull_test_conn)
    blocker_intent = _seed_intent(ebull_test_conn, event_id=event_id)
    _seed_blocking_trade(ebull_test_conn, intent_id=blocker_intent, reconciliation_state=state)
    intent_id = _seed_intent(ebull_test_conn, event_id=event_id)
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_trade_in_flight")
    assert state in (verdict.detail or "")


@pytest.mark.parametrize("state", ["resolved", "rejected"])
def test_a_reconciled_core_order_does_not_block(ebull_test_conn: psycopg.Connection[Any], state: str) -> None:
    """⚠ The other half, and the one an over-tight rule would break silently.

    An open core position is the mandate's STEADY STATE. A rule that blocked on
    "an open core trade exists" would admit the first rebalance and refuse every
    later one, for ever, while looking like a working control.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    event_id = _seed_mandate(ebull_test_conn)
    held_intent = _seed_intent(ebull_test_conn, event_id=event_id)
    _seed_blocking_trade(ebull_test_conn, intent_id=held_intent, status="open", reconciliation_state=state)
    intent_id = _seed_intent(ebull_test_conn, event_id=event_id)
    assert _admit(ebull_test_conn, intent_id, account).admitted is True


def test_a_core_trade_with_no_linked_order_blocks(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The belt: unreachable through the executor, reachable through a fixture.

    ``sql/349``'s own rule — the invariant changes at the migration, not at the
    writer — and this repo's tests insert trade rows directly.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    event_id = _seed_mandate(ebull_test_conn)
    stranded = _seed_intent(ebull_test_conn, event_id=event_id)
    _seed_blocking_trade(ebull_test_conn, intent_id=stranded, status="planned", link_order=False)
    intent_id = _seed_intent(ebull_test_conn, event_id=event_id)
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_trade_in_flight")
    assert "no_order" in (verdict.detail or "")


def test_a_linked_order_with_no_reconciliation_row_blocks(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """⚠⚠ The fail-OPEN a first draft shipped, and the one that costs money.

    Nothing in the schema requires a linked order to have a
    ``strategy_order_reconciliation_state`` row. A predicate that blocks only on
    a NON-TERMINAL state therefore admits a second rebalance while the first
    order's broker effect is entirely unknown — neither blocking by state (it is
    NULL) nor by absence (the order exists). Missing must block exactly as
    non-terminal does.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    event_id = _seed_mandate(ebull_test_conn)
    unknown = _seed_intent(ebull_test_conn, event_id=event_id)
    _seed_blocking_trade(ebull_test_conn, intent_id=unknown, status="submitted", reconciliation_state=None)
    intent_id = _seed_intent(ebull_test_conn, event_id=event_id)
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_trade_in_flight")
    assert "no_reconciliation_row" in (verdict.detail or "")


def test_a_terminal_core_trade_never_blocks_even_if_its_order_is_unreconciled(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Otherwise one stranded reconciliation row denies every later rebalance.

    ``closed`` and ``failed`` have nothing outstanding on either arm, so the
    blocking population is bounded by the trade's own status first.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account)
    event_id = _seed_mandate(ebull_test_conn)
    dead = _seed_intent(ebull_test_conn, event_id=event_id)
    _seed_blocking_trade(ebull_test_conn, intent_id=dead, status="failed", reconciliation_state="unresolved")
    intent_id = _seed_intent(ebull_test_conn, event_id=event_id)
    assert _admit(ebull_test_conn, intent_id, account).admitted is True


def test_an_unproved_instrument_is_refused_and_the_cause_survives(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """One code covers four causes, so the message is carried rather than dropped."""
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn))
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_eligibility_unproved")
    assert "no etoro demo eligibility proof" in (verdict.detail or "")


@pytest.mark.parametrize("allow_partial_close", [False, None])
def test_a_sell_without_a_partial_close_capability_is_refused(
    ebull_test_conn: psycopg.Connection[Any], allow_partial_close: bool | None
) -> None:
    """⚠ A passing verdict proves ``allow_open_position`` — a different capability.

    A rebalance sell can never be a full close: ``validate_core_mandate`` requires
    ``core_target_pct - rebalance_band_pct > 0`` and the allocator sells only down
    to the lower band edge, so post-trade core value is strictly positive.

    ``None`` is parameterised alongside ``False`` because the column is nullable
    and means "the response did not say" — a truthiness test would read the same,
    a ``not`` on a future inversion would not.
    """
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account, allow_partial_close=allow_partial_close)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn), action="sell_core")
    verdict = _admit(ebull_test_conn, intent_id, account)
    assert (verdict.admitted, verdict.reason_code) == (False, "core_partial_close_unproved")


def test_a_buy_is_unaffected_by_the_partial_close_capability(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The direction split is real: a buy needs open authority, which the verdict carries."""
    _seed_instrument(ebull_test_conn)
    account = _seed_account(ebull_test_conn)
    _seed_proof(ebull_test_conn, account, allow_partial_close=False)
    intent_id = _seed_intent(ebull_test_conn, event_id=_seed_mandate(ebull_test_conn), action="buy_core")
    assert _admit(ebull_test_conn, intent_id, account).admitted is True
