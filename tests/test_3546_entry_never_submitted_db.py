"""#3546 slice 3: an alpha-arm entry the broker provably never received is released.

The write-ordering marker (``authority_committed`` → ``broker_verb_entered``) and the
paper allocator key are the whole evidence; no test here lets the release call the broker.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import psycopg
import pytest

from app.providers.broker import BrokerOrderNotFound, BrokerOrderSubmission, BrokerProvider
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_control_plane import PAPER_ALLOCATOR_ADVISORY_LOCK
from app.services.strategy_core_submission_gate import CORE_SUBMISSION_ADVISORY_LOCK
from app.services.strategy_monitoring import load_attribution
from app.services.strategy_order_reconciliation import (
    ENTRY_NEVER_SUBMITTED_CODE,
    StrategyReconciliationBusy,
    enforce_reconciliation_slo,
    ensure_strategy_request_id,
    reconcile_backlog,
    reconcile_strategy_order,
)
from app.services.strategy_paper_executor import (
    StrategyPaperExecutionError,
    _allocator_lock,
    _existing_result,
    _submit_recorded_order,
    mark_entry_verb_entered,
)
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_strategy_order_reconciliation import _seed_trade

pytestmark = pytest.mark.integration


@pytest.fixture
def holder(ebull_test_conn: psycopg.Connection[Any]) -> Any:
    """A second real connection, to hold a key against the subject (skips with the test DB)."""
    conn = psycopg.connect(test_database_url())
    try:
        yield conn
    finally:
        conn.close()


def _authority(conn: psycopg.Connection[Any]) -> tuple[int, int, UUID]:
    """The state a crash between the authority commit and the marker leaves behind."""
    trade_id, order_id = _seed_trade(conn)
    request_id = ensure_strategy_request_id(conn, order_id=order_id)
    conn.commit()
    return trade_id, order_id, request_id


def _states(conn: psycopg.Connection[Any], trade_id: int, order_id: int) -> tuple[Any, ...]:
    row = conn.execute(
        """
        SELECT o.status, s.state, s.last_error_code, s.submission_phase, t.status
        FROM orders o
        JOIN strategy_order_reconciliation_state s ON s.order_id=o.order_id
        JOIN strategy_trades t ON t.strategy_trade_id=%s
        WHERE o.order_id=%s
        """,
        (trade_id, order_id),
    ).fetchone()
    conn.commit()
    assert row is not None
    return tuple(row)


def _silent_broker() -> MagicMock:
    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.side_effect = BrokerOrderNotFound("404")
    return broker


def test_a_crash_before_the_marker_is_released_by_the_backlog_with_no_broker_call(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    conn = ebull_test_conn
    trade_id, order_id, _ = _authority(conn)
    assert _states(conn, trade_id, order_id)[3] == "authority_committed"
    broker = _silent_broker()

    results = reconcile_backlog(conn, broker=broker)

    assert [(r.order_id, r.state, r.error_code) for r in results] == [
        (order_id, "rejected", ENTRY_NEVER_SUBMITTED_CODE)
    ]
    assert _states(conn, trade_id, order_id) == (
        "rejected",
        "rejected",
        ENTRY_NEVER_SUBMITTED_CODE,
        "authority_committed",
        "failed",
    )
    assert broker.method_calls == []
    # Terminal: a second pass neither selects nor rewrites it.
    assert reconcile_backlog(conn, broker=broker) == ()


def test_the_release_skips_while_another_session_holds_the_allocator_key(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    conn = ebull_test_conn
    trade_id, order_id, _ = _authority(conn)
    holder.execute("SELECT pg_advisory_lock(%s, %s)", PAPER_ALLOCATOR_ADVISORY_LOCK)
    holder.commit()
    broker = _silent_broker()

    with pytest.raises(StrategyReconciliationBusy):
        reconcile_strategy_order(conn, broker=broker, order_id=order_id)
    assert reconcile_backlog(conn, broker=broker) == ()
    assert _states(conn, trade_id, order_id)[1:] == ("unresolved", None, "authority_committed", "planned")
    assert broker.method_calls == []

    holder.execute("SELECT pg_advisory_unlock(%s, %s)", PAPER_ALLOCATOR_ADVISORY_LOCK)
    holder.commit()
    assert reconcile_strategy_order(conn, broker=broker, order_id=order_id).state == "rejected"


def test_an_unrelated_core_submission_does_not_hold_up_an_alpha_release(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    """Codex ckpt-1 #1: the core precheck is arm-scoped, so the core key is never consulted."""
    conn = ebull_test_conn
    trade_id, order_id, _ = _authority(conn)
    holder.execute("SELECT pg_advisory_lock(%s, %s)", CORE_SUBMISSION_ADVISORY_LOCK)
    holder.commit()

    result = reconcile_strategy_order(conn, broker=_silent_broker(), order_id=order_id)

    assert (result.state, result.error_code) == ("rejected", ENTRY_NEVER_SUBMITTED_CODE)


def test_a_session_holding_the_allocator_key_itself_may_release(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    """The reentrancy exemption: the one same-session caller only looks orders up."""
    conn = ebull_test_conn
    trade_id, order_id, _ = _authority(conn)
    with _allocator_lock(conn):
        result = reconcile_strategy_order(conn, broker=_silent_broker(), order_id=order_id)
    assert result.state == "rejected"


@pytest.mark.parametrize("phase", ["broker_verb_entered", None])
def test_an_entered_or_unmarked_order_goes_to_the_ordinary_lookup(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
    phase: str | None,
) -> None:
    conn = ebull_test_conn
    trade_id, order_id, request_id = _authority(conn)
    conn.execute(
        "UPDATE strategy_order_reconciliation_state SET submission_phase=%s WHERE order_id=%s",
        (phase, order_id),
    )
    conn.commit()
    broker = _silent_broker()

    result = reconcile_strategy_order(conn, broker=broker, order_id=order_id)

    assert result.state == "not_found"
    assert broker.lookup_order.call_args.kwargs == {"reference_id": str(request_id)}
    # Contained by the ordinary path, never released: a miss proves nothing (#2961).
    assert _states(conn, trade_id, order_id)[4] == "reconcile_required"


def test_a_pre_existing_identity_is_never_certified_unsubmitted(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    """Codex ckpt-1 #7: only a freshly minted UUID gets the marker."""
    conn = ebull_test_conn
    trade_id, order_id = _seed_trade(conn)
    conn.execute(
        "UPDATE orders SET strategy_request_id=%s WHERE order_id=%s",
        (UUID("00000000-0000-4000-8000-000000003546"), order_id),
    )
    ensure_strategy_request_id(conn, order_id=order_id)
    conn.commit()
    assert _states(conn, trade_id, order_id)[3] is None


def test_the_release_clears_the_entry_block_it_caused(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    conn = ebull_test_conn
    trade_id, order_id, _ = _authority(conn)
    conn.execute(
        "UPDATE strategy_order_reconciliation_state SET first_unresolved_at=now() - interval '1 hour' "
        "WHERE order_id=%s",
        (order_id,),
    )
    assert enforce_reconciliation_slo(conn, max_unresolved_seconds=60).active_block
    conn.commit()

    reconcile_backlog(conn, broker=_silent_broker())

    assert not enforce_reconciliation_slo(conn, max_unresolved_seconds=60).active_block
    conn.commit()


def test_the_marker_refuses_outside_the_allocator_and_after_a_release(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    conn = ebull_test_conn
    trade_id, order_id, _ = _authority(conn)
    with pytest.raises(StrategyPaperExecutionError, match="allocator lock"):
        mark_entry_verb_entered(conn, order_id=order_id)
    assert _states(conn, trade_id, order_id)[3] == "authority_committed"

    reconcile_strategy_order(conn, broker=_silent_broker(), order_id=order_id)
    with _allocator_lock(conn):
        # A released authority must never then be sent.
        with pytest.raises(StrategyPaperExecutionError, match="no longer a pre-broker authority"):
            mark_entry_verb_entered(conn, order_id=order_id)


def test_the_marker_is_committed_before_the_provider_call(
    ebull_test_conn: psycopg.Connection[Any],
    holder: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    conn = ebull_test_conn
    trade_id, order_id, request_id = _authority(conn)
    seen: list[Any] = []

    def place(*_args: Any, **_kwargs: Any) -> BrokerOrderSubmission:
        row = holder.execute(
            "SELECT submission_phase FROM strategy_order_reconciliation_state WHERE order_id=%s",
            (order_id,),
        ).fetchone()
        holder.commit()
        seen.append(row[0] if row else None)
        # Mid-call, the release must find nothing to release.
        seen.append(reconcile_backlog(holder, broker=_silent_broker()))
        return BrokerOrderSubmission(broker_order_ref="3546001", reference_id=request_id, token=request_id)

    broker = MagicMock(spec=BrokerProvider)
    broker.place_demo_strategy_order.side_effect = place
    with _allocator_lock(conn):
        result = _submit_recorded_order(
            conn,
            broker=broker,
            signal_id=0,
            trade_id=trade_id,
            order_id=order_id,
            request_id=request_id,
            instrument_id=2451001,
            amount=Decimal("100"),
            stop_rate=Decimal("90"),
            take_rate=Decimal("120"),
        )

    assert result.verdict == "submitted"
    assert seen[0] == "broker_verb_entered"
    assert all(r.error_code != ENTRY_NEVER_SUBMITTED_CODE for r in seen[1])


def test_a_released_entry_is_reported_as_a_local_refusal_never_a_broker_rejection(
    ebull_test_conn: psycopg.Connection[Any],
    registered_strategy_test_candidates: None,
) -> None:
    """Codex ckpt-2: `orders.status='rejected'` alone used to mean "the broker refused"."""
    conn = ebull_test_conn
    trade_id, order_id, _ = _authority(conn)
    reconcile_backlog(conn, broker=_silent_broker())
    signal = conn.execute(
        """
        SELECT d.signal_id, s.strategy_id, s.strategy_version
        FROM strategy_trades t
        JOIN strategy_funding_decisions d ON d.funding_decision_id=t.funding_decision_id
        JOIN strategy_signals s ON s.signal_id=d.signal_id
        WHERE t.strategy_trade_id=%s
        """,
        (trade_id,),
    ).fetchone()
    conn.commit()
    assert signal is not None
    signal_id, strategy_id, version = int(signal[0]), str(signal[1]), str(signal[2])

    existing = _existing_result(conn, signal_id)
    assert existing is not None
    assert (existing.verdict, existing.reason_code) == ("rejected", ENTRY_NEVER_SUBMITTED_CODE)

    def broker_rejected() -> int:
        attribution = load_attribution(
            conn,
            versions=[version],
            outcome_version=OUTCOME_RULE_SET_VERSION,
            input_version=QUARANTINE_RULE_SET_VERSION,
        )
        conn.commit()
        return attribution[(strategy_id, version)].broker_rejected_entries

    assert broker_rejected() == 0
    # Control: the same row with a broker's rejection code IS counted.
    conn.execute(
        "UPDATE strategy_order_reconciliation_state SET last_error_code='broker_submission_rejected' WHERE order_id=%s",
        (order_id,),
    )
    conn.commit()
    assert broker_rejected() == 1
