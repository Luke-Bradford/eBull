"""#2942 half 2 slice B — the pending recommendation-order poller, against real rows.

What needs a database here is not the arithmetic, it is the CLAIM. The only
interesting question the poller asks is whether
``idx_orders_recommendation_open_attempt`` actually lifts for a terminalised row
and actually holds for every other verdict, and a mocked cursor can prove the
UPDATE text contains ``'rejected'`` but not that a partial index changed its
mind. The rotation key has the same property: #2948's absorbing state is a
property of an ``ORDER BY`` over stored rows, not of any Python.

The verdict mapping itself is pure and is asserted in ``tests/test_order_client.py``.

Own module because the ``db`` marker is module-scoped: one DB test inside
``tests/test_order_client.py`` would evict that whole file from the
``-m "not db"`` push gate.
"""

from __future__ import annotations

import contextlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest
from psycopg.pq import TransactionStatus

from app.providers.broker import (
    BrokerCloseOrderDetail,
    BrokerOrderDetail,
    BrokerOrderLookupError,
    BrokerOrderNotFound,
    BrokerPositionExecution,
    BrokerPositionMutationError,
    BrokerPositionMutationUncertain,
    BrokerProvider,
    OrderStatus,
)
from app.services.order_client import (
    RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS,
    _persist_submitted_intent,
    count_pending_recommendation_orders,
    reconcile_pending_recommendation_orders,
)
from app.workers.scheduler import _has_pending_recommendation_orders
from tests.fixtures.ebull_test_db import test_database_url

INSTRUMENT_ID = 991_942
_NOW = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)
_REF = "13902598"


_ENV = "demo"


def _reconcile(conn: psycopg.Connection[Any], **kwargs: Any) -> tuple[Any, ...]:
    """``reconcile_pending_recommendation_orders`` with this suite's environment.

    ``env`` is REQUIRED on the service (#3189 finding 4b) — a poller that
    guessed which broker environment it was talking to is the defect the
    parameter exists to remove. Every test here seeds rows for one environment,
    so defaulting it once keeps each test's own subject visible; the tests that
    are ABOUT the environment pass it explicitly.
    """
    kwargs.setdefault("env", _ENV)
    return reconcile_pending_recommendation_orders(conn, **kwargs)


def _detail(broker_status: str, *, ref: str = _REF) -> BrokerOrderDetail:
    return BrokerOrderDetail(
        broker_order_ref=ref,
        reference_id=None,
        status="filled" if broker_status in ("Filled", "Executed") else "pending",
        broker_status=broker_status,
        instrument_id=INSTRUMENT_ID,
        position_executions=(),
        last_update=_NOW,
        raw_payload={"orderID": int(ref), "status": {"name": broker_status}},
    )


def _broker(*, detail: BrokerOrderDetail | None = None, error: Exception | None = None) -> MagicMock:
    broker = MagicMock(spec=BrokerProvider)
    if error is not None:
        broker.lookup_order.side_effect = error
    else:
        broker.lookup_order.return_value = detail
    # #3007 half 2: the open-order path must never reach the close-order route.
    # A bare MagicMock would answer it happily and the routing assertion below
    # would pass for an order that was resolved twice.
    broker.get_close_order.side_effect = AssertionError("an open order must not use the close-order route")
    return broker


def _close_detail(
    status: OrderStatus,
    *,
    ref: str = _REF,
    position_ids: tuple[int, ...] = (),
    broker_status: str = "3",
    instrument_id: int | None = INSTRUMENT_ID,
) -> BrokerCloseOrderDetail:
    """One close-order lookup answer, shaped as ``get_close_order`` returns it.

    ⚠ ``broker_status`` defaults to the NUMERIC string the live demo broker
    actually returned on 2026-09-22 (#3007). It is opaque evidence, not a code
    we map: ``status`` is derived from the response SHAPE, which is why the two
    are separate arguments here rather than one being computed from the other.
    """
    return BrokerCloseOrderDetail(
        broker_order_ref=ref,
        status=status,
        broker_status=broker_status,
        position_ids=position_ids,
        reference_id=None,
        raw_payload={
            "orderID": int(ref),
            "statusID": int(broker_status),
            "instrumentID": instrument_id,
            "positions": list(position_ids),
        },
        instrument_id=instrument_id,
    )


def _close_broker(*, detail: BrokerCloseOrderDetail | None = None, error: Exception | None = None) -> MagicMock:
    broker = MagicMock(spec=BrokerProvider)
    if error is not None:
        broker.get_close_order.side_effect = error
    else:
        broker.get_close_order.return_value = detail
    broker.lookup_order.side_effect = AssertionError("an EXIT must not use the v2 order-lookup route")
    return broker


def _seed_instrument(conn: psycopg.Connection[Any]) -> None:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'POLL.2942','Pending Poller Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )


def _seed_recommendation(conn: psycopg.Connection[Any], *, status: str = "execution_pending") -> int:
    row = conn.execute(
        "INSERT INTO trade_recommendations (instrument_id, action, rationale, status) "
        "VALUES (%s,'BUY','poller test',%s) RETURNING recommendation_id",
        (INSTRUMENT_ID, status),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _seed_order(
    conn: psycopg.Connection[Any],
    *,
    recommendation_id: int | None,
    status: str = "pending",
    ref: str | None = _REF,
    origin: str = "manual",
    last_polled_at: datetime | None = None,
    broker_env: str | None = _ENV,
    action: str = "BUY",
) -> int:
    """Seed one order row.

    ``broker_env`` defaults to this suite's environment because that is what a
    real submitted order now carries (#3189 finding 4b) — the poller refuses a
    NULL rather than assuming it is ours, so a NULL default would make every
    test here a test of the environment gate. The tests that ARE about the gate
    pass it explicitly.
    """
    row = conn.execute(
        """
        INSERT INTO orders
            (instrument_id, recommendation_id, action, order_type, status,
             broker_order_ref, raw_payload_json, created_at, execution_origin,
             recommendation_request_id, recommendation_submission_phase,
             recommendation_last_polled_at, broker_environment)
        VALUES
            (%(iid)s, %(rid)s, %(action)s, 'market', %(status)s,
             %(ref)s, '{}'::jsonb, %(now)s, %(origin)s,
             %(req)s, 'broker_verb_entered', %(polled)s, %(benv)s)
        RETURNING order_id
        """,
        {
            "iid": INSTRUMENT_ID,
            "rid": recommendation_id,
            "action": action,
            "status": status,
            "ref": ref,
            "now": _NOW,
            "origin": origin,
            # The request-id CHECK admits it only alongside a recommendation.
            "req": uuid4() if recommendation_id is not None else None,
            "polled": last_polled_at,
            "benv": broker_env,
        },
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def _seed_decision(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(
        "INSERT INTO decision_audit (decision_time, instrument_id, stage, pass_fail, explanation) "
        "VALUES (%s,%s,'execution','PASS','poller test') RETURNING decision_id",
        (_NOW, INSTRUMENT_ID),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _order_row(conn: psycopg.Connection[Any], order_id: int) -> dict[str, Any]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT status, recommendation_last_polled_at FROM orders WHERE order_id=%s",
            (order_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row


def _parked_reason(conn: psycopg.Connection[Any], order_id: int) -> str | None:
    row = conn.execute("SELECT recommendation_poll_parked_reason FROM orders WHERE order_id=%s", (order_id,)).fetchone()
    assert row is not None
    return None if row[0] is None else str(row[0])


def _rec_status(conn: psycopg.Connection[Any], recommendation_id: int) -> str:
    row = conn.execute(
        "SELECT status FROM trade_recommendations WHERE recommendation_id=%s", (recommendation_id,)
    ).fetchone()
    assert row is not None
    return str(row[0])


def _audit_count(conn: psycopg.Connection[Any], recommendation_id: int) -> int:
    row = conn.execute(
        "SELECT count(*) FROM decision_audit WHERE recommendation_id=%s", (recommendation_id,)
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_a_broker_rejection_terminalises_and_lifts_the_claim(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The one verdict that releases a claim, and the reason the slice exists:
    without it this recommendation is unsubmittable for ever on an order the
    broker has already thrown away.

    The claim is asserted by a second INSERT, not by the status string — the
    partial index is what refuses, and only it can say the claim lifted."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    results = _reconcile(ebull_test_conn, broker=_broker(detail=_detail("Rejected")), now=_NOW)

    assert [r.verdict for r in results] == ["terminalised_rejected"]
    assert _order_row(ebull_test_conn, order_id)["status"] == "rejected"
    assert _rec_status(ebull_test_conn, rec) == "execution_failed"
    assert _audit_count(ebull_test_conn, rec) == 1
    # The claim really lifted: this INSERT raises UniqueViolation while held.
    second = _seed_order(ebull_test_conn, recommendation_id=rec)
    assert _order_row(ebull_test_conn, second)["status"] == "pending"


def test_a_rejected_status_carrying_executions_keeps_the_claim(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#3189 finding 3 — the contradiction must not release the claim.

    ``classify_broker_order_status`` reads the status word only. A detail that
    says "Rejected" while naming positions the broker actually opened is the
    shape ``strategy_order_reconciliation`` already refuses outright, against
    the same eToro contract. Here it mattered more, because terminalising is the
    ONE verdict that lifts the submission claim — so acting on the word would
    leave a partial execution unbooked and make a second economic order for the
    same recommendation possible.

    The claim is asserted the way this module always asserts it: by a second
    INSERT, which the partial index refuses while the claim is held.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    detail = _detail("Rejected")
    contradictory = BrokerOrderDetail(
        broker_order_ref=detail.broker_order_ref,
        reference_id=detail.reference_id,
        status=detail.status,
        broker_status=detail.broker_status,
        instrument_id=detail.instrument_id,
        position_executions=(
            BrokerPositionExecution(
                position_id=778_001,
                state="Open",
                remaining_units=Decimal("1"),
                opening_units=Decimal("1"),
                average_price=Decimal("10"),
                execution_time=_NOW,
                fees=Decimal("0"),
                raw_payload={},
            ),
        ),
        last_update=detail.last_update,
        raw_payload=detail.raw_payload,
    )

    results = _reconcile(ebull_test_conn, broker=_broker(detail=contradictory), now=_NOW)

    assert [(r.verdict, r.broker_status) for r in results] == [("filled_not_booked", "Rejected")]
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending", "the order was terminalised on the status word alone"
    assert row["recommendation_last_polled_at"] == _NOW
    assert _rec_status(ebull_test_conn, rec) == "execution_pending"
    # The observation is DURABLE, not just this attempt: parked, so a later
    # `Rejected` that omitted the executions can never reach terminalisation.
    assert _parked_reason(ebull_test_conn, order_id) == "filled_unbooked"
    assert _audit_count(ebull_test_conn, rec) == 1, "the contradiction left no auditable record"
    # The claim really is still held: this INSERT must hit the partial index.
    with pytest.raises(psycopg.errors.UniqueViolation):
        _seed_order(ebull_test_conn, recommendation_id=rec)
    ebull_test_conn.rollback()

    # And it is no longer selectable, so the contradiction cannot burn a broker
    # read every hour.
    assert _reconcile(ebull_test_conn, broker=_broker(detail=_detail("Rejected"))) == ()
    assert _order_row(ebull_test_conn, order_id)["status"] == "pending"


def test_an_order_settled_during_the_broker_round_trip_is_not_regressed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#3189 finding 5 — the terminalising write is compare-and-set.

    The row is re-read under the lock, but the broker round-trip after that runs
    OUTSIDE any transaction, and the advisory-lock namespace covers only the
    recommendation-submission writers. So the state the ``Rejected`` verdict was
    taken on is not necessarily the state being written, and this is the one
    verdict that releases a submission claim.

    The probe settles the order from a SECOND session while the lookup is in
    flight — the realistic shape, a writer outside the namespace resolving the
    row — and the poller must then write nothing at all rather than regress a
    settled order to ``rejected`` and demote its recommendation.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    def _settled_elsewhere_mid_lookup(*_args: Any, **_kwargs: Any) -> BrokerOrderDetail:
        with psycopg.connect(test_database_url()) as other:
            other.execute("UPDATE orders SET status='filled' WHERE order_id=%s", (order_id,))
            other.commit()
        return _detail("Rejected")

    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.side_effect = _settled_elsewhere_mid_lookup

    results = _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert [(r.verdict, r.broker_status) for r in results] == [("no_longer_pending", "Rejected")]
    assert _order_row(ebull_test_conn, order_id)["status"] == "filled", (
        "a settled order was regressed to rejected by a decision taken before it settled"
    )
    assert _rec_status(ebull_test_conn, rec) == "execution_pending"
    assert _audit_count(ebull_test_conn, rec) == 0, "nothing happened, so nothing may be audited as having happened"


def test_a_recommendation_that_moved_blocks_the_claim_release(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#3189 finding 5, the other half — half a claim release is worse than none.

    Here the ORDER is still the pending row that was looked up, so its
    compare-and-set holds, but the recommendation has moved off
    ``execution_pending`` under us. Releasing the claim on an order whose
    recommendation says something else is a state nothing downstream can read,
    so the write refuses: both UPDATEs are one implicit transaction until the
    audit row commits, the lock context manager's ``finally`` rolls them back,
    and #3189 finding 7's per-row containment reports ``poll_error`` — which
    degrades the run — instead of wedging the batch.

    The claim is asserted the way this module always asserts it: by a second
    INSERT, which the partial index refuses while the claim is held.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    def _recommendation_moved_mid_lookup(*_args: Any, **_kwargs: Any) -> BrokerOrderDetail:
        with psycopg.connect(test_database_url()) as other:
            other.execute(
                "UPDATE trade_recommendations SET status='executed' WHERE recommendation_id=%s",
                (rec,),
            )
            other.commit()
        return _detail("Rejected")

    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.side_effect = _recommendation_moved_mid_lookup

    results = _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert [r.verdict for r in results] == ["poll_error"]
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending", "the order half of the claim release was committed on its own"
    assert row["recommendation_last_polled_at"] == _NOW, "containment must still advance the rotation key"
    assert _rec_status(ebull_test_conn, rec) == "executed", "the poller overwrote a status it did not read"
    assert _audit_count(ebull_test_conn, rec) == 0
    # The claim really is still held: this INSERT must hit the partial index.
    with pytest.raises(psycopg.errors.UniqueViolation):
        _seed_order(ebull_test_conn, recommendation_id=rec)
    ebull_test_conn.rollback()


@pytest.mark.parametrize("broker_status", ["Rejected", "Failed", "Cancelled", "Canceled", "Expired"])
def test_every_documented_rejection_status_terminalises(
    ebull_test_conn: psycopg.Connection[tuple], broker_status: str
) -> None:
    """The whole rejected vocabulary, not the one word the fixture happened to
    use. These are the five statuses ``classify_broker_order_status`` calls
    rejected, and the poller must not silently act on a subset of them."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    _reconcile(ebull_test_conn, broker=_broker(detail=_detail(broker_status)), now=_NOW)

    assert _order_row(ebull_test_conn, order_id)["status"] == "rejected"


def test_a_still_pending_order_is_stamped_and_otherwise_untouched(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The ordinary tick. Nothing may move except the rotation key."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    results = _reconcile(ebull_test_conn, broker=_broker(detail=_detail("Pending")), now=_NOW)

    assert [r.verdict for r in results] == ["still_pending"]
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending"
    assert row["recommendation_last_polled_at"] == _NOW
    assert _rec_status(ebull_test_conn, rec) == "execution_pending"
    assert _audit_count(ebull_test_conn, rec) == 0


def test_a_filled_order_is_recorded_and_NOT_booked_and_keeps_the_claim(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The deliberate gap, asserted so it cannot be closed by accident.

    Booking a late fill needs the submission-time ``exit_lot`` and an attended
    observation to verify; until then the safe state is the one that cannot
    double-submit. Assert BOTH halves: no fill row appears, and the claim is
    still held."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    results = _reconcile(ebull_test_conn, broker=_broker(detail=_detail("Filled")), now=_NOW)

    assert [r.verdict for r in results] == ["filled_not_booked"]
    assert _order_row(ebull_test_conn, order_id)["status"] == "pending"
    assert _rec_status(ebull_test_conn, rec) == "execution_pending"
    # Recorded, loudly and durably, rather than silently dropped.
    assert _audit_count(ebull_test_conn, rec) == 1
    fills = ebull_test_conn.execute("SELECT count(*) FROM fills WHERE order_id=%s", (order_id,)).fetchone()
    assert fills is not None and int(fills[0]) == 0
    # The claim is still held — a second attempt must still be refused.
    with pytest.raises(psycopg.errors.UniqueViolation):
        _seed_order(ebull_test_conn, recommendation_id=rec)
    ebull_test_conn.rollback()


def test_an_unbooked_fill_is_parked_so_it_cannot_poll_for_ever(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR #3168's WARNING. The order is terminal at the broker, so re-asking
    hourly could only spend a shared eToro read and append another identical
    audit row for ever. One poll, one audit row, then silence — with the claim
    still held, which is what makes parking safe rather than a quiet release."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)
    broker = _broker(detail=_detail("Filled"))

    _reconcile(ebull_test_conn, broker=broker, now=_NOW)
    second_pass = _reconcile(ebull_test_conn, broker=broker, now=_NOW + timedelta(hours=1))

    assert second_pass == ()
    assert broker.lookup_order.call_count == 1
    assert _audit_count(ebull_test_conn, rec) == 1
    assert count_pending_recommendation_orders(ebull_test_conn) == 0
    ebull_test_conn.commit()
    # Parked is NOT resolved: the claim is still held.
    assert _order_row(ebull_test_conn, order_id)["status"] == "pending"
    with pytest.raises(psycopg.errors.UniqueViolation):
        _seed_order(ebull_test_conn, recommendation_id=rec)
    ebull_test_conn.rollback()


def test_a_non_pollable_ref_is_parked_too(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A ref we cannot call ``lookup_order`` with is a permanent property of the
    row. Left unparked it would burn a whole tick's selection slot for ever
    without even reaching the broker."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=rec, ref="v1-echo")

    broker = _broker(detail=_detail("Rejected"))
    _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert _reconcile(ebull_test_conn, broker=broker, now=_NOW) == ()


@pytest.mark.parametrize(
    "error",
    [BrokerOrderNotFound("no such order"), BrokerOrderLookupError("transport blew up")],
)
def test_a_transient_failure_is_NOT_parked(ebull_test_conn: psycopg.Connection[tuple], error: Exception) -> None:
    """The dangerous direction of the parking fix. A transport failure is not a
    fact about the order — parking on it would silently retire a live order from
    reconciliation, which is the same wedge this ticket is closing."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=rec)

    broker = _broker(error=error)
    _reconcile(ebull_test_conn, broker=broker, now=_NOW)
    second_pass = _reconcile(ebull_test_conn, broker=broker, now=_NOW + timedelta(hours=1))

    assert len(second_pass) == 1
    assert broker.lookup_order.call_count == 2


def test_an_unsettled_partial_fill_is_NOT_parked(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2965's statuses can still progress to Filled, so the poller must keep
    asking. Parking them would freeze the order at the one status nobody has
    settled how to handle."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=rec)

    broker = _broker(detail=_detail("PartiallyFilled"))
    _reconcile(ebull_test_conn, broker=broker, now=_NOW)
    second_pass = _reconcile(ebull_test_conn, broker=broker, now=_NOW + timedelta(hours=1))

    assert [r.verdict for r in second_pass] == ["unsafe_status"]


@pytest.mark.parametrize(
    "error",
    [BrokerOrderNotFound("no such order"), BrokerOrderLookupError("transport blew up")],
)
def test_a_failed_lookup_never_terminalises(ebull_test_conn: psycopg.Connection[tuple], error: Exception) -> None:
    """The direction that would reintroduce the bug. A 404 on an id the broker
    itself issued is unexplained, and an unexplained answer must leave the claim
    held — the alternative is releasing it on a live order."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    results = _reconcile(ebull_test_conn, broker=_broker(error=error), now=_NOW)

    assert results[0].verdict in ("not_found", "lookup_error")
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending"
    assert row["recommendation_last_polled_at"] == _NOW


def test_an_unsettled_partial_fill_status_never_advances_the_order(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2965's four statuses stay unrecognised on purpose. The poller must not
    become the place that quietly admits them."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    results = _reconcile(ebull_test_conn, broker=_broker(detail=_detail("PartiallyFilled")), now=_NOW)

    assert [r.verdict for r in results] == ["unsafe_status"]
    assert _order_row(ebull_test_conn, order_id)["status"] == "pending"


@pytest.mark.parametrize(
    ("field", "value"),
    [("broker_order_ref", "99999999"), ("instrument_id", INSTRUMENT_ID + 1)],
)
def test_a_response_about_another_order_advances_nothing(
    ebull_test_conn: psycopg.Connection[tuple], field: str, value: object
) -> None:
    """#3189 finding 4 — the response has to describe the order we asked about.

    Nothing in the classification path reads the identity of what came back, so
    a wrong or ambiguous broker answer would resolve whichever local row
    happened to be polled. Both halves are parametrised because they fail
    differently: a wrong ``instrument_id`` books against the wrong company, a
    wrong ``broker_order_ref`` resolves the wrong order for the right one.

    The verdict deliberately does NOT park — a statement about the answer is
    not a permanent property of the row, and a later correct response must
    still be readable. ``Rejected`` is the status used precisely because it is
    the one that would otherwise release the claim.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    base = _detail("Rejected")
    mismatched = replace(base, **{field: value})

    results = _reconcile(ebull_test_conn, broker=_broker(detail=mismatched), now=_NOW)

    assert [(r.verdict, r.broker_status) for r in results] == [("identity_mismatch", "Rejected")]
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending", "the poller acted on a status belonging to another order"
    assert row["recommendation_last_polled_at"] == _NOW, "an attempt path must still advance the rotation key"
    assert _rec_status(ebull_test_conn, rec) == "execution_pending"
    assert _parked_reason(ebull_test_conn, order_id) is None, "a wrong answer is not a fact about the row"
    assert _audit_count(ebull_test_conn, rec) == 0
    # The claim is still held: this INSERT must hit the partial index.
    with pytest.raises(psycopg.errors.UniqueViolation):
        _seed_order(ebull_test_conn, recommendation_id=rec)
    ebull_test_conn.rollback()


def test_an_order_from_another_broker_environment_is_never_looked_up(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#3189 finding 4b — broker order ids are namespaced per environment.

    This job is deliberately not demo-gated, so a demo-configured process can
    select a row submitted to ``real``. Looking that id up on the demo API does
    not merely fail: it can return a real, well-formed DEMO order carrying the
    same id, and the identity guard then PASSES because the id is the one we
    asked for. So the refusal has to happen before the lookup, from the row.

    ``Rejected`` is the detail deliberately: if the broker were reached, the
    claim would be released.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, broker_env="real")

    broker = _broker(detail=_detail("Rejected"))
    results = _reconcile(ebull_test_conn, broker=broker, env="demo", now=_NOW)

    assert [r.verdict for r in results] == ["environment_mismatch"]
    broker.lookup_order.assert_not_called()
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending"
    assert row["recommendation_last_polled_at"] == _NOW, "an attempt path must still advance the rotation key"
    assert _parked_reason(ebull_test_conn, order_id) is None, (
        "the deployment's environment is not a property of the row"
    )
    # The claim is still held: this INSERT must hit the partial index.
    with pytest.raises(psycopg.errors.UniqueViolation):
        _seed_order(ebull_test_conn, recommendation_id=rec)
    ebull_test_conn.rollback()


def test_a_matching_environment_is_polled(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """What the environment gate must NOT reject: its own environment."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, broker_env="demo")

    results = _reconcile(ebull_test_conn, broker=_broker(detail=_detail("Rejected")), env="demo", now=_NOW)

    assert [r.verdict for r in results] == ["terminalised_rejected"]
    assert _order_row(ebull_test_conn, order_id)["status"] == "rejected"


def test_an_unrecorded_environment_fails_closed(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A NULL environment is refused, not assumed to be ours (Codex ckpt-2, P1).

    The first draft polled NULL rows on the ground that refusing them would
    wedge outstanding orders. Measured instead of assumed: no current path
    writes a pollable NULL row (the synthetic-fill branch resolves to ``filled``
    or ``failed``, never ``pending``) and the dev corpus holds zero of them. So
    the only NULL pollable row is one left by another deployment — exactly the
    case where a colliding id would terminalise the wrong order — and it
    surfaces as a degraded run needing an operator.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, broker_env=None)

    broker = _broker(detail=_detail("Rejected"))
    results = _reconcile(ebull_test_conn, broker=broker, env="demo", now=_NOW)

    assert [r.verdict for r in results] == ["environment_mismatch"]
    broker.lookup_order.assert_not_called()
    assert _order_row(ebull_test_conn, order_id)["status"] == "pending"


def test_the_claim_insert_records_the_broker_environment(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The producing end, against the real column (#3189 finding 4b).

    Every other test here seeds `orders` directly, so none of them can tell
    whether the intent INSERT records the environment at all — the same blind
    spot `test_the_claim_insert_stamps_claim_committed_and_the_marker_moves_it`
    exists for in the marker suite. Without this, the gate above could pass
    while the writer stored NULL for every live order.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    decision_id = _seed_decision(ebull_test_conn)

    order_id, _request_id = _persist_submitted_intent(
        ebull_test_conn,
        instrument_id=INSTRUMENT_ID,
        recommendation_id=rec,
        decision_id=decision_id,
        action="BUY",
        requested_amount=None,
        requested_units=None,
        broker_env="real",
        order_params=None,
        exit_lot=None,
        now=_NOW,
    )
    ebull_test_conn.commit()

    stored = ebull_test_conn.execute("SELECT broker_environment FROM orders WHERE order_id=%s", (order_id,)).fetchone()
    assert stored is not None
    assert stored[0] == "real"


def test_a_non_canonical_numeric_ref_is_not_an_identity_mismatch(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """What the identity guard must NOT reject (#3189 finding 4, Codex ckpt-2).

    ``broker_order_ref`` is a TEXT column and the pollability check accepts any
    positive digit string, so a row carrying a leading zero is looked up as the
    integer and answered with the canonical string. A textual comparison would
    call that a mismatch — refusing a response the broker got exactly right,
    for ever, and degrading every run while it did.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, ref=f"000{_REF}")

    results = _reconcile(ebull_test_conn, broker=_broker(detail=_detail("Rejected")), now=_NOW)

    assert [r.verdict for r in results] == ["terminalised_rejected"]
    assert _order_row(ebull_test_conn, order_id)["status"] == "rejected"


def test_a_strategy_origin_order_is_never_selected(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The strategy arm has its own reconciler with its own tables and its own
    ownership claims. Two reconcilers on one order is the serialisation defect
    #2962 spent a ticket removing."""
    _seed_instrument(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=None, origin="strategy")

    broker = _broker(detail=_detail("Rejected"))
    results = _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert results == ()
    broker.lookup_order.assert_not_called()


@pytest.mark.parametrize("status", ["submitted", "uncertain", "filled", "refused"])
def test_only_pending_rows_are_selected(ebull_test_conn: psycopg.Connection[tuple], status: str) -> None:
    """``uncertain`` is the one that matters: it is inside the claim predicate,
    so it looks selectable — but it carries no ``broker_order_ref`` by
    construction and there is nothing to look it up by."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=rec, status=status)

    broker = _broker(detail=_detail("Rejected"))
    assert _reconcile(ebull_test_conn, broker=broker, now=_NOW) == ()
    broker.lookup_order.assert_not_called()


def test_a_pending_row_without_a_broker_ref_is_not_selected(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """There is no durable identity to poll by. Selecting it would spend a
    request to learn nothing, or worse, raise inside the loop."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=rec, ref=None)

    broker = _broker(detail=_detail("Rejected"))
    assert _reconcile(ebull_test_conn, broker=broker, now=_NOW) == ()
    assert count_pending_recommendation_orders(ebull_test_conn) == 0
    broker.lookup_order.assert_not_called()


def test_a_non_numeric_broker_ref_is_reported_not_raised(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``lookup_order`` raises ValueError on a non-positive-integer id. That is
    a permanent property of the row, so it is stamped and named rather than
    allowed to abort the batch."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, ref="v1-echo")

    broker = _broker(detail=_detail("Rejected"))
    results = _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert [r.verdict for r in results] == ["ref_not_pollable"]
    broker.lookup_order.assert_not_called()
    assert _order_row(ebull_test_conn, order_id)["recommendation_last_polled_at"] == _NOW


def test_an_order_whose_recommendation_key_is_held_elsewhere_is_skipped(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A live submitter owns this recommendation's span. The poller must not
    call the broker, and must not touch the claim — ``status`` and the
    recommendation stay exactly as the submitter left them.

    ⚠⚠ It MUST still advance the rotation key (#3189 finding 7). This test
    previously asserted ``recommendation_last_polled_at is None`` under a
    docstring reading *"must not write anything"*, and that rule was too broad:
    it collides with ``_stamp_polled``'s own invariant that the key moves on
    every attempt path, so contention became the absorbing state #2948 exists
    to prevent. The narrow rule is what is asserted now — nothing that is part
    of the SUBMISSION is written, and the rotation key, which the submitter
    neither reads nor owns, is.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    broker = _broker(detail=_detail("Rejected"))
    with psycopg.connect(test_database_url()) as holder:
        holder.execute("SELECT pg_advisory_lock(%s, %s)", (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, rec))
        holder.commit()
        results = _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert [r.verdict for r in results] == ["lock_busy"]
    broker.lookup_order.assert_not_called()
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending"
    assert _rec_status(ebull_test_conn, rec) == "execution_pending"
    assert row["recommendation_last_polled_at"] == _NOW


def test_contended_rows_do_not_occupy_the_bounded_head_for_ever(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2948's absorbing state, reached through CONTENTION instead of a stuck
    status (#3189 finding 7).

    The first order's recommendation key is held elsewhere, so it can only ever
    return ``lock_busy``. With ``limit=1`` and a rotation key that did not move
    on that path, the bounded window would select it again on every fire and
    the second order would never be visited at all.
    """
    _seed_instrument(ebull_test_conn)
    held_rec = _seed_recommendation(ebull_test_conn)
    free_rec = _seed_recommendation(ebull_test_conn)
    held = _seed_order(ebull_test_conn, recommendation_id=held_rec, last_polled_at=_NOW - timedelta(hours=2))
    free = _seed_order(ebull_test_conn, recommendation_id=free_rec, last_polled_at=_NOW - timedelta(hours=1))

    broker = _broker(detail=_detail("Pending"))
    with psycopg.connect(test_database_url()) as holder:
        holder.execute("SELECT pg_advisory_lock(%s, %s)", (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, held_rec))
        holder.commit()
        first_pass = _reconcile(ebull_test_conn, broker=broker, limit=1, now=_NOW)
        second_pass = _reconcile(ebull_test_conn, broker=broker, limit=1, now=_NOW + timedelta(minutes=1))

    assert [(r.order_id, r.verdict) for r in first_pass] == [(held, "lock_busy")]
    assert [r.order_id for r in second_pass] == [free], "the contended row starved the tail"


def test_the_rotation_key_never_moves_backward(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Overlapping pollers must not regress the key (#3189, Codex checkpoint 2).

    Two pollers overlap by design — a boot catch-up racing the scheduled fire —
    and each carries the ``now`` captured when its own batch began. So a run
    holding the lock since ``T1`` can finish and stamp *after* a later run
    stamped ``lock_busy`` at ``T2 > T1``. A plain assignment would move the key
    backward and re-postpone every row stamped between the two, undoing exactly
    the fairness the contention stamp was added to provide.

    Driven through the poller rather than through ``_stamp_polled`` directly, so
    it is the shipped call path that is pinned.
    """
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)
    broker = _broker(detail=_detail("Pending"))

    later = _NOW + timedelta(minutes=5)
    _reconcile(ebull_test_conn, broker=broker, now=later)
    assert _order_row(ebull_test_conn, order_id)["recommendation_last_polled_at"] == later
    # ⚠ `_order_row` opens a read transaction and does not close it, and the
    # poller refuses a non-idle connection. Other tests never hit this because
    # they read only after their last call.
    ebull_test_conn.rollback()

    # The straggler: a poller that began earlier and is only now stamping.
    _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert _order_row(ebull_test_conn, order_id)["recommendation_last_polled_at"] == later, (
        "a late stamp from an earlier batch moved the rotation key backward"
    )


def test_a_row_that_raises_unexpectedly_is_contained_and_the_batch_continues(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """One unpredicted raise must not abort the batch (#3189 finding 7).

    Without per-row containment the exception escapes before any later row is
    attempted, and because the raising row never stamps, the next fire selects
    the same head and raises again — the same absorbing state, reached through
    an exception. ``RuntimeError`` is deliberately outside the broker-error
    vocabulary the poller handles: the paths it predicted already stamp, so the
    only failures that reach this guard are the ones nobody predicted.
    """
    _seed_instrument(ebull_test_conn)
    first_rec = _seed_recommendation(ebull_test_conn)
    second_rec = _seed_recommendation(ebull_test_conn)
    boom = _seed_order(ebull_test_conn, recommendation_id=first_rec, last_polled_at=_NOW - timedelta(hours=2))
    later = _seed_order(ebull_test_conn, recommendation_id=second_rec, last_polled_at=_NOW - timedelta(hours=1))

    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.side_effect = [RuntimeError("something nobody predicted"), _detail("Pending")]

    results = _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert [(r.order_id, r.verdict) for r in results] == [(boom, "poll_error"), (later, "still_pending")]
    assert _order_row(ebull_test_conn, boom)["recommendation_last_polled_at"] == _NOW, (
        "an uncontained row keeps its rotation key and re-occupies the head"
    )
    assert _order_row(ebull_test_conn, boom)["status"] == "pending", "an unexplained failure must not move the claim"


def test_containment_survives_a_raise_that_left_a_failed_transaction(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Containment still stamps when the raise poisoned the transaction first.

    ⚠ This test exists because of what it FAILED to prove. It was written to
    make a defensive ``conn.rollback()`` in the containment block load-bearing,
    and it could not: a revert-probe deleting that rollback stayed green, because
    ``_recommendation_submission_try_lock``'s own ``finally`` has already rolled
    back by the time the exception reaches the batch loop. The rollback was
    removed rather than kept as unexercised insurance; what remains worth
    pinning is that a poisoned transaction does not defeat the stamp, whichever
    layer cleans it up.
    """
    _seed_instrument(ebull_test_conn)
    first_rec = _seed_recommendation(ebull_test_conn)
    second_rec = _seed_recommendation(ebull_test_conn)
    boom = _seed_order(ebull_test_conn, recommendation_id=first_rec, last_polled_at=_NOW - timedelta(hours=2))
    later = _seed_order(ebull_test_conn, recommendation_id=second_rec, last_polled_at=_NOW - timedelta(hours=1))

    calls: dict[str, Any] = {"n": 0, "poisoned": False}

    def _first_call_poisons_then_raises(*_args: Any, **_kwargs: Any) -> BrokerOrderDetail:
        calls["n"] += 1
        if calls["n"] > 1:
            return _detail("Pending")
        with contextlib.suppress(psycopg.Error):
            ebull_test_conn.execute("SELECT 1 / 0")
        # ⚠ RECORDED, not asserted here. An assertion inside the region under
        # containment is structurally unable to fail the test — the guard would
        # be caught and reported as `poll_error` like any other raise. Checked
        # after the call instead.
        calls["poisoned"] = ebull_test_conn.info.transaction_status == TransactionStatus.INERROR
        raise RuntimeError("raised with a failed transaction open")

    broker = MagicMock(spec=BrokerProvider)
    broker.lookup_order.side_effect = _first_call_poisons_then_raises

    results = _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert calls["poisoned"] is True, "the probe never poisoned the transaction, so it proves nothing"
    assert [(r.order_id, r.verdict) for r in results] == [(boom, "poll_error"), (later, "still_pending")]
    assert _order_row(ebull_test_conn, boom)["recommendation_last_polled_at"] == _NOW


def test_the_backlog_rotates_and_does_not_starve_the_tail(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#2948's absorbing state, as a probe. With a bounded limit and an ordering
    key that never moved for a non-terminal row, the second order would never be
    visited. Two successive bounded passes must visit DIFFERENT orders."""
    _seed_instrument(ebull_test_conn)
    first_rec = _seed_recommendation(ebull_test_conn)
    second_rec = _seed_recommendation(ebull_test_conn)
    first = _seed_order(ebull_test_conn, recommendation_id=first_rec, last_polled_at=_NOW - timedelta(hours=2))
    second = _seed_order(ebull_test_conn, recommendation_id=second_rec, last_polled_at=_NOW - timedelta(hours=1))

    broker = _broker(detail=_detail("Pending"))
    first_pass = _reconcile(ebull_test_conn, broker=broker, limit=1, now=_NOW)
    second_pass = _reconcile(ebull_test_conn, broker=broker, limit=1, now=_NOW + timedelta(minutes=1))

    assert [r.order_id for r in first_pass] == [first]
    assert [r.order_id for r in second_pass] == [second]


def test_the_scheduler_prerequisite_closes_once_every_row_is_parked(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """PR #3168 round 2. The prerequisite is the only thing standing between a
    permanently-unselectable row and an hourly no-op fire, and it used to carry
    its own inlined copy of the predicate — which drifted the moment the park
    column landed in the service's copy and not in it.

    Exercise the SCHEDULER function, not the service helper: the service helper
    agreeing with itself proves nothing about the gate."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=rec)

    assert _has_pending_recommendation_orders(ebull_test_conn)[0] is True
    ebull_test_conn.commit()

    _reconcile(ebull_test_conn, broker=_broker(detail=_detail("Filled")), now=_NOW)

    open_gate, reason = _has_pending_recommendation_orders(ebull_test_conn)
    assert open_gate is False
    assert "no pending recommendation orders" in reason


def test_count_matches_what_the_poller_would_select(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The scheduler prerequisite and the service selection must agree, or the
    job fires for nothing (or worse, never fires while work waits)."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=rec)
    other = _seed_recommendation(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=other, status="uncertain", ref=None)

    assert count_pending_recommendation_orders(ebull_test_conn) == 1
    ebull_test_conn.commit()  # the count opened a transaction; the poller needs an idle conn
    results = _reconcile(ebull_test_conn, broker=_broker(detail=_detail("Pending")), now=_NOW)
    assert len(results) == 1


def test_reconciliation_refuses_a_connection_inside_a_transaction(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Broker I/O must never run inside a DB transaction, and the advisory-lock
    acquire commits. A caller that hands over a dirty connection has to be told,
    not silently accommodated."""
    _seed_instrument(ebull_test_conn)
    ebull_test_conn.execute("SELECT 1")  # opens a transaction under psycopg3

    with pytest.raises(RuntimeError, match="idle connection"):
        _reconcile(ebull_test_conn, broker=_broker(detail=_detail("Pending")), now=_NOW)


# ---------------------------------------------------------------------------
# #3007 half 2 — an EXIT is resolved through the close-order route
# ---------------------------------------------------------------------------


def test_an_exit_is_resolved_through_the_close_order_route(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The routing itself, which is the whole of #3007 half 2.

    ``execute_order`` submits an EXIT through ``close_position``, so its
    ``broker_order_ref`` is a CLOSE-order id and the documented confirmation for
    it is ``/api/v1/trading/info/{env}/close-orders/{orderId}``. Both mocks
    raise if the wrong route is taken, so this fails in both directions rather
    than only asserting that the right call happened."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, action="EXIT")
    broker = _close_broker(detail=_close_detail("filled", position_ids=(3_602_456_774,)))

    results = _reconcile(ebull_test_conn, broker=broker, now=_NOW)

    assert [r.verdict for r in results] == ["filled_not_booked"]
    assert broker.get_close_order.call_args.kwargs == {"order_id": _REF}
    # A late EXIT fill is booked only on a recorded submission context (#3007 part
    # 2, check 1). This row was not claimed, so it parks: the claim stays held and
    # nothing is booked. Booking itself: tests/test_3007_late_exit_booking_db.py.
    assert _order_row(ebull_test_conn, order_id)["status"] == "pending"
    assert _parked_reason(ebull_test_conn, order_id) == "filled_unbooked"
    fills = ebull_test_conn.execute("SELECT count(*) FROM fills WHERE order_id=%s", (order_id,)).fetchone()
    assert fills is not None and int(fills[0]) == 0
    # The audit carries what a hand reconciliation needs: the lookup verbatim and
    # the submission cost (sql/413; NULL here, as this row was not claimed).
    audit = ebull_test_conn.execute(
        "SELECT evidence_json FROM decision_audit WHERE evidence_json->>'refusal' = 'pending_order_filled_not_booked' "
        "AND (evidence_json->>'order_id')::bigint = %s",
        (order_id,),
    ).fetchone()
    assert audit is not None
    assert "exit_avg_cost" in audit[0] and audit[0]["exit_avg_cost"] is None
    assert audit[0]["reason"] == "context_not_recorded"
    assert audit[0]["response"] == broker.get_close_order.return_value.raw_payload
    with pytest.raises(psycopg.errors.UniqueViolation):
        _seed_order(ebull_test_conn, recommendation_id=rec, action="EXIT")
    ebull_test_conn.rollback()


def test_a_rejected_close_order_terminalises_and_lifts_the_claim(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``errorCode`` set and no affected positions: the broker refused the
    close, nothing moved, so the claim is safe to release and the EXIT becomes
    proposable again."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, action="EXIT")

    results = _reconcile(ebull_test_conn, broker=_close_broker(detail=_close_detail("rejected")), now=_NOW)

    assert [r.verdict for r in results] == ["terminalised_rejected"]
    assert _order_row(ebull_test_conn, order_id)["status"] == "rejected"
    assert _rec_status(ebull_test_conn, rec) == "execution_failed"
    ebull_test_conn.commit()
    # The claim has lifted: a fresh attempt is now admissible.
    _seed_order(ebull_test_conn, recommendation_id=rec, action="EXIT")


def test_a_rejected_close_order_carrying_positions_keeps_the_claim(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """#3189 finding 3, carried to the close-order route.

    ``get_close_order`` resolves ``errorCode`` to ``rejected`` BEFORE it looks
    at ``positions[]``, so a response carrying both says the close was refused
    and names positions it affected. Terminalising on that would lift the claim
    over a real execution; the honest verdict is that positions exist and we
    have not booked them."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, action="EXIT")
    detail = _close_detail("rejected", position_ids=(3_602_456_774,))

    results = _reconcile(ebull_test_conn, broker=_close_broker(detail=detail), now=_NOW)

    assert [r.verdict for r in results] == ["filled_not_booked"]
    assert _order_row(ebull_test_conn, order_id)["status"] == "pending"
    assert _rec_status(ebull_test_conn, rec) == "execution_pending"
    with pytest.raises(psycopg.errors.UniqueViolation):
        _seed_order(ebull_test_conn, recommendation_id=rec, action="EXIT")
    ebull_test_conn.rollback()


def test_a_pending_close_order_is_stamped_and_otherwise_untouched(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """No ``errorCode``, no affected positions yet: the broker has the close and
    has not finished it."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, action="EXIT")

    results = _reconcile(ebull_test_conn, broker=_close_broker(detail=_close_detail("pending")), now=_NOW)

    assert [r.verdict for r in results] == ["still_pending"]
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending"
    assert row["recommendation_last_polled_at"] == _NOW
    assert _parked_reason(ebull_test_conn, order_id) is None


@pytest.mark.parametrize(
    "error",
    [
        BrokerPositionMutationError("demo close-order lookup failed with HTTP 404"),
        BrokerPositionMutationError("real close-order lookup failed with HTTP 500"),
        BrokerPositionMutationUncertain("demo close-order lookup transport failed"),
    ],
)
def test_a_close_order_lookup_failure_never_advances_and_is_never_parked(
    ebull_test_conn: psycopg.Connection[tuple], error: Exception
) -> None:
    """⚠⚠ The 404 case is the one that matters, and it is NOT an absence.

    The attended session on 2026-09-22 got a 404 on an order id the broker had
    just issued, then ``statusID=3`` with the affected position on the same id
    seconds later (#2961, #3007). Treating that as "no such close order" would
    release the claim on a close that filled. So every lookup failure stamps
    (the rotation key must move) and parks nothing (the row must stay
    pollable)."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, action="EXIT")

    results = _reconcile(ebull_test_conn, broker=_close_broker(error=error), now=_NOW)

    assert [r.verdict for r in results] == ["lookup_error"]
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending"
    assert row["recommendation_last_polled_at"] == _NOW
    assert _parked_reason(ebull_test_conn, order_id) is None
    assert _rec_status(ebull_test_conn, rec) == "execution_pending"
    assert count_pending_recommendation_orders(ebull_test_conn) == 1


@pytest.mark.parametrize("answered_instrument", [INSTRUMENT_ID + 1, None])
def test_a_close_order_about_another_instrument_advances_nothing(
    ebull_test_conn: psycopg.Connection[tuple], answered_instrument: int | None
) -> None:
    """Codex checkpoint 2 on #3007 half 2, and the ABSENT case fails closed too.

    ``instrumentID`` is ``required`` on ``OrderForCloseInfoResponse``, so a
    response carrying the right ``orderID`` and a different instrument is the
    broker answering about something else. The status it carries is
    ``rejected`` here deliberately: that is the ONE verdict that releases the
    submission claim, so a mis-addressed answer acted on would make a second
    economic close possible. A missing field is refused for the same reason a
    missing ``broker_environment`` is — it is not "probably ours"."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec, action="EXIT")
    detail = _close_detail("rejected", instrument_id=answered_instrument)

    results = _reconcile(ebull_test_conn, broker=_close_broker(detail=detail), now=_NOW)

    assert [r.verdict for r in results] == ["identity_mismatch"]
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending"
    assert row["recommendation_last_polled_at"] == _NOW
    # NOT parked: a statement about the ANSWER, not about the row, so a later
    # correct response must still be readable.
    assert _parked_reason(ebull_test_conn, order_id) is None
    assert _rec_status(ebull_test_conn, rec) == "execution_pending"
