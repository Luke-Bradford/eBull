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

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.providers.broker import (
    BrokerOrderDetail,
    BrokerOrderLookupError,
    BrokerOrderNotFound,
    BrokerProvider,
)
from app.services.order_client import (
    RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS,
    count_pending_recommendation_orders,
    reconcile_pending_recommendation_orders,
)
from tests.fixtures.ebull_test_db import test_database_url

INSTRUMENT_ID = 991_942
_NOW = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)
_REF = "13902598"


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
) -> int:
    row = conn.execute(
        """
        INSERT INTO orders
            (instrument_id, recommendation_id, action, order_type, status,
             broker_order_ref, raw_payload_json, created_at, execution_origin,
             recommendation_request_id, recommendation_submission_phase,
             recommendation_last_polled_at)
        VALUES
            (%(iid)s, %(rid)s, 'BUY', 'market', %(status)s,
             %(ref)s, '{}'::jsonb, %(now)s, %(origin)s,
             %(req)s, 'broker_verb_entered', %(polled)s)
        RETURNING order_id
        """,
        {
            "iid": INSTRUMENT_ID,
            "rid": recommendation_id,
            "status": status,
            "ref": ref,
            "now": _NOW,
            "origin": origin,
            # The request-id CHECK admits it only alongside a recommendation.
            "req": uuid4() if recommendation_id is not None else None,
            "polled": last_polled_at,
        },
    ).fetchone()
    assert row is not None
    conn.commit()
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

    results = reconcile_pending_recommendation_orders(
        ebull_test_conn, broker=_broker(detail=_detail("Rejected")), now=_NOW
    )

    assert [r.verdict for r in results] == ["terminalised_rejected"]
    assert _order_row(ebull_test_conn, order_id)["status"] == "rejected"
    assert _rec_status(ebull_test_conn, rec) == "execution_failed"
    assert _audit_count(ebull_test_conn, rec) == 1
    # The claim really lifted: this INSERT raises UniqueViolation while held.
    second = _seed_order(ebull_test_conn, recommendation_id=rec)
    assert _order_row(ebull_test_conn, second)["status"] == "pending"


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

    reconcile_pending_recommendation_orders(ebull_test_conn, broker=_broker(detail=_detail(broker_status)), now=_NOW)

    assert _order_row(ebull_test_conn, order_id)["status"] == "rejected"


def test_a_still_pending_order_is_stamped_and_otherwise_untouched(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The ordinary tick. Nothing may move except the rotation key."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    results = reconcile_pending_recommendation_orders(
        ebull_test_conn, broker=_broker(detail=_detail("Pending")), now=_NOW
    )

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

    results = reconcile_pending_recommendation_orders(
        ebull_test_conn, broker=_broker(detail=_detail("Filled")), now=_NOW
    )

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

    results = reconcile_pending_recommendation_orders(ebull_test_conn, broker=_broker(error=error), now=_NOW)

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

    results = reconcile_pending_recommendation_orders(
        ebull_test_conn, broker=_broker(detail=_detail("PartiallyFilled")), now=_NOW
    )

    assert [r.verdict for r in results] == ["unsafe_status"]
    assert _order_row(ebull_test_conn, order_id)["status"] == "pending"


def test_a_strategy_origin_order_is_never_selected(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The strategy arm has its own reconciler with its own tables and its own
    ownership claims. Two reconcilers on one order is the serialisation defect
    #2962 spent a ticket removing."""
    _seed_instrument(ebull_test_conn)
    _seed_order(ebull_test_conn, recommendation_id=None, origin="strategy")

    broker = _broker(detail=_detail("Rejected"))
    results = reconcile_pending_recommendation_orders(ebull_test_conn, broker=broker, now=_NOW)

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
    assert reconcile_pending_recommendation_orders(ebull_test_conn, broker=broker, now=_NOW) == ()
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
    assert reconcile_pending_recommendation_orders(ebull_test_conn, broker=broker, now=_NOW) == ()
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
    results = reconcile_pending_recommendation_orders(ebull_test_conn, broker=broker, now=_NOW)

    assert [r.verdict for r in results] == ["ref_not_pollable"]
    broker.lookup_order.assert_not_called()
    assert _order_row(ebull_test_conn, order_id)["recommendation_last_polled_at"] == _NOW


def test_an_order_whose_recommendation_key_is_held_elsewhere_is_skipped(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A live submitter owns this recommendation's span. The poller must not
    call the broker or write anything while somebody else is mid-submission."""
    _seed_instrument(ebull_test_conn)
    rec = _seed_recommendation(ebull_test_conn)
    order_id = _seed_order(ebull_test_conn, recommendation_id=rec)

    broker = _broker(detail=_detail("Rejected"))
    with psycopg.connect(test_database_url()) as holder:
        holder.execute("SELECT pg_advisory_lock(%s, %s)", (RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS, rec))
        holder.commit()
        results = reconcile_pending_recommendation_orders(ebull_test_conn, broker=broker, now=_NOW)

    assert [r.verdict for r in results] == ["lock_busy"]
    broker.lookup_order.assert_not_called()
    row = _order_row(ebull_test_conn, order_id)
    assert row["status"] == "pending"
    assert row["recommendation_last_polled_at"] is None


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
    first_pass = reconcile_pending_recommendation_orders(ebull_test_conn, broker=broker, limit=1, now=_NOW)
    second_pass = reconcile_pending_recommendation_orders(
        ebull_test_conn, broker=broker, limit=1, now=_NOW + timedelta(minutes=1)
    )

    assert [r.order_id for r in first_pass] == [first]
    assert [r.order_id for r in second_pass] == [second]


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
    results = reconcile_pending_recommendation_orders(
        ebull_test_conn, broker=_broker(detail=_detail("Pending")), now=_NOW
    )
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
        reconcile_pending_recommendation_orders(ebull_test_conn, broker=_broker(detail=_detail("Pending")), now=_NOW)
