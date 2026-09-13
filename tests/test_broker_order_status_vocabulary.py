"""#2965 broker order status vocabulary, asserted against the documented enum.

Pure logic, no database: this module must stay out of the ``db`` tier so the push
gate actually runs it.

Source rule: the eToro live portal's ``status.name`` enum for
``GET /api/v2/trading/info/{demo|real}/orders:lookup`` (portal slug
``trading--demo/get-order-information-and-position-details``, OpenAPI v1.375.0,
verified 2026-09-13). The twelve documented values are enumerated below; a
thirteenth appearing in the portal is a change this test is meant to catch.
"""

from __future__ import annotations

import pytest

from app.services.strategy_order_reconciliation import (
    StrategyReconciliationError,
    classify_broker_order_status,
)

#: Every value the live portal documents for ``status.name``, verbatim and complete.
DOCUMENTED_BROKER_STATUSES = (
    "Received",
    "Placed",
    "Filled",
    "Rejected",
    "PartiallyFilled",
    "PendingCancel",
    "Canceled",
    "Expired",
    "CanceledPartiallyFilled",
    "RejectedPartiallyFilled",
    "WaitingForMarket",
    "PendingTriggeredRate",
)

#: Not in the portal enum, retained because nothing establishes the demo
#: connection never emits them. Dropping one is a narrowing change.
UNDOCUMENTED_RETAINED_STATUSES = ("Pending", "Executed", "Failed", "Cancelled")

CLASSIFIED = {
    "Received": ("pending", "pending"),
    "Placed": ("pending", "pending"),
    "WaitingForMarket": ("pending", "pending"),
    "PendingTriggeredRate": ("pending", "pending"),
    "Pending": ("pending", "pending"),
    "Filled": ("resolved", "filled"),
    "Executed": ("resolved", "filled"),
    "Rejected": ("rejected", "rejected"),
    "Canceled": ("rejected", "rejected"),
    "Cancelled": ("rejected", "rejected"),
    "Failed": ("rejected", "rejected"),
    "Expired": ("rejected", "rejected"),
}

#: Documented, and deliberately refused. Each can carry ``positionExecutions`` on a
#: non-``Filled`` order, which activates the ownership lifecycle #2965 leaves open.
#: Admitting one means DELETING an assertion here, which is the point of listing
#: them explicitly rather than letting them fall through to "unknown".
UNSETTLED_PARTIAL_FILL_STATUSES = (
    "PartiallyFilled",
    "PendingCancel",
    "CanceledPartiallyFilled",
    "RejectedPartiallyFilled",
)


@pytest.mark.parametrize(("broker_status", "expected"), sorted(CLASSIFIED.items()))
def test_classified_statuses_map_to_their_documented_outcome(broker_status: str, expected: tuple[str, str]) -> None:
    assert classify_broker_order_status(broker_status) == expected


@pytest.mark.parametrize("broker_status", UNSETTLED_PARTIAL_FILL_STATUSES)
def test_partial_fill_statuses_are_refused_with_their_own_reason(broker_status: str) -> None:
    """They must not be silently treated as pending, nor as merely unrecognised."""
    with pytest.raises(StrategyReconciliationError, match="ownership lifecycle is unsettled"):
        classify_broker_order_status(broker_status)


def test_every_documented_status_is_either_classified_or_explicitly_refused() -> None:
    """No documented value may fall through to the generic unknown-status raise.

    A value that does is a way for the broker to wedge an order in the
    non-terminal ``ambiguous`` state, which nothing unattended can advance
    (#2961, #2962).
    """
    accounted = set(CLASSIFIED) | set(UNSETTLED_PARTIAL_FILL_STATUSES)
    assert set(DOCUMENTED_BROKER_STATUSES) <= accounted


def test_retained_undocumented_statuses_still_classify() -> None:
    for broker_status in UNDOCUMENTED_RETAINED_STATUSES:
        assert broker_status in CLASSIFIED
        classify_broker_order_status(broker_status)


@pytest.mark.parametrize("broker_status", ["", "Unknown", "filled", "FILLED", "Partially Filled"])
def test_unrecognised_status_raises_rather_than_defaulting(broker_status: str) -> None:
    """Matching is exact and case-sensitive; nothing defaults to pending."""
    with pytest.raises(StrategyReconciliationError, match="unknown broker order status"):
        classify_broker_order_status(broker_status)
