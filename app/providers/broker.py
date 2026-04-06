"""
Broker provider interface.

eToro is the v1 implementation.  All domain code imports this interface only —
never the concrete provider.

The broker provider handles write operations: placing orders, closing positions,
and checking order status.  It does not own DB access or domain logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

OrderStatus = Literal["filled", "pending", "rejected", "failed"]


@dataclass(frozen=True)
class BrokerOrderResult:
    """Result of a broker order or close-position call."""

    broker_order_ref: str | None
    status: OrderStatus
    filled_price: Decimal | None
    filled_units: Decimal | None
    fees: Decimal
    raw_payload: dict[str, Any]


class BrokerProvider(ABC):
    """
    Interface for broker write operations.

    v1 implementation: EtoroBrokerProvider
    """

    @abstractmethod
    def place_order(
        self,
        instrument_id: int,
        action: str,
        amount: Decimal | None,
        units: Decimal | None,
    ) -> BrokerOrderResult:
        """
        Place an order with the broker.

        Exactly one of amount or units should be provided.
        Returns the broker's response, including fill details if immediately filled.
        """

    @abstractmethod
    def close_position(self, instrument_id: int) -> BrokerOrderResult:
        """
        Close an existing position for the given instrument.

        Returns the broker's response with fill details.
        """

    @abstractmethod
    def get_order_status(self, broker_order_ref: str) -> BrokerOrderResult:
        """
        Check the current status of a previously placed order.

        Returns the latest state from the broker.
        """
