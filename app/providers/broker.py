"""
Broker provider interface.

eToro is the v1 implementation.  All domain code imports this interface only —
never the concrete provider.

The broker provider handles trading operations (placing orders, closing
positions, checking status) and portfolio reads (open positions, account
balance).  It does not own DB access or domain logic.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
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


@dataclass(frozen=True)
class OrderParams:
    """Optional parameters for order placement.

    All fields are optional — omitting them preserves the current
    behaviour (no SL, no TP, leverage 1).
    """

    stop_loss_rate: Decimal | None = None
    take_profit_rate: Decimal | None = None
    is_tsl_enabled: bool = False
    leverage: int = 1


@dataclass(frozen=True)
class BrokerPosition:
    """A single open position as reported by the broker.

    After the broker_positions migration (024), the sync writes one row
    per BrokerPosition into the ``broker_positions`` table and derives the
    per-instrument ``positions`` summary from it.

    Fields with defaults are optional for backwards-compat with existing
    test code that constructs BrokerPosition with only the original fields.
    """

    instrument_id: int
    units: Decimal
    open_price: Decimal
    current_price: Decimal
    raw_payload: dict[str, Any]

    # --- Per-position fields (populated from eToro payload) ---
    position_id: int | None = None
    is_buy: bool = True
    amount: Decimal = Decimal("0")
    initial_amount_in_dollars: Decimal = Decimal("0")
    open_conversion_rate: Decimal = Decimal("1")
    open_date_time: datetime | None = None
    initial_units: Decimal | None = None
    stop_loss_rate: Decimal | None = None
    take_profit_rate: Decimal | None = None
    is_no_stop_loss: bool = True
    is_no_take_profit: bool = True
    leverage: int = 1
    is_tsl_enabled: bool = False
    total_fees: Decimal = Decimal("0")


@dataclass(frozen=True)
class BrokerMirrorPosition:
    """A single nested position inside a copy-trader mirror.

    `amount` is the pre-converted USD cost basis reported by eToro.
    `open_rate` is the entry price in the instrument's native
    currency; `open_conversion_rate` is the native→USD FX rate at
    open. Both are required — see spec §1.3 "openConversionRate NOT
    NULL" for the AUM correctness reason.
    """

    position_id: int
    parent_position_id: int
    instrument_id: int
    is_buy: bool
    units: Decimal
    amount: Decimal
    initial_amount_in_dollars: Decimal
    open_rate: Decimal
    open_conversion_rate: Decimal
    open_date_time: datetime
    take_profit_rate: Decimal | None
    stop_loss_rate: Decimal | None
    total_fees: Decimal
    leverage: int
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerMirror:
    """A single copy-trading mirror (one per copy session with a trader)."""

    mirror_id: int
    parent_cid: int
    parent_username: str
    initial_investment: Decimal
    deposit_summary: Decimal
    withdrawal_summary: Decimal
    available_amount: Decimal
    closed_positions_net_profit: Decimal
    stop_loss_percentage: Decimal | None
    stop_loss_amount: Decimal | None
    mirror_status_id: int | None
    mirror_calculation_type: int | None
    pending_for_closure: bool
    started_copy_date: datetime
    positions: Sequence[BrokerMirrorPosition]
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerPortfolio:
    """Snapshot of the broker account: positions + available cash + mirrors."""

    positions: Sequence[BrokerPosition]
    available_cash: Decimal
    raw_payload: dict[str, Any]
    mirrors: Sequence[BrokerMirror] = ()


class BrokerProvider(ABC):
    """
    Interface for broker operations.

    v1 implementation: EtoroBrokerProvider
    """

    @abstractmethod
    def place_order(
        self,
        instrument_id: int,
        action: str,
        amount: Decimal | None,
        units: Decimal | None,
        params: OrderParams | None = None,
    ) -> BrokerOrderResult:
        """
        Place an order with the broker.

        Exactly one of amount or units should be provided.
        params: optional SL/TP and leverage settings. None = broker defaults.
        Returns the broker's response, including fill details if immediately filled.
        """

    @abstractmethod
    def close_position(
        self,
        position_id: int,
        units_to_deduct: Decimal | None = None,
    ) -> BrokerOrderResult:
        """
        Close an existing position by broker position ID.

        units_to_deduct: if provided, partial close. None = close entire position.
        Returns the broker's response with fill details.
        """

    @abstractmethod
    def get_order_status(self, broker_order_ref: str) -> BrokerOrderResult:
        """
        Check the current status of a previously placed order.

        Returns the latest state from the broker.
        """

    @abstractmethod
    def get_portfolio(self) -> BrokerPortfolio:
        """
        Fetch the current portfolio from the broker.

        Returns all open positions and available cash.
        """
