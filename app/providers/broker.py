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
TradeDirection = Literal["buy", "sellShort"]
SettlementType = Literal["cfd", "real", "realFutures", "marginTrade"]
PreflightOrderType = Literal["mkt", "mit", "limitIOC"]


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
class BrokerWhatIfOrder:
    """A non-executing order shape for eToro's v2 cost preflight.

    v1 deliberately supports the two transactions the current endpoint accepts
    for an ``open`` action.  This type is evidence collection, not permission to
    execute shorts: the execution path remains long-only until a separately
    validated strategy and guard contract promote it.
    """

    instrument_id: int
    transaction: TradeDirection
    settlement_type: SettlementType
    amount: Decimal | None = None
    units: Decimal | None = None
    order_type: PreflightOrderType = "mkt"
    leverage: int = 1
    order_currency: str = "usd"

    def __post_init__(self) -> None:
        if self.instrument_id <= 0:
            raise ValueError("instrument_id must be positive")
        if (self.amount is None) == (self.units is None):
            raise ValueError("exactly one of amount or units must be provided")
        value = self.amount if self.amount is not None else self.units
        if value is None or value <= 0:
            raise ValueError("amount or units must be positive")
        if self.leverage < 1:
            raise ValueError("leverage must be at least 1")
        if self.order_currency.lower() != "usd":
            raise ValueError("the current eToro preflight endpoint supports USD only")


@dataclass(frozen=True)
class BrokerLeverageConfig:
    """One direction/settlement arm returned by trading eligibility."""

    settlement_type: str
    direction: str
    leverage_values: tuple[int, ...]
    min_position_amount: Decimal | None
    allow_edit_stop_loss: bool | None
    allow_edit_take_profit: bool | None
    allow_stop_loss_take_profit: bool | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerInstrumentEligibility:
    """Account-specific eligibility for one instrument at request time."""

    instrument_id: int
    symbol: str | None
    min_position_exposure: Decimal | None
    max_units_per_order: Decimal | None
    allow_open_position: bool
    allow_close_position: bool
    allow_partial_close_position: bool
    allow_trailing_stop_loss: bool
    leverage_configs: tuple[BrokerLeverageConfig, ...]
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerEligibilityResponse:
    """The complete resolved/not-found result of an eligibility preflight."""

    currency: str
    eligibilities: tuple[BrokerInstrumentEligibility, ...]
    not_found_instrument_ids: tuple[int, ...]
    not_found_symbols: tuple[str, ...]
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerCostComponent:
    """One broker-named cost component; the vocabulary remains provider-owned."""

    cost_type: str
    # The docs show ``amount`` while the live demo response used ``value``.
    # Preserve both until the provider documents or a controlled probe proves
    # the unit semantics. Neither may be silently substituted for the other.
    amount: Decimal | None
    value: Decimal | None
    currency: str
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerWhatIfCostResponse:
    """Current broker cost estimate for a hypothetical order."""

    instrument_id: int
    symbol: str | None
    costs: tuple[BrokerCostComponent, ...]
    last_updated: datetime
    raw_payload: dict[str, Any]


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


@dataclass(frozen=True)
class BrokerClosedTrade:
    """One closed-position slice from the broker's trade history.

    eToro returns one row per closed slice carrying BOTH legs
    (open + close). Partial closes reduce the same positionId, so a
    position may appear in several rows (#1593 spec §4).

    Money fields (net_profit, fees, investment, initial_investment)
    are in the account currency (USD). Rates are in the instrument's
    native currency.
    """

    position_id: int
    instrument_id: int
    is_buy: bool
    units: Decimal
    open_rate: Decimal | None
    open_timestamp: datetime
    close_rate: Decimal | None
    close_timestamp: datetime
    net_profit: Decimal | None
    fees: Decimal | None
    investment: Decimal | None
    initial_investment: Decimal | None
    leverage: int
    order_id: int | None
    social_trade_id: int | None
    parent_position_id: int | None
    raw_payload: dict[str, Any]


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

    def get_trade_history(self, min_date: datetime, page_size: int = 200) -> Sequence[BrokerClosedTrade]:
        """
        Fetch closed-trade history rows with close timestamp >= min_date.

        Non-abstract with a NotImplementedError default so existing test
        fakes that implement only the abstract surface keep working;
        the eToro implementation overrides it.
        """
        raise NotImplementedError

    def check_instrument_eligibility(
        self,
        instrument_ids: Sequence[int],
    ) -> BrokerEligibilityResponse:
        """Fetch current account-specific trading constraints without trading.

        Non-abstract so existing provider fakes remain source compatible.  A
        strategy execution gate must treat ``NotImplementedError`` as a refusal,
        never as eligibility.
        """
        raise NotImplementedError

    def get_what_if_costs(self, order: BrokerWhatIfOrder) -> BrokerWhatIfCostResponse:
        """Fetch current broker-estimated costs without placing an order."""
        raise NotImplementedError
