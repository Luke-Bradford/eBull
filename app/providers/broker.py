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
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

OrderStatus = Literal["filled", "pending", "rejected", "failed"]
TradeDirection = Literal["buy", "sellShort"]
SettlementType = Literal["cfd", "real", "realFutures", "marginTrade"]
PreflightOrderType = Literal["mkt", "mit", "limitIOC"]

#: The what-if COST endpoint's action, which is wider than the execution vocabulary above
#: and is informational on both arms -- pricing a close is not closing anything.
#: Measured 2026-08-14 (#2712), demo.
PreflightAction = Literal["open", "close"]

#: The what-if endpoint's own transaction vocabulary.  ⚠ WIDER than ``TradeDirection``,
#: which lists the two OPEN transactions the execution path supports; ``sell`` and
#: ``buyToCover`` are the close-side pair and exist here ONLY for cost preflight.
#: Deliberately a separate alias rather than a widening of ``TradeDirection``: that type
#: gates what may be EXECUTED, and shorting is barred outside research (`.claude/CLAUDE.md`).
PreflightTransaction = Literal["buy", "sellShort", "sell", "buyToCover"]


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
class BrokerPositionExecution:
    """One exact position created or affected by a detailed broker order."""

    position_id: int
    state: str
    remaining_units: Decimal | None
    opening_units: Decimal | None
    average_price: Decimal | None
    execution_time: datetime | None
    fees: Decimal | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerOrderDetail:
    """Current v2 order detail resolved by order id or submission UUID."""

    broker_order_ref: str
    reference_id: str | None
    status: OrderStatus
    broker_status: str
    instrument_id: int
    position_executions: tuple[BrokerPositionExecution, ...]
    last_update: datetime | None
    raw_payload: dict[str, Any]


class BrokerOrderLookupError(RuntimeError):
    """A detailed order lookup failed or returned an unsafe shape."""


class BrokerOrderNotFound(BrokerOrderLookupError):
    """No order currently resolves for the supplied durable identity."""


class BrokerOrderSubmissionError(RuntimeError):
    """A demo strategy submission failed before acceptance was established."""


class BrokerOrderSubmissionUncertain(BrokerOrderSubmissionError):
    """Transport/response failure requires lookup by the same request UUID."""


class BrokerPositionMutationError(RuntimeError):
    """A demo strategy position mutation was explicitly rejected."""

    def __init__(self, message: str, *, raw_payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.raw_payload = raw_payload


class BrokerPositionMutationUncertain(BrokerPositionMutationError):
    """A position mutation may have reached the broker and must be re-synced."""


@dataclass(frozen=True)
class BrokerPositionEditSubmission:
    """Asynchronous acceptance identity for an exact-position SL/TP edit."""

    operation_id: UUID
    position_id: int
    reference_id: UUID
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerPositionCloseSubmission:
    """Acceptance identity for an exact-position close order."""

    broker_order_ref: str
    position_id: int
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerCloseOrderDetail:
    """Current exact close-order result and the positions it affected."""

    broker_order_ref: str
    status: OrderStatus
    broker_status: str
    position_ids: tuple[int, ...]
    reference_id: UUID | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class BrokerStrategyOrder:
    """The deliberately narrow order shape allowed by the paper MVP."""

    instrument_id: int
    amount: Decimal
    settlement_type: Literal["real"]
    stop_loss_rate: Decimal
    take_profit_rate: Decimal

    def __post_init__(self) -> None:
        if self.instrument_id <= 0 or self.amount <= 0:
            raise ValueError("strategy order instrument and amount must be positive")
        if self.settlement_type != "real":
            raise ValueError("the paper MVP supports real settlement only")
        if self.stop_loss_rate <= 0 or self.take_profit_rate <= 0:
            raise ValueError("fixed stop-loss and take-profit rates are required")


@dataclass(frozen=True)
class BrokerOrderSubmission:
    """Broker acceptance identity; execution detail is reconciled separately."""

    broker_order_ref: str
    reference_id: UUID
    token: UUID


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


#: Which transactions belong to which arm.  Used to reject a meaningless combination
#: locally rather than spending a request from the 20/60s informational lane on it.
_PREFLIGHT_TRANSACTIONS_BY_ACTION: dict[PreflightAction, frozenset[str]] = {
    "open": frozenset({"buy", "sellShort"}),
    "close": frozenset({"sell", "buyToCover"}),
}


@dataclass(frozen=True)
class BrokerWhatIfOrder:
    """A non-executing order shape for eToro's v2 cost preflight.

    This type is evidence collection, not permission to execute: the execution path
    remains long-only until a separately validated strategy and guard contract promote it.

    ⚠⚠ **The CLOSE arm requires ``position_ids``, measured 2026-08-14 (#2712).** Sending
    ``action="close"`` without it returns 400 *"PositionIds must be provided for close
    action"*; with a real position id it returns 200 and real cost rows.  The live portal
    documents that field as *"For `close` action; currently rejected"* -- the doc and the
    endpoint disagree, and the endpoint won.  Validated here rather than left to the
    server, so a doomed request is never spent against the 20/60s lane.

    ⚠⚠ **An open-arm quote does NOT bound the close-arm cost, and must never be
    substituted for one.**  Measured on every held demo position (5 instruments, both arms
    decodable on all 5, same ticket seconds apart): the close was DEARER on 4 of the 5, by
    5.7x, 8.5x, 13.0x and 18.5x, and cheaper on the fifth (0.5x).  Substituting would
    under-state a bound by an order of magnitude, which is the one direction a cost bound
    cannot be wrong in.  See `.claude/skills/data-sources/etoro-api.md`.
    """

    instrument_id: int
    transaction: PreflightTransaction
    settlement_type: SettlementType
    amount: Decimal | None = None
    units: Decimal | None = None
    order_type: PreflightOrderType = "mkt"
    leverage: int = 1
    order_currency: str = "usd"
    action: PreflightAction = "open"
    position_ids: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if self.instrument_id <= 0:
            raise ValueError("instrument_id must be positive")
        if self.transaction not in _PREFLIGHT_TRANSACTIONS_BY_ACTION[self.action]:
            # `Literal` is a static check only, so a dynamically built order reaches here
            # unvalidated.  ⚠ The PAIRING is inferred from the vocabulary's structure --
            # `buy`/`sellShort` are the two ways to OPEN and `sell`/`buyToCover` close
            # them respectively -- NOT measured: the probe exercised open/buy,
            # close/sell and close/buyToCover, never open/sell.  It is a local refusal
            # of a combination that has no meaning, so relaxing it costs one request if
            # the inference is ever wrong.
            raise ValueError(f"transaction {self.transaction!r} is not a {self.action!r} transaction")
        if self.action == "close" and not self.position_ids:
            raise ValueError("the close arm requires position_ids (measured: 400 without them)")
        if self.action == "open" and self.position_ids:
            raise ValueError("position_ids is meaningless on the open arm")
        if any(position_id <= 0 for position_id in self.position_ids):
            raise ValueError("position_ids must be positive")
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
class BrokerInstrumentInvestment:
    """What this account holds in one instrument, under TWO different questions.

    ``amount`` is the *total invested* term: capital currently COMMITTED to this
    instrument under any ownership -- direct positions, copy-trader mirrors and
    pending orders alike, exactly as eToro's `calculate-total-invested` formula
    folds them.  It is cost basis, verified on the live demo account 2026-08-14:
    ``amount / units`` reproduces the independently reported ``openRate`` to
    within 0.005% on 7/7 positions.  Three capital controls read it with that
    meaning and are right to (`_risk_and_amount`'s instrument capacity, and the
    portfolio-capacity / drawdown gates sharing the snapshot).

    ⚠⚠ It is therefore NOT this instrument's market value, and the gap is not
    small: measured across the same account, all 38 reported instruments
    disagreed, direct holdings by -27.42% to +26.66%, and **33 of the 38 had no
    direct position at all** -- they are copy-trader mirrors folded in by the
    formula.  Feeding ``amount`` to a weight computation yields a coherent
    verdict to the wrong question, with every field internally consistent and
    nothing malformed to refuse on (#2704).

    ``direct_long_market_value`` answers the other question: the DIRECT long
    holding's market value, ``sum(amount + unrealizedPnL.pnL)`` over this
    instrument's ``isBuy`` positions, with mirrors and pending orders excluded.
    That reading is established by independent measurement rather than by
    decomposing the equity identity -- ``(amount + pnL) / units`` lands on our
    separately fed ``quotes.last`` at -0.00% on four of seven live positions,
    the residuals tracking quote staleness, and two lots of one instrument
    opened at different rates imply the same current price.

    ⚠⚠ The short arm is a COUNT and deliberately not a money total.  No monetary
    sum can carry "a short exists": two lots can offset to zero and a single
    short can sit at ``amount + pnL == 0``, either of which would be
    indistinguishable from absence.  A count is zero only when there is nothing
    to count.  ``direct_long_positions`` exists for the same reason one level
    down -- a zero market value is otherwise ambiguous between "no holding" and
    "a holding wiped out", and the 33 mirror-only rows above are the first case.

    ⚠ A negative ``direct_long_market_value`` is NOT refused at parse time: it
    sums a signed term, so it is an extreme-but-legitimate state rather than
    response drift, unlike ``amount`` whose terms are all documented
    non-negative.  The refusal lives where the number is used, at
    ``strategy_core_allocator._state_refusal``.
    """

    instrument_id: int
    amount: Decimal
    direct_long_market_value: Decimal
    direct_long_positions: int
    direct_short_positions: int


@dataclass(frozen=True)
class BrokerAccountRiskSnapshot:
    """Decision-bearing account totals derived from eToro's P&L endpoint.

    Every money field is denominated in ``account_currency_id`` -- the id the
    broker reported for this account, not one we chose.  ``None`` means the
    payload did not carry it, which is an absence to refuse on (#2602 item 2),
    never a licence to assume USD.
    """

    available_cash: Decimal
    total_invested: Decimal
    unrealized_pnl: Decimal
    equity: Decimal
    instrument_investments: tuple[BrokerInstrumentInvestment, ...]
    observed_at: datetime
    raw_payload: dict[str, Any]
    account_currency_id: int | None = None


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
        *,
        request_id: UUID | None = None,
    ) -> BrokerOrderResult:
        """
        Place an order with the broker.

        Exactly one of amount or units should be provided.
        params: optional SL/TP and leverage settings. None = broker defaults.
        request_id: optional durable broker idempotency identity. Strategy
        callers must commit this UUID before I/O and reuse it after uncertainty.
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

    def lookup_order(
        self,
        *,
        order_id: str | None = None,
        reference_id: str | None = None,
    ) -> BrokerOrderDetail:
        """Resolve exact order/position detail using one durable identity.

        ``reference_id`` is the idempotency UUID supplied when the order was
        submitted.  It is therefore usable even when a process crashed before
        persisting the broker-assigned order id.
        """
        raise NotImplementedError

    def edit_demo_strategy_position(
        self,
        *,
        position_id: int,
        stop_loss_rate: Decimal,
        take_profit_rate: Decimal | None,
        request_id: UUID,
        persist_response: Callable[[dict[str, Any]], None] | None = None,
    ) -> BrokerPositionEditSubmission:
        """Edit one demo position; automated strategy code has no real path."""
        raise NotImplementedError

    def close_demo_strategy_position(
        self,
        *,
        position_id: int,
        instrument_id: int,
        request_id: UUID,
        persist_response: Callable[[dict[str, Any]], None] | None = None,
    ) -> BrokerPositionCloseSubmission:
        """Close one whole demo position by its exact broker id."""
        raise NotImplementedError

    def get_demo_close_order(
        self,
        *,
        order_id: str,
        persist_response: Callable[[dict[str, Any]], None] | None = None,
    ) -> BrokerCloseOrderDetail:
        """Resolve one exact demo close order and its affected position ids."""
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

    def place_demo_strategy_order(
        self,
        order: BrokerStrategyOrder,
        *,
        request_id: UUID,
    ) -> BrokerOrderSubmission:
        """Submit the strict v2 demo-only strategy shape.

        Non-abstract for compatibility with existing test providers. A caller
        must treat ``NotImplementedError`` as refusal, never fall back to the
        generic/live-capable writer.
        """
        raise NotImplementedError

    def get_account_risk_snapshot(self) -> BrokerAccountRiskSnapshot:
        """Get demo-account cash, invested capital, P&L and equity.

        A strategy gate must refuse when this capability is absent; the older
        portfolio reader's ``credit`` field is not available cash because it
        omits pending orders.
        """
        raise NotImplementedError
