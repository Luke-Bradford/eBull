"""The core/cash rebalance decision (#2603 item 3, decision half).

Mandate plus observed sleeve state in, one verdict out.  Pure: no connection, no
clock, no broker, no persistence.

⚠ This module AUTHORISES NOTHING and has NO CALLER.  It is the first reader of
``strategy_core_mandate_events`` -- which is what item 1's spec said item 3 would
be -- but nothing calls ``evaluate_core_rebalance``, and no verdict it returns
causes anything to happen.  #2437's R4 comment records *a control on a path the
decision does not take* seven times over, so the state is named here rather than
left to be inferred.  Its caller is item 3's execution half, which needs the
operator-attended session #2603 reserves.

The verdict is emphatically NOT an eligibility finding: item 2 owns the proof that
the core instrument is the underlying product and not a CFD, and has no table yet.

Spec: ``docs/proposals/ta/2026-08-13-core-cash-allocator.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_DOWN, Decimal
from typing import Literal

from app.services.strategy_core_mandate import (
    AMOUNT_PLACES,
    AMOUNT_PRECISION,
    CORE_MANDATE_POLICY_VERSION,
    PERCENT_BASIS,
    CoreMandate,
    CoreMandateError,
    validate_core_mandate,
)

CoreRebalanceAction = Literal["hold", "buy_core", "sell_core", "refused"]

# One quantum of the NUMERIC(18,6) amount shape.  Rounding a rebalance DOWN leaves
# a residual strictly below this, and `min_rebalance_amount > 0` on the same column
# type is at least this, so the residual can never re-trigger on the next pass.
_AMOUNT_QUANTUM = Decimal(1).scaleb(-AMOUNT_PLACES)
# Exclusive upper bound on a NUMERIC(18,6) amount: 12 integer digits.
_MAX_AMOUNT = Decimal(1).scaleb(AMOUNT_PRECISION - AMOUNT_PLACES)
_ZERO = Decimal("0")


@dataclass(frozen=True)
class CoreSleeveState:
    """The observed core sleeve: one instrument and cash, at one instant.

    ⚠ Caller obligations the arithmetic depends on and this module CANNOT check.
    The supplier warrants that both components are valued at ``as_of`` from ONE
    snapshot rather than two; that ``core_market_value`` is that one instrument's
    net long value with lots netted and no other holding folded in; that
    ``cash_balance`` is settled and unreserved, with pending orders, unsettled
    proceeds and accrued charges already deducted; and that no rebalance from a
    previous verdict is still in flight -- this function is stateless and will
    re-recommend an in-flight trade.

    Staleness of ``as_of`` is the caller's rule to set.  This module holds no clock
    and enforces none; ``as_of`` is carried so a verdict can be attributed to a
    valuation instant, not so it can be judged here.
    """

    core_instrument_id: int
    core_market_value: Decimal
    cash_balance: Decimal
    currency: str
    as_of: datetime


@dataclass(frozen=True)
class CoreRebalanceDecision:
    """One rebalance verdict, carrying the arithmetic that produced it."""

    action: CoreRebalanceAction
    reason_code: str | None
    amount: Decimal
    core_pct: Decimal | None
    target_pct: Decimal | None
    lower_pct: Decimal | None
    upper_pct: Decimal | None
    effective_floor: Decimal | None
    floor_source: Literal["mandate", "broker"] | None
    reserve_breached: bool | None
    reserve_margin_pct: Decimal | None
    """Pre-cost margin over the liquidity reserve in the state THIS VERDICT LEAVES
    YOU IN: post-trade for a trade, current-state for a hold, None on a refusal or
    when no weight could be computed.  One meaning, because the quantity that
    decides whether a cost breaches the reserve must not be ambiguous between
    three."""


def _refused(reason_code: str) -> CoreRebalanceDecision:
    return CoreRebalanceDecision(
        action="refused",
        reason_code=reason_code,
        amount=_ZERO,
        core_pct=None,
        target_pct=None,
        lower_pct=None,
        upper_pct=None,
        effective_floor=None,
        floor_source=None,
        reserve_breached=None,
        reserve_margin_pct=None,
    )


def _mandate_refusal(mandate: CoreMandate | None) -> str | None:
    """The mandate half of the precedence order, or None when it is usable.

    Order matters and is a construction choice: an unsupported policy version is
    checked BEFORE validity, because `validate_core_mandate` implements THIS
    version's arithmetic and reporting "invalid" for a row written under a later
    policy would blame the row for our own staleness.
    """
    if mandate is None:
        # Item 1: no mandate configured is a state, not a default.
        return "core_mandate_absent"
    if mandate.policy_version != CORE_MANDATE_POLICY_VERSION:
        return "core_mandate_policy_unsupported"
    try:
        validate_core_mandate(
            enabled=mandate.enabled,
            base_currency=mandate.base_currency,
            core_instrument_id=mandate.core_instrument_id,
            core_target_pct=mandate.core_target_pct,
            liquidity_reserve_pct=mandate.liquidity_reserve_pct,
            rebalance_band_pct=mandate.rebalance_band_pct,
            min_rebalance_amount=mandate.min_rebalance_amount,
        )
    except CoreMandateError:
        # `CoreMandate` is a public frozen dataclass anyone can construct, so the
        # schema CHECKs are not reachable from here.  Without this, the spec's
        # "a reserve breach implies a band breach" proof -- which assumes a
        # schema-valid mandate -- would be relied on for objects it does not cover.
        return "core_mandate_invalid"
    if not mandate.enabled:
        return "core_mandate_disabled"
    if mandate.core_instrument_id is None:
        # Unreachable via the CHECK and via validate_core_mandate; kept because the
        # dataclass is constructible directly.
        return "core_instrument_unset"
    return None


def _state_refusal(mandate: CoreMandate, state: CoreSleeveState, broker_minimum: Decimal | None) -> str | None:
    """The state half of the precedence order, or None when it is usable."""
    if state.currency.strip().upper() != mandate.base_currency:
        # Otherwise the allocator weighs two currencies as if they were one.  #2363
        # owns FX and it is unmodelled, so this refuses rather than converts.
        return "sleeve_currency_mismatch"
    if state.core_instrument_id != mandate.core_instrument_id:
        return "sleeve_instrument_mismatch"
    for value in (state.core_market_value, state.cash_balance):
        # Finiteness first: `Decimal("NaN") < 0` raises InvalidOperation rather
        # than returning False, so every comparison below is only safe after this.
        if not value.is_finite():
            return "sleeve_valuation_invalid"
        if value < 0:
            # Negative cash is borrowed money and leverage is barred; a negative
            # core value is not a state this two-holding sleeve has.
            return "sleeve_valuation_invalid"
        # Per-component BEFORE the sum, and not merely as a shortcut: two finite
        # components near Decimal's Emax (`Decimal("9e999999")` each) raise
        # decimal.Overflow on the addition itself, escaping the refusal this
        # function promises.  Bounding each first caps the sum at 2 * _MAX_AMOUNT,
        # which cannot overflow.
        if value >= _MAX_AMOUNT:
            return "sleeve_valuation_invalid"
    # Then the SLEEVE, which is the bound that carries the contract: a rebalance
    # amount is at most the sleeve value, so this is what makes every `amount`
    # expressible in the NUMERIC(18,6) shape and `quantize` provably unable to
    # raise.  Not implied by the per-component check -- two components at 60% of
    # the bound each pass it and sum past it.
    if state.core_market_value + state.cash_balance >= _MAX_AMOUNT:
        return "sleeve_valuation_invalid"
    if broker_minimum is not None and (not broker_minimum.is_finite() or broker_minimum <= 0):
        return "broker_minimum_invalid"
    return None


def _quantise_down(value: Decimal) -> Decimal:
    """Round toward zero to the NUMERIC(18,6) amount shape.

    ROUND_DOWN is the construction choice: it never trades MORE than the boundary
    demands, so a rounding step cannot overshoot the near edge into the far side
    of the band.  It leaves the state fractionally outside the band, and that
    cannot loop -- the residual is strictly below one quantum while
    `min_rebalance_amount > 0` on the same column type is at least one quantum, so
    the next pass suppresses it.

    Cannot raise ``InvalidOperation``: `_state_refusal` bounds the sleeve below
    ``_MAX_AMOUNT`` and the amount is at most the sleeve, so the result needs at
    most 18 significant digits against a default context precision of 28.
    """
    return value.quantize(_AMOUNT_QUANTUM, rounding=ROUND_DOWN)


def evaluate_core_rebalance(
    mandate: CoreMandate | None,
    state: CoreSleeveState,
    *,
    broker_minimum: Decimal | None = None,
) -> CoreRebalanceDecision:
    """Decide whether the core sleeve needs rebalancing, and by how much.

    Trades to the NEAR EDGE of the declared band, not to the target weight: under
    proportional transaction costs the optimal policy is a no-trade region about
    the target, and outside it one trades to the region's BOUNDARY -- Leland
    (2000), "Optimal Portfolio Management with Transactions Costs and Capital
    Gains Taxes", RPF-290, Haas School of Business (SSRN 206871); the region's
    existence is Constantinides (1986) and Davis & Norman (1990).  Only the
    boundary-targeting result is adopted: the band's WIDTH here is an operator
    declaration, not Leland's derived optimum, and none of his magnitudes carry
    over.  See the spec's "Source rule".

    ``broker_minimum`` is optional and ``None`` means THE CALLER HAS NO APPLICABLE
    MINIMUM TO SUPPLY, not that the broker has none.  ⚠ Whether a given broker
    minimum applies to an incremental buy or a partial sell is the caller's
    determination: item 1's spec cited eToro's `min_position_amount`, which is an
    entry-path field, and nothing here establishes it governs a rebalance leg.

    Returns a verdict for every input.  No refusal raises: a caller that must catch
    to learn the mandate is disabled will eventually catch too broadly.
    """
    refusal = _mandate_refusal(mandate)
    if refusal is not None:
        return _refused(refusal)
    assert mandate is not None  # narrowed by _mandate_refusal

    refusal = _state_refusal(mandate, state, broker_minimum)
    if refusal is not None:
        return _refused(refusal)

    sleeve_value = state.core_market_value + state.cash_balance
    if sleeve_value == 0:
        # A zero denominator is a state, not a division.
        return _refused("core_sleeve_empty")

    core_pct = PERCENT_BASIS * state.core_market_value / sleeve_value
    target = mandate.core_target_pct
    lower = target - mandate.rebalance_band_pct
    upper = target + mandate.rebalance_band_pct
    # A reserve breach strictly implies an upper-band breach for any schema-valid
    # mandate (spec Q1), so this is an observable and never a second trigger.
    reserve_breached = (PERCENT_BASIS - core_pct) < mandate.liquidity_reserve_pct

    floor = mandate.min_rebalance_amount
    floor_source: Literal["mandate", "broker"] = "mandate"
    # Strict `>`, so a TIE reports "mandate".  The applied value is identical
    # either way; the mandate is named because it is the floor that always
    # exists, and attributing an equal broker minimum would suggest the verdict
    # would have differed without it.
    if broker_minimum is not None and broker_minimum > floor:
        floor, floor_source = broker_minimum, "broker"

    def _decide(
        action: CoreRebalanceAction,
        amount: Decimal,
        reason_code: str | None,
        resulting_core_pct: Decimal,
    ) -> CoreRebalanceDecision:
        return CoreRebalanceDecision(
            action=action,
            reason_code=reason_code,
            amount=amount,
            core_pct=core_pct,
            target_pct=target,
            lower_pct=lower,
            upper_pct=upper,
            effective_floor=floor,
            floor_source=floor_source,
            reserve_breached=reserve_breached,
            reserve_margin_pct=(PERCENT_BASIS - resulting_core_pct) - mandate.liquidity_reserve_pct,
        )

    # Strictly outside, by construction: a band is an ALLOWANCE, and an allowance
    # consumed exactly is still within it.  (Storability of the edge does not
    # settle actionability -- see the spec's Q2, which corrects an earlier draft
    # that claimed this was derived from the schema CHECK.)
    if core_pct > upper:
        edge = upper
    elif core_pct < lower:
        edge = lower
    else:
        return _decide("hold", _ZERO, None, core_pct)

    # Pre-cost a core/cash trade leaves the sleeve value unchanged: cash falls by
    # exactly what core rises by.  So the amount is the gap to the near edge.
    target_core_value = edge * sleeve_value / PERCENT_BASIS
    raw_amount = target_core_value - state.core_market_value
    action: CoreRebalanceAction = "buy_core" if raw_amount > 0 else "sell_core"
    amount = _quantise_down(abs(raw_amount))

    if amount < floor:
        # The floor wins and the breach, if any, is reported (spec Q4).  The
        # allocator has no authority to trade through one operator declaration to
        # satisfy another.  Reported against the CURRENT state, because no trade
        # happens.
        return _decide("hold", _ZERO, "below_min_rebalance_amount", core_pct)

    # `amount` is a CURRENCY amount, not a quantity.  Conversion to instrument
    # units, unit rounding and the residual that leaves belong to the execution
    # half, as does re-solving the size against the cost actually quoted -- a
    # cash-deducted fee leaves a sell ABOVE `upper` and can push post-trade cash
    # below the reserve (spec Q3).  `reserve_margin_pct` is the pre-cost slack it
    # has to work with; zero means none.
    resulting_core_value = state.core_market_value + (amount if action == "buy_core" else -amount)
    return _decide(action, amount, None, PERCENT_BASIS * resulting_core_value / sleeve_value)


__all__ = [
    "CoreRebalanceAction",
    "CoreRebalanceDecision",
    "CoreSleeveState",
    "evaluate_core_rebalance",
]
