"""Re-solve a core rebalance size against the cost actually quoted (#2603 step 3b-2 item 1).

``evaluate_core_rebalance`` sizes the trade PRE-COST and says so: its closing comment
hands "re-solving the size against the cost actually quoted" to the execution half.  This
module is that re-solve.

⚠⚠ **The inherited derivation does not apply and is not used here.**  The allocator spec's
Q3 derives its corrections for *a fee deducted from cash, not embedded in the execution
price*, and is explicit about that domain.  #2598 decoded what we are actually quoted: for
an unleveraged long the only non-zero component is ``marketSpread``, the cost of crossing
the book, which is embedded in the price.  A cash fee makes a buy SMALLER and a sell
LARGER; a price-embedded one makes both larger, by different multipliers.  Substituting one
for the other is not a small error.

Pure: no connection, no clock, no broker call, no writes.  Decodes one already-fetched
what-if response and does arithmetic.

This pure calculation authorises nothing by itself. The broker preflight calls it inside
the attended executor path, whose later durable submission gate owns order authority.

⚠⚠ THE SELL PATH IS NOW FEEDABLE -- corrected 2026-08-14 (#2712), and this docstring said
the opposite for one merge.  ``get_what_if_costs`` DID hardcode ``"action": "open"``, but
the endpoint has a close arm: it requires ``positionIds`` (400 without, 200 with, measured
on demo) and ``BrokerWhatIfOrder`` now carries both.

⚠⚠ AND THE TWO ARMS DO NOT AGREE, so a sell must be quoted on the SELL arm.  Measured over
every held demo position -- 5 instruments, both arms decodable on all 5, same ticket
seconds apart -- the close was dearer on 4 of the 5 (5.7x, 8.5x, 13.0x, 18.5x) and cheaper
on the fifth (0.5x).  Feeding a buy-arm quote to a ``sell_core`` would under-state ``gamma``
by an order of magnitude, which is the one direction the ``mu <= gamma`` guarantee cannot
survive.  The spec declined to fabricate that substitution on the grounds that it was
plausible and unmeasured; it is now measured, and it is wrong.

Spec: ``docs/proposals/ta/2026-08-14-core-rebalance-cost-sizing.md``
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_UP, Decimal, DecimalException
from typing import Final, Literal

from app.providers.broker import BrokerWhatIfCostResponse
from app.services.strategy_core_allocator import (
    CoreRebalanceDecision,
    CoreSleeveState,
    evaluate_core_rebalance,
)
from app.services.strategy_core_mandate import AMOUNT_PLACES, PERCENT_BASIS, CoreMandate

#: Frozen with the rule set it stamps.  v1 fixes, BY CONSTRUCTION: the two closed forms,
#: the ``[0, gamma]`` bracket, the ROUND_UP direction, the quantum bound on an omitted
#: row, the relative freshness rule, and the extrapolation limit.
CORE_SIZING_POLICY_VERSION: Final = "core-sizing-v1"

#: One quantum of the NUMERIC(18,6) amount shape -- the same shape the allocator
#: quantises to, imported from the same source rather than re-declared.
_AMOUNT_QUANTUM: Final = Decimal(1).scaleb(-AMOUNT_PLACES)

#: eToro rounds every cost row to 0.01 in the row's currency (#2598, established by the
#: rounding-quantum line of the unit decode).  A component that rounds away is therefore
#: strictly below this -- which is a BOUND, not a coercion to zero.
_COST_ROUNDING_QUANTUM: Final = Decimal("0.01")

#: The documented cost vocabulary: ``markup``, ``marketSpread``, ``transactionFee``,
#: ``overnightFee``, ``overWeekendFee``, ``sdrt`` (portal, per the etoro-api skill).  Used
#: as the ROUNDING-SLACK COUNT rather than as a validation list -- the vocabulary stays
#: provider-owned, and an unrecognised row is summed like any other.
_DOCUMENTED_COST_COMPONENTS: Final = 6

#: How far the solved size may leave the ticket the quote was requested for, in either
#: direction.  The linearity evidence behind holding the cost RATE constant spans one
#: decade at selected instruments (1x->10x moves ``marketSpread`` 9.93-10.14x), so a solve
#: that lands far from its own quote is extrapolating.  The correction is small by
#: construction, so this should not fire; it refuses rather than extrapolating silently.
_MAX_TICKET_EXTRAPOLATION: Final = Decimal("2")

#: Two clocks: ``last_updated`` is the broker's stamp on its own cost figure, while
#: ``CoreSleeveState.as_of`` is OUR receipt time for the position payload -- which carries
#: no broker valuation stamp at all (#2704).  The freshness rule below compares them, so it
#: needs a tolerance for that mismatch.  Named as an allowance for two clocks, not tuned
#: against any observation.
_CLOCK_SKEW: Final = timedelta(seconds=30)

_ZERO: Final = Decimal("0")
_ONE: Final = Decimal("1")

#: The closed vocabulary of reasons a sizing attempt can refuse.  A `Literal` rather than
#: `str` for the same reason the allocator's is: pyright checks every `return "..."` site
#: against it, so a new code cannot be introduced without appearing here.
CoreSizingRefusalCode = Literal[
    "cost_quote_unusable",
    "cost_quote_stale",
    "cost_rate_implausible",
    "cost_quote_ticket_mismatch",
    "cost_breaches_far_edge",
]

CostBoundSource = Literal["quoted", "rounding_quantum"]


class CoreSizingContractError(ValueError):
    """The decision does not describe the (mandate, state) it was handed with.

    Raised, not returned as a refusal, and the distinction is the same one
    ``CoreSleeveObservationError`` draws: a refusal is a verdict about the WORLD, whereas a
    decision computed from a different sleeve is a CALLER DEFECT.  Sizing it would produce
    a coherent number for the wrong sleeve -- the #2704 failure shape exactly -- with
    nothing malformed to refuse on.

    Also raised when there is no trade to size, and when a non-refusing decision arrives
    without a band.
    """


@dataclass(frozen=True)
class QuotedTradeCost:
    """An UPPER BOUND on what one ticket costs, decoded from one what-if response.

    ⚠ An upper bound, deliberately, and every decode rule leans that way: components are
    summed rather than filtered, duplicate rows are summed rather than de-duplicated, and
    an omitted row is bounded at the rounding quantum.  The whole guarantee downstream is
    ``realised cost rate <= this / ticket_amount``, so a rule that could UNDER-state is
    unsafe in a way an over-statement is not.

    ``ticket_amount`` is what the CALLER says the quote was requested for.  The response
    does not echo it -- nor the direction, settlement type, leverage or account -- so it
    cannot be checked here.  The instrument is the one identity that can be, and is.
    """

    instrument_id: int
    ticket_amount: Decimal
    cost_upper_bound: Decimal
    bound_source: CostBoundSource
    currency: str
    last_updated: datetime

    @property
    def rate(self) -> Decimal:
        """The cost as a fraction of the ticket -- ``gamma`` in the spec."""
        return self.cost_upper_bound / self.ticket_amount


@dataclass(frozen=True)
class CoreSizingResult:
    """The re-solved size, or one refusal.

    ``amount`` is ``0`` on a refusal and is otherwise the CURRENCY amount to trade, in the
    mandate's base currency, in the direction the decision already chose.  Conversion to
    units belongs to the execution half.
    """

    sized: bool
    refusal_code: CoreSizingRefusalCode | None
    amount: Decimal
    pre_cost_amount: Decimal
    cost_rate: Decimal | None
    resulting_core_pct_at_bound: Decimal | None
    """Post-trade core weight at ``mu = gamma`` -- the near-edge end of the bracket."""
    resulting_core_pct_at_zero_cost: Decimal | None
    """Post-trade core weight at ``mu = 0`` -- the far-edge end, and the one that refuses."""
    policy_version: str = CORE_SIZING_POLICY_VERSION


def _refused(code: CoreSizingRefusalCode, pre_cost_amount: Decimal) -> CoreSizingResult:
    return CoreSizingResult(
        sized=False,
        refusal_code=code,
        amount=_ZERO,
        pre_cost_amount=pre_cost_amount,
        cost_rate=None,
        resulting_core_pct_at_bound=None,
        resulting_core_pct_at_zero_cost=None,
    )


def decode_quoted_trade_cost(
    response: BrokerWhatIfCostResponse,
    *,
    instrument_id: int,
    ticket_amount: Decimal,
    base_currency: str,
    valuation_as_of: datetime,
) -> QuotedTradeCost | CoreSizingRefusalCode:
    """Bound one ticket's cost from one what-if response, or name why we cannot.

    Returns the refusal CODE rather than a result object so the caller composes it into
    whichever verdict it is building; the sizing entry point below does exactly that.

    The rules, each of which is a #2598 finding rather than a choice made here:

    * **Sum every returned component**, not ``marketSpread`` alone.  The vocabulary is
      provider-owned; ``markup`` and ``overnightFee`` read ``0.0`` today and are not
      promised to.  A component we have never seen must widen the bound, never vanish.
    * **``amount`` if present, else ``value``.**  The portal documents ``amount``; the live
      response ships ``value`` and omits ``amount`` AS A KEY (re-verified 2026-08-12).
      Both present and DISAGREEING is a refusal -- that is drift, and #2598's rule is to
      preserve both and fail rather than pick.
    * **An omitted row bounds at the rounding quantum; it is never zero.**  Membership is
      not tested here at all, which is the point: an absent row contributes nothing to the
      sum, so the quantum is added once to cover whatever rounded away.
    * **A present row with a null value refuses.**  Malformed, not omitted.  It has not been
      observed, and an unobserved shape is the one to refuse rather than to bound by
      analogy with the omitted case.
    * **Per-row currency**, not a response-level check.
    * **The quote must not predate the valuation it will size** (see ``_CLOCK_SKEW``).
    """
    if response.instrument_id != instrument_id:
        return "cost_quote_unusable"
    if not ticket_amount.is_finite() or ticket_amount <= _ZERO:
        return "cost_quote_unusable"
    if not response.costs:
        # No rows at all is not "no cost" -- it is a response we cannot read.  The census
        # returned three components for every decodable observation.
        return "cost_quote_unusable"
    if response.last_updated < valuation_as_of - _CLOCK_SKEW:
        return "cost_quote_stale"

    total = _ZERO
    for component in response.costs:
        if component.currency.strip().upper() != base_currency:
            return "cost_quote_unusable"
        documented, measured = component.amount, component.value
        if documented is not None and measured is not None and documented != measured:
            return "cost_quote_unusable"
        figure = documented if documented is not None else measured
        if figure is None or not figure.is_finite() or figure < _ZERO:
            # Covers the present-but-null row and any non-finite or negative figure.
            # Written inline rather than behind a helper so pyright narrows `figure`
            # here -- a `-> bool` helper does not, and `assert` is stripped under -O
            # (prevention log, #2019).
            return "cost_quote_unusable"
        try:
            total += figure
        except DecimalException:
            # ⚠ `is_finite()` is TRUE for `Decimal("9e999999")`, so a finite-but-enormous
            # provider value reaches the addition and raises `decimal.Overflow` -- which
            # is NOT an `InvalidOperation`, as an earlier draft of this line assumed.
            # `DecimalException` is the parent of both, so the promised refusal holds for
            # every arithmetic failure rather than the one that was guessed at.
            return "cost_quote_unusable"

    # ⚠⚠ ONE QUANTUM PER COMPONENT, not one per response.  Rounding is applied per row, so
    # N components each carry up to a quantum of understatement independently: three rows
    # reported as zero can each stand for a real cost just under 0.01, and a single
    # quantum would bound the trio at 0.01 against a realised 0.03.  That is an
    # UNDER-statement of `gamma`, which is the one direction the whole downstream
    # guarantee (`mu <= gamma`) cannot survive.
    #
    # The count is the documented vocabulary, not the rows returned, because an OMITTED
    # component is exactly the one that rounded away and it contributes no row to count.
    # `max` of the two so a provider that grows a seventh component still gets a bound.
    slack = _COST_ROUNDING_QUANTUM * max(len(response.costs), _DOCUMENTED_COST_COMPONENTS)
    return QuotedTradeCost(
        instrument_id=instrument_id,
        ticket_amount=ticket_amount,
        cost_upper_bound=total + slack,
        bound_source="quoted" if total > _ZERO else "rounding_quantum",
        currency=base_currency,
        last_updated=response.last_updated,
    )


def _quantise_up(value: Decimal) -> Decimal:
    """Round the MAGNITUDE away from zero to the NUMERIC(18,6) amount shape.

    ⚠⚠ The opposite of ``strategy_core_allocator._quantise_down``, and the reversal is the
    point.  The allocator rounds DOWN because its amount is a distance TRAVELLED and
    rounding down cannot overshoot the near edge into the far side of the band.  Here the
    amount is the distance REQUIRED to reach that edge after cost, so rounding down leaves
    a buy still below ``lower`` and a sell still above ``upper`` -- outside the band the
    correction exists to restore.  Rounding up overshoots into the band, which is
    admissible, and the far-edge check runs on the rounded value.
    """
    return value.quantize(_AMOUNT_QUANTUM, rounding=ROUND_UP)


def _assert_decision_describes(mandate: CoreMandate, state: CoreSleeveState, decision: CoreRebalanceDecision) -> None:
    """Refuse to size a decision computed from some other sleeve.

    Cheap (the allocator is pure and side-effect free) and it closes the gap a docstring
    cannot: a structurally valid ``CoreRebalanceDecision`` from a DIFFERENT state carries
    plausible edges and a plausible amount, and sizing it yields a coherent number for the
    wrong sleeve.  "Caller obligation" is not a control.

    ⚠ Recomputed WITHOUT ``broker_minimum``, and that cannot produce a false mismatch on
    the only decisions this module sizes: a broker minimum can only raise the effective
    floor, and raising a floor can only turn a trade into a ``hold``.  So an inbound
    ``buy_core`` / ``sell_core`` already cleared a floor at least as high as the mandate's,
    and recomputing with the lower floor returns the same action and the same amount.
    """
    recomputed = evaluate_core_rebalance(mandate, state)
    if (
        recomputed.action != decision.action
        or recomputed.amount != decision.amount
        or recomputed.core_pct != decision.core_pct
        or recomputed.lower_pct != decision.lower_pct
        or recomputed.upper_pct != decision.upper_pct
    ):
        raise CoreSizingContractError("decision was not produced by evaluate_core_rebalance from this (mandate, state)")


def resolve_core_trade_size(
    mandate: CoreMandate,
    state: CoreSleeveState,
    decision: CoreRebalanceDecision,
    cost: QuotedTradeCost,
) -> CoreSizingResult:
    """Re-solve ``decision.amount`` so the band is restored AFTER a price-embedded cost.

    The arithmetic, with everything on the mark and as fractions of one -- ``V = M + C``
    the pre-trade sleeve, ``e`` the near edge the allocator chose, and ``mu`` the fraction
    of the TICKET that leaves the sleeve as cost::

        buy   f(mu) = (M + A(1 - mu)) / (V - mu.A)   decreasing   =>  A = (eV - M) / (1 - mu(1 - e))
        sell  g(mu) = (M - S)         / (V - mu.S)   increasing   =>  S = (M - eV) / (1 - mu.e)

    ``mu`` is unknown; ``gamma = cost.rate`` bounds it.  So the size is solved at ``gamma``
    -- which, by the monotonicity above, satisfies the NEAR edge for every ``mu <= gamma``
    -- and the ``mu = 0`` end is then checked against the FAR edge.  **The mark's position
    in the book never enters**, which is why no reading of where ``quotes.last`` sits is
    load-bearing.

    ⚠ What bracketing does NOT buy is independence from the quote: the guarantee is
    ``mu <= gamma`` and rests entirely on ``gamma`` being a true upper bound.  Slippage, a
    partial fill at several prices, and any price move between sizing and execution are not
    representable as a scalar rate and are NOT bounded here.

    The reserve is not checked separately, and that is a proof rather than an omission:
    post-trade cash fraction is ``1 - core fraction`` because both are ratios of the same
    post-trade sleeve, both ends above bound the core fraction by ``upper``, and
    ``sql/336_strategy_core_mandate.sql:50`` CHECKs ``upper <= 100 - reserve`` for every
    schema-valid mandate.  ⚠ It is a statement about the two-component model only --
    charges outside the sleeve, delayed fees and unsettled proceeds are the cash warranties
    ``CoreSleeveState`` already says are partly unsourced.
    """
    _assert_decision_describes(mandate, state, decision)

    pre_cost = decision.amount
    if decision.action not in ("buy_core", "sell_core"):
        # Nothing to size.  Not a refusal about the world -- the allocator already decided.
        raise CoreSizingContractError(f"no trade to size for action {decision.action!r}")
    if cost.instrument_id != mandate.core_instrument_id or cost.currency != mandate.base_currency:
        return _refused("cost_quote_unusable", pre_cost)
    if not cost.ticket_amount.is_finite() or cost.ticket_amount <= _ZERO:
        return _refused("cost_quote_unusable", pre_cost)
    if not cost.cost_upper_bound.is_finite() or cost.cost_upper_bound < _ZERO:
        return _refused("cost_quote_unusable", pre_cost)

    gamma = cost.rate
    if gamma >= _ONE:
        # The quote claims the cost is the whole ticket.  Also the guard that keeps both
        # denominators strictly positive: with e in (0, 1), gamma < 1 gives
        # 1 - gamma(1 - e) > 0 and 1 - gamma.e > 0.
        return _refused("cost_rate_implausible", pre_cost)

    sleeve = state.core_market_value + state.cash_balance
    if sleeve <= _ZERO or decision.lower_pct is None or decision.upper_pct is None:
        # Unreachable through `evaluate_core_rebalance`, which refuses a zero sleeve
        # before any action is chosen and sets both edges on every non-refusal.  An
        # explicit raise rather than `assert`, which `python -O` strips (prevention log,
        # #2019: a fail-closed invariant that silently vanishes under -O).
        raise CoreSizingContractError("decision carries no band, or the sleeve is empty")

    lower = decision.lower_pct / PERCENT_BASIS
    upper = decision.upper_pct / PERCENT_BASIS
    buying = decision.action == "buy_core"
    edge = lower if buying else upper

    target_core = edge * sleeve
    if buying:
        raw = (target_core - state.core_market_value) / (_ONE - gamma * (_ONE - edge))
    else:
        raw = (state.core_market_value - target_core) / (_ONE - gamma * edge)
    amount = _quantise_up(raw)

    ratio_hi = max(amount, cost.ticket_amount)
    ratio_lo = min(amount, cost.ticket_amount)
    if ratio_lo <= _ZERO or ratio_hi / ratio_lo > _MAX_TICKET_EXTRAPOLATION:
        return _refused("cost_quote_ticket_mismatch", pre_cost)

    # ⚠ NO fundability check, and that is a proof rather than an omission.  A buy exceeds
    # cash exactly when `A > C`, and substituting `A = A₀ / (1 - gamma(1 - e))` with
    # `C = V(1 - e) + A₀` reduces that to `gamma > V / C`.  Since `C <= V`, the threshold
    # is at least 1, so `cost_rate_implausible` has already refused.  A check here would
    # be a control on a path the decision cannot take -- #2437's R4 shape, which this
    # ticket has now hit fifteen times.  Confirmed empirically over a sweep of every
    # mandate/sleeve/gamma combination the schema admits: it never fired once.
    # The sell side needs no mirror check either: `S <= M` follows from `gamma < 1`.

    # Both ends of the bracket, computed on the ROUNDED amount, so what is checked is what
    # is returned.
    if buying:
        at_bound = (state.core_market_value + amount * (_ONE - gamma)) / (sleeve - gamma * amount)
        at_zero = (state.core_market_value + amount) / sleeve
    else:
        at_bound = (state.core_market_value - amount) / (sleeve - gamma * amount)
        at_zero = (state.core_market_value - amount) / sleeve

    core_at_bound = PERCENT_BASIS * at_bound
    core_at_zero = PERCENT_BASIS * at_zero
    if core_at_zero > decision.upper_pct or core_at_zero < decision.lower_pct:
        # The zero-cost end leaves the band.  The spec's obligation -- refuse if the size
        # cannot restore both band and reserve -- with the reserve folded in by the proof
        # in this function's docstring.
        return _refused("cost_breaches_far_edge", pre_cost)

    return CoreSizingResult(
        sized=True,
        refusal_code=None,
        amount=amount,
        pre_cost_amount=pre_cost,
        cost_rate=gamma,
        resulting_core_pct_at_bound=core_at_bound,
        resulting_core_pct_at_zero_cost=core_at_zero,
    )


__all__ = [
    "CORE_SIZING_POLICY_VERSION",
    "CoreSizingContractError",
    "CoreSizingRefusalCode",
    "CoreSizingResult",
    "CostBoundSource",
    "QuotedTradeCost",
    "decode_quoted_trade_cost",
    "resolve_core_trade_size",
]
