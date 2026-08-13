"""What counts as the UNDERLYING product, in one place.

eToro's eligibility response documents a closed four-value ``settlementType``
vocabulary, each with the provider's own definition (live portal 2026-08-13,
``trading--demo/check-instrument-trading-eligibility``):

===============  ==========================================================
``real``         "the real instrument held in full value"
``realFutures``  "the real future contract, which is a derivative of an
                 underlying instrument"
``marginTrade``  "the real instrument held with only a portion of its value
                 called margin (leveraged asset)"
``cfd``          "contract for difference, which is a derivative following
                 the underlying instrument"
===============  ==========================================================

Exactly one of those is ownership at full value.  ``marginTrade`` IS the real
instrument, but leveraged, which the standing no-leverage posture bars; the
other two are derivatives by the provider's own wording.

Two callers share this definition: ``strategy_core_eligibility`` (#2603 item 2's
proof) and ``strategy_paper_executor._eligibility_reason`` (strategy entry arm
selection).  Two copies of "what counts as the underlying" is exactly the drift
#2437 keeps recording, so it lives here rather than in either.

⚠ This is arm SELECTION only, and is deliberately narrower than any caller's
full eligibility rule.  The executor additionally requires exactly one matching
row, an ``allowStopLossTakeProfit`` arm and a satisfied minimum; a core sleeve is
a stop-less indefinite holding and must not inherit an entry rule.  Calling this
does not make a caller eligible to trade.
"""

from __future__ import annotations

from collections.abc import Iterable

from app.providers.broker import BrokerInstrumentEligibility, BrokerLeverageConfig

# The one settlement type that means ownership at full value.
UNDERLYING_SETTLEMENT_TYPE = "real"
UNDERLYING_DIRECTION = "long"
UNLEVERAGED_LEVERAGE = 1


def offers_unleveraged(leverage_values: Iterable[object]) -> bool:
    """True when the arm offers x1.

    ⚠ ``bool`` is a subclass of ``int`` in Python, so a naive ``1 in values`` is
    true for ``leverageValues: [true]`` -- and the provider parser admits that,
    because ``isinstance(True, int)`` passes its integer check.  Malformed broker
    data must not be able to prove x1 eligibility.
    """
    return any(value == UNLEVERAGED_LEVERAGE and not isinstance(value, bool) for value in leverage_values)


def is_underlying_long_arm(arm: BrokerLeverageConfig) -> bool:
    """True when this arm is the underlying product, held long, unleveraged.

    Compared case-insensitively: the documented vocabulary is the provider's
    promise, not ours to depend on, and the demo response already answers its
    response-level ``currency`` in lower case where the request sent upper.
    Anything that is not ``real`` is not the underlying, including a value that
    is not in the documented vocabulary at all -- unknown fails closed.
    """
    return (
        arm.settlement_type.strip().lower() == UNDERLYING_SETTLEMENT_TYPE
        and arm.direction.strip().lower() == UNDERLYING_DIRECTION
        and offers_unleveraged(arm.leverage_values)
    )


def select_underlying_long_arms(
    row: BrokerInstrumentEligibility,
) -> tuple[BrokerLeverageConfig, ...]:
    """Every arm on ``row`` that is the underlying product, held long, unleveraged.

    Returns all matches rather than one: zero and many are different answers, and
    a caller that cannot tell them apart reports "ambiguous" for an instrument
    that is simply not offered as the underlying.
    """
    return tuple(arm for arm in row.leverage_configs if is_underlying_long_arm(arm))


__all__ = [
    "UNDERLYING_DIRECTION",
    "UNDERLYING_SETTLEMENT_TYPE",
    "UNLEVERAGED_LEVERAGE",
    "is_underlying_long_arm",
    "offers_unleveraged",
    "select_underlying_long_arms",
]
